#!/usr/bin/env python3
import argparse
import os
import re
import sqlite3
import subprocess
from pathlib import Path
from typing import Optional, Tuple


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))
RUN_DIR_RE = re.compile(r"^run(\d{4})$")


def parse_remote(remote_folder: str, remote_host: str) -> tuple[str, str]:
    remote_folder = remote_folder or ""
    remote_host = remote_host or ""
    if ":" in remote_folder:
        host, path = remote_folder.split(":", 1)
        return host, path
    if remote_host:
        return remote_host, remote_folder
    raise ValueError(f"Invalid remote_folder: {remote_folder}")


def build_new_path(path: str) -> Optional[Tuple[str, str]]:
    norm = path.rstrip("/")
    base = os.path.basename(norm)
    match = RUN_DIR_RE.match(base)
    if not match:
        return None
    num = match.group(1)
    new_base = f"batch{num}"
    return norm, os.path.join(os.path.dirname(norm), new_base)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rename remote runXXXX folders to batchXXXX and update local DB."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (otherwise dry run).",
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        default=0,
        help="Only migrate a specific gk_batch id.",
    )
    parser.add_argument(
        "--batch-name",
        default="",
        help="Only migrate a specific batch database filename.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SSH timeout in seconds.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    query = """
        SELECT id, batch_database_name, remote_folder, remote_host
        FROM gk_batch
        WHERE remote_folder IS NOT NULL AND remote_folder != ''
    """
    params = []
    if args.batch_id:
        query += " AND id = ?"
        params.append(args.batch_id)
    if args.batch_name:
        query += " AND batch_database_name = ?"
        params.append(args.batch_name)
    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No gk_batch rows found for migration.")
        return 0

    updates = []
    for row in rows:
        batch_id = int(row["id"])
        batch_name = row["batch_database_name"]
        remote_folder = row["remote_folder"] or ""
        remote_host = row["remote_host"] or ""
        try:
            host, remote_path = parse_remote(remote_folder, remote_host)
        except ValueError:
            continue
        path_pair = build_new_path(remote_path)
        if not path_pair:
            continue
        old_path, new_path = path_pair
        updates.append(
            {
                "batch_id": batch_id,
                "batch_name": batch_name,
                "host": host,
                "old_path": old_path,
                "new_path": new_path,
                "remote_folder_raw": remote_folder,
            }
        )

    if not updates:
        print("No runXXXX folders detected to migrate.")
        return 0

    for item in updates:
        print(
            f"{item['batch_name']} (id={item['batch_id']}): "
            f"{item['host']}:{item['old_path']} -> {item['new_path']}"
        )

    if not args.apply:
        print("Dry run only. Re-run with --apply to execute.")
        return 0

    grouped: dict[str, list[dict]] = {}
    for item in updates:
        grouped.setdefault(item["host"], []).append(item)

    for host, items in grouped.items():
        payload_lines = ["set -euo pipefail"]
        for item in items:
            old_path = item["old_path"]
            new_path = item["new_path"]
            db_name = item["batch_name"]
            payload_lines.append(f'echo "CHECK\\t{db_name}\\t{old_path}\\t{new_path}"')
            payload_lines.append(f'if [ ! -d "{old_path}" ]; then echo "MISSING\\t{db_name}\\t{old_path}"; continue; fi')
            payload_lines.append(f'if [ -d "{new_path}" ]; then echo "SKIP_EXISTS\\t{db_name}\\t{new_path}"; continue; fi')
            payload_lines.append(f'mv "{old_path}" "{new_path}"')
            payload_lines.append(f'echo "RENAMED\\t{db_name}\\t{new_path}"')
            payload_lines.append(
                f'python3 - <<\'PY\'\n'
                f'import sqlite3\n'
                f'db_path = "{new_path}/{db_name}"\n'
                f'try:\n'
                f'    conn = sqlite3.connect(db_path)\n'
                f'    cols = [row[1] for row in conn.execute("PRAGMA table_info(gk_run)")]\n'
                f'    if "remote_folder" in cols:\n'
                f'        conn.execute("UPDATE gk_run SET remote_folder = ? WHERE remote_folder = ?", ("{new_path}", "{old_path}"))\n'
                f'        conn.commit()\n'
                f'    conn.close()\n'
                f'except Exception as exc:\n'
                f'    print("WARN_DB_UPDATE\\t{db_name}\\t" + str(exc))\n'
                f'PY'
            )
        payload = "\n".join(payload_lines) + "\n"
        result = subprocess.run(
            ["ssh", host, "bash", "-s"],
            input=payload,
            text=True,
            capture_output=True,
            timeout=args.timeout,
        )
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            raise SystemExit(err or f"Remote command failed on {host}")
        for line in (result.stdout or "").splitlines():
            if line.startswith("RENAMED\t") or line.startswith("SKIP_EXISTS\t"):
                _, db_name, new_path = line.split("\t", 2)
                for item in items:
                    if item["batch_name"] == db_name:
                        item["applied"] = True
                        item["new_path"] = new_path
            if line.startswith("MISSING\t"):
                print(line)
            if line.startswith("SKIP_EXISTS\t"):
                print(line)
            if line.startswith("WARN_DB_UPDATE\t"):
                print(line)

    conn = sqlite3.connect(args.db)
    try:
        for item in updates:
            if not item.get("applied"):
                continue
            new_path = item["new_path"]
            old_path = item["old_path"]
            remote_folder_raw = item["remote_folder_raw"]
            if ":" in remote_folder_raw:
                new_remote_folder = f"{item['host']}:{new_path}"
            else:
                new_remote_folder = new_path
            conn.execute(
                "UPDATE gk_batch SET remote_folder = ? WHERE id = ?",
                (new_remote_folder, item["batch_id"]),
            )
            conn.execute(
                "UPDATE gk_run SET remote_folder = ? WHERE remote_folder = ?",
                (new_path, old_path),
            )
        conn.commit()
    finally:
        conn.close()

    print("Migration complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
