#!/usr/bin/env python3
import argparse
import sqlite3
import subprocess
from pathlib import Path
import os
import json


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def parse_remote(remote_folder: str, remote_host: str) -> tuple[str, str]:
    if ":" in remote_folder:
        host, path = remote_folder.split(":", 1)
        return host, path
    if remote_host:
        return remote_host, remote_folder
    raise ValueError(f"Invalid remote_folder: {remote_folder}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check LAUNCHED gk_batch runs for finished gk_run rows."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--status",
        default="SUCCESS",
        help="Status to treat as finished (default: SUCCESS).",
    )
    parser.add_argument(
        "--sqlite",
        default="sqlite3",
        help="Remote sqlite3 command name.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SSH timeout in seconds per host.",
    )
    parser.add_argument(
        "--remote-check",
        action="store_true",
        help="Query remote batch databases (defaults to local listing only).",
    )
    parser.add_argument(
        "--dump-gk-run",
        action="store_true",
        help="Dump full gk_run content from remote batch databases.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT batch_database_name, remote_folder
        FROM gk_batch
        WHERE status = 'LAUNCHED'
        """
    ).fetchall()
    conn.close()

    if not rows:
        print("No LAUNCHED gk_batch rows found.")
        return

    if not args.remote_check:
        for row in rows:
            remote_folder = row["remote_folder"]
            try:
                remote_host = row["remote_host"]
            except Exception:
                remote_host = ""
            if not remote_host and remote_folder and ":" in remote_folder:
                remote_host = remote_folder.split(":", 1)[0]
            warning = ""
            if not remote_folder or (not remote_host and ":" not in remote_folder):
                warning = " WARNING: missing remote info"
            print(
                f"{row['batch_database_name']}: "
                f"status=LAUNCHED, "
                f"remote_folder={remote_folder}, "
                f"remote_host={remote_host}"
                f"{warning}"
            )
        return

    by_host: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        db_name = row["batch_database_name"]
        remote_folder = row["remote_folder"]
        remote_host = ""
        try:
            remote_host = row["remote_host"]
        except Exception:
            remote_host = ""
        try:
            host, remote_path = parse_remote(remote_folder, remote_host)
        except ValueError as exc:
            print(f"{db_name}: {exc}")
            continue
        by_host.setdefault(host, []).append((db_name, remote_path))

    run_columns = [
        "id",
        "gk_input_id",
        "gk_batch_id",
        "input_folder",
        "job_folder",
        "archive_folder",
        "input_name",
        "nb_nodes",
        "job_id",
        "status",
        "input_content",
    ]

    def ensure_local_gk_run(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gk_run (
                id INTEGER NOT NULL,
                gk_input_id INTEGER,
                gk_batch_id INTEGER,
                input_folder TEXT,
                job_folder TEXT,
                archive_folder TEXT,
                input_name TEXT,
                nb_nodes INTEGER,
                job_id TEXT,
                status TEXT,
                input_content TEXT,
                remote_host TEXT,
                remote_folder TEXT,
                creation_date TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
        if "gk_batch_id" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN gk_batch_id INTEGER")

    local_conn = sqlite3.connect(args.db)
    try:
        ensure_local_gk_run(local_conn)
        for host, items in by_host.items():
            db_list = []
            for db_name, remote_path in items:
                remote_db = f"{remote_path.rstrip('/')}/{db_name}"
                db_list.append((db_name, remote_db))
            db_literal = repr(db_list)
            payload = f"""
set -euo pipefail
python3 - <<'PY'
import json
import os
import sqlite3

dbs = {db_literal}

for name, path in dbs:
    if not os.path.exists(path):
        print(f"MISSING\\t{{name}}\\t{{path}}")
        continue
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        for row in conn.execute("SELECT * FROM gk_run"):
            data = dict(row)
            print("ROW\\t" + name + "\\t" + json.dumps(data))
    finally:
        conn.close()
PY
"""
            print(f"Checking {len(items)} batch DB(s) on {host}...")
            result = subprocess.run(
                ["ssh", host, "bash", "-c", payload],
                text=True,
                capture_output=True,
                timeout=args.timeout,
            )
            if result.returncode != 0:
                if result.stderr:
                    print(result.stderr.strip())
                raise SystemExit(result.returncode)

            batch_rows: dict[str, list[dict]] = {}
            missing: list[tuple[str, str]] = []
            for line in result.stdout.splitlines():
                if line.startswith("MISSING\t"):
                    _, db_name, path = line.split("\t", 2)
                    missing.append((db_name, path))
                    continue
                if not line.startswith("ROW\t"):
                    continue
                _, db_name, payload_json = line.split("\t", 2)
                batch_rows.setdefault(db_name, []).append(json.loads(payload_json))

            for db_name, path in missing:
                print(f"=== {db_name} ===")
                print(f"missing: {path}")

            for db_name, rows_data in batch_rows.items():
                remote_folder = next(
                    (p for n, p in items if n == db_name), ""
                )
                remote_path = remote_folder
                if remote_path.endswith(db_name):
                    remote_path = remote_path[: -len(db_name)].rstrip("/")
                gk_batch_id = local_conn.execute(
                    "SELECT id FROM gk_batch WHERE batch_database_name = ?",
                    (db_name,),
                ).fetchone()
                gk_batch_id_val = gk_batch_id[0] if gk_batch_id else None
                for row in rows_data:
                    if row.get("status") == "ANALYZED":
                        local_status = local_conn.execute(
                            """
                            SELECT status FROM gk_run
                            WHERE remote_host = ? AND remote_folder = ? AND input_name = ?
                            """,
                            (host, remote_path, row.get("input_name")),
                        ).fetchone()
                        if not local_status or local_status[0] != "ANALYZED":
                            print(
                                "WARNING: remote ANALYZED but local not ANALYZED "
                                f"for {db_name} input_name={row.get('input_name')}"
                            )
                        continue
                    values = [row.get(col) for col in run_columns if col != "gk_batch_id"]
                    local_conn.execute(
                        """
                        INSERT OR REPLACE INTO gk_run (
                            id, gk_input_id, gk_batch_id, input_folder, job_folder, archive_folder,
                            input_name, nb_nodes, job_id, status, input_content,
                            remote_host, remote_folder
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (*values, gk_batch_id_val, host, remote_path),
                    )
                local_conn.commit()

                if args.dump_gk_run:
                    print(f"=== {db_name} ===")
                    cols = [c for c in run_columns if c != "input_content"]
                    widths = [len(c) for c in cols]
                    for row in rows_data:
                        for idx, col in enumerate(cols):
                            val = "" if row.get(col) is None else str(row.get(col))
                            widths[idx] = max(widths[idx], len(val))
                    header = "  ".join(col.ljust(widths[idx]) for idx, col in enumerate(cols))
                    print(header)
                    for row in rows_data:
                        line = "  ".join(
                            ("" if row.get(col) is None else str(row.get(col))).ljust(widths[idx])
                            for idx, col in enumerate(cols)
                        )
                        print(line.rstrip())
                else:
                    count = sum(1 for r in rows_data if r.get("status") == args.status)
                    print(f"{db_name}: {args.status} rows = {count}")
    finally:
        local_conn.close()

if __name__ == "__main__":
    main()
