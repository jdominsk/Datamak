#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import subprocess
from pathlib import Path


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def parse_remote(remote_folder: str, remote_host: str) -> tuple[str, str]:
    remote_folder = remote_folder or ""
    remote_host = remote_host or ""
    if ":" in remote_folder:
        host, path = remote_folder.split(":", 1)
        return host, path
    if remote_host:
        return remote_host, remote_folder
    raise ValueError(f"Invalid remote_folder: {remote_folder}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan remote batch DB input_content for invalid bare tokens."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SSH timeout in seconds per host.",
    )
    parser.add_argument(
        "--status",
        default="",
        help="Comma-separated statuses to include (empty = all).",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT batch_database_name, remote_folder, remote_host
        FROM gk_batch
        WHERE status IN ('LAUNCHED', 'SYNCED')
        """
    ).fetchall()
    conn.close()

    if not rows:
        print("No LAUNCHED/SYNCED gk_batch rows found.")
        return 0

    by_host: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        db_name = row["batch_database_name"]
        remote_folder = row["remote_folder"] or ""
        remote_host = ""
        try:
            remote_host = row["remote_host"] or ""
        except Exception:
            remote_host = ""
        try:
            host, remote_path = parse_remote(remote_folder, remote_host)
        except ValueError as exc:
            print(f"{db_name}: {exc}")
            continue
        by_host.setdefault(host, []).append((db_name, remote_path))

    status_set = {s.strip() for s in args.status.split(",") if s.strip()}
    status_literal = json.dumps(sorted(status_set))

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
import re
import sqlite3

dbs = {db_literal}
status_set = set({status_literal})
bad_line_re = re.compile(r"^\\s*[^#\\s=\\[][^=]*\\s*$")
header_re = re.compile(r"^\\s*\\[.+\\]\\s*$")

for name, path in dbs:
    if not os.path.exists(path):
        print(f"MISSING\\t{{name}}\\t{{path}}")
        continue
    print(f"USING\\t{{name}}\\t{{path}}")
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT id, status, input_content FROM gk_run"):
            if status_set and row["status"] not in status_set:
                continue
            content = row["input_content"] or ""
            bad_lines = []
            for idx, line in enumerate(content.splitlines(), start=1):
                if not line.strip():
                    continue
                if line.lstrip().startswith("#"):
                    continue
                if header_re.match(line):
                    continue
                if "=" in line:
                    continue
                if bad_line_re.match(line):
                    bad_lines.append((idx, line.strip()))
            if bad_lines:
                print("BAD\\t" + name + "\\t" + str(row["id"]) + "\\t" + json.dumps(bad_lines))
PY
"""
        print(f"Checking {len(items)} batch DB(s) on {host}...")
        result = subprocess.run(
            ["ssh", host, "bash", "-s"],
            input=payload,
            text=True,
            capture_output=True,
            timeout=args.timeout,
        )
        if result.returncode != 0:
            if result.stderr:
                print(result.stderr.strip())
            raise SystemExit(result.returncode)
        for line in result.stdout.splitlines():
            if line.startswith("MISSING\t"):
                _, db_name, path = line.split("\t", 2)
                print(f"{db_name}: missing {path}")
                continue
            if line.startswith("USING\t"):
                _, db_name, path = line.split("\t", 2)
                print(f"{db_name}: {path}")
                continue
            if line.startswith("BAD\t"):
                _, db_name, run_id, payload_json = line.split("\t", 3)
                bad_lines = json.loads(payload_json)
                print(f"{db_name}: run_id={run_id} invalid lines:")
                for idx, content in bad_lines:
                    print(f"  line {idx}: {content}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
