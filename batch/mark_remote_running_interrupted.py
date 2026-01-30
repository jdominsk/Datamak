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
        description="Mark RUNNING rows as INTERRUPTED in a remote batch DB."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch database filename (e.g., batch_database_YYYYMMDD_HHMMSS.db).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SSH timeout in seconds.",
    )
    parser.add_argument(
        "--follow-monitor",
        action="store_true",
        help="Run monitor_remote_runs.py after updating the remote DB.",
    )
    parser.add_argument(
        "--monitor-user",
        default="",
        help="Username to pass to monitor_remote_runs.py.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT batch_database_name, remote_folder, remote_host
        FROM gk_batch
        WHERE batch_database_name = ?
        """,
        (args.batch,),
    ).fetchone()
    conn.close()

    if row is None:
        raise SystemExit(f"Batch not found in local DB: {args.batch}")

    db_name = row["batch_database_name"]
    remote_folder = row["remote_folder"] or ""
    remote_host = row["remote_host"] or ""
    host, remote_path = parse_remote(remote_folder, remote_host)
    remote_db = f"{remote_path.rstrip('/')}/{db_name}"

    payload = """
set -euo pipefail
python3 - <<'PY'
import sqlite3

db_path = "<<<REMOTE_DB>>>"
with sqlite3.connect(db_path) as conn:
    cur = conn.execute("UPDATE gk_run SET status = 'INTERRUPTED' WHERE status = 'RUNNING'")
    conn.commit()
    print(cur.rowcount)
PY
"""
    payload = payload.replace("<<<REMOTE_DB>>>", remote_db)

    result = subprocess.run(
        ["ssh", host, "bash", "-s"],
        input=payload,
        text=True,
        capture_output=True,
        timeout=args.timeout,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise SystemExit(stderr or f"Remote command failed on {host}")
    updated = (result.stdout or "").strip()
    print(f"{db_name}: marked {updated} RUNNING rows as INTERRUPTED.")

    if args.follow_monitor:
        monitor_path = ROOT_DIR / "batch" / "monitor_remote_runs.py"
        cmd = ["python3", str(monitor_path), "--db", args.db]
        if args.monitor_user:
            cmd.extend(["--user", args.monitor_user])
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(f"Monitor follow-up failed: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
