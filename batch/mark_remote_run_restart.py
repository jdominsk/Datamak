#!/usr/bin/env python3
import argparse
import os
import sqlite3
import subprocess
from pathlib import Path

try:
    from batch.ssh_utils import (
        build_ssh_base_args,
        get_default_remote_user,
        get_ssh_connect_timeout,
    )
except ImportError:
    from ssh_utils import build_ssh_base_args, get_default_remote_user, get_ssh_connect_timeout

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
        description="Mark remote gk_run rows as RESTART."
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
        "--run-id",
        action="append",
        type=int,
        required=True,
        help="Remote gk_run id to mark as RESTART (repeatable).",
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
    parser.add_argument(
        "--monitor-timeout",
        type=int,
        default=None,
        help="Timeout in seconds to pass to monitor_remote_runs.py.",
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

    run_ids = sorted({int(rid) for rid in args.run_id if rid is not None})
    if not run_ids:
        raise SystemExit("No run ids provided.")
    run_ids_literal = ",".join(str(rid) for rid in run_ids)

    payload = f"""
set -euo pipefail
python3 - <<'PY'
import sqlite3

db_path = "{remote_db}"
run_ids = [{run_ids_literal}]
with sqlite3.connect(db_path) as conn:
    columns = {{row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}}
    if "restart_keep_tmax" not in columns:
        conn.execute(
            "ALTER TABLE gk_run ADD COLUMN restart_keep_tmax INTEGER NOT NULL DEFAULT 0"
        )
    placeholders = ",".join(["?"] * len(run_ids))
    sql = (
        "UPDATE gk_run "
        "SET status = 'RESTART', restart_keep_tmax = 1 "
        "WHERE id IN (" + placeholders + ")"
    )
    cur = conn.execute(sql, run_ids)
    conn.commit()
    print(cur.rowcount)
PY
"""
    connect_timeout = get_ssh_connect_timeout(min(10, max(1, args.timeout)))
    ssh_cmd = [*build_ssh_base_args(host, connect_timeout), "bash", "-s"]
    result = subprocess.run(
        ssh_cmd,
        input=payload,
        text=True,
        capture_output=True,
        timeout=args.timeout,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise SystemExit(stderr or f"Remote command failed on {host}")
    updated = (result.stdout or "").strip()
    run_ids_str = ",".join(str(rid) for rid in run_ids)
    print(f"{db_name}: marked run_id={run_ids_str} as RESTART ({updated} rows).")
    if args.follow_monitor:
        monitor_path = ROOT_DIR / "batch" / "monitor_remote_runs.py"
        cmd = ["python3", str(monitor_path), "--db", args.db]
        monitor_user = args.monitor_user.strip() or get_default_remote_user()
        if monitor_user:
            cmd.extend(["--user", monitor_user])
        if args.monitor_timeout is not None:
            cmd.extend(["--timeout", str(args.monitor_timeout)])
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(f"Monitor follow-up failed: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
