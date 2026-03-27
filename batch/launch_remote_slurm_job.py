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
        description="Launch job_submit_large.sh on remote host for a batch."
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
        "--user",
        default="",
        help="Username for squeue checks (defaults to the Datamak Perlmutter user setting).",
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
        help="Run monitor_remote_runs.py after launching the job.",
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
    base_dir = remote_path.rstrip("/")
    job_script = f"{base_dir}/job_submit_large.sh"
    squeue_user = args.user.strip() or get_default_remote_user()

    payload = f"""
set -euo pipefail
base_dir="{base_dir}"
job_script="{job_script}"
if [ ! -f "$job_script" ]; then
  echo "MISSING_JOB_SCRIPT"
  exit 3
fi
if [ -n "{squeue_user}" ]; then
  jobs=$(squeue -u "{squeue_user}" -h -o "%i|%T|%j|%Z" || true)
  if echo "$jobs" | awk -F'|' -v dir="$base_dir" '$4==dir {{found=1}} END {{exit !found}}'; then
    echo "ACTIVE_JOB_PRESENT"
    exit 4
  fi
fi
cd "$base_dir"
sbatch "$(basename "$job_script")"
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
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        message = stdout or stderr or f"Remote command failed on {host}"
        raise SystemExit(message)
    print(f"{db_name}: {stdout}")
    if args.follow_monitor:
        monitor_path = ROOT_DIR / "batch" / "monitor_remote_runs.py"
        cmd = ["python3", str(monitor_path), "--db", args.db]
        if args.monitor_user:
            cmd.extend(["--user", args.monitor_user])
        if args.monitor_timeout is not None:
            cmd.extend(["--timeout", str(args.monitor_timeout)])
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(f"Monitor follow-up failed: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
