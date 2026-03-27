#!/usr/bin/env python3
import argparse
import os
import shlex
import sqlite3
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_perlmutter_profile  # noqa: E402
try:
    from batch.ssh_utils import build_ssh_base_args, get_ssh_connect_timeout  # noqa: E402
except ImportError:
    from ssh_utils import build_ssh_base_args, get_ssh_connect_timeout  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare new batches and submit remote jobs."
    )
    parser.add_argument(
        "--remote",
        default="",
        help="Remote host (user@host). Defaults to the Datamak Perlmutter profile.",
    )
    parser.add_argument(
        "--remote-dir",
        default="",
        help="Remote folder containing prepare_newbatch.sh.",
    )
    parser.add_argument(
        "--base-dir",
        default="",
        help="Remote base folder containing batchXXXX directories.",
    )
    parser.add_argument(
        "--max-submit",
        type=int,
        default=9999,
        help="Maximum number of jobs to submit in one run.",
    )
    parser.add_argument(
        "--gk-db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database for logging gk_batch entries.",
    )
    parser.add_argument(
        "--batch-dir",
        default=str(ROOT_DIR / "batch" / "new"),
        help="Local folder containing batch_database_*.db files.",
    )
    args = parser.parse_args()
    profile = resolve_perlmutter_profile(
        {
            "remote": args.remote,
            "batch_dir": args.remote_dir,
            "base_dir": args.base_dir,
        }
    )
    remote = str(profile["remote"] or "").strip()
    remote_dir = str((args.remote_dir or "").strip() or profile["batch_dir"]).strip()
    base_dir = str((args.base_dir or "").strip() or profile["base_dir"]).strip()
    gx_path = str(profile.get("gx_path") or "").strip()
    if not remote:
        raise SystemExit("Perlmutter remote host is empty. Configure it in Datamak settings.")
    if not remote_dir:
        raise SystemExit("Perlmutter remote batch directory is empty.")
    if not base_dir:
        raise SystemExit("Perlmutter remote base directory is empty.")

    batch_root = Path(args.batch_dir).resolve().parent
    batch_new = Path(args.batch_dir).resolve()
    batch_sent = batch_root / "sent"
    hpc_dir = batch_root / "hpc"
    scripts = [
        hpc_dir / "job_submit.sh",
        hpc_dir / "job_execute.sh",
        hpc_dir / "prepare_newbatch.sh",
        hpc_dir / "gx_analyze.py",
        hpc_dir / "linear_convergence.py",
        hpc_dir / "ky_growth_rates.py",
        hpc_dir / "job_interactive.sh",
    ]

    missing_scripts = [str(path) for path in scripts if not path.is_file()]
    if missing_scripts:
        raise SystemExit(f"Missing required scripts: {', '.join(missing_scripts)}")
    if not batch_new.is_dir():
        raise SystemExit(f"Batch directory not found: {batch_new}")
    batch_sent.mkdir(exist_ok=True)

    conn = sqlite3.connect(args.gk_db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gk_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_database_name TEXT NOT NULL,
                remote_folder TEXT NOT NULL,
                status TEXT NOT NULL,
                remote_host TEXT
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_batch)")}
        if "remote_host" not in columns:
            conn.execute("ALTER TABLE gk_batch ADD COLUMN remote_host TEXT")
        created_rows = conn.execute(
            """
            SELECT batch_database_name
            FROM gk_batch
            WHERE status = 'CREATED'
            """
        ).fetchall()
        created_names = {row[0] for row in created_rows}
    finally:
        conn.close()

    if not created_names:
        print("No CREATED gk_batch rows found.")
        return

    db_files = sorted(batch_new.glob("batch_database_*.db"))
    db_by_name = {path.name: path for path in db_files}
    missing_files = sorted(name for name in created_names if name not in db_by_name)
    extra_files = sorted(name for name in db_by_name if name not in created_names)

    for name in missing_files:
        print(f"Warning: gk_batch row without file: {name}")
    for name in extra_files:
        print(f"Warning: file without gk_batch row: {name}")

    candidate_dbs = [db_by_name[name] for name in sorted(created_names) if name in db_by_name]
    if not candidate_dbs:
        print("No batch databases found for CREATED rows.")
        return

    non_empty_dbs = []
    for db_path in candidate_dbs:
        db_conn = sqlite3.connect(db_path)
        try:
            count = db_conn.execute("SELECT COUNT(*) FROM gk_run").fetchone()[0]
        except sqlite3.Error:
            count = 0
        finally:
            db_conn.close()
        if count == 0:
            target = db_path.with_name(f"empty_{db_path.name}")
            if target.exists():
                stem = target.stem
                suffix = target.suffix
                counter = 1
                while True:
                    candidate = target.with_name(f"{stem}_{counter}{suffix}")
                    if not candidate.exists():
                        target = candidate
                        break
                    counter += 1
            db_path.rename(target)
            print(f"Marked empty database: {target}")
            continue
        non_empty_dbs.append(db_path)

    if not non_empty_dbs:
        print("No non-empty batch databases to send.")
        return

    sbatch_cmd = 'sbatch "$(basename "$job")"'
    if gx_path:
        sbatch_cmd = (
            f'sbatch --export=ALL,DTWIN_GX_PATH={shlex.quote(gx_path)} '
            '"$(basename "$job")"'
        )
    remote_dir_q = shlex.quote(remote_dir)
    base_dir_q = shlex.quote(base_dir)
    remote_script = f"""
set -euo pipefail
mkdir -p {remote_dir_q}
tar -xf - -C {remote_dir_q}
cd {remote_dir_q}/hpc
chmod +x ./prepare_newbatch.sh
BASE_DIR={base_dir_q} bash ./prepare_newbatch.sh
shopt -s nullglob
count=0
manifest={shlex.quote(remote_dir + "/new_runs.txt")}
if [ ! -f "$manifest" ]; then
  echo "No new_runs.txt found; nothing to submit."
  exit 0
fi
while IFS= read -r run_dir; do
  [ -d "$run_dir" ] || continue
  job="$run_dir/job_submit.sh"
  [ -f "$job" ] || continue
  db_path="$(ls "$run_dir"/batch_database_*.db 2>/dev/null | head -n 1 || true)"
  db_name=""
  if [ -n "$db_path" ]; then
    db_name="$(basename "$db_path")"
  fi
  echo "Submitting: $job"
  count=$((count+1))
  (cd "$run_dir" && {sbatch_cmd})
  echo "SUBMITTED	${{run_dir}}	${{db_name}}"
  if [ "$count" -ge {args.max_submit} ]; then
    echo "Reached MAX_SUBMIT={args.max_submit}, stopping."
    break
  fi
done < "$manifest"
rm -f "$manifest"
"""
    tar_root = batch_root
    transfer_items = sorted(
        {
            str(path.relative_to(tar_root))
            for path in non_empty_dbs + scripts
        }
    )
    tar_cmd = ["tar", "-cf", "-", "-C", str(tar_root), *transfer_items]
    tar_proc = subprocess.Popen(
        tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        result = subprocess.run(
            [*build_ssh_base_args(remote, get_ssh_connect_timeout(10)), "bash", "-lc", remote_script],
            text=True,
            capture_output=True,
            stdin=tar_proc.stdout,
        )
    finally:
        if tar_proc.stdout is not None:
            tar_proc.stdout.close()
        stderr = tar_proc.stderr.read() if tar_proc.stderr is not None else ""
        tar_proc.wait()
    if tar_proc.returncode != 0:
        message = stderr.strip() or "Failed to create transfer archive."
        raise SystemExit(message)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise SystemExit(result.returncode)
    submitted = []
    for line in result.stdout.splitlines():
        if not line.startswith("SUBMITTED\t"):
            continue
        _, run_dir, db_name = line.split("\t", 2)
        if db_name:
            submitted.append((run_dir, db_name))

    conn = sqlite3.connect(args.gk_db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gk_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_database_name TEXT NOT NULL,
                remote_folder TEXT NOT NULL,
                status TEXT NOT NULL,
                remote_host TEXT
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_batch)")}
        if "remote_host" not in columns:
            conn.execute("ALTER TABLE gk_batch ADD COLUMN remote_host TEXT")
        remote_folder = f"{remote}:{remote_dir}"
        for db_path in non_empty_dbs:
            conn.execute(
                """
                UPDATE gk_batch
                SET status = 'SENT',
                    remote_folder = ?,
                    remote_host = ?
                WHERE batch_database_name = ?
                """,
                (remote_folder, remote, db_path.name),
            )
            db_path.replace(batch_sent / db_path.name)
        for run_dir, db_name in submitted:
            remote_folder = f"{remote}:{run_dir}"
            conn.execute(
                """
                UPDATE gk_batch
                SET status = 'LAUNCHED',
                    remote_folder = ?,
                    remote_host = ?
                WHERE batch_database_name = ?
                """,
                (remote_folder, remote, db_name),
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
