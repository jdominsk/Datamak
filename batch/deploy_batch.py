#!/usr/bin/env python3
import argparse
import sqlite3
import subprocess
from pathlib import Path
import os


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare new batches and submit remote jobs."
    )
    parser.add_argument(
        "--remote",
        default="jdominsk@perlmutter.nersc.gov",
        help="Remote host (user@host).",
    )
    parser.add_argument(
        "--remote-dir",
        default="/pscratch/sd/j/jdominsk/DTwin/newbatch",
        help="Remote folder containing prepare_newbatch.sh.",
    )
    parser.add_argument(
        "--base-dir",
        default="/pscratch/sd/j/jdominsk/DTwin",
        help="Remote base folder containing runXXXX directories.",
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
    args = parser.parse_args()

    prepare_script = ROOT_DIR / "batch" / "hpc" / "prepare_newbatch.sh"
    if not prepare_script.is_file():
        raise SystemExit(f"Missing prepare_newbatch.sh: {prepare_script}")

    with prepare_script.open("r", encoding="utf-8") as handle:
        prepare_content = handle.read()

    remote_script = f"""
set -euo pipefail
mkdir -p {args.remote_dir}
cat > {args.remote_dir}/prepare_newbatch.sh <<'PREPARE_EOF'
{prepare_content}
PREPARE_EOF
cd {args.remote_dir}
if [ ! -f ./prepare_newbatch.sh ]; then
  echo "prepare_newbatch.sh not found in {args.remote_dir}"
  exit 127
fi
chmod +x ./prepare_newbatch.sh
bash ./prepare_newbatch.sh
shopt -s nullglob
count=0
manifest="{args.remote_dir}/new_runs.txt"
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
  (cd "$run_dir" && sbatch "$(basename "$job")")
  echo "SUBMITTED	${{run_dir}}	${{db_name}}"
  if [ "$count" -ge {args.max_submit} ]; then
    echo "Reached MAX_SUBMIT={args.max_submit}, stopping."
    break
  fi
done < "$manifest"
rm -f "$manifest"
"""
    result = subprocess.run(
        ["ssh", args.remote, "bash", "-lc", remote_script],
        text=True,
        capture_output=True,
    )
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

    if not submitted:
        return

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
        for run_dir, db_name in submitted:
            remote_folder = f"{args.remote}:{run_dir}"
            conn.execute(
                """
                UPDATE gk_batch
                SET status = 'LAUNCHED',
                    remote_folder = ?,
                    remote_host = ?
                WHERE batch_database_name = ?
                """,
                (remote_folder, args.remote, db_name),
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
