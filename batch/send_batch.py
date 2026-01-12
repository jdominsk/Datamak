#!/usr/bin/env python3
import argparse
import shlex
import sqlite3
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy batch databases to Perlmutter in one transfer."
    )
    parser.add_argument(
        "--batch-dir",
        default="./batch",
        help="Local folder containing batch_database_*.db files.",
    )
    parser.add_argument(
        "--remote",
        default="jdominsk@perlmutter.nersc.gov:/pscratch/sd/j/jdominsk/DTwin/newbatch",
        help="Remote base destination (user@host:/path).",
    )
    parser.add_argument(
        "--gk-db",
        default="gyrokinetic_database.db",
        help="Local gyrokinetic database for logging gk_batch entries.",
    )
    args = parser.parse_args()

    batch_path = Path(args.batch_dir)
    if not batch_path.is_dir():
        raise SystemExit(f"Batch directory not found: {batch_path}")

    db_files = sorted(batch_path.glob("batch_database_*.db"))
    if not db_files:
        print(f"No batch_database_*.db files found in {batch_path}")
        return

    scripts = [
        batch_path / "hpc" / "job_submit.sh",
        batch_path / "hpc" / "job_execute.sh",
    ]
    missing_scripts = [str(path) for path in scripts if not path.is_file()]
    if missing_scripts:
        raise SystemExit(f"Missing required scripts: {', '.join(missing_scripts)}")

    sent_dir = batch_path / "sent"
    sent_dir.mkdir(exist_ok=True)

    conn = sqlite3.connect(args.gk_db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gk_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_database_name TEXT NOT NULL,
                remote_folder TEXT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
        non_empty_dbs = []
        for db_path in db_files:
            db_conn = sqlite3.connect(db_path)
            try:
                db_conn.row_factory = sqlite3.Row
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

        remote_base = args.remote.rstrip("/")
        if ":" not in remote_base:
            raise SystemExit("Remote must be in the form user@host:/path")
        remote_host, remote_root = remote_base.split(":", 1)
        remote_root_quoted = shlex.quote(remote_root)

        transfer_items = sorted(
            {
                str(path.relative_to(batch_path))
                for path in non_empty_dbs + scripts
            }
        )
        tar_cmd = ["tar", "-cf", "-", "-C", str(batch_path), *transfer_items]
        remote_cmd = f"mkdir -p {remote_root_quoted} && tar -xf - -C {remote_root_quoted}"
        tar_proc = subprocess.Popen(
            tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            subprocess.run(
                ["ssh", remote_host, remote_cmd],
                check=True,
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

        for db_path in non_empty_dbs:
            conn.execute(
                """
                INSERT INTO gk_batch (batch_database_name, remote_folder, status)
                VALUES (?, ?, 'SENT')
                """,
                (db_path.name, f"{remote_host}:{remote_root}"),
            )
            conn.commit()
            db_path.replace(sent_dir / db_path.name)
            print(f"Copied {db_path} -> {remote_host}:{remote_root}/")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
