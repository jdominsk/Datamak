#!/usr/bin/env python3
import argparse
import shlex
import sqlite3
import subprocess
from pathlib import Path
import os


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy batch databases to Perlmutter in one transfer."
    )
    parser.add_argument(
        "--batch-dir",
        default=str(ROOT_DIR / "batch" / "new"),
        help="Local folder containing batch_database_*.db files.",
    )
    parser.add_argument(
        "--remote",
        default="jdominsk@perlmutter.nersc.gov:/pscratch/sd/j/jdominsk/DTwin/newbatch",
        help="Remote base destination (user@host:/path).",
    )
    parser.add_argument(
        "--gk-db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database for logging gk_batch entries.",
    )
    args = parser.parse_args()

    batch_path = Path(args.batch_dir)
    if not batch_path.is_dir():
        raise SystemExit(f"Batch directory not found: {batch_path}")

    db_files = sorted(batch_path.glob("batch_database_*.db"))
    hpc_dir = ROOT_DIR / "batch" / "hpc"
    scripts = [
        hpc_dir / "job_submit_large.sh",
        hpc_dir / "job_execute.sh",
        hpc_dir / "prepare_newbatch_large.sh",
    ]
    missing_scripts = [str(path) for path in scripts if not path.is_file()]
    if missing_scripts:
        raise SystemExit(f"Missing required scripts: {', '.join(missing_scripts)}")

    sent_dir = batch_path.parent / "sent"
    sent_dir.mkdir(exist_ok=True)

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

        if not created_names:
            print("No CREATED gk_batch rows found.")
            return

        db_by_name = {path.name: path for path in db_files}
        missing_files = sorted(name for name in created_names if name not in db_by_name)
        extra_files = sorted(name for name in db_by_name if name not in created_names)

        for name in missing_files:
            print(f"Warning: gk_batch row without file: {name}")
        for name in extra_files:
            print(f"Warning: file without gk_batch row: {name}")

        candidate_dbs = [
            db_by_name[name] for name in sorted(created_names) if name in db_by_name
        ]
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

        remote_base = args.remote.rstrip("/")
        if ":" not in remote_base:
            raise SystemExit("Remote must be in the form user@host:/path")
        remote_host, remote_root = remote_base.split(":", 1)
        remote_root_quoted = shlex.quote(remote_root)

        tar_root = batch_path.parent
        transfer_items = sorted(
            {
                str(path.relative_to(tar_root))
                for path in non_empty_dbs + scripts
            }
        )
        tar_cmd = ["tar", "-cf", "-", "-C", str(tar_root), *transfer_items]
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
            remote_location = f"{remote_host}:{remote_root}"
            conn.execute(
                """
                UPDATE gk_batch
                SET remote_folder = ?, remote_host = ?, status = 'SENT'
                WHERE batch_database_name = ?
                """,
                (remote_location, remote_host, db_path.name),
            )
            conn.commit()
            db_path.replace(sent_dir / db_path.name)
            print(f"Copied {db_path} -> {remote_host}:{remote_root}/")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
