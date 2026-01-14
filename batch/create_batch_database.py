#!/usr/bin/env python3
import argparse
import sqlite3
from datetime import datetime
import os
from pathlib import Path


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def ensure_gk_run_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_run (
          id INTEGER PRIMARY KEY,
          gk_input_id INTEGER,
          input_content TEXT NOT NULL,
          input_folder TEXT,
          job_folder TEXT,
          archive_folder TEXT,
          input_name TEXT,
          nb_nodes INTEGER,
          job_id TEXT,
          status TEXT,
          t_max_initial REAL NOT NULL DEFAULT 0,
          t_max REAL NOT NULL DEFAULT 0
        );
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
    if "input_content" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN input_content TEXT")
    if "job_id" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN job_id TEXT")
    if "t_max_initial" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN t_max_initial REAL NOT NULL DEFAULT 0")
    if "t_max" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN t_max REAL NOT NULL DEFAULT 0")


def ensure_gk_convergence_timeseries_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_convergence_timeseries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_run_id INTEGER NOT NULL,
            gk_input_id INTEGER NOT NULL,
            phi2_tot_f32 BLOB NOT NULL,
            n_points INTEGER NOT NULL DEFAULT 100 CHECK (n_points = 100),
            window_t_min REAL,
            window_t_max REAL,
            gamma_mean REAL,
            relstd REAL,
            slope_norm REAL,
            method TEXT NOT NULL DEFAULT 'A',
            r2 REAL,
            is_converged INTEGER NOT NULL DEFAULT 0 CHECK (is_converged IN (0, 1)),
            creation_date TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_convergence_timeseries_run_id
        ON gk_convergence_timeseries (gk_run_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_convergence_timeseries_input_id
        ON gk_convergence_timeseries (gk_input_id)
        """
    )


def copy_torun_rows(source_db: str, batch_db: str) -> int:
    with sqlite3.connect(source_db) as source_conn:
        source_conn.row_factory = sqlite3.Row
        rows = source_conn.execute(
            "SELECT id, content FROM gk_input WHERE status = 'TORUN'"
        ).fetchall()

    with sqlite3.connect(batch_db) as batch_conn:
        ensure_gk_run_table(batch_conn)
        batch_conn.executemany(
            """
            INSERT INTO gk_run (gk_input_id, input_content, status)
            VALUES (?, ?, 'TORUN')
            """,
            [(row["id"], row["content"]) for row in rows],
        )

    if rows:
        with sqlite3.connect(source_db) as source_conn:
            source_conn.executemany(
                "UPDATE gk_input SET status = 'BATCH' WHERE id = ?",
                [(row["id"],) for row in rows],
            )

    return len(rows)


def ensure_gk_batch_table(conn: sqlite3.Connection) -> None:
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


def log_batch_created(source_db: str, batch_db: str) -> None:
    with sqlite3.connect(source_db) as conn:
        ensure_gk_batch_table(conn)
        conn.execute(
            """
            INSERT INTO gk_batch (batch_database_name, remote_folder, status)
            VALUES (?, ?, 'CREATED')
            """,
            (os.path.basename(batch_db), 'N/A'),
        )
        conn.commit()


def main() -> None:
    default_name = datetime.now().strftime("batch_database_%Y%m%d_%H%M%S.db")
    default_path = ROOT_DIR / "batch" / "new" / default_name
    parser = argparse.ArgumentParser(
        description=(
            "Create a batch database and optionally copy TORUN gk_input rows."
        )
    )
    parser.add_argument(
        "db",
        nargs="?",
        default=str(default_path),
        help=(
            "Path to the SQLite database file "
            f"(default: {default_path})."
        ),
    )
    parser.add_argument(
        "--copy-torun",
        action="store_true",
        help="Copy gk_input rows with status=TORUN into gk_run.",
    )
    parser.add_argument(
        "--source-db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Path to the source gyrokinetic database.",
    )
    args = parser.parse_args()

    db_dir = os.path.dirname(args.db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(args.db) as conn:
        ensure_gk_run_table(conn)
        ensure_gk_convergence_timeseries_table(conn)

    print(
        f"Created/verified {args.db} with tables gk_run and gk_convergence_timeseries."
    )
    log_batch_created(args.source_db, args.db)

    if args.copy_torun:
        copied = copy_torun_rows(args.source_db, args.db)
        print(f"Copied {copied} TORUN rows into {args.db}.")


if __name__ == "__main__":
    main()
