#!/usr/bin/env python3
import argparse
import sqlite3
from datetime import datetime
import os


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
          status TEXT
        );
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
    if "input_content" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN input_content TEXT")


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


def main() -> None:
    default_name = datetime.now().strftime("batch_database_%Y%m%d_%H%M%S.db")
    default_path = f"batch/{default_name}"
    parser = argparse.ArgumentParser(
        description=(
            "Create a batch database and optionally copy TORUN gk_input rows."
        )
    )
    parser.add_argument(
        "db",
        nargs="?",
        default=default_path,
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
        default="gyrokinetic_simulations.db",
        help="Path to the source gyrokinetic database.",
    )
    args = parser.parse_args()

    db_dir = os.path.dirname(args.db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(args.db) as conn:
        ensure_gk_run_table(conn)

    print(f"Created/verified {args.db} with table gk_run.")

    if args.copy_torun:
        copied = copy_torun_rows(args.source_db, args.db)
        print(f"Copied {copied} TORUN rows into {args.db}.")


if __name__ == "__main__":
    main()
