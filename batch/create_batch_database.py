#!/usr/bin/env python3
import argparse
import sqlite3


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_run (
          id INTEGER PRIMARY KEY,
          gk_input_id INTEGER,
          input_folder TEXT,
          job_folder TEXT,
          archive_folder TEXT,
          input_name TEXT,
          nb_nodes INTEGER,
          status TEXT
        );
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create batch_database.db with gk_run table."
    )
    parser.add_argument(
        "db",
        nargs="?",
        default="batch_database.db",
        help="Path to the SQLite database file (default: batch_database.db).",
    )
    args = parser.parse_args()

    with sqlite3.connect(args.db) as conn:
        create_schema(conn)

    print(f"Created/verified {args.db} with table gk_run.")


if __name__ == "__main__":
    main()
