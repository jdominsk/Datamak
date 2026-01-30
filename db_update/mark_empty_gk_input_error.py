#!/usr/bin/env python3
import argparse
import os
import sqlite3


DEFAULT_DB = os.path.join(
    os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
    "gyrokinetic_simulations.db",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mark gk_input rows with empty content as ERROR.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to main DB.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report the number of rows that would be updated.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    conn = sqlite3.connect(args.db)
    try:
        conn.row_factory = sqlite3.Row
        where_clause = "status = 'WAIT' AND (content IS NULL OR TRIM(content) = '')"
        count = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM gk_input WHERE {where_clause}"
        ).fetchone()["cnt"]
        if args.dry_run:
            print(f"Would update {count} gk_input rows to ERROR.")
            return
        if count == 0:
            print("No gk_input rows need updating.")
            return
        cur = conn.execute(
            f"UPDATE gk_input SET status = 'ERROR' WHERE {where_clause}"
        )
        conn.commit()
        print(f"Updated {cur.rowcount} gk_input rows to ERROR.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
