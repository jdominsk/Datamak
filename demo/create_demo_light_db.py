#!/usr/bin/env python3
import argparse
import sqlite3
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DB = ROOT_DIR / "demo" / "gyrokinetic_simulations_demo.db"
DEFAULT_DEST_DB = ROOT_DIR / "demo" / "gyrokinetic_simulations_demo_light.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a lighter copy of the Datamak demo DB by blanking "
            "gk_input.content for rows that have no associated gk_run."
        )
    )
    parser.add_argument(
        "--source-db",
        default=str(DEFAULT_SOURCE_DB),
        help="Path to the source demo SQLite database.",
    )
    parser.add_argument(
        "--dest-db",
        default=str(DEFAULT_DEST_DB),
        help="Path to the light demo SQLite database to create.",
    )
    return parser.parse_args()


def create_demo_light_db(source_db: str, dest_db: str) -> dict[str, int]:
    source_path = Path(source_db)
    dest_path = Path(dest_db)
    if not source_path.exists():
        raise SystemExit(f"Source demo DB not found: {source_path}")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()

    with sqlite3.connect(source_path) as src, sqlite3.connect(dest_path) as dst:
        src.backup(dst)
        dst.execute("PRAGMA foreign_keys = ON")

        row = dst.execute(
            """
            SELECT
                COUNT(*),
                COUNT(CASE WHEN length(content) > 0 THEN 1 END),
                COALESCE(SUM(length(content)), 0)
            FROM gk_input AS gi
            LEFT JOIN gk_run AS gr
              ON gr.gk_input_id = gi.id
            WHERE gr.id IS NULL
            """
        ).fetchone()
        total_no_run = int(row[0] or 0)
        nonempty_before = int(row[1] or 0)
        bytes_before = int(row[2] or 0)

        dst.execute(
            """
            UPDATE gk_input
            SET content = ''
            WHERE id IN (
                SELECT gi.id
                FROM gk_input AS gi
                LEFT JOIN gk_run AS gr
                  ON gr.gk_input_id = gi.id
                WHERE gr.id IS NULL
            )
            """
        )

        row_after = dst.execute(
            """
            SELECT
                COUNT(CASE WHEN length(content) > 0 THEN 1 END),
                COALESCE(SUM(length(content)), 0)
            FROM gk_input AS gi
            LEFT JOIN gk_run AS gr
              ON gr.gk_input_id = gi.id
            WHERE gr.id IS NULL
            """
        ).fetchone()
        nonempty_after = int(row_after[0] or 0)
        bytes_after = int(row_after[1] or 0)

        dst.commit()
        integrity = dst.execute("PRAGMA integrity_check").fetchone()
        if not integrity or str(integrity[0]).lower() != "ok":
            raise SystemExit(f"Integrity check failed in light demo DB: {integrity}")
        fk_errors = dst.execute("PRAGMA foreign_key_check").fetchall()
        if fk_errors:
            raise SystemExit(f"Foreign-key check failed in light demo DB: {fk_errors[:5]}")
        dst.execute("VACUUM")

    return {
        "total_no_run_rows": total_no_run,
        "nonempty_before": nonempty_before,
        "nonempty_after": nonempty_after,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
    }


def main() -> None:
    args = parse_args()
    stats = create_demo_light_db(args.source_db, args.dest_db)
    print(f"Wrote light demo DB: {args.dest_db}")
    print(f"No-run gk_input rows: {stats['total_no_run_rows']}")
    print(f"Non-empty no-run contents before: {stats['nonempty_before']}")
    print(f"Non-empty no-run contents after: {stats['nonempty_after']}")
    print(f"Bytes in no-run contents before: {stats['bytes_before']}")
    print(f"Bytes in no-run contents after: {stats['bytes_after']}")


if __name__ == "__main__":
    main()
