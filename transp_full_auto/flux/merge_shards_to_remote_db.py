#!/usr/bin/env python3
import argparse
import glob
import sqlite3
from typing import Iterable, List, Tuple


DEFAULT_MAIN_DB = "/u/jdominsk/DTwin/remote_gk_inputs/remote_gk_inputs_20260116_160506.work.db"
DEFAULT_SHARD_GLOB = "/u/jdominsk/DTwin/remote_gk_inputs/shards/remote_gk_inputs_shard_*.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge shard remote_gk_input rows back into the main DB."
    )
    parser.add_argument("--main-db", default=DEFAULT_MAIN_DB)
    parser.add_argument("--shard-glob", default=DEFAULT_SHARD_GLOB)
    parser.add_argument(
        "--include-pending",
        action="store_true",
        help="Also merge rows still marked PENDING in shards.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Rows per executemany batch.",
    )
    return parser.parse_args()


def iter_rows(
    shard_path: str, include_pending: bool
) -> Iterable[Tuple[int, str, str, str, str, str, int]]:
    conn = sqlite3.connect(shard_path)
    try:
        if include_pending:
            where = "1=1"
        else:
            where = "status != 'PENDING' OR (file_name IS NOT NULL AND file_name != '')"
        rows = conn.execute(
            f"""
            SELECT id, status, file_name, file_path, content, comment, synced
            FROM remote_gk_input
            WHERE {where}
            """
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        yield row


def chunked(rows: List[Tuple], size: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def main() -> None:
    args = parse_args()
    shard_paths = sorted(glob.glob(args.shard_glob))
    if not shard_paths:
        raise SystemExit(f"No shard DBs found for {args.shard_glob}")

    main_conn = sqlite3.connect(args.main_db)
    try:
        main_conn.execute("PRAGMA busy_timeout = 60000")
        main_conn.execute("PRAGMA journal_mode = WAL")
        update_sql = """
            UPDATE remote_gk_input
            SET status = ?,
                file_name = ?,
                file_path = ?,
                content = ?,
                comment = ?,
                synced = ?
            WHERE id = ?
        """
        for shard in shard_paths:
            rows = list(iter_rows(shard, args.include_pending))
            for batch in chunked(rows, args.chunk_size):
                params = [
                    (r[1], r[2], r[3], r[4], r[5], r[6], r[0]) for r in batch
                ]
                main_conn.executemany(update_sql, params)
                main_conn.commit()
            print(f"{shard}: merged {len(rows)} rows")
    finally:
        main_conn.close()


if __name__ == "__main__":
    main()
