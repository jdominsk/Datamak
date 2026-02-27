#!/usr/bin/env python3
import argparse
import os
import sqlite3
from typing import List, Optional, Tuple


def _tables(conn: sqlite3.Connection) -> List[str]:
    return [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]


def _fetch_paths(conn: sqlite3.Connection, surrogate_id: int) -> Optional[Tuple[str, str]]:
    row = conn.execute(
        "SELECT model_path, meta_path FROM gk_surrogate WHERE id = ?",
        (surrogate_id,),
    ).fetchone()
    if row is None:
        return None
    return (row[0] or "", row[1] or "")


def _delete_files(paths: List[str]) -> List[str]:
    deleted = []
    for path in paths:
        if not path:
            continue
        try:
            if os.path.exists(path):
                os.remove(path)
                deleted.append(path)
        except OSError:
            continue
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete a surrogate model and its estimates."
    )
    parser.add_argument("--db", required=True, help="Path to gyrokinetic_simulations.db")
    parser.add_argument("--surrogate-id", required=True, type=int, help="gk_surrogate id to delete")
    parser.add_argument(
        "--keep-files",
        action="store_true",
        help="Do not delete model files on disk.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    conn = sqlite3.connect(args.db)
    try:
        tables = _tables(conn)
        if "gk_surrogate" not in tables:
            raise SystemExit("Table gk_surrogate not found.")
        paths = _fetch_paths(conn, args.surrogate_id)
        if paths is None:
            raise SystemExit(f"No gk_surrogate entry for id={args.surrogate_id}")
        model_path, meta_path = paths
        estimate_count = 0
        conn.execute("BEGIN")
        if "sg_estimate" in tables:
            cur = conn.execute(
                "DELETE FROM sg_estimate WHERE gk_surrogate_id = ?",
                (args.surrogate_id,),
            )
            estimate_count = cur.rowcount or 0
        cur = conn.execute(
            "DELETE FROM gk_surrogate WHERE id = ?",
            (args.surrogate_id,),
        )
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            raise SystemExit(f"No gk_surrogate entry for id={args.surrogate_id}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        conn.close()
        raise
    finally:
        conn.close()

    deleted_files: List[str] = []
    if not args.keep_files:
        deleted_files = _delete_files([model_path, meta_path])

    print(
        f"Deleted gk_surrogate id={args.surrogate_id}; "
        f"sg_estimate rows deleted={estimate_count}."
    )
    if args.keep_files:
        print("Model files kept.")
    else:
        if deleted_files:
            print("Deleted files:")
            for path in deleted_files:
                print(path)
        else:
            print("No model files deleted.")


if __name__ == "__main__":
    main()
