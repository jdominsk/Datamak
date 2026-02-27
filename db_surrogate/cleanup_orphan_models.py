#!/usr/bin/env python3
import argparse
import os
import sqlite3
from typing import Set


def collect_tracked_paths(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute("SELECT model_path, meta_path FROM gk_surrogate").fetchall()
    tracked: Set[str] = set()
    for model_path, meta_path in rows:
        for path in (model_path, meta_path):
            if not path:
                continue
            tracked.add(os.path.realpath(os.path.abspath(path)))
            tracked.add(os.path.basename(path))
    return tracked


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete surrogate model files not referenced by gk_surrogate."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to gyrokinetic_simulations.db",
    )
    parser.add_argument(
        "--models-dir",
        default=os.path.join(os.path.dirname(__file__), "models"),
        help="Directory that stores trained surrogate model files.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete orphan files (default is dry-run).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    if not os.path.isdir(args.models_dir):
        raise SystemExit(f"Models directory not found: {args.models_dir}")

    conn = sqlite3.connect(args.db)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "gk_surrogate" not in tables:
            raise SystemExit("Table gk_surrogate not found in DB.")
        tracked = collect_tracked_paths(conn)
    finally:
        conn.close()

    orphans = []
    for name in os.listdir(args.models_dir):
        if not (name.endswith(".pkl") or name.endswith(".pkl.json")):
            continue
        path = os.path.join(args.models_dir, name)
        real = os.path.realpath(os.path.abspath(path))
        if real in tracked or name in tracked:
            continue
        orphans.append(path)

    if not orphans:
        print("No orphan surrogate model files found.")
        return

    if not args.apply:
        print("Orphan surrogate model files (dry-run):")
        for path in sorted(orphans):
            print(path)
        print("Run with --apply to delete.")
        return

    for path in sorted(orphans):
        try:
            os.remove(path)
            print(f"Deleted {path}")
        except OSError as exc:
            print(f"Failed to delete {path}: {exc}")


if __name__ == "__main__":
    main()
