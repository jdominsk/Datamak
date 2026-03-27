#!/usr/bin/env python3
import argparse
import glob
import os
import sqlite3
import sys
from typing import List, Tuple

ROOT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dtwin_config import require_source_path  # noqa: E402


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".",
    "gyrokinetic_simulations.db",
)
DEFAULT_ACTIVATE_SQL = "update_activate_Mate_KinEFIT.sql"
DEFAULT_FILE_TYPE = "EFIT"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate data_equil with p/g file pairs from subfolders.",
    )
    parser.add_argument(
        "--root",
        default="",
        help="Root folder to scan. Defaults to DTWIN_MATE_ROOT or the Datamak user config.",
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, help="Path to the SQLite database file."
    )
    parser.add_argument("--origin-name", default="Kinetic EFIT (Mate)", help="data_origin.name value.")
    parser.add_argument("--origin", default="Google drive", help="data_origin.origin value.")
    parser.add_argument(
        "--copy",
        default="",
        help="data_origin.copy value. Defaults to the resolved Mate root.",
    )
    return parser.parse_args()


def list_subfolders(root: str) -> List[str]:
    subfolders: List[str] = []
    with os.scandir(root) as entries:
        for entry in entries:
            if entry.is_dir():
                subfolders.append(entry.path)
    return subfolders


def build_pairs(root: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for folder in list_subfolders(root):
        p_files = glob.glob(os.path.join(folder, "p*"))
        g_files = glob.glob(os.path.join(folder, "g*"))
        if not p_files or not g_files:
            continue
        for p_file in p_files:
            for g_file in g_files:
                pairs.append((p_file, g_file))
    return pairs


def get_or_create_origin_id(
    conn: sqlite3.Connection, name: str, origin: str, copy: str
) -> int:
    name_candidates = [(name or "").strip()]
    if name_candidates[0] == "Kinetic EFIT (Mate)":
        name_candidates.append("Mate Kinetic EFIT")
    elif name_candidates[0] == "Mate Kinetic EFIT":
        name_candidates.append("Kinetic EFIT (Mate)")
    placeholders = ", ".join("?" for _ in name_candidates)
    row = conn.execute(
        f"SELECT id FROM data_origin WHERE name IN ({placeholders}) AND origin=? AND copy=?",
        (*name_candidates, origin, copy),
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute(
        "INSERT INTO data_origin (name, origin, copy, file_type, tokamak) VALUES (?, ?, ?, ?, ?)",
        (name, origin, copy, DEFAULT_FILE_TYPE, "NSTX"),
    )
    return int(cur.lastrowid)


def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def insert_pairs(
    conn: sqlite3.Connection, origin_id: int, origin_name: str, pairs: List[Tuple[str, str]]
) -> int:
    rows = []
    for p, g in pairs:
        p_dir = os.path.dirname(p)
        g_dir = os.path.dirname(g)
        if p_dir != g_dir:
            raise SystemExit(f"pfile and gfile are in different folders: {p_dir} vs {g_dir}")
        rows.append(
            (
                origin_id,
                p_dir,
                os.path.basename(p),
                read_text_file(p),
                os.path.basename(g),
                read_text_file(g),
            )
        )
    existing = conn.execute(
        """
        SELECT de.pfile, de.gfile
        FROM data_equil AS de
        JOIN data_origin AS do ON do.id = de.data_origin_id
        WHERE de.data_origin_id = ? AND do.name = ?
        """,
        (origin_id, origin_name),
    ).fetchall()
    existing_pairs = {(row[0], row[1]) for row in existing}
    rows_to_insert = [row for row in rows if (row[2], row[4]) not in existing_pairs]
    if rows_to_insert:
        conn.executemany(
            """
            INSERT INTO data_equil (
                data_origin_id,
                folder_path,
                pfile,
                pfile_content,
                gfile,
                gfile_content
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
    return len(rows_to_insert)


def main() -> None:
    args = parse_args()
    root = require_source_path("mate_root", args.root)
    copy_value = (args.copy or "").strip() or root
    if not os.path.exists(args.db):
        raise SystemExit(f"Database not found: {args.db}")
    pairs = build_pairs(root)
    if not pairs:
        raise SystemExit("No p/g pairs found.")
    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        origin_id = get_or_create_origin_id(conn, args.origin_name, args.origin, copy_value)
        inserted = insert_pairs(conn, origin_id, args.origin_name, pairs)
        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_ACTIVATE_SQL)
        with open(sql_path, "r", encoding="utf-8") as handle:
            conn.executescript(handle.read())
        conn.commit()
    finally:
        conn.close()
    print(f"Inserted {inserted} rows into data_equil (origin_id={origin_id}).")


if __name__ == "__main__":
    main()
