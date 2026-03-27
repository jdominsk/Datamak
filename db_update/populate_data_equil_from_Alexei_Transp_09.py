#!/usr/bin/env python3
import argparse
import os
import sqlite3
import subprocess
import sys
from typing import List, Optional, Tuple

ROOT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dtwin_config import require_source_path, resolve_flux_profile  # noqa: E402


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".",
    "gyrokinetic_simulations.db",
)
DEFAULT_ORIGIN_NAME = "Transp 09 (semi-auto)"
DEFAULT_ORIGIN = "/p/transparch/result/NSTX/09"
DEFAULT_ACTIVATE_SQL = "update_activate_Alexei_Transp_09.sql"
DEFAULT_FILE_TYPE = "TRANSP"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate data_origin and data_equil from remote TRANSP .CDF files.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to the SQLite database file.")
    parser.add_argument("--origin-name", default=DEFAULT_ORIGIN_NAME, help="data_origin.name value.")
    parser.add_argument("--origin", default=DEFAULT_ORIGIN, help="data_origin.origin value.")
    parser.add_argument(
        "--copy",
        default="",
        help="data_origin.copy value. Defaults to DTWIN_TRANSP_COPY_ROOT_09 or the Datamak user config.",
    )
    parser.add_argument(
        "--remote",
        default="",
        help="SSH host for remote listing. Defaults to the Datamak Flux profile.",
    )
    parser.add_argument(
        "--remote-path",
        default=DEFAULT_ORIGIN,
        help="Remote directory to scan for .CDF files.",
    )
    return parser.parse_args()


def list_remote_cdf_files(remote: str, remote_path: str) -> List[str]:
    cmd = [
        "ssh",
        remote,
        f"ls -1 {remote_path}/*.CDF 2>/dev/null || true",
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return [os.path.basename(path) for path in files]


def _origin_name_candidates(name: str) -> Tuple[str, ...]:
    value = (name or "").strip()
    if not value:
        return tuple()
    if value.startswith("Alexei "):
        canonical = value[len("Alexei ") :]
        return (value, canonical)
    if value.startswith("Transp "):
        return (value, f"Alexei {value}")
    return (value,)


def get_or_create_origin_id(conn: sqlite3.Connection, name: str, origin: str, copy: str) -> int:
    name_candidates = _origin_name_candidates(name)
    placeholders = ", ".join("?" for _ in name_candidates)
    row = conn.execute(
        f"""
        SELECT id
        FROM data_origin
        WHERE name IN ({placeholders}) AND origin = ? AND copy = ?
        """,
        (*name_candidates, origin, copy),
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute(
        "INSERT INTO data_origin (name, origin, copy, file_type, tokamak) VALUES (?, ?, ?, ?, ?)",
        (name, origin, copy, DEFAULT_FILE_TYPE, "NSTX"),
    )
    return int(cur.lastrowid)


def parse_transpfile(transpfile: str) -> Tuple[Optional[str], Optional[str]]:
    if not transpfile or not transpfile.upper().endswith(".CDF"):
        return None, None
    base = transpfile[:-4]
    if len(base) <= 3:
        return None, None
    return base[:-3], base[-3:]


def insert_equil_rows(conn: sqlite3.Connection, origin_id: int, folder_path: str, files: List[str]) -> int:
    existing = conn.execute(
        "SELECT transpfile FROM data_equil WHERE data_origin_id = ?",
        (origin_id,),
    ).fetchall()
    existing_files = {row[0] for row in existing if row[0]}
    rows_to_insert = [
        (
            origin_id,
            folder_path,
            None,
            None,
            None,
            None,
            file,
            *parse_transpfile(file),
            0,
        )
        for file in files
        if file not in existing_files
    ]
    if rows_to_insert:
        conn.executemany(
            """
            INSERT INTO data_equil (
                data_origin_id,
                folder_path,
                pfile,
                pfile_content,
                gfile,
                gfile_content,
                transpfile,
                shot_number,
                shot_variant,
                active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
    return len(rows_to_insert)


def main() -> None:
    args = parse_args()
    flux = resolve_flux_profile({"remote": args.remote})
    remote = str(flux["remote"] or "").strip()
    if not remote:
        raise SystemExit("Flux remote host is empty. Configure it in Datamak settings.")
    copy_value = require_source_path("transp_copy_root_09", args.copy)
    if not os.path.exists(args.db):
        raise SystemExit(f"Database not found: {args.db}")
    files = list_remote_cdf_files(remote, args.remote_path)
    if not files:
        raise SystemExit("No .CDF files found.")
    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        origin_id = get_or_create_origin_id(conn, args.origin_name, args.origin, copy_value)
        inserted = insert_equil_rows(conn, origin_id, args.remote_path, files)
        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_ACTIVATE_SQL)
        with open(sql_path, "r", encoding="utf-8") as handle:
            conn.executescript(handle.read())
        conn.commit()
    finally:
        conn.close()
    print(f"Inserted {inserted} rows into data_equil (origin_id={origin_id}).")


if __name__ == "__main__":
    main()
