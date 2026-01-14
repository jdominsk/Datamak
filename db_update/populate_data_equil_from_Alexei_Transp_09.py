#!/usr/bin/env python3
import argparse
import os
import sqlite3
import subprocess
from typing import List


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".",
    "gyrokinetic_simulations.db",
)
DEFAULT_ORIGIN_NAME = "Alexei Transp 09"
DEFAULT_ORIGIN = "/p/transparch/result/NSTX/09"
DEFAULT_COPY = "/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin/tmp_copy_transp/NSTX/09"
DEFAULT_REMOTE = "jdominsk@flux"
DEFAULT_ACTIVATE_SQL = "update_activate_Alexei_Transp_09.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate data_origin and data_equil from remote TRANSP .CDF files.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to the SQLite database file.")
    parser.add_argument("--origin-name", default=DEFAULT_ORIGIN_NAME, help="data_origin.name value.")
    parser.add_argument("--origin", default=DEFAULT_ORIGIN, help="data_origin.origin value.")
    parser.add_argument("--copy", default=DEFAULT_COPY, help="data_origin.copy value.")
    parser.add_argument("--remote", default=DEFAULT_REMOTE, help="SSH host for remote listing.")
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


def get_or_create_origin_id(conn: sqlite3.Connection, name: str, origin: str, copy: str) -> int:
    row = conn.execute(
        "SELECT id FROM data_origin WHERE name=? AND origin=? AND copy=?",
        (name, origin, copy),
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute(
        "INSERT INTO data_origin (name, origin, copy, tokamak) VALUES (?, ?, ?, ?)",
        (name, origin, copy, "NSTX"),
    )
    return int(cur.lastrowid)


def insert_equil_rows(conn: sqlite3.Connection, origin_id: int, folder_path: str, files: List[str]) -> int:
    existing = conn.execute(
        "SELECT transpfile FROM data_equil WHERE data_origin_id = ?",
        (origin_id,),
    ).fetchall()
    existing_files = {row[0] for row in existing if row[0]}
    rows_to_insert = [
        (origin_id, folder_path, None, None, None, None, file, 0)
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
                active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
    return len(rows_to_insert)


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"Database not found: {args.db}")
    files = list_remote_cdf_files(args.remote, args.remote_path)
    if not files:
        raise SystemExit("No .CDF files found.")
    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        origin_id = get_or_create_origin_id(conn, args.origin_name, args.origin, args.copy)
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
