#!/usr/bin/env python3
import argparse
import json
import random
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_DB = "/u/jdominsk/DTwin/transp_full_auto/flux_equil_inputs.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate data_equil and transp_timeseries on Flux."
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Flux temp DB path.")
    parser.add_argument(
        "--origin-name",
        required=True,
        help="data_origin.name to populate (e.g. Alexei Transp 09 (full-auto)).",
    )
    parser.add_argument(
        "--remote-path",
        default="",
        help="Override data_origin.origin with a Flux path.",
    )
    parser.add_argument(
        "--activate",
        action="store_true",
        help="Deprecated; data_equil.active remains 0 when shot_time is NULL.",
    )
    parser.add_argument(
        "--create-studies",
        action="store_true",
        help="Create gk_study rows for active data_equil and active gk_model.",
    )
    return parser.parse_args()


def parse_transpfile(transpfile: str) -> Tuple[Optional[str], Optional[str]]:
    if not transpfile or not transpfile.upper().endswith(".CDF"):
        return None, None
    base = transpfile[:-4]
    if len(base) <= 3:
        return None, None
    return base[:-3], base[-3:]


def list_cdf_files(remote_path: str) -> List[str]:
    root = Path(remote_path)
    files = sorted(p.name for p in root.glob("*.CDF"))
    return [name for name in files if not name.upper().endswith("PH.CDF")]


def read_time_array(path: str) -> List[float]:
    try:
        import netCDF4 as nc
    except ModuleNotFoundError as exc:
        raise SystemExit("netCDF4 is required to read TIME3 on Flux.") from exc
    with nc.Dataset(path) as ds:
        if "TIME3" not in ds.variables:
            raise SystemExit(f"TIME3 variable not found in {path}")
        return [float(v) for v in ds["TIME3"][:]]


def ensure_transp_timeseries(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transp_timeseries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_origin_id INTEGER NOT NULL,
            shot_number TEXT NOT NULL,
            shot_variant TEXT,
            time_array TEXT NOT NULL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_transp_timeseries_key
        ON transp_timeseries (data_origin_id, shot_number, shot_variant)
        """
    )


def get_origin(conn: sqlite3.Connection, name: str) -> Tuple[int, str]:
    row = conn.execute(
        "SELECT id, origin FROM data_origin WHERE name = ?",
        (name,),
    ).fetchone()
    if not row:
        raise SystemExit(f"data_origin not found for {name}")
    return int(row[0]), str(row[1] or "")


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"Temp DB not found: {args.db}")

    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        ensure_transp_timeseries(conn)
        origin_id, origin_path = get_origin(conn, args.origin_name)
        remote_path = args.remote_path or origin_path
        if not remote_path:
            raise SystemExit("remote_path is empty; provide --remote-path.")
        files = list_cdf_files(remote_path)
        if not files:
            raise SystemExit(f"No .CDF files found in {remote_path}")

        existing_ts = {
            (row[0], row[1], row[2])
            for row in conn.execute(
                """
                SELECT data_origin_id, shot_number, shot_variant
                FROM transp_timeseries
                """
            ).fetchall()
        }
        existing_equil_rows = conn.execute(
            """
            SELECT id, data_origin_id, shot_number, shot_variant, shot_time
            FROM data_equil
            WHERE data_origin_id = ?
            """,
            (origin_id,),
        ).fetchall()
        existing_equil = {
            (row[1], row[2], row[3], row[4]) for row in existing_equil_rows
        }
        existing_equil_any = {(row[1], row[2], row[3]) for row in existing_equil_rows}
        null_equil_ids = {
            (row[1], row[2], row[3]): int(row[0])
            for row in existing_equil_rows
            if row[4] is None
        }

        inserted_equil = 0
        inserted_ts = 0

        # Ensure data_equil rows exist before creating time series.
        for name in files:
            shot_number, shot_variant = parse_transpfile(name)
            if not shot_number:
                continue
            key_any = (origin_id, shot_number, shot_variant)
            if key_any in existing_equil_any:
                continue
            cursor = conn.execute(
                """
                INSERT INTO data_equil (
                    data_origin_id,
                    folder_path,
                    pfile,
                    pfile_content,
                    gfile,
                    gfile_content,
                    transpfile,
                    shot_time,
                    shot_number,
                    shot_variant,
                    active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    origin_id,
                    remote_path,
                    None,
                    None,
                    None,
                    None,
                    name,
                    None,
                    shot_number,
                    shot_variant,
                    0,
                ),
            )
            inserted_equil += 1
            existing_equil_any.add(key_any)
            existing_equil.add((origin_id, shot_number, shot_variant, None))
            null_equil_ids[key_any] = int(cursor.lastrowid)

        for name in files:
            shot_number, shot_variant = parse_transpfile(name)
            if not shot_number:
                continue
            key = (origin_id, shot_number, shot_variant)
            selected_time = None
            if key not in existing_ts:
                time_array = read_time_array(os.path.join(remote_path, name))
                if time_array:
                    selected_time = random.choice(time_array)
                conn.execute(
                    """
                    INSERT INTO transp_timeseries (
                        data_origin_id, shot_number, shot_variant, time_array
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (origin_id, shot_number, shot_variant, json.dumps(time_array)),
                )
                existing_ts.add(key)
                inserted_ts += 1

            if selected_time is not None:
                if (origin_id, shot_number, shot_variant) in null_equil_ids:
                    conn.execute(
                        """
                        UPDATE data_equil
                        SET shot_time = ?, active = 1
                        WHERE id = ?
                        """,
                        (selected_time, null_equil_ids[(origin_id, shot_number, shot_variant)]),
                    )
                    existing_equil.discard((origin_id, shot_number, shot_variant, None))
                    existing_equil.add((origin_id, shot_number, shot_variant, selected_time))
                else:
                    equil_key = (origin_id, shot_number, shot_variant, selected_time)
                    if equil_key not in existing_equil:
                        conn.execute(
                            """
                            INSERT INTO data_equil (
                                data_origin_id,
                                folder_path,
                                pfile,
                                pfile_content,
                                gfile,
                                gfile_content,
                                transpfile,
                                shot_time,
                                shot_number,
                                shot_variant,
                                active
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                origin_id,
                                remote_path,
                                None,
                                None,
                                None,
                                None,
                                name,
                                selected_time,
                                shot_number,
                                shot_variant,
                                1,
                            ),
                        )
                        existing_equil.add(equil_key)
                        inserted_equil += 1

        conn.commit()
        if args.create_studies:
            gk_code_id = conn.execute(
                "SELECT id FROM gk_code WHERE name = 'GX'"
            ).fetchone()
            gk_code_id_val = int(gk_code_id[0]) if gk_code_id else None
            if gk_code_id_val is None:
                raise SystemExit("GX not found in gk_code.")
            conn.execute(
                """
                INSERT INTO gk_study (data_equil_id, gk_code_id, comment)
                SELECT de.id, ?, 'auto-added'
                FROM data_equil AS de
                JOIN data_origin AS do ON do.id = de.data_origin_id
                JOIN gk_model AS gm ON gm.active = 1
                LEFT JOIN gk_study AS gs
                    ON gs.data_equil_id = de.id
                    AND gs.gk_code_id = ?
                WHERE de.active = 1
                  AND do.name = ?
                  AND gs.id IS NULL
                """,
                (gk_code_id_val, gk_code_id_val, args.origin_name),
            )
            conn.commit()
    finally:
        conn.close()

    print(
        f"Inserted data_equil={inserted_equil}, transp_timeseries={inserted_ts} "
        f"for {args.origin_name}"
    )


if __name__ == "__main__":
    main()
