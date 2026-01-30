#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
from typing import Dict, Iterable, List, Tuple


DEFAULT_MAIN_DB = os.path.join(
    os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
    "gyrokinetic_simulations.db",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Flux temp DB (data_equil + transp_timeseries) into main DB."
    )
    parser.add_argument("--main-db", default=DEFAULT_MAIN_DB, help="Main DB path.")
    parser.add_argument(
        "--flux-db",
        required=True,
        help="Flux temp DB path (flux_equil_inputs_*.db).",
    )
    return parser.parse_args()


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


def upsert_transp_timeseries(
    main: sqlite3.Connection, rows: Iterable[Tuple]
) -> int:
    upsert_sql = """
        INSERT INTO transp_timeseries (
            data_origin_id, shot_number, shot_variant, time_array
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(data_origin_id, shot_number, shot_variant)
        DO UPDATE SET time_array = excluded.time_array
    """
    count = 0
    for row in rows:
        main.execute(upsert_sql, row)
        count += 1
    return count


def fetch_flux_equil(conn: sqlite3.Connection) -> List[Tuple]:
    return conn.execute(
        """
        SELECT data_origin_id, folder_path, transpfile, shot_number, shot_variant, shot_time, active
        FROM data_equil
        """
    ).fetchall()


def ensure_equil(
    main: sqlite3.Connection,
    row: Tuple,
) -> int:
    data_origin_id, folder_path, transpfile, shot_number, shot_variant, shot_time, active = row
    existing = main.execute(
        """
        SELECT id FROM data_equil
        WHERE data_origin_id = ?
          AND shot_number IS ?
          AND shot_variant IS ?
          AND shot_time IS ?
        """,
        (data_origin_id, shot_number, shot_variant, shot_time),
    ).fetchone()
    if existing:
        main.execute(
            """
            UPDATE data_equil
            SET folder_path = ?, transpfile = ?, active = ?
            WHERE id = ?
            """,
            (folder_path, transpfile, active, int(existing[0])),
        )
        return int(existing[0])
    cur = main.execute(
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
            data_origin_id,
            folder_path,
            None,
            None,
            None,
            None,
            transpfile,
            shot_time,
            shot_number,
            shot_variant,
            active,
        ),
    )
    return int(cur.lastrowid)


def ensure_gk_study(
    main: sqlite3.Connection,
    data_equil_id: int,
    gk_code_id: int,
    comment: str,
) -> int:
    existing = main.execute(
        """
        SELECT id FROM gk_study
        WHERE data_equil_id = ? AND gk_code_id = ?
        """,
        (data_equil_id, gk_code_id),
    ).fetchone()
    if existing:
        main.execute(
            "UPDATE gk_study SET comment = ? WHERE id = ?",
            (comment, int(existing[0])),
        )
        return int(existing[0])
    cur = main.execute(
        """
        INSERT INTO gk_study (data_equil_id, gk_code_id, comment)
        VALUES (?, ?, ?)
        """,
        (data_equil_id, gk_code_id, comment),
    )
    return int(cur.lastrowid)


def upsert_gk_input(
    main: sqlite3.Connection,
    row: Tuple,
    study_id_map: Dict[int, int],
) -> None:
    (
        flux_gk_study_id,
        gk_model_id,
        file_name,
        file_path,
        content,
        psin,
        status,
        comment,
    ) = row
    content_str = str(content or "").strip()
    if not content_str:
        status = "ERROR"
    main_gk_study_id = study_id_map.get(int(flux_gk_study_id))
    if not main_gk_study_id:
        return
    main.execute(
        """
        INSERT INTO gk_input (
            gk_study_id,
            gk_model_id,
            file_name,
            file_path,
            content,
            psin,
            status,
            comment
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(gk_study_id, gk_model_id, psin)
        DO UPDATE SET
            file_name = excluded.file_name,
            file_path = excluded.file_path,
            content = excluded.content,
            status = excluded.status,
            comment = excluded.comment
        WHERE gk_input.status IN ('WAIT', 'ERROR')
        """,
        (
            main_gk_study_id,
            gk_model_id,
            file_name,
            file_path,
            content_str,
            psin,
            status,
            comment,
        ),
    )


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.main_db):
        raise SystemExit(f"Main DB not found: {args.main_db}")
    if not os.path.exists(args.flux_db):
        raise SystemExit(f"Flux DB not found: {args.flux_db}")

    flux = sqlite3.connect(args.flux_db)
    main = sqlite3.connect(args.main_db)
    try:
        main.execute("PRAGMA foreign_keys = ON")
        ensure_transp_timeseries(main)

        ts_rows = flux.execute(
            """
            SELECT data_origin_id, shot_number, shot_variant, time_array
            FROM transp_timeseries
            """
        ).fetchall()
        ts_count = upsert_transp_timeseries(main, ts_rows)

        equil_rows = fetch_flux_equil(flux)
        equil_count = 0
        equil_id_map: Dict[int, int] = {}
        for row in equil_rows:
            main_equil_id = ensure_equil(
                main,
                row,
            )
            flux_equil_id = flux.execute(
                """
                SELECT id FROM data_equil
                WHERE data_origin_id = ?
                  AND shot_number IS ?
                  AND shot_variant IS ?
                  AND shot_time IS ?
                """,
                (row[0], row[3], row[4], row[5]),
            ).fetchone()
            if flux_equil_id:
                equil_id_map[int(flux_equil_id[0])] = main_equil_id
            equil_count += 1

        flux_studies = flux.execute(
            """
            SELECT id, data_equil_id, gk_code_id, comment
            FROM gk_study
            """
        ).fetchall()
        study_id_map: Dict[int, int] = {}
        for flux_id, flux_equil_id, gk_code_id, comment in flux_studies:
            main_equil_id = equil_id_map.get(int(flux_equil_id))
            if not main_equil_id:
                continue
            main_study_id = ensure_gk_study(
                main, main_equil_id, int(gk_code_id), str(comment)
            )
            study_id_map[int(flux_id)] = main_study_id

        flux_inputs = flux.execute(
            """
            SELECT gk_study_id, gk_model_id, file_name, file_path, content,
                   psin, status, comment
            FROM gk_input
            WHERE status = 'WAIT'
            """
        ).fetchall()
        for row in flux_inputs:
            upsert_gk_input(main, row, study_id_map)

        main.commit()
    finally:
        main.close()
        flux.close()

    print(
        f"Synced data_equil={equil_count}, transp_timeseries={ts_count} "
        f"from {os.path.basename(args.flux_db)}"
    )


if __name__ == "__main__":
    main()
