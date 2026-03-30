#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import random
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_MAIN_DB = os.path.join(
    os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
    "gyrokinetic_simulations.db",
)
DEFAULT_OUT_DIR = os.path.join(
    os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
    "tmp",
    "transp_full_auto",
)

TABLES_TO_COPY = ["data_origin", "gk_code", "gk_model"]
TABLES_TO_CREATE = ["data_equil", "gk_study", "gk_input"]
GK_INPUT_STATUS_WITH_NEW_SQL = (
    "status in ('new', 'wait', 'torun', 'batch', 'crashed', 'success', 'error')"
)


def _gk_model_signature(
    row: Tuple[object, ...],
) -> Tuple[int, int, int, int, str]:
    return (
        int(row[1]),
        int(row[2]),
        int(row[3]),
        int(row[4]),
        str(row[5] or ""),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a Flux temp DB for full-auto TRANSP processing."
    )
    parser.add_argument("--db", default=DEFAULT_MAIN_DB, help="Main DB path.")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output folder.")
    parser.add_argument(
        "--flux-db",
        default="",
        help="Use an existing flux_equil_inputs DB instead of creating one.",
    )
    parser.add_argument(
        "--create-gk-inputs",
        action="store_true",
        help="Create gk_input rows from existing gk_study rows.",
    )
    parser.add_argument(
        "--origin-name",
        default="",
        help="data_origin.name to filter gk_study rows.",
    )
    parser.add_argument(
        "--origin-id",
        type=int,
        help="data_origin.id to filter rows. Preferred over --origin-name.",
    )
    parser.add_argument(
        "--remote-path",
        default="",
        help="Override data_origin.origin to locate CDF files on Flux.",
    )
    parser.add_argument(
        "--populate-equil",
        action="store_true",
        help="Populate data_equil and transp_timeseries from CDFs on Flux.",
    )
    parser.add_argument(
        "--create-studies",
        action="store_true",
        help="Create gk_study rows for active data_equil and active gk_model.",
    )
    parser.add_argument("--psin-start", type=float, default=0.1)
    parser.add_argument("--psin-end", type=float, default=0.9)
    parser.add_argument("--psin-step", type=float, default=0.1)
    parser.add_argument(
        "--status",
        default="NEW",
        help="Status for newly created gk_input rows (default: NEW).",
    )
    return parser.parse_args()


def fetch_table_sql(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not row or not row[0]:
        raise SystemExit(f"Table {table} not found in main DB.")
    return str(row[0])


def _normalize_sql(sql: Optional[str]) -> str:
    if not sql:
        return ""
    return " ".join(str(sql).lower().split())


def _gk_input_sql(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'gk_input'"
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def _rebuild_gk_input_with_new_status(conn: sqlite3.Connection) -> None:
    columns = [str(row[1]) for row in conn.execute("PRAGMA table_info(gk_input)").fetchall()]
    target_columns = [
        "id",
        "gk_study_id",
        "gk_model_id",
        "file_name",
        "file_path",
        "content",
        "psin",
        "status",
        "comment",
        "geo_option",
        "rhoc",
        "Rmaj",
        "R_geo",
        "qinp",
        "shat",
        "shift",
        "akappa",
        "akappri",
        "tri",
        "tripri",
        "betaprim",
        "beta",
        "electron_z",
        "electron_mass",
        "electron_dens",
        "electron_temp",
        "electron_temp_ev",
        "electron_tprim",
        "electron_fprim",
        "electron_vnewk",
        "ion_z",
        "ion_mass",
        "ion_dens",
        "ion_temp",
        "ion_temp_ev",
        "ion_tprim",
        "ion_fprim",
        "ion_vnewk",
        "creation_date",
    ]
    extra_columns = sorted(column for column in columns if column not in target_columns)
    if extra_columns:
        raise SystemExit(
            "Refusing to rebuild gk_input because it contains unexpected columns: "
            + ", ".join(extra_columns)
        )

    table_sql = """
        CREATE TABLE gk_input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_study_id INTEGER NOT NULL,
            gk_model_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            content TEXT NOT NULL,
            psin REAL NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('NEW', 'WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS', 'ERROR')),
            comment TEXT NOT NULL DEFAULT '',
            geo_option TEXT,
            rhoc REAL,
            Rmaj REAL,
            R_geo REAL,
            qinp REAL,
            shat REAL,
            shift REAL,
            akappa REAL,
            akappri REAL,
            tri REAL,
            tripri REAL,
            betaprim REAL,
            beta REAL,
            electron_z REAL,
            electron_mass REAL,
            electron_dens REAL,
            electron_temp REAL,
            electron_temp_ev REAL,
            electron_tprim REAL,
            electron_fprim REAL,
            electron_vnewk REAL,
            ion_z REAL,
            ion_mass REAL,
            ion_dens REAL,
            ion_temp REAL,
            ion_temp_ev REAL,
            ion_tprim REAL,
            ion_fprim REAL,
            ion_vnewk REAL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE,
            FOREIGN KEY (gk_model_id) REFERENCES gk_model(id)
        )
    """
    joined_columns = ", ".join(target_columns)

    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute("BEGIN")
        conn.execute("ALTER TABLE gk_input RENAME TO gk_input__old")
        conn.execute(table_sql)
        conn.execute(
            f"""
            INSERT INTO gk_input ({joined_columns})
            SELECT {joined_columns}
            FROM gk_input__old
            """
        )
        conn.execute("DROP TABLE gk_input__old")
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def ensure_gk_input_status_allows_new(conn: sqlite3.Connection) -> None:
    gk_input_sql = _normalize_sql(_gk_input_sql(conn))
    if gk_input_sql and GK_INPUT_STATUS_WITH_NEW_SQL not in gk_input_sql:
        _rebuild_gk_input_with_new_status(conn)


def _prefer_source_gk_input_row(target: sqlite3.Row, source: sqlite3.Row) -> bool:
    target_content = str(target["content"] or "").strip()
    source_content = str(source["content"] or "").strip()
    if source_content and not target_content:
        return True
    target_status = str(target["status"] or "").strip().upper()
    source_status = str(source["status"] or "").strip().upper()
    if target_status == "NEW" and source_status != "NEW":
        return True
    if not str(target["comment"] or "").strip() and str(source["comment"] or "").strip():
        return True
    return False


def canonicalize_flux_gk_models(conn: sqlite3.Connection) -> int:
    model_rows = conn.execute(
        """
        SELECT id, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template
        FROM gk_model
        ORDER BY id
        """
    ).fetchall()
    canonical_by_signature: Dict[Tuple[int, int, int, int, str], int] = {}
    remapped = 0
    for model_row in model_rows:
        model_id = int(model_row[0])
        signature = _gk_model_signature(model_row)
        canonical_id = canonical_by_signature.get(signature)
        if canonical_id is None:
            canonical_by_signature[signature] = model_id
            continue

        duplicate_inputs = conn.execute(
            """
            SELECT id, gk_study_id, psin, file_name, file_path, content, status, comment
            FROM gk_input
            WHERE gk_model_id = ?
            ORDER BY id
            """,
            (model_id,),
        ).fetchall()
        for source in duplicate_inputs:
            target = conn.execute(
                """
                SELECT id, file_name, file_path, content, status, comment
                FROM gk_input
                WHERE gk_model_id = ?
                  AND gk_study_id = ?
                  AND psin = ?
                LIMIT 1
                """,
                (canonical_id, int(source["gk_study_id"]), float(source["psin"])),
            ).fetchone()
            if target is None:
                conn.execute(
                    "UPDATE gk_input SET gk_model_id = ? WHERE id = ?",
                    (canonical_id, int(source["id"])),
                )
                continue
            if _prefer_source_gk_input_row(target, source):
                conn.execute(
                    """
                    UPDATE gk_input
                    SET file_name = ?, file_path = ?, content = ?, status = ?, comment = ?
                    WHERE id = ?
                    """,
                    (
                        str(source["file_name"] or ""),
                        str(source["file_path"] or ""),
                        str(source["content"] or ""),
                        str(source["status"] or ""),
                        str(source["comment"] or ""),
                        int(target["id"]),
                    ),
                )
            conn.execute("DELETE FROM gk_input WHERE id = ?", (int(source["id"]),))

        conn.execute("DELETE FROM gk_model WHERE id = ?", (model_id,))
        remapped += 1
    return remapped


def copy_table_data(conn_src: sqlite3.Connection, conn_dst: sqlite3.Connection, table: str) -> int:
    columns = [row[1] for row in conn_src.execute(f"PRAGMA table_info({table})")]
    if not columns:
        return 0
    cols_csv = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    rows = conn_src.execute(f"SELECT {cols_csv} FROM {table}").fetchall()
    if not rows:
        return 0
    conn_dst.executemany(
        f"INSERT INTO {table} ({cols_csv}) VALUES ({placeholders})",
        rows,
    )
    return len(rows)


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


def resolve_origin(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    origin_name: str,
) -> Tuple[int, str, str]:
    if origin_id is not None:
        row = conn.execute(
            "SELECT id, name, origin FROM data_origin WHERE id = ?",
            (origin_id,),
        ).fetchone()
        if row:
            return int(row[0]), str(row[1] or ""), str(row[2] or "")
    if origin_name.strip():
        row = conn.execute(
            "SELECT id, name, origin FROM data_origin WHERE name = ?",
            (origin_name.strip(),),
        ).fetchone()
        if row:
            return int(row[0]), str(row[1] or ""), str(row[2] or "")
    if origin_id is not None:
        raise SystemExit(f"data_origin not found for id={origin_id}")
    raise SystemExit(f"data_origin not found for {origin_name}")


def psin_values(start: float, end: float, step: float) -> List[float]:
    values: List[float] = []
    current = start
    while current <= end + 1e-9:
        values.append(round(current, 10))
        current += step
    return values


def fetch_active_models(conn: sqlite3.Connection) -> List[Dict[str, object]]:
    rows = conn.execute(
        """
        SELECT MIN(id) AS id, input_template
        FROM gk_model
        WHERE active = 1
        GROUP BY gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template
        ORDER BY MIN(id)
        """
    ).fetchall()
    if not rows:
        raise SystemExit("No active gk_model rows found.")
    return [{"id": int(row[0]), "input_template": str(row[1])} for row in rows]


def fetch_active_studies(
    conn: sqlite3.Connection, origin_id: int
) -> List[int]:
    rows = conn.execute(
        """
        SELECT gs.id
        FROM gk_study AS gs
        JOIN data_equil AS de ON de.id = gs.data_equil_id
        WHERE de.active = 1
          AND de.data_origin_id = ?
        """,
        (origin_id,),
    ).fetchall()
    return [int(row[0]) for row in rows]


def create_gk_studies(conn: sqlite3.Connection, origin_id: int) -> None:
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
        JOIN gk_model AS gm ON gm.active = 1
        LEFT JOIN gk_study AS gs
            ON gs.data_equil_id = de.id
            AND gs.gk_code_id = ?
        WHERE de.active = 1
          AND de.data_origin_id = ?
          AND gs.id IS NULL
        """,
        (gk_code_id_val, gk_code_id_val, origin_id),
    )


def create_gk_inputs(
    conn: sqlite3.Connection,
    origin_id: int,
    psins: List[float],
    status: str,
) -> int:
    models = fetch_active_models(conn)
    study_ids = fetch_active_studies(conn, origin_id)
    if not study_ids:
        raise SystemExit("No active gk_study rows found for this origin.")
    existing = {
        (row[0], row[1], float(row[2]))
        for row in conn.execute(
            "SELECT gk_study_id, gk_model_id, psin FROM gk_input"
        ).fetchall()
    }
    to_insert: List[Tuple] = []
    for study_id in study_ids:
        for model in models:
            for psin in psins:
                key = (study_id, model["id"], psin)
                if key in existing:
                    continue
                to_insert.append(
                    (
                        study_id,
                        model["id"],
                        "",
                        "",
                        "",
                        psin,
                        status,
                    )
                )
    if to_insert:
        conn.executemany(
            """
            INSERT INTO gk_input (
                gk_study_id,
                gk_model_id,
                file_name,
                file_path,
                content,
                psin,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            to_insert,
        )
    return len(to_insert)


def populate_equil_and_timeseries(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    origin_name: str,
    remote_path: str,
    create_studies: bool,
) -> Tuple[int, int, int]:
    ensure_transp_timeseries(conn)
    origin_id_val, _, origin_path = resolve_origin(conn, origin_id, origin_name)
    resolved_path = remote_path or origin_path
    if not resolved_path:
        raise SystemExit("remote_path is empty; provide --remote-path.")
    files = list_cdf_files(resolved_path)
    if not files:
        raise SystemExit(f"No .CDF files found in {resolved_path}")

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
        SELECT id, transpfile, shot_number, shot_variant, shot_time
        FROM data_equil
        WHERE data_origin_id = ?
        """,
        (origin_id_val,),
    ).fetchall()
    equil_by_key: Dict[Tuple[str, Optional[str]], List[Tuple[int, Optional[float]]]] = {}
    processed_transpfiles = set()
    for row_id, transpfile, shot_number, shot_variant, shot_time in existing_equil_rows:
        key = (str(shot_number), shot_variant)
        equil_by_key.setdefault(key, []).append((int(row_id), shot_time))
        if transpfile and shot_time is not None:
            ts_key = (origin_id_val, str(shot_number), shot_variant)
            if ts_key in existing_ts:
                processed_transpfiles.add(str(transpfile))

    inserted_equil = 0
    inserted_ts = 0
    skipped_files = 0

    total_files = len(files)
    for idx, name in enumerate(files, start=1):
        print(f"\rReading CDF [{idx}/{total_files}]: {name}", end="", flush=True)
        if name in processed_transpfiles:
            continue
        shot_number, shot_variant = parse_transpfile(name)
        if not shot_number:
            continue
        key = (shot_number, shot_variant)
        existing = equil_by_key.get(key, [])
        equil_id: Optional[int] = None
        equil_has_time = False
        created_equil_this_file = False
        for row_id, shot_time in existing:
            equil_id = row_id
            equil_has_time = shot_time is not None
            if shot_time is None:
                break
        if equil_id is None:
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
                    origin_id_val,
                    resolved_path,
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
            equil_id = int(cursor.lastrowid)
            inserted_equil += 1
            created_equil_this_file = True
            equil_by_key.setdefault(key, []).append((equil_id, None))

        ts_key = (origin_id_val, shot_number, shot_variant)
        time_array: List[float] = []
        if ts_key not in existing_ts:
            try:
                time_array = read_time_array(os.path.join(resolved_path, name))
            except Exception as exc:
                skipped_files += 1
                print(
                    f"\nWarning: skipping {name} because TIME3 could not be read: {exc}",
                    flush=True,
                )
                if created_equil_this_file and equil_id is not None:
                    conn.execute("DELETE FROM data_equil WHERE id = ?", (equil_id,))
                    inserted_equil -= 1
                    remaining_rows = [
                        (row_id, shot_time)
                        for row_id, shot_time in equil_by_key.get(key, [])
                        if row_id != equil_id
                    ]
                    if remaining_rows:
                        equil_by_key[key] = remaining_rows
                    else:
                        equil_by_key.pop(key, None)
                continue
            conn.execute(
                """
                INSERT INTO transp_timeseries (
                    data_origin_id, shot_number, shot_variant, time_array
                )
                VALUES (?, ?, ?, ?)
                """,
                (origin_id_val, shot_number, shot_variant, json.dumps(time_array)),
            )
            existing_ts.add(ts_key)
            inserted_ts += 1
        elif not equil_has_time:
            row = conn.execute(
                """
                SELECT time_array
                FROM transp_timeseries
                WHERE data_origin_id = ?
                  AND shot_number = ?
                  AND shot_variant IS ?
                """,
                (origin_id_val, shot_number, shot_variant),
            ).fetchone()
            if row and row[0]:
                time_array = json.loads(row[0])

        if time_array and not equil_has_time and equil_id is not None:
            selected_time = random.choice(time_array)
            conn.execute(
                """
                UPDATE data_equil
                SET shot_time = ?, active = 1
                WHERE id = ?
                """,
                (selected_time, equil_id),
            )
        if idx % 100 == 0:
            conn.commit()

    if files:
        print()
    if create_studies:
        create_gk_studies(conn, origin_id_val)
    return inserted_equil, inserted_ts, skipped_files


def main() -> None:
    args = parse_args()
    if args.create_gk_inputs and args.origin_id is None and not args.origin_name:
        raise SystemExit("--origin-id or --origin-name is required with --create-gk-inputs")
    if args.populate_equil and args.origin_id is None and not args.origin_name:
        raise SystemExit("--origin-id or --origin-name is required with --populate-equil")
    if args.create_studies and args.origin_id is None and not args.origin_name:
        raise SystemExit("--origin-id or --origin-name is required with --create-studies")
    if args.populate_equil and not args.flux_db:
        raise SystemExit("--populate-equil requires --flux-db")

    if args.flux_db:
        if not os.path.exists(args.flux_db):
            raise SystemExit(f"Flux DB not found: {args.flux_db}")
        dst = sqlite3.connect(args.flux_db)
        dst.row_factory = sqlite3.Row
        try:
            dst.execute("PRAGMA foreign_keys = ON")
            ensure_gk_input_status_allows_new(dst)
            remapped_models = canonicalize_flux_gk_models(dst)
            if remapped_models:
                dst.commit()
                print(
                    f"Canonicalized {remapped_models} duplicate gk_model rows in {args.flux_db}"
                )
            if args.populate_equil:
                inserted_equil, inserted_ts, skipped_files = populate_equil_and_timeseries(
                    dst,
                    args.origin_id,
                    args.origin_name,
                    args.remote_path,
                    args.create_studies,
                )
                dst.commit()
                print(
                    f"Inserted data_equil={inserted_equil}, "
                    f"transp_timeseries={inserted_ts}, skipped={skipped_files} "
                    f"for {args.origin_name or f'id={args.origin_id}'}"
                )
            if args.create_gk_inputs:
                origin_id_val, _, _ = resolve_origin(dst, args.origin_id, args.origin_name)
                psins = psin_values(args.psin_start, args.psin_end, args.psin_step)
                created = create_gk_inputs(dst, origin_id_val, psins, args.status)
                dst.commit()
                print(f"Created {created} gk_input rows in {args.flux_db}")
        finally:
            dst.close()
        return

    if not os.path.exists(args.db):
        raise SystemExit(f"Main DB not found: {args.db}")
    os.makedirs(args.out_dir, exist_ok=True)

    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_db = os.path.join(args.out_dir, f"flux_equil_inputs_{timestamp}.db")

    src = sqlite3.connect(args.db)
    dst = sqlite3.connect(out_db)
    try:
        dst.row_factory = sqlite3.Row
        dst.execute("PRAGMA foreign_keys = ON")
        for table in TABLES_TO_COPY + TABLES_TO_CREATE:
            dst.execute(fetch_table_sql(src, table))
        ensure_transp_timeseries(dst)
        ensure_gk_input_status_allows_new(dst)
        total = 0
        for table in TABLES_TO_COPY:
            total += copy_table_data(src, dst, table)
        remapped_models = canonicalize_flux_gk_models(dst)
        if remapped_models:
            print(f"Canonicalized {remapped_models} duplicate gk_model rows in {out_db}")
        if args.create_gk_inputs:
            origin_id_val, _, _ = resolve_origin(dst, args.origin_id, args.origin_name)
            psins = psin_values(args.psin_start, args.psin_end, args.psin_step)
            created = create_gk_inputs(dst, origin_id_val, psins, args.status)
            print(f"Created {created} gk_input rows in {out_db}")
        dst.commit()
    finally:
        dst.close()
        src.close()
    print(f"Wrote Flux DB: {out_db} (copied {total} rows)")


if __name__ == "__main__":
    main()
