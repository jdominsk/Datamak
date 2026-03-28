#!/usr/bin/env python3
import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DB = ROOT_DIR / "gyrokinetic_simulations.db"
DEFAULT_DEST_DB = ROOT_DIR / "demo" / "gyrokinetic_simulations_demo.db"
EXCLUDED_TABLES = {"transp_timeseries"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a smaller Datamak demo database containing only rows related "
            "to the selected data_origin ids."
        )
    )
    parser.add_argument(
        "--source-db",
        default=str(DEFAULT_SOURCE_DB),
        help="Path to the main Datamak SQLite database.",
    )
    parser.add_argument(
        "--dest-db",
        default=str(DEFAULT_DEST_DB),
        help="Path to the demo SQLite database to create.",
    )
    parser.add_argument(
        "--origin-ids",
        default="1,3",
        help="Comma-separated data_origin ids to keep (default: 1,3).",
    )
    return parser.parse_args()


def _parse_origin_ids(raw: str) -> list[int]:
    values: list[int] = []
    for item in raw.split(","):
        token = item.strip()
        if not token:
            continue
        try:
            values.append(int(token))
        except ValueError as exc:
            raise SystemExit(f"Invalid origin id: {token}") from exc
    if not values:
        raise SystemExit("At least one origin id is required.")
    return sorted(set(values))


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _create_schema(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    excluded_tables: set[str],
) -> None:
    table_rows = src.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
          AND sql IS NOT NULL
        ORDER BY name
        """
    ).fetchall()
    for name, sql in table_rows:
        if str(name) in excluded_tables:
            continue
        dst.execute(str(sql))


def _create_indexes_and_triggers(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    excluded_tables: set[str],
) -> None:
    rows = src.execute(
        """
        SELECT tbl_name, sql
        FROM sqlite_master
        WHERE type IN ('index', 'trigger')
          AND name NOT LIKE 'sqlite_%'
          AND sql IS NOT NULL
        ORDER BY type, name
        """
    ).fetchall()
    for table_name, sql in rows:
        if str(table_name or "") in excluded_tables:
            continue
        dst.execute(str(sql))


def _fetch_ids(
    conn: sqlite3.Connection,
    query: str,
    params: Sequence[object] = (),
) -> list[int]:
    return [int(row[0]) for row in conn.execute(query, params).fetchall() if row[0] is not None]


def _build_in_clause(values: Sequence[int]) -> tuple[str, list[object]]:
    if not values:
        return "(NULL)", []
    return "(" + ", ".join("?" for _ in values) + ")", list(values)


def _copy_rows(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    where_clause: str = "",
    params: Sequence[object] = (),
) -> int:
    columns = [str(row[1]) for row in src.execute(f"PRAGMA table_info({table})").fetchall()]
    if not columns:
        return 0
    cols_csv = ", ".join(columns)
    query = f"SELECT {cols_csv} FROM {table}"
    if where_clause:
        query += f" WHERE {where_clause}"
    rows = src.execute(query, params).fetchall()
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in columns)
    dst.executemany(
        f"INSERT INTO {table} ({cols_csv}) VALUES ({placeholders})",
        rows,
    )
    return len(rows)


def _thin_demo_gk_input_ids(
    src: sqlite3.Connection,
    input_ids: Sequence[int],
) -> list[int]:
    if not input_ids:
        return []
    input_clause, input_params = _build_in_clause(input_ids)
    candidate_ids = _fetch_ids(
        src,
        f"""
        SELECT gi.id
        FROM gk_input AS gi
        LEFT JOIN gk_run AS gr
          ON gr.gk_input_id = gi.id
        WHERE gi.id IN {input_clause}
          AND gi.status IN ('WAIT', 'ERROR')
          AND gr.id IS NULL
        ORDER BY gi.id
        """,
        input_params,
    )
    removed_ids = set(candidate_ids[1::2])
    return [input_id for input_id in input_ids if input_id not in removed_ids]


def _update_sqlite_sequence(dst: sqlite3.Connection, tables: Iterable[str]) -> None:
    sqlite_tables = _existing_tables(dst)
    if "sqlite_sequence" not in sqlite_tables:
        return
    for table in tables:
        row = dst.execute(f"SELECT MAX(id) FROM {table}").fetchone()
        max_id = int(row[0]) if row and row[0] is not None else None
        if max_id is None:
            continue
        dst.execute(
            "INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES (?, ?)",
            (table, max_id),
        )


def export_demo_db(source_db: str, dest_db: str, origin_ids: Sequence[int]) -> dict[str, int]:
    source_path = Path(source_db)
    dest_path = Path(dest_db)
    if not source_path.exists():
        raise SystemExit(f"Source DB not found: {source_path}")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()

    with sqlite3.connect(source_path) as src, sqlite3.connect(dest_path) as dst:
        src.execute("PRAGMA foreign_keys = OFF")
        dst.execute("PRAGMA foreign_keys = OFF")

        tables = _existing_tables(src)
        _create_schema(src, dst, EXCLUDED_TABLES)

        origin_clause, origin_params = _build_in_clause(origin_ids)
        origin_names = [
            str(row[0])
            for row in src.execute(
                f"SELECT name FROM data_origin WHERE id IN {origin_clause}",
                origin_params,
            ).fetchall()
        ]
        if not origin_names:
            raise SystemExit("None of the requested data_origin ids were found in the source DB.")

        equil_ids: list[int] = []
        study_ids: list[int] = []
        input_ids: list[int] = []
        run_ids: list[int] = []
        batch_ids: list[int] = []
        surrogate_ids: list[int] = []

        if "data_equil" in tables:
            equil_ids = _fetch_ids(
                src,
                f"SELECT id FROM data_equil WHERE data_origin_id IN {origin_clause}",
                origin_params,
            )
        if "gk_study" in tables and equil_ids:
            equil_clause, equil_params = _build_in_clause(equil_ids)
            study_ids = _fetch_ids(
                src,
                f"SELECT id FROM gk_study WHERE data_equil_id IN {equil_clause}",
                equil_params,
            )
        if "gk_input" in tables and study_ids:
            study_clause, study_params = _build_in_clause(study_ids)
            input_ids = _fetch_ids(
                src,
                f"SELECT id FROM gk_input WHERE gk_study_id IN {study_clause}",
                study_params,
            )
            input_ids = _thin_demo_gk_input_ids(src, input_ids)
        if "gk_run" in tables and input_ids:
            input_clause, input_params = _build_in_clause(input_ids)
            run_ids = _fetch_ids(
                src,
                f"SELECT id FROM gk_run WHERE gk_input_id IN {input_clause}",
                input_params,
            )
            batch_ids = _fetch_ids(
                src,
                f"SELECT DISTINCT gk_batch_id FROM gk_run WHERE gk_input_id IN {input_clause} AND gk_batch_id IS NOT NULL",
                input_params,
            )
        if "gk_surrogate" in tables:
            surrogate_ids.extend(
                _fetch_ids(
                    src,
                    f"SELECT id FROM gk_surrogate WHERE origin_id IN {origin_clause}",
                    origin_params,
                )
            )
        if "sg_estimate" in tables and input_ids:
            input_clause, input_params = _build_in_clause(input_ids)
            surrogate_ids.extend(
                _fetch_ids(
                    src,
                    f"SELECT DISTINCT gk_surrogate_id FROM sg_estimate WHERE gk_input_id IN {input_clause}",
                    input_params,
                )
            )
        surrogate_ids = sorted(set(surrogate_ids))

        copied: dict[str, int] = {}

        copied["data_origin"] = _copy_rows(
            src,
            dst,
            "data_origin",
            f"id IN {origin_clause}",
            origin_params,
        )

        if "gk_code" in tables:
            copied["gk_code"] = _copy_rows(src, dst, "gk_code")
        if "gk_model" in tables:
            copied["gk_model"] = _copy_rows(src, dst, "gk_model")

        if "data_equil" in tables:
            copied["data_equil"] = _copy_rows(
                src,
                dst,
                "data_equil",
                f"data_origin_id IN {origin_clause}",
                origin_params,
            )
        if "transp_timeseries" in tables and "transp_timeseries" not in EXCLUDED_TABLES:
            copied["transp_timeseries"] = _copy_rows(
                src,
                dst,
                "transp_timeseries",
                f"data_origin_id IN {origin_clause}",
                origin_params,
            )
        if "flux_action_log" in tables:
            name_placeholders = ", ".join("?" for _ in origin_names) or "NULL"
            flux_where = f"data_origin_id IN {origin_clause}"
            flux_params: list[object] = list(origin_params)
            if origin_names:
                flux_where += f" OR data_origin_name IN ({name_placeholders})"
                flux_params.extend(origin_names)
            copied["flux_action_log"] = _copy_rows(
                src,
                dst,
                "flux_action_log",
                flux_where,
                flux_params,
            )

        if "gk_study" in tables:
            study_clause, study_params = _build_in_clause(study_ids)
            copied["gk_study"] = _copy_rows(
                src,
                dst,
                "gk_study",
                f"id IN {study_clause}",
                study_params,
            )
        if "gk_input" in tables:
            input_clause, input_params = _build_in_clause(input_ids)
            copied["gk_input"] = _copy_rows(
                src,
                dst,
                "gk_input",
                f"id IN {input_clause}",
                input_params,
            )
        if "gk_batch" in tables:
            batch_clause, batch_params = _build_in_clause(batch_ids)
            copied["gk_batch"] = _copy_rows(
                src,
                dst,
                "gk_batch",
                f"id IN {batch_clause}",
                batch_params,
            )
        if "gk_run" in tables:
            run_clause, run_params = _build_in_clause(run_ids)
            copied["gk_run"] = _copy_rows(
                src,
                dst,
                "gk_run",
                f"id IN {run_clause}",
                run_params,
            )
        if "gk_convergence_timeseries" in tables:
            run_clause, run_params = _build_in_clause(run_ids)
            copied["gk_convergence_timeseries"] = _copy_rows(
                src,
                dst,
                "gk_convergence_timeseries",
                f"gk_run_id IN {run_clause}",
                run_params,
            )
        if "gk_linear_run" in tables:
            study_clause, study_params = _build_in_clause(study_ids)
            copied["gk_linear_run"] = _copy_rows(
                src,
                dst,
                "gk_linear_run",
                f"gk_study_id IN {study_clause}",
                study_params,
            )
        if "gk_nonlinear_run" in tables:
            study_clause, study_params = _build_in_clause(study_ids)
            copied["gk_nonlinear_run"] = _copy_rows(
                src,
                dst,
                "gk_nonlinear_run",
                f"gk_study_id IN {study_clause}",
                study_params,
            )
        if "gk_surrogate" in tables:
            surrogate_clause, surrogate_params = _build_in_clause(surrogate_ids)
            copied["gk_surrogate"] = _copy_rows(
                src,
                dst,
                "gk_surrogate",
                f"id IN {surrogate_clause}",
                surrogate_params,
            )
        if "sg_estimate" in tables:
            input_clause, input_params = _build_in_clause(input_ids)
            copied["sg_estimate"] = _copy_rows(
                src,
                dst,
                "sg_estimate",
                f"gk_input_id IN {input_clause}",
                input_params,
            )

        _create_indexes_and_triggers(src, dst, EXCLUDED_TABLES)
        _update_sqlite_sequence(dst, copied.keys())
        dst.commit()
        dst.execute("PRAGMA foreign_keys = ON")
        fk_errors = dst.execute("PRAGMA foreign_key_check").fetchall()
        if fk_errors:
            raise SystemExit(f"Foreign-key check failed in demo DB: {fk_errors[:5]}")
        integrity = dst.execute("PRAGMA integrity_check").fetchone()
        if not integrity or str(integrity[0]).lower() != "ok":
            raise SystemExit(f"Integrity check failed in demo DB: {integrity}")
        dst.execute("VACUUM")

    return copied


def main() -> None:
    args = parse_args()
    origin_ids = _parse_origin_ids(args.origin_ids)
    copied = export_demo_db(args.source_db, args.dest_db, origin_ids)
    print(f"Wrote demo DB: {args.dest_db}")
    print(f"Origins kept: {', '.join(str(v) for v in origin_ids)}")
    for table in sorted(copied):
        print(f"  {table}: {copied[table]}")


if __name__ == "__main__":
    main()
