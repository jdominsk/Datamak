#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_update.Transp_full_auto.build_flux_equil_inputs import canonicalize_flux_gk_models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean a Flux full-auto temp DB by removing duplicate gk_model rows and merging duplicate gk_input rows."
    )
    parser.add_argument(
        "--flux-db",
        required=True,
        help="Path to flux_equil_inputs_*.db",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after cleanup.",
    )
    return parser.parse_args()


def count_duplicate_model_signatures(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, COUNT(*) AS c
            FROM gk_model
            GROUP BY gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template
            HAVING c > 1
        )
        """
    ).fetchone()
    return int(row[0] or 0)


def count_duplicate_input_signature_groups(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT gi.gk_study_id,
                   gm.gk_code_id,
                   gm.is_linear,
                   gm.is_adiabatic,
                   gm.is_electrostatic,
                   gm.input_template,
                   gi.psin,
                   COUNT(*) AS c
            FROM gk_input AS gi
            JOIN gk_model AS gm ON gm.id = gi.gk_model_id
            GROUP BY gi.gk_study_id,
                     gm.gk_code_id,
                     gm.is_linear,
                     gm.is_adiabatic,
                     gm.is_electrostatic,
                     gm.input_template,
                     gi.psin
            HAVING c > 1
        )
        """
    ).fetchone()
    return int(row[0] or 0)


def main() -> None:
    args = parse_args()
    db_path = Path(args.flux_db).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"Flux DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        before_models = int(
            conn.execute("SELECT COUNT(*) FROM gk_model").fetchone()[0] or 0
        )
        before_dupe_models = count_duplicate_model_signatures(conn)
        before_dupe_inputs = count_duplicate_input_signature_groups(conn)

        remapped_models = canonicalize_flux_gk_models(conn)
        conn.commit()

        if args.vacuum:
            conn.execute("VACUUM")

        after_models = int(
            conn.execute("SELECT COUNT(*) FROM gk_model").fetchone()[0] or 0
        )
        after_dupe_models = count_duplicate_model_signatures(conn)
        after_dupe_inputs = count_duplicate_input_signature_groups(conn)

        print(f"Cleaned {db_path.name}")
        print(f"gk_model rows: {before_models} -> {after_models}")
        print(
            f"duplicate gk_model signatures: {before_dupe_models} -> {after_dupe_models}"
        )
        print(
            f"duplicate gk_input signature groups: {before_dupe_inputs} -> {after_dupe_inputs}"
        )
        print(f"duplicate model ids removed: {remapped_models}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
