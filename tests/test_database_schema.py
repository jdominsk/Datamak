import sqlite3
import unittest

from database.create_gyrokinetic_db import create_schema, seed_gk_code, seed_gk_model


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _index_columns(conn: sqlite3.Connection, index_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA index_info({index_name})").fetchall()
    return [str(row[2]) for row in rows]


class DatabaseSchemaTests(unittest.TestCase):
    def test_create_schema_creates_expected_tables_and_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            create_schema(conn)
            tables = _table_names(conn)
            expected_tables = {
                "data_origin",
                "data_equil",
                "gk_code",
                "gk_model",
                "gk_study",
                "gk_input",
                "gk_batch",
                "gk_run",
                "gk_linear_run",
                "gk_nonlinear_run",
                "gk_convergence_timeseries",
            }
            self.assertTrue(expected_tables.issubset(tables))

            data_equil_cols = _column_names(conn, "data_equil")
            self.assertIn("shot_time", data_equil_cols)
            self.assertNotIn("time", data_equil_cols)

            gk_run_cols = _column_names(conn, "gk_run")
            self.assertTrue(
                {"ky_abs_mean", "gamma_max", "diffusion", "t_max"}.issubset(gk_run_cols)
            )

            self.assertEqual(
                _index_columns(conn, "uq_data_equil_transpfile_time"),
                ["data_origin_id", "transpfile", "shot_time"],
            )
        finally:
            conn.close()

    def test_create_schema_is_idempotent(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            create_schema(conn)
            create_schema(conn)
            self.assertEqual(
                _index_columns(conn, "uq_gk_input_key"),
                ["gk_study_id", "gk_model_id", "psin"],
            )
        finally:
            conn.close()

    def test_seed_scripts_populate_code_and_model_tables(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            create_schema(conn)
            seed_gk_code(conn)
            seed_gk_model(conn)

            code_rows = conn.execute("SELECT name FROM gk_code ORDER BY name").fetchall()
            self.assertEqual([str(row[0]) for row in code_rows], ["CGYRO", "GX"])

            model_count = conn.execute("SELECT COUNT(*) FROM gk_model").fetchone()[0]
            self.assertGreaterEqual(int(model_count), 5)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
