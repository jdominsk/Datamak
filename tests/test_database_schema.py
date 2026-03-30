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
                "flux_action_log",
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

            data_origin_cols = _column_names(conn, "data_origin")
            self.assertIn("file_type", data_origin_cols)
            self.assertIn("color", data_origin_cols)

            flux_action_log_cols = _column_names(conn, "flux_action_log")
            self.assertTrue(
                {"status", "slurm_job_id", "submitted_at", "synced_at"}.issubset(
                    flux_action_log_cols
                )
            )

            gk_run_cols = _column_names(conn, "gk_run")
            self.assertTrue(
                {"ky_abs_mean", "gamma_max", "diffusion", "t_max"}.issubset(gk_run_cols)
            )

            self.assertEqual(
                _index_columns(conn, "uq_data_equil_transpfile_time"),
                ["data_origin_id", "transpfile", "shot_time"],
            )
            self.assertEqual(
                _index_columns(conn, "uq_gk_model_key"),
                ["gk_code_id", "is_linear", "is_adiabatic", "is_electrostatic", "input_template"],
            )
        finally:
            conn.close()

    def test_create_schema_backfills_data_origin_file_type(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE data_origin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    copy TEXT NOT NULL,
                    color TEXT,
                    tokamak TEXT NOT NULL DEFAULT 'NSTX',
                    creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE data_equil (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_origin_id INTEGER NOT NULL,
                    folder_path TEXT NOT NULL,
                    pfile TEXT,
                    pfile_content TEXT,
                    gfile TEXT,
                    gfile_content TEXT,
                    transpfile TEXT,
                    shot_number TEXT,
                    shot_time REAL,
                    shot_variant TEXT,
                    active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
                    creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                    comment TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO data_origin (name, origin, copy, color)
                VALUES
                    ('Mate Kinetic EFIT', 'Google drive', '/tmp/mate', '#112233'),
                    ('Alexei Transp 09 (semi-auto)', '/p/transparch/result/NSTX/09', '/tmp/transp', '#445566')
                """
            )
            conn.execute(
                """
                INSERT INTO data_equil (data_origin_id, folder_path, pfile, gfile, active)
                VALUES (1, '/tmp/mate/case1', 'p1234', 'g1234', 0)
                """
            )
            conn.execute(
                """
                INSERT INTO data_equil (data_origin_id, folder_path, transpfile, active)
                VALUES (2, '/tmp/transp/case1', '204118A05.CDF', 0)
                """
            )

            create_schema(conn)

            rows = conn.execute(
                "SELECT name, file_type, color FROM data_origin ORDER BY id"
            ).fetchall()
            self.assertEqual(
                [(str(row[0]), str(row[1]), str(row[2])) for row in rows],
                [
                    ("Kinetic EFIT (Mate)", "EFIT", "#112233"),
                    ("Transp 09 (semi-auto)", "TRANSP", "#445566"),
                ],
            )
            table_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'data_origin'"
            ).fetchone()
            self.assertIsNotNone(table_sql)
            self.assertIn(
                "file_type TEXT NOT NULL CHECK (file_type IN ('EFIT', 'TRANSP'))",
                str(table_sql[0]),
            )
            self.assertIn("color TEXT", str(table_sql[0]))
        finally:
            conn.close()

    def test_create_schema_adds_flux_action_log_status_tracking_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE flux_action_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_origin_id INTEGER,
                    data_origin_name TEXT,
                    flux_db_name TEXT NOT NULL,
                    remote_host TEXT NOT NULL,
                    remote_dir TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                INSERT INTO flux_action_log (
                    data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir
                )
                VALUES (4, 'Alexei Transp 10 (full-auto)', 'flux_equil_inputs.db', 'alice@flux', '/u/alice/DTwin/transp_full_auto')
                """
            )

            create_schema(conn)

            columns = _column_names(conn, "flux_action_log")
            self.assertTrue(
                {
                    "status",
                    "slurm_job_id",
                    "status_detail",
                    "submitted_at",
                    "status_checked_at",
                    "synced_at",
                }.issubset(columns)
            )
            row = conn.execute(
                """
                SELECT data_origin_name, status, slurm_job_id, status_detail, submitted_at, status_checked_at, synced_at
                FROM flux_action_log
                WHERE id = 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "Transp 10 (full-auto)")
            self.assertEqual(str(row[1]), "STAGED")
            self.assertIsNone(row[2])
            self.assertIsNone(row[3])
            self.assertIsNone(row[4])
            self.assertIsNone(row[5])
            self.assertIsNone(row[6])
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

    def test_create_schema_deduplicates_gk_model_and_remaps_gk_input(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            create_schema(conn)
            seed_gk_code(conn)
            seed_gk_model(conn)
            conn.execute("DROP INDEX uq_gk_model_key")
            conn.execute(
                """
                INSERT INTO data_origin (name, origin, copy, file_type, tokamak)
                VALUES ('Kinetic EFIT (Mate)', 'Google drive', '/tmp/mate', 'EFIT', 'NSTX')
                """
            )
            conn.execute(
                """
                INSERT INTO data_equil (data_origin_id, folder_path, pfile, gfile, active)
                VALUES (1, '/tmp/mate/case1', 'p1', 'g1', 1)
                """
            )
            conn.execute(
                """
                INSERT INTO gk_study (data_equil_id, gk_code_id, comment)
                VALUES (1, 1, 'demo')
                """
            )
            conn.execute(
                """
                INSERT INTO gk_model (
                    gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, active
                )
                VALUES (1, 1, 1, 1, 'gx_template_miller_linear_adiabe.in', 1)
                """
            )
            duplicate_model_id = int(
                conn.execute("SELECT MAX(id) FROM gk_model").fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO gk_input (
                    gk_study_id, gk_model_id, file_name, file_path, content, psin, status, comment
                )
                VALUES (1, ?, 'input.in', '/tmp/input.in', 'content', 0.5, 'WAIT', '')
                """,
                (duplicate_model_id,),
            )

            create_schema(conn)

            duplicate_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM gk_model
                    WHERE gk_code_id = 1
                      AND is_linear = 1
                      AND is_adiabatic = 1
                      AND is_electrostatic = 1
                      AND input_template = 'gx_template_miller_linear_adiabe.in'
                    """
                ).fetchone()[0]
            )
            remapped_model_id = int(
                conn.execute("SELECT gk_model_id FROM gk_input WHERE id = 1").fetchone()[0]
            )
            self.assertEqual(duplicate_count, 1)
            self.assertEqual(remapped_model_id, 1)
        finally:
            conn.close()

    def test_create_schema_restores_new_status_in_gk_input_constraint(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE gk_study (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_equil_id INTEGER NOT NULL,
                    gk_code_id INTEGER NOT NULL,
                    comment TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_model (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_code_id INTEGER NOT NULL,
                    is_linear INTEGER NOT NULL DEFAULT 1,
                    is_adiabatic INTEGER NOT NULL DEFAULT 0,
                    is_electrostatic INTEGER NOT NULL DEFAULT 0,
                    input_template TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_study_id INTEGER NOT NULL,
                    gk_model_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    content TEXT NOT NULL,
                    psin REAL NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS', 'ERROR')),
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
                    creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gk_input (
                    gk_study_id, gk_model_id, file_name, file_path, content, psin, status, comment
                )
                VALUES (1, 1, 'input.in', '/tmp/input.in', 'content', 0.5, 'WAIT', '')
                """
            )

            create_schema(conn)

            table_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'gk_input'"
            ).fetchone()
            self.assertIsNotNone(table_sql)
            self.assertIn(
                "status TEXT NOT NULL CHECK (status IN ('NEW', 'WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS', 'ERROR'))",
                str(table_sql[0]),
            )
            row = conn.execute(
                "SELECT status, file_name, psin FROM gk_input WHERE id = 1"
            ).fetchone()
            self.assertEqual((str(row[0]), str(row[1]), float(row[2])), ("WAIT", "input.in", 0.5))
        finally:
            conn.close()

    def test_seed_scripts_populate_code_and_model_tables(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            create_schema(conn)
            seed_gk_code(conn)
            seed_gk_model(conn)
            seed_gk_model(conn)

            code_rows = conn.execute("SELECT name FROM gk_code ORDER BY name").fetchall()
            self.assertEqual([str(row[0]) for row in code_rows], ["CGYRO", "GX"])

            model_count = conn.execute("SELECT COUNT(*) FROM gk_model").fetchone()[0]
            self.assertEqual(int(model_count), 5)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
