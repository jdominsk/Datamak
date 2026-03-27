import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from batch.create_batch_database import copy_torun_rows, log_batch_created
from gui.app import app


class BatchWorkflowTests(unittest.TestCase):
    def test_copy_torun_rows_moves_rows_into_batch_and_updates_source_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_db = Path(tmpdir) / "source.db"
            batch_db = Path(tmpdir) / "batch.db"
            self._create_source_gk_input(
                source_db,
                rows=[
                    (1, "input 1", "TORUN"),
                    (2, "input 2", "WAIT"),
                    (3, "input 3", "TORUN"),
                ],
            )

            copied = copy_torun_rows(str(source_db), str(batch_db))
            self.assertEqual(copied, 2)

            with sqlite3.connect(batch_db) as conn:
                rows = conn.execute(
                    "SELECT gk_input_id, input_content, status FROM gk_run ORDER BY gk_input_id"
                ).fetchall()
            self.assertEqual(
                [(int(r[0]), str(r[1]), str(r[2])) for r in rows],
                [(1, "input 1", "TORUN"), (3, "input 3", "TORUN")],
            )

            with sqlite3.connect(source_db) as conn:
                statuses = conn.execute(
                    "SELECT id, status FROM gk_input ORDER BY id"
                ).fetchall()
            self.assertEqual(
                [(int(r[0]), str(r[1])) for r in statuses],
                [(1, "BATCH"), (2, "WAIT"), (3, "BATCH")],
            )

    def test_log_batch_created_inserts_tracking_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_db = Path(tmpdir) / "source.db"
            batch_db = Path(tmpdir) / "batch_database_20260216_120000.db"
            self._create_source_gk_input(source_db, rows=[])

            batch_id = log_batch_created(str(source_db), str(batch_db))
            self.assertGreater(batch_id, 0)

            with sqlite3.connect(source_db) as conn:
                row = conn.execute(
                    """
                    SELECT batch_database_name, remote_folder, status
                    FROM gk_batch
                    WHERE id = ?
                    """,
                    (batch_id,),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), batch_db.name)
            self.assertEqual(str(row[1]), "N/A")
            self.assertEqual(str(row[2]), "CREATED")

    @staticmethod
    def _create_source_gk_input(db_path: Path, rows: list[tuple[int, str, str]]) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY,
                    content TEXT NOT NULL,
                    status TEXT NOT NULL
                )
                """
            )
            if rows:
                conn.executemany(
                    "INSERT INTO gk_input (id, content, status) VALUES (?, ?, ?)",
                    rows,
                )
            conn.commit()


class GuiWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "gui_workflow.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    content TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.executemany(
                "INSERT INTO gk_input (id, status, content) VALUES (?, ?, ?)",
                [
                    (1, "WAIT", "a"),
                    (2, "WAIT", "b"),
                    (3, "SUCCESS", "c"),
                ],
            )
            conn.commit()
        self.client = app.test_client()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_update_status_promotes_wait_row_to_torun_only(self) -> None:
        with mock.patch("gui.app.log_usage"):
            response = self.client.post(
                "/update_status",
                data={
                    "db": str(self.db_path),
                    "table": "gk_input",
                    "row_id": "1",
                    "panel": "tables",
                },
            )
        self.assertEqual(response.status_code, 302)

        with sqlite3.connect(self.db_path) as conn:
            statuses = conn.execute(
                "SELECT id, status FROM gk_input ORDER BY id"
            ).fetchall()
        self.assertEqual(
            [(int(r[0]), str(r[1])) for r in statuses],
            [(1, "TORUN"), (2, "WAIT"), (3, "SUCCESS")],
        )

    def test_update_status_bulk_promotes_only_wait_rows(self) -> None:
        with mock.patch("gui.app.log_usage"):
            response = self.client.post(
                "/update_status_bulk",
                data={
                    "db": str(self.db_path),
                    "row_ids": "1, 2, 3, not-a-number",
                    "panel": "tables",
                },
            )
        self.assertEqual(response.status_code, 302)

        with sqlite3.connect(self.db_path) as conn:
            statuses = conn.execute(
                "SELECT id, status FROM gk_input ORDER BY id"
            ).fetchall()
        self.assertEqual(
            [(int(r[0]), str(r[1])) for r in statuses],
            [(1, "TORUN"), (2, "TORUN"), (3, "SUCCESS")],
        )

    def test_legacy_app2_path_redirects_to_main_app(self) -> None:
        response = self.client.get(f"/app2?db={self.db_path}&panel=overview")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers["Location"],
            f"/?db={self.db_path}&panel=overview",
        )

    def test_equilibria_tab_renders_on_sparse_db(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=equilibria")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Equilibria", body)
        self.assertIn("No `data_origin` rows are available yet.", body)

    def test_equilibria_tab_shows_workflow_status_for_selected_origin(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE data_origin (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    file_type TEXT,
                    tokamak TEXT,
                    origin TEXT,
                    color TEXT
                );
                CREATE TABLE data_equil (
                    id INTEGER PRIMARY KEY,
                    data_origin_id INTEGER NOT NULL,
                    active INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE transp_timeseries (
                    id INTEGER PRIMARY KEY,
                    data_origin_id INTEGER NOT NULL
                );
                CREATE TABLE gk_code (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE gk_model (
                    id INTEGER PRIMARY KEY,
                    gk_code_id INTEGER,
                    is_linear INTEGER,
                    is_adiabatic INTEGER,
                    is_electrostatic INTEGER,
                    input_template TEXT,
                    active INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE flux_action_log (
                    id INTEGER PRIMARY KEY,
                    data_origin_id INTEGER,
                    data_origin_name TEXT,
                    flux_db_name TEXT NOT NULL,
                    remote_host TEXT NOT NULL,
                    remote_dir TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT,
                    slurm_job_id TEXT,
                    status_detail TEXT,
                    status_checked_at TEXT
                );
                """
            )
            conn.execute(
                """
                INSERT INTO data_origin (id, name, file_type, tokamak, origin, color)
                VALUES (4, 'Transp 10 (full-auto)', 'TRANSP', 'NSTX', '/p/transparch/result/NSTX/10', '#ff7f0e')
                """
            )
            conn.executemany(
                "INSERT INTO data_equil (id, data_origin_id, active) VALUES (?, ?, ?)",
                [(1, 4, 1), (2, 4, 1)],
            )
            conn.executemany(
                "INSERT INTO transp_timeseries (id, data_origin_id) VALUES (?, ?)",
                [(1, 4), (2, 4)],
            )
            conn.execute("INSERT INTO gk_code (id, name) VALUES (1, 'GX')")
            conn.executemany(
                """
                INSERT INTO gk_model (
                    id, gk_code_id, is_linear, is_adiabatic,
                    is_electrostatic, input_template, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (1, 1, 1, 1, 1, "templates/gx_template_linear.in", 1),
                    (2, 1, 0, 0, 1, "templates/gx_template_nonlinear.in", 0),
                ],
            )
            conn.execute(
                """
                INSERT INTO flux_action_log (
                    id, data_origin_id, data_origin_name, flux_db_name,
                    remote_host, remote_dir, created_at, status, slurm_job_id,
                    status_detail, status_checked_at
                ) VALUES (
                    1, 4, 'Transp 10 (full-auto)', 'flux_equil_inputs_demo.db',
                    'jdominsk@flux-login2', '/u/jdominsk/DTwin/transp_full_auto',
                    '2026-03-27 12:00:00', 'SYNCED', '2004386;flux',
                    'SYNCED', '2026-03-27 13:12:11'
                )
                """
            )
            conn.commit()

        response = self.client.get(
            f"/?db={self.db_path}&panel=equilibria&origin_id=4"
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Workflow Status", body)
        self.assertIn("Launch Batch of Simulations:", body)
        self.assertIn("No equil+plasma sampling data available.", body)
        self.assertIn("Flux workflow", body)
        self.assertIn("data_equil", body)
        self.assertIn("transp_timeseries", body)
        self.assertIn("gk_study", body)
        self.assertIn("gk_model", body)
        self.assertIn("gk_input", body)
        self.assertIn("gk_batch", body)
        self.assertIn("gk_run", body)
        self.assertIn(
            "Flux sync completed, but this origin still has no gk_study rows in the main DB.",
            body,
        )

    def test_save_hpc_config_writes_user_local_config(self) -> None:
        with tempfile.TemporaryDirectory() as cfgdir:
            config_path = Path(cfgdir) / "datamak-config.json"
            legacy_path = Path(cfgdir) / "legacy-hpc-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "sources": {
                            "mate_root": "/data/mate",
                            "transp_copy_root_09": "/data/transp09",
                            "transp_copy_root_10": "/data/transp10",
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                "os.environ",
                {
                    "DTWIN_CONFIG": str(config_path),
                    "USER": "alice",
                },
                clear=False,
            ), mock.patch(
                "dtwin_config.get_legacy_config_path",
                return_value=legacy_path,
            ), mock.patch("gui.app.log_usage"):
                response = self.client.post(
                    "/save_hpc_config",
                    data={
                        "db": str(self.db_path),
                        "ssh_user": "operator",
                        "ssh_host": "perlmutter.example.org",
                        "ssh_identity": "~/.ssh/operator_id",
                        "ssh_control_path": "/tmp/datamak-control",
                        "ssh_control_persist": "15m",
                        "ssh_connect_timeout": "25",
                        "monitor_timeout": "180",
                        "perlmutter_base_dir": "/pscratch/sd/o/operator/DTwin",
                        "perlmutter_batch_dir": "/pscratch/sd/o/operator/DTwin/newbatch",
                        "gx_path": "/global/homes/o/operator/GX/custom",
                        "flux_user": "fluxuser",
                        "flux_host": "flux.example.org",
                        "flux_base_dir": "/u/fluxuser/DTwin/transp_full_auto",
                        "flux_python_bin": "/u/fluxuser/envs/pyro/bin/python",
                    },
                )
            self.assertEqual(response.status_code, 302)
            self.assertTrue(config_path.exists())
            self.assertFalse(legacy_path.exists())

            payload = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["perlmutter"]["user"], "operator")
            self.assertEqual(payload["perlmutter"]["host"], "perlmutter.example.org")
            self.assertEqual(payload["perlmutter"]["connect_timeout"], 25)
            self.assertEqual(
                payload["perlmutter"]["batch_dir"],
                "/pscratch/sd/o/operator/DTwin/newbatch",
            )
            self.assertEqual(payload["flux"]["user"], "fluxuser")
            self.assertEqual(
                payload["flux"]["python_bin"],
                "/u/fluxuser/envs/pyro/bin/python",
            )
            self.assertEqual(payload["sources"]["mate_root"], "/data/mate")
            self.assertEqual(payload["sources"]["transp_copy_root_09"], "/data/transp09")
            self.assertEqual(payload["sources"]["transp_copy_root_10"], "/data/transp10")


if __name__ == "__main__":
    unittest.main()
