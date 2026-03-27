import importlib.util
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gui.app import get_equilibria_origin_actions


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TranspFullAutoGuiActionTests(unittest.TestCase):
    def test_fullauto_origin_actions_use_flux_run_and_sync(self) -> None:
        actions, notes = get_equilibria_origin_actions(
            "Alexei Transp 09 (full-auto) NEW",
            "TRANSP",
        )
        self.assertEqual(
            [(item["title"], item["key"]) for item in actions],
            [
                ("Run On Flux", "run_on_flux"),
                ("Sync Back From Flux", "sync_back_from_flux"),
            ],
        )
        self.assertTrue(notes)
        self.assertIn("reuses an existing staged Flux DB", notes[0])

    def test_fullauto_origin_actions_lock_run_when_latest_flux_job_is_submitted(self) -> None:
        actions, notes = get_equilibria_origin_actions(
            "Alexei Transp 10 (full-auto)",
            "TRANSP",
            {"status": "SUBMITTED", "slurm_job_id": "2004386;flux"},
        )
        self.assertEqual(
            [(item["title"], item["key"]) for item in actions],
            [
                ("Check Flux Status", "check_flux_status"),
                ("Sync Back From Flux", "sync_back_from_flux"),
            ],
        )
        self.assertTrue(notes)
        self.assertIn("already active", notes[0])
        self.assertIn("2004386;flux", notes[0])

    def test_fullauto_origin_actions_show_status_check_when_job_was_previously_submitted(self) -> None:
        actions, _ = get_equilibria_origin_actions(
            "Alexei Transp 10 (full-auto)",
            "TRANSP",
            {"status": "DONE", "slurm_job_id": "2004386;flux"},
        )
        self.assertEqual(
            [(item["title"], item["key"]) for item in actions],
            [
                ("Run On Flux", "run_on_flux"),
                ("Check Flux Status", "check_flux_status"),
                ("Sync Back From Flux", "sync_back_from_flux"),
            ],
        )

    def test_run_on_flux_reuses_existing_logged_db(self) -> None:
        module = load_module(
            "run_on_flux_module",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "run_on_flux.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        origin TEXT,
                        file_type TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name, origin, file_type)
                    VALUES (3, 'Alexei Transp 09 (full-auto) NEW', '/p/transparch/result/NSTX/09', 'TRANSP')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        3,
                        "Alexei Transp 09 (full-auto) NEW",
                        "flux_equil_inputs_existing.db",
                        "alice@flux.example.org",
                        "/u/alice/DTwin/transp_full_auto",
                        "2026-03-27 03:00:00",
                    ),
                )
                conn.commit()

            flux_profile = {
                "user": "alice",
                "host": "flux.example.org",
                "remote": "alice@flux.example.org",
                "base_dir": "/u/alice/DTwin/transp_full_auto",
                "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
            }
            with mock.patch.object(module, "resolve_flux_profile", return_value=flux_profile), mock.patch.object(
                module, "run_stage_step"
            ) as stage_mock, mock.patch.object(module, "sync_runtime_support") as sync_mock, mock.patch.object(
                module, "submit_remote_slurm"
            ) as submit_mock:
                submit_mock.return_value = "2004386;flux"
                result = module.run_for_origin(
                    main_db=str(db_path),
                    origin_id=3,
                    origin_name="",
                    partition="all",
                    walltime="04:00:00",
                    memory="8G",
                )

            stage_mock.assert_not_called()
            sync_mock.assert_called_once_with(
                "alice@flux.example.org",
                "/u/alice/DTwin/transp_full_auto",
                flux_profile,
            )
            submit_mock.assert_called_once_with(
                remote_host="alice@flux.example.org",
                remote_dir="/u/alice/DTwin/transp_full_auto",
                remote_db_path="/u/alice/DTwin/transp_full_auto/flux_equil_inputs_existing.db",
                origin_name="Alexei Transp 09 (full-auto) NEW",
                remote_path="/p/transparch/result/NSTX/09",
                partition="all",
                walltime="04:00:00",
                memory="8G",
                flux_profile=flux_profile,
            )
            self.assertEqual(result["flux_db_name"], "flux_equil_inputs_existing.db")
            self.assertEqual(result["slurm_job_id"], "2004386;flux")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT status, slurm_job_id, submitted_at, synced_at
                    FROM flux_action_log
                    WHERE data_origin_id = 3
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "SUBMITTED")
            self.assertEqual(str(row[1]), "2004386;flux")
            self.assertIsNotNone(row[2])
            self.assertIsNone(row[3])

    def test_run_on_flux_prefers_configured_flux_host_for_stale_logged_rows(self) -> None:
        module = load_module(
            "run_on_flux_module_stale_host",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "run_on_flux.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        origin TEXT,
                        file_type TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name, origin, file_type)
                    VALUES (3, 'Alexei Transp 09 (full-auto) NEW', '/p/transparch/result/NSTX/09', 'TRANSP')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        3,
                        "Alexei Transp 09 (full-auto) NEW",
                        "flux_equil_inputs_existing.db",
                        "alice@flux",
                        "/u/alice/DTwin/transp_full_auto",
                        "2026-03-27 03:00:00",
                    ),
                )
                conn.commit()

            flux_profile = {
                "user": "alice",
                "host": "flux-login2",
                "remote": "alice@flux-login2",
                "base_dir": "/u/alice/DTwin/transp_full_auto",
                "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
            }
            with mock.patch.object(module, "resolve_flux_profile", return_value=flux_profile), mock.patch.object(
                module, "run_stage_step"
            ) as stage_mock, mock.patch.object(module, "sync_runtime_support") as sync_mock, mock.patch.object(
                module, "submit_remote_slurm"
            ) as submit_mock:
                submit_mock.return_value = "2004388;flux"
                result = module.run_for_origin(
                    main_db=str(db_path),
                    origin_id=3,
                    origin_name="",
                    partition="all",
                    walltime="04:00:00",
                    memory="8G",
                )

            stage_mock.assert_not_called()
            sync_mock.assert_called_once_with(
                "alice@flux-login2",
                "/u/alice/DTwin/transp_full_auto",
                flux_profile,
            )
            submit_mock.assert_called_once_with(
                remote_host="alice@flux-login2",
                remote_dir="/u/alice/DTwin/transp_full_auto",
                remote_db_path="/u/alice/DTwin/transp_full_auto/flux_equil_inputs_existing.db",
                origin_name="Alexei Transp 09 (full-auto) NEW",
                remote_path="/p/transparch/result/NSTX/09",
                partition="all",
                walltime="04:00:00",
                memory="8G",
                flux_profile=flux_profile,
            )
            self.assertEqual(result["remote_host"], "alice@flux-login2")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT remote_host
                    FROM flux_action_log
                    WHERE data_origin_id = 3
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "alice@flux-login2")

    def test_run_on_flux_stages_when_no_logged_db_exists(self) -> None:
        module = load_module(
            "run_on_flux_module_stage",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "run_on_flux.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        origin TEXT,
                        file_type TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name, origin, file_type)
                    VALUES (4, 'Alexei Transp 10 (full-auto)', '/p/transparch/result/NSTX/10', 'TRANSP')
                    """
                )
                conn.commit()

            flux_profile = {
                "user": "alice",
                "host": "flux.example.org",
                "remote": "alice@flux.example.org",
                "base_dir": "/u/alice/DTwin/transp_full_auto",
                "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
            }

            def fake_stage(origin_name: str) -> None:
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO flux_action_log (
                            data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            4,
                            origin_name,
                            "flux_equil_inputs_new.db",
                            "alice@flux.example.org",
                            "/u/alice/DTwin/transp_full_auto",
                            "2026-03-27 03:05:00",
                        ),
                    )
                    conn.commit()

            with mock.patch.object(module, "resolve_flux_profile", return_value=flux_profile), mock.patch.object(
                module, "run_stage_step", side_effect=fake_stage
            ) as stage_mock, mock.patch.object(module, "sync_runtime_support") as sync_mock, mock.patch.object(
                module, "submit_remote_slurm"
            ) as submit_mock:
                submit_mock.return_value = "2004387;flux"
                result = module.run_for_origin(
                    main_db=str(db_path),
                    origin_id=4,
                    origin_name="",
                    partition="all",
                    walltime="04:00:00",
                    memory="8G",
                )

            stage_mock.assert_called_once_with("Alexei Transp 10 (full-auto)")
            sync_mock.assert_not_called()
            submit_mock.assert_called_once_with(
                remote_host="alice@flux.example.org",
                remote_dir="/u/alice/DTwin/transp_full_auto",
                remote_db_path="/u/alice/DTwin/transp_full_auto/flux_equil_inputs_new.db",
                origin_name="Alexei Transp 10 (full-auto)",
                remote_path="/p/transparch/result/NSTX/10",
                partition="all",
                walltime="04:00:00",
                memory="8G",
                flux_profile=flux_profile,
            )
            self.assertEqual(result["flux_db_name"], "flux_equil_inputs_new.db")
            self.assertEqual(result["slurm_job_id"], "2004387;flux")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT status, slurm_job_id, submitted_at, synced_at
                    FROM flux_action_log
                    WHERE data_origin_id = 4
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "SUBMITTED")
            self.assertEqual(str(row[1]), "2004387;flux")
            self.assertIsNotNone(row[2])
            self.assertIsNone(row[3])

    def test_run_on_flux_blocks_parallel_resubmission_when_latest_row_is_submitted(self) -> None:
        module = load_module(
            "run_on_flux_module_submitted",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "run_on_flux.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        origin TEXT,
                        file_type TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name, origin, file_type)
                    VALUES (4, 'Alexei Transp 10 (full-auto)', '/p/transparch/result/NSTX/10', 'TRANSP')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir,
                        status, slurm_job_id, submitted_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        4,
                        "Alexei Transp 10 (full-auto)",
                        "flux_equil_inputs_new.db",
                        "alice@flux.example.org",
                        "/u/alice/DTwin/transp_full_auto",
                        "SUBMITTED",
                        "2004386;flux",
                        "2026-03-27 03:50:00",
                        "2026-03-27 03:40:00",
                    ),
                )
                conn.commit()

            with mock.patch.object(
                module,
                "resolve_flux_profile",
                return_value={
                    "user": "alice",
                    "host": "flux.example.org",
                    "remote": "alice@flux.example.org",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            ), mock.patch.object(module, "run_stage_step") as stage_mock, mock.patch.object(
                module, "submit_remote_slurm"
            ) as submit_mock:
                with self.assertRaises(SystemExit) as ctx:
                    module.run_for_origin(
                        main_db=str(db_path),
                        origin_id=4,
                        origin_name="",
                        partition="all",
                        walltime="04:00:00",
                        memory="8G",
                    )

            stage_mock.assert_not_called()
            submit_mock.assert_not_called()
            self.assertIn("already active", str(ctx.exception))

    def test_sync_back_uses_origin_specific_flux_action_log_row(self) -> None:
        module = load_module(
            "sync_back_from_flux_module",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "sync_back_from_flux.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.executemany(
                    "INSERT INTO data_origin (id, name) VALUES (?, ?)",
                    [
                        (3, "Alexei Transp 09 (full-auto) NEW"),
                        (4, "Alexei Transp 10 (full-auto)"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir,
                        status, slurm_job_id, submitted_at, synced_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            3,
                            "Alexei Transp 09 (full-auto) NEW",
                            "flux_equil_inputs_origin3.db",
                            "alice@flux",
                            "/u/alice/DTwin/transp_full_auto",
                            "STAGED",
                            None,
                            None,
                            None,
                            "2026-03-27 03:10:00",
                        ),
                        (
                            4,
                            "Alexei Transp 10 (full-auto)",
                            "flux_equil_inputs_origin4.db",
                            "bob@flux",
                            "/u/bob/DTwin/transp_full_auto",
                            "STAGED",
                            None,
                            None,
                            None,
                            "2026-03-27 03:11:00",
                        ),
                    ],
                )
                conn.commit()

            with mock.patch.object(
                module,
                "resolve_flux_profile",
                return_value={
                    "user": "bob",
                    "host": "flux-login2",
                    "remote": "bob@flux-login2",
                    "base_dir": "/u/bob/DTwin/transp_full_auto",
                    "python_bin": "/u/bob/pyrokinetics/.venv/bin/python",
                },
            ), mock.patch.object(module.subprocess, "run") as run_mock:
                result = module.run_sync(str(db_path), 4, "")

            run_mock.assert_called_once()
            cmd = run_mock.call_args.args[0]
            env = run_mock.call_args.kwargs["env"]
            self.assertEqual(cmd[0], "bash")
            self.assertEqual(cmd[1], str(module.STEP3_SCRIPT))
            self.assertEqual(
                cmd[2],
                "/u/bob/DTwin/transp_full_auto/flux_equil_inputs_origin4.db",
            )
            self.assertEqual(env["DTWIN_FLUX_USER"], "bob")
            self.assertEqual(env["DTWIN_FLUX_HOST"], "flux-login2")
            self.assertEqual(env["DTWIN_FLUX_BASE_DIR"], "/u/bob/DTwin/transp_full_auto")
            self.assertEqual(result["origin_name"], "Alexei Transp 10 (full-auto)")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT status, synced_at, remote_host
                    FROM flux_action_log
                    WHERE data_origin_id = 4
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "SYNCED")
            self.assertIsNotNone(row[1])
            self.assertEqual(str(row[2]), "bob@flux-login2")

    def test_check_flux_status_marks_running_from_squeue(self) -> None:
        module = load_module(
            "check_flux_status_module_running",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "check_flux_job_status.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name)
                    VALUES (4, 'Alexei Transp 10 (full-auto)')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir,
                        status, slurm_job_id, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        4,
                        "Alexei Transp 10 (full-auto)",
                        "flux_equil_inputs_new.db",
                        "alice@flux",
                        "/u/alice/DTwin/transp_full_auto",
                        "SUBMITTED",
                        "2004386;flux",
                        "2026-03-27 03:40:00",
                    ),
                )
                conn.commit()

            with mock.patch.object(
                module,
                "resolve_flux_profile",
                return_value={
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            ), mock.patch.object(module, "_prime_ssh") as prime_mock, mock.patch.object(
                module,
                "fetch_queue_state",
                return_value=("RUNNING", "RUNNING|00:03:21|flux-node01"),
            ) as queue_mock, mock.patch.object(module, "fetch_sacct_state") as sacct_mock:
                result = module.run_status_check(str(db_path), 4, "")

            prime_mock.assert_called_once_with(
                "alice@flux-login2",
                {
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            )
            queue_mock.assert_called_once_with(
                "alice@flux-login2",
                "2004386",
                {
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            )
            sacct_mock.assert_not_called()
            self.assertEqual(result["status"], "RUNNING")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT status, status_detail, status_checked_at, remote_host
                    FROM flux_action_log
                    WHERE data_origin_id = 4
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "RUNNING")
            self.assertEqual(str(row[1]), "RUNNING|00:03:21|flux-node01")
            self.assertIsNotNone(row[2])
            self.assertEqual(str(row[3]), "alice@flux-login2")

    def test_check_flux_status_marks_done_from_sacct_when_queue_is_empty(self) -> None:
        module = load_module(
            "check_flux_status_module_done",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "check_flux_job_status.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        status_detail TEXT,
                        submitted_at TEXT,
                        status_checked_at TEXT,
                        synced_at TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (id, name)
                    VALUES (3, 'Alexei Transp 09 (full-auto) NEW')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO flux_action_log (
                        data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir,
                        status, slurm_job_id, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        3,
                        "Alexei Transp 09 (full-auto) NEW",
                        "flux_equil_inputs_existing.db",
                        "alice@flux",
                        "/u/alice/DTwin/transp_full_auto",
                        "RUNNING",
                        "2004386;flux",
                        "2026-03-27 03:40:00",
                    ),
                )
                conn.commit()

            with mock.patch.object(
                module,
                "resolve_flux_profile",
                return_value={
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            ), mock.patch.object(module, "_prime_ssh") as prime_mock, mock.patch.object(
                module,
                "fetch_queue_state",
                return_value=("", ""),
            ) as queue_mock, mock.patch.object(
                module,
                "fetch_sacct_state",
                return_value=("COMPLETED", "2004386|COMPLETED|0:0|00:42:10"),
            ) as sacct_mock:
                result = module.run_status_check(str(db_path), 3, "")

            prime_mock.assert_called_once_with(
                "alice@flux-login2",
                {
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            )
            queue_mock.assert_called_once_with(
                "alice@flux-login2",
                "2004386",
                {
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            )
            sacct_mock.assert_called_once_with(
                "alice@flux-login2",
                "2004386",
                {
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                },
            )
            self.assertEqual(result["status"], "DONE")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT status, status_detail, status_checked_at
                    FROM flux_action_log
                    WHERE data_origin_id = 3
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "DONE")
            self.assertEqual(str(row[1]), "2004386|COMPLETED|0:0|00:42:10")
            self.assertIsNotNone(row[2])

    def test_submit_remote_slurm_uses_login_bash_and_sbatch_bootstrap(self) -> None:
        module = load_module(
            "run_on_flux_module_submit",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "run_on_flux.py",
        )
        with mock.patch.object(module.subprocess, "run") as run_mock:
            run_mock.return_value = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout=(
                    "Submitted Flux step 2 job 2004386;flux "
                    "for Alexei Transp 10 (full-auto) using "
                    "/u/alice/DTwin/transp_full_auto/flux_equil_inputs.db\n"
                ),
                stderr="",
            )
            job_id = module.submit_remote_slurm(
                remote_host="alice@flux-login2",
                remote_dir="/u/alice/DTwin/transp_full_auto",
                remote_db_path="/u/alice/DTwin/transp_full_auto/flux_equil_inputs.db",
                origin_name="Alexei Transp 10 (full-auto)",
                remote_path="/p/transparch/result/NSTX/10",
                partition="all",
                walltime="04:00:00",
                memory="8G",
                flux_profile={
                    "user": "alice",
                    "host": "flux-login2",
                    "remote": "alice@flux-login2",
                    "base_dir": "/u/alice/DTwin/transp_full_auto",
                    "python_bin": "/u/alice/pyrokinetics/.venv/bin/python",
                    "duo_option": "",
                },
            )

        cmd = run_mock.call_args.args[0]
        self.assertEqual(job_id, "2004386;flux")
        self.assertEqual(cmd[:3], ["ssh", "-o", "ControlMaster=auto"])
        self.assertEqual(cmd[-3], "bash")
        self.assertEqual(cmd[-2], "-lc")
        remote_script = cmd[-1]
        self.assertIn("source /etc/profile", remote_script)
        self.assertIn("module load slurm", remote_script)
        self.assertIn("command -v sbatch", remote_script)
        self.assertIn("sbatch --parsable --export=ALL --partition all --time 04:00:00 --mem 8G flux/run_mainsteps2_slurm.sh /u/alice/DTwin/transp_full_auto/flux_equil_inputs.db", remote_script)


if __name__ == "__main__":
    unittest.main()
