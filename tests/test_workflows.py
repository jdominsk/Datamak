import json
import sqlite3
import subprocess
import tempfile
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlparse, unquote

from batch.create_batch_database import copy_torun_rows, log_batch_created
from gui.actions import ActionSpec
from gui.app import (
    ACTION_LOCK,
    ACTION_STATE,
    _build_human_friendly_failure_message,
    _build_human_friendly_success_message,
    _compose_support_bundle_mail,
    _run_action,
    _should_suppress_request_log,
    app,
    ensure_gk_surrogate_table,
    get_action_state,
    surrogate_commentary,
    surrogate_relative_uncertainty_label,
)


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

    def _set_action_state(self, **overrides) -> dict:
        with ACTION_LOCK:
            original = dict(ACTION_STATE)
            ACTION_STATE.update(overrides)
        return original

    def _restore_action_state(self, original: dict) -> None:
        with ACTION_LOCK:
            ACTION_STATE.clear()
            ACTION_STATE.update(original)

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

    def test_surrogate_commentary_flags_large_uncertainty_as_low_confidence(self) -> None:
        commentary = surrogate_commentary(
            {
                "count": 34990,
                "train_rows": 95,
                "test_rows": 11,
                "eval_count": 11,
                "eval_relative_uncertainty_pct": 167.0,
                "eval_coverage": 0.18,
                "eval_r2": 0.12,
            }
        )
        self.assertIsNotNone(commentary)
        assert commentary is not None
        self.assertIn("only 11 held-out or truth-matched points", commentary)
        self.assertIn("1.67x", commentary)
        self.assertIn("low-confidence regime", commentary)
        self.assertIn("not yet well calibrated", commentary)

    def test_surrogate_commentary_handles_missing_ground_truth(self) -> None:
        commentary = surrogate_commentary(
            {
                "count": 120,
                "train_rows": 95,
                "test_rows": 0,
                "eval_count": 0,
            }
        )
        self.assertIsNotNone(commentary)
        assert commentary is not None
        self.assertIn("no ground-truth points are currently available", commentary)

    def test_surrogate_relative_uncertainty_label_uses_qualitative_wording(self) -> None:
        label = surrogate_relative_uncertainty_label(
            {
                "eval_relative_uncertainty_pct": 167.0,
            }
        )
        self.assertEqual(label, "Very high relative uncertainty (~167%)")

    def test_human_friendly_failure_message_handles_remote_login_errors(self) -> None:
        message = _build_human_friendly_failure_message(
            "Check Flux Status",
            "Action 'Check Flux Status' failed: Command ... returned non-zero exit status 1.",
            stderr_text=(
                "jdominskprrt@flux-login2: Permission denied "
                "(publickey,gssapi-with-mic,keyboard-interactive)."
            ),
            returncode=1,
        )
        self.assertIn("could not log into the remote system", message)
        self.assertIn("username, SSH access, and Duo setup", message)

    def test_human_friendly_success_message_summarizes_batch_sync_output(self) -> None:
        raw = (
            "Checking 1 batch DB(s) on jdominsk@perlmutter.nersc.gov... using:\n"
            "batch_database_20260128_151957.db -> /pscratch/sd/j/jdominsk/DTwin/batch0007/batch_database_20260128_151957.db\n"
            "batch_database_20260128_151957.db: fetched_unsynced=8, finished(SUCCESS,CONVERGED)=0, status_counts: ERROR=3, RESTART=1, RUNNING=4\n"
            "batch_database_20260128_151957.db: syncing pngs for 16 updated runs from jdominsk@perlmutter.nersc.gov:/pscratch/sd/j/jdominsk/DTwin/batch0007 -> /Users/jdominsk/Documents/Projects/AIML_database/Datamak/batch/plots/batch_database_20260128_151957\n"
            "batch_database_20260128_151957.db: rsynced 2 png files.\n"
            "batch_database_20260128_151957.db: synchronizing 8 runs"
        )
        message, technical = _build_human_friendly_success_message(
            "check_launched_batches",
            "Check Launched Batches",
            raw,
        )
        self.assertEqual(technical, raw)
        self.assertIn(
            "Checked 1 batch DB on jdominsk@perlmutter.nersc.gov.",
            message,
        )
        self.assertIn(
            "Batch 01/28/2026: Synchronizing 8 runs, including 2 png files.",
            message,
        )
        self.assertIn("Current statuses: RUNNING 4, ERROR 3, RESTART 1.", message)

    def test_run_action_failure_sets_human_summary_and_technical_details(self) -> None:
        spec = ActionSpec(
            key="fake_failure",
            label="Fake Failure",
            script="/tmp/fake_failure.py",
            capture_output=True,
        )
        original = self._set_action_state(running=True, name="Fake Failure", key="fake_failure")
        called_process_error = subprocess.CalledProcessError(
            1,
            ["/tmp/fake_failure.py"],
            output="stdout sample",
            stderr="stderr sample",
        )
        try:
            with mock.patch(
                "gui.app.subprocess.run",
                side_effect=called_process_error,
            ), mock.patch(
                "gui.app.create_support_bundle",
                return_value="output/support_bundles/fake_bundle.zip",
            ) as bundle_mock:
                _run_action(
                    spec,
                    str(self.db_path),
                    None,
                    None,
                    {"panel": "tables", "redirect_params": {"origin_id": "3"}},
                )
            bundle_mock.assert_called_once()
            with ACTION_LOCK:
                self.assertFalse(bool(ACTION_STATE["running"]))
                self.assertEqual(
                    ACTION_STATE["support_bundle"],
                    "output/support_bundles/fake_bundle.zip",
                )
                self.assertIn("Fake Failure could not complete.", str(ACTION_STATE["message"]))
                self.assertEqual(ACTION_STATE["technical_message"], "Action 'Fake Failure' failed:\nstdout sample\nstderr sample Support bundle: output/support_bundles/fake_bundle.zip")
        finally:
            self._restore_action_state(original)

    def test_send_support_bundle_opens_compose_helper_and_redirects_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            bundle_dir = project_dir / "output" / "support_bundles"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            bundle_path = bundle_dir / "sample_bundle.zip"
            bundle_path.write_bytes(b"zip")

            with mock.patch("gui.app.PROJECT_DIR", str(project_dir)), mock.patch(
                "gui.app.SUPPORT_BUNDLE_DIR", str(bundle_dir)
            ), mock.patch(
                "gui.app._compose_support_bundle_mail",
                return_value=(True, ""),
            ) as compose_mock:
                response = self.client.post(
                    "/send_support_bundle",
                    data={
                        "db": str(self.db_path),
                        "panel": "surrogate",
                        "surrogate_tab": "models",
                        "compose_mode": "mail",
                        "bundle_path": "output/support_bundles/sample_bundle.zip",
                    },
                )
            self.assertEqual(response.status_code, 302)
            self.assertEqual(
                response.headers["Location"],
                f"/?panel=surrogate&db={self.db_path}&surrogate_tab=models",
            )
            compose_mock.assert_called_once()
            sent_path = compose_mock.call_args.args[0]
            self.assertEqual(Path(sent_path), bundle_path.resolve())
            self.assertEqual(compose_mock.call_args.kwargs["compose_mode"], "mail")

    def test_action_status_disables_http_caching(self) -> None:
        response = self.client.get("/action_status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("Cache-Control"),
            "no-store, no-cache, must-revalidate, max-age=0",
        )
        self.assertEqual(response.headers.get("Pragma"), "no-cache")
        self.assertEqual(response.headers.get("Expires"), "0")

    def test_compose_support_bundle_mail_embeds_error_details_in_gmail_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_path = Path(tmpdir) / "sample_bundle.zip"
            with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "error.txt",
                    "Action: Check Flux Status\nFailure kind: subprocess\nReturn code: 1\n",
                )
                archive.writestr(
                    "stderr.txt",
                    "Permission denied (publickey,gssapi-with-mic,keyboard-interactive).\n",
                )
                archive.writestr("traceback.txt", "Traceback line\n")
            with mock.patch("gui.app.shutil.which", return_value="/usr/bin/open"), mock.patch(
                "gui.app.subprocess.run"
            ) as run_mock:
                run_mock.return_value = subprocess.CompletedProcess(
                    ["open", "https://mail.google.com"], 0, "", ""
                )
                ok, error = _compose_support_bundle_mail(
                    bundle_path, "jdominsk@pppl.gov", "gmail"
                )
            self.assertTrue(ok)
            self.assertEqual(error, "")
            run_mock.assert_called_once()
            gmail_url = str(run_mock.call_args.args[0][1])
            self.assertIn("mail.google.com", gmail_url)
            body = unquote(parse_qs(urlparse(gmail_url).query)["body"][0])
            self.assertIn("Action: Check Flux Status", body)
            self.assertIn("Permission denied", body)
            self.assertIn(str(bundle_path), body)

    def test_compose_support_bundle_mail_uses_applescript_for_mail_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_path = Path(tmpdir) / "sample_bundle.zip"
            with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("error.txt", "Action: Sync Back From Flux\n")
            with mock.patch("gui.app.shutil.which", side_effect=lambda name: "/usr/bin/osascript" if name == "osascript" else None), mock.patch(
                "gui.app.subprocess.run"
            ) as run_mock:
                run_mock.return_value = subprocess.CompletedProcess(
                    ["osascript", "-e", "script"], 0, "", ""
                )
                ok, error = _compose_support_bundle_mail(
                    bundle_path, "jdominsk@pppl.gov", "mail"
                )
            self.assertTrue(ok)
            self.assertEqual(error, "")
            run_mock.assert_called_once()
            self.assertEqual(run_mock.call_args.args[0][0], "osascript")
            applescript = str(run_mock.call_args.args[0][2])
            self.assertIn("tell application \"Mail\"", applescript)
            self.assertIn(str(bundle_path), applescript)

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

    def test_gui_stylesheet_route_serves_extracted_control_css(self) -> None:
        response = self.client.get("/gui-static/app.css")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn(".origin-picker", body)
        self.assertIn(".tab", body)
        response.close()

    def test_request_log_suppression_targets_polling_routes_only(self) -> None:
        self.assertTrue(_should_suppress_request_log("/action_status"))
        self.assertTrue(_should_suppress_request_log("/usage"))
        self.assertFalse(_should_suppress_request_log("/"))
        self.assertFalse(_should_suppress_request_log("/gui-static/app.css"))

    def test_get_action_state_strips_stale_technical_details_sentence(self) -> None:
        original = self._set_action_state(
            running=False,
            message=(
                "Sync Back From Flux could not complete. The underlying command "
                "stopped before finishing. Technical details are available below "
                "if you need them."
            ),
            technical_message="traceback here",
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            state = get_action_state()
        finally:
            self._restore_action_state(original)
        self.assertEqual(
            state["message"],
            "Sync Back From Flux could not complete. The underlying command stopped before finishing.",
        )

    def test_database_panel_renders_table_schema_summary(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=tables&table=gk_input")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn(">Column<", body)
        self.assertIn(">Type<", body)
        self.assertIn(">PK<", body)
        self.assertIn(">Not null<", body)
        self.assertIn(">Default<", body)
        self.assertIn(">id<", body)
        self.assertIn(">status<", body)
        self.assertIn(">content<", body)

    def test_database_panel_defers_large_table_counts_until_requested(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=tables&table=gk_input")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Total row counts are deferred for this table until requested.", body)
        self.assertIn(">Load counts<", body)

        counted = self.client.get(
            f"/?db={self.db_path}&panel=tables&table=gk_input&table_counts=1"
        )
        self.assertEqual(counted.status_code, 200)
        counted_body = counted.get_data(as_text=True)
        self.assertIn("Rows shown: 3 of 3", counted_body)
        self.assertNotIn("Total row counts are deferred for this table until requested.", counted_body)

    def test_tables_panel_request_skips_other_tab_queries(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            ensure_gk_surrogate_table(conn)
            conn.execute(
                """
                CREATE TABLE sg_estimate (
                    id INTEGER PRIMARY KEY,
                    gk_surrogate_id INTEGER NOT NULL,
                    gk_input_id INTEGER NOT NULL,
                    sg_estimate REAL,
                    sg_quality REAL
                )
                """
            )
            conn.commit()

        with mock.patch(
            "gui.app.get_gk_input_points",
            side_effect=AssertionError("sampling points should not be loaded for the tables tab"),
        ), mock.patch(
            "gui.app.get_gk_run_results_points",
            side_effect=AssertionError("results points should not be loaded for the tables tab"),
        ), mock.patch(
            "gui.app.build_workflow_panel_context",
            side_effect=AssertionError("workflow context should not be loaded for the tables tab"),
        ), mock.patch(
            "gui.app.get_sg_estimate_summary",
            side_effect=AssertionError("surrogate estimates should not be loaded for the tables tab"),
        ):
            response = self.client.get(
                f"/?db={self.db_path}&panel=tables&table=gk_input&surrogate_id=1"
            )

        self.assertEqual(response.status_code, 200)

    def test_results_panel_request_skips_sampling_workflow_and_surrogate_summary_queries(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_run (
                    id INTEGER PRIMARY KEY,
                    gk_input_id INTEGER NOT NULL,
                    status TEXT,
                    gamma_max REAL
                )
                """
            )
            ensure_gk_surrogate_table(conn)
            conn.execute(
                """
                INSERT INTO gk_surrogate (
                    name, sg_label, mapsto, status_filter, created_at,
                    train_rows, test_rows, metrics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "gamma_v1",
                    "gamma_max",
                    "gk_run.gamma_max",
                    "SUCCESS,CONVERGED",
                    "2026-03-28T12:00:00Z",
                    100,
                    10,
                    json.dumps({"mae": 0.1, "rmse": 0.2, "r2": 0.9}),
                ),
            )
            conn.execute(
                """
                CREATE TABLE sg_estimate (
                    id INTEGER PRIMARY KEY,
                    gk_surrogate_id INTEGER NOT NULL,
                    gk_input_id INTEGER NOT NULL,
                    sg_estimate REAL,
                    sg_quality REAL
                )
                """
            )
            conn.commit()

        with mock.patch(
            "gui.app.get_gk_input_points",
            side_effect=AssertionError("sampling points should not be loaded for the results tab"),
        ), mock.patch(
            "gui.app.build_workflow_panel_context",
            side_effect=AssertionError("workflow context should not be loaded for the results tab"),
        ), mock.patch(
            "gui.app.get_sg_estimate_summary",
            side_effect=AssertionError("surrogate estimate summary should not be loaded for the results tab"),
        ), mock.patch(
            "gui.app.get_gk_run_results_points",
            return_value=([], False),
        ), mock.patch(
            "gui.app.get_gamma_max_status_report",
            return_value=None,
        ):
            response = self.client.get(
                f"/?db={self.db_path}&panel=results&surrogate_id=1"
            )

        self.assertEqual(response.status_code, 200)

    def test_surrogate_panel_renders_clickable_selected_model_row(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            ensure_gk_surrogate_table(conn)
            conn.execute(
                """
                INSERT INTO gk_surrogate (
                    name, sg_label, mapsto, status_filter, created_at,
                    train_rows, test_rows, metrics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "gamma_v1",
                    "gamma_max",
                    "gk_run.gamma_max",
                    "SUCCESS,CONVERGED",
                    "2026-03-28T12:00:00Z",
                    100,
                    10,
                    json.dumps({"mae": 0.1, "rmse": 0.2, "r2": 0.9}),
                ),
            )
            conn.commit()

        response = self.client.get(f"/?db={self.db_path}&panel=surrogate&surrogate_id=1")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('id="surrogate-row-select-form"', body)
        self.assertIn('id="surrogate-row-select-input"', body)
        self.assertIn('class="surrogate-model-row active"', body)
        self.assertIn('data-surrogate-id="1"', body)
        self.assertIn('id="surrogate-refresh-actions-data"', body)
        self.assertIn('"run_surrogate_estimate"', body)
        self.assertIn('"delete_surrogate_model"', body)
        self.assertIn("Models & Estimates", body)
        self.assertIn("Train New Model", body)
        self.assertIn('data-surrogate-subpanel="models"', body)
        self.assertIn('data-surrogate-subpanel="train"', body)
        self.assertIn('name="surrogate_tab" value="models"', body)
        self.assertIn('name="surrogate_tab" value="train"', body)
        self.assertIn(
            'title="Train and manage surrogate models. Models are saved under db_surrogate/models with JSON metadata."',
            body,
        )
        self.assertIn("Model Estimates &amp; Validation: gamma_v1", body)
        self.assertIn("Compute estimates", body)
        self.assertIn("Delete model", body)
        self.assertNotIn(
            "Train and manage surrogate models. Models are saved under <code>db_surrogate/models</code>",
            body,
        )
        self.assertNotIn("<th>Run</th>", body)

    def test_expired_action_message_is_pruned_from_surrogate_panel(self) -> None:
        original_state = self._set_action_state(
            running=False,
            name=None,
            message="Updated stale surrogate estimates.",
            key="run_surrogate_estimate",
            completed_at=(
                datetime.now(timezone.utc) - timedelta(seconds=25)
            ).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
        try:
            response = self.client.get(f"/?db={self.db_path}&panel=surrogate")
            self.assertEqual(response.status_code, 200)
            body = response.get_data(as_text=True)
            self.assertNotIn("Updated stale surrogate estimates.", body)
            with ACTION_LOCK:
                self.assertIsNone(ACTION_STATE["message"])
                self.assertIsNone(ACTION_STATE["completed_at"])
        finally:
            self._restore_action_state(original_state)

    def test_recent_action_message_still_renders_in_surrogate_panel(self) -> None:
        original_state = self._set_action_state(
            running=False,
            name=None,
            message="Updated fresh surrogate estimates.",
            technical_message="Traceback line 1",
            key="run_surrogate_estimate",
            completed_at=(
                datetime.now(timezone.utc) - timedelta(seconds=5)
            ).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
        try:
            response = self.client.get(f"/?db={self.db_path}&panel=surrogate")
            self.assertEqual(response.status_code, 200)
            body = response.get_data(as_text=True)
            self.assertIn("Updated fresh surrogate estimates.", body)
            self.assertIn("Technical details", body)
            self.assertIn("Traceback line 1", body)
            self.assertIn("notice-error", body)
        finally:
            self._restore_action_state(original_state)

    def test_equilibria_tab_renders_on_sparse_db(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=equilibria")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Equilibria", body)
        self.assertIn("No `data_origin` rows are available yet.", body)

    def test_missing_db_renders_error_without_crashing(self) -> None:
        missing_db = Path(self.tmpdir.name) / "missing.db"
        response = self.client.get(f"/?db={missing_db}&panel=equilibria")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Database not found", body)
        self.assertIn('class="tab active" type="button" data-tab="tables"', body)
        self.assertIn("Database recovery", body)
        self.assertIn("Open Demo Database In Browser", body)
        self.assertIn("1XS1jpbqQICJNDR6AU_wXeYG7BM_nt4yM", body)

    def test_directory_db_path_renders_error_without_crashing(self) -> None:
        response = self.client.get(f"/?db={self.tmpdir.name}&panel=equilibria")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Database not found", body)
        self.assertIn('class="tab active" type="button" data-tab="tables"', body)
        self.assertIn("Database recovery", body)

    def test_missing_db_shows_detected_demo_database_actions(self) -> None:
        missing_db = Path(self.tmpdir.name) / "missing.db"
        detected_db = Path(self.tmpdir.name) / "gyrokinetic_simulations_demo_light.db"
        with mock.patch(
            "gui.app.find_demo_database_candidates",
            return_value=[
                {
                    "path": str(detected_db),
                    "name": detected_db.name,
                    "directory": str(detected_db.parent),
                    "modified": "2026-03-28 10:00:00",
                }
            ],
        ):
            response = self.client.get(f"/?db={missing_db}&panel=tables")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Use Detected Demo DB", body)
        self.assertIn("Copy Into Datamak Root And Use It", body)
        self.assertIn(str(detected_db), body)

    def test_copy_demo_database_copies_detected_file_and_redirects(self) -> None:
        source_db = Path(self.tmpdir.name) / "gyrokinetic_simulations_demo_light.db"
        source_db.write_text("demo db", encoding="utf-8")
        copied_db = Path(self.tmpdir.name) / "copied_demo_light.db"
        with mock.patch(
            "gui.app.find_demo_database_candidates",
            return_value=[
                {
                    "path": str(source_db),
                    "name": source_db.name,
                    "directory": str(source_db.parent),
                    "modified": "2026-03-28 10:00:00",
                }
            ],
        ), mock.patch(
            "gui.app.get_demo_copy_target_path", return_value=copied_db
        ):
            response = self.client.post(
                "/copy_demo_database",
                data={"db": str(self.db_path), "source_path": str(source_db)},
            )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(copied_db.exists())
        self.assertEqual(copied_db.read_text(encoding="utf-8"), "demo db")
        self.assertIn("panel=tables", response.headers["Location"])
        self.assertIn(copied_db.name, response.headers["Location"])

    def test_hpc_drawer_renders_perlmutter_and_flux_tabs(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=equilibria&hpc=1")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn(">Perlmutter<", body)
        self.assertIn(">Flux<", body)
        self.assertIn("Auto Duo option", body)
        self.assertIn("GX path", body)

    def test_legacy_workflow_panel_aliases_to_equilibria(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=action")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Workflow", body)

    def test_legacy_batch_monitor_panel_aliases_to_equilibria(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=monitor")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Workflow", body)
        self.assertNotIn("Batch Monitor", body)

    def test_legacy_schema_panel_aliases_to_database(self) -> None:
        response = self.client.get(f"/?db={self.db_path}&panel=schema")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('class="tab active" type="button" data-tab="tables"', body)
        self.assertNotIn('data-tab="schema"', body)
        self.assertNotIn('data-panel="schema"', body)

    def test_equilibria_tab_defers_sampling_analysis_until_requested(self) -> None:
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
                    active INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE gk_study (
                    id INTEGER PRIMARY KEY,
                    data_equil_id INTEGER NOT NULL
                );
                """
            )
            conn.execute("ALTER TABLE gk_input ADD COLUMN gk_study_id INTEGER")
            conn.execute(
                """
                INSERT INTO data_origin (id, name, file_type, tokamak, origin, color)
                VALUES (4, 'Transp 10 (full-auto)', 'TRANSP', 'NSTX', '/p/transparch/result/NSTX/10', '#ff7f0e')
                """
            )
            conn.execute(
                "INSERT INTO data_equil (id, data_origin_id, active) VALUES (1, 4, 1)"
            )
            conn.execute(
                "INSERT INTO gk_study (id, data_equil_id) VALUES (1, 1)"
            )
            conn.commit()

        with mock.patch(
            "gui.app.get_equil_plasma_dataset",
            side_effect=AssertionError("workflow should not load equil/plasma datasets by default"),
        ), mock.patch(
            "gui.app.build_sampling_report",
            side_effect=AssertionError("workflow should not build sampling reports by default"),
        ), mock.patch(
            "gui.app.build_sampling_coverage",
            side_effect=AssertionError("workflow should not build sampling coverage by default"),
        ), mock.patch(
            "gui.app.build_sampling_selection",
            side_effect=AssertionError("workflow should not build sampling selection by default"),
        ), mock.patch(
            "gui.app.build_kmeans_selection",
            side_effect=AssertionError("workflow should not build kmeans selection by default"),
        ):
            response = self.client.get(
                f"/?db={self.db_path}&panel=equilibria&origin_id=4"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("This sampling analysis is now loaded on demand.", body)
        self.assertIn("Analyze inputs", body)

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

        monitor_report = {
            "generated_at": "2026-03-27T12:23:23Z",
            "db_path": str(self.db_path),
            "errors": [],
            "batches": [
                {
                    "batch_id": 7,
                    "batch": "batch_database_origin4.db",
                    "origin_names": ["Transp 10 (full-auto)"],
                    "remote_host": "jdominsk@perlmutter",
                    "base_dir": "/remote/batch7",
                    "status_counts": {"RUNNING": 2},
                    "jobs": [],
                    "suggestions": [],
                    "can_launch_job": False,
                    "pending_analysis": [],
                    "unsynced_count": 0,
                    "running_without_job": False,
                    "running_log_missing": 0,
                    "failures": [],
                    "running_logs": [],
                },
                {
                    "batch_id": 8,
                    "batch": "batch_database_other_origin.db",
                    "origin_names": ["Transp 09 (full-auto) NEW"],
                    "remote_host": "jdominsk@perlmutter",
                    "base_dir": "/remote/batch8",
                    "status_counts": {"RUNNING": 1},
                    "jobs": [],
                    "suggestions": [],
                    "can_launch_job": False,
                    "pending_analysis": [],
                    "unsynced_count": 0,
                    "running_without_job": False,
                    "running_log_missing": 0,
                    "failures": [],
                    "running_logs": [],
                },
            ],
        }
        with mock.patch("gui.app.load_monitor_report", return_value=monitor_report):
            response = self.client.get(
                f"/?db={self.db_path}&panel=equilibria&origin_id=4"
            )
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("AI advisor:", body)
        self.assertIn("Tool-calling supervisor v1", body)
        self.assertIn("Supervisor tools used:", body)
        self.assertIn("get_origin_workflow_state", body)
        self.assertIn("list_allowed_actions", body)
        self.assertIn("check_flux_status", body)
        self.assertIn("check_simulations", body)
        self.assertIn("Read-only typed tools only | actions remain approval-gated", body)
        self.assertIn("Recommended next step", body)
        self.assertIn("Direct actions currently allowed:", body)
        self.assertIn("Workflow Status", body)
        self.assertIn("Launch Batch of Simulations:", body)
        self.assertIn("Batch monitoring:", body)
        self.assertIn("Transp 10 (full-auto)", body)
        self.assertIn("batch_database_origin4.db", body)
        self.assertNotIn("batch_database_other_origin.db", body)
        self.assertIn("This sampling analysis is now loaded on demand.", body)
        self.assertIn("Analyze inputs", body)
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
