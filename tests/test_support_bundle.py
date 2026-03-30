import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from gui.support_bundle import create_support_bundle


class SupportBundleTests(unittest.TestCase):
    def test_create_support_bundle_writes_expected_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bundle_dir = root / "bundles"
            db_path = root / "sample.db"
            usage_log_path = root / "usage.json"

            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE data_origin (id INTEGER PRIMARY KEY, name TEXT)")
                conn.execute(
                    "CREATE TABLE gk_input (id INTEGER PRIMARY KEY, status TEXT, content TEXT)"
                )
                conn.execute(
                    "INSERT INTO data_origin (id, name) VALUES (1, 'Origin 1')"
                )
                conn.execute(
                    "INSERT INTO gk_input (id, status, content) VALUES (1, 'WAIT', 'x')"
                )
                conn.commit()

            usage_log_path.write_text(
                json.dumps(
                    {
                        "events": [
                            {"ts": "2026-03-29T12:00:00Z", "event": "action_click", "details": {"action": "run_on_flux"}},
                            {"ts": "2026-03-29T12:00:05Z", "event": "usage", "details": {"panel": "equilibria"}},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            bundle_path = create_support_bundle(
                bundle_dir=str(bundle_dir),
                project_dir=str(root),
                usage_log_path=str(usage_log_path),
                action_key="run_on_flux",
                action_label="Run On Flux",
                script_path="/tmp/run_on_flux.py",
                command=["python3", "/tmp/run_on_flux.py", "--db", str(db_path)],
                db_path=str(db_path),
                panel="equilibria",
                redirect_params={"origin_id": "4"},
                env_overrides={"DTWIN_SSH_IDENTITY": "/tmp/id_rsa"},
                failure_kind="called_process_error",
                returncode=1,
                stdout="stdout sample",
                stderr="stderr sample",
                traceback_text="Traceback sample",
                action_state={"running": False, "name": None, "key": "run_on_flux"},
                hpc_config={"ssh_identity": "/tmp/id_rsa", "ssh_host": "flux-login2"},
            )

            self.assertIsNotNone(bundle_path)
            assert bundle_path is not None
            archive_path = root / bundle_path
            self.assertTrue(archive_path.is_file())

            with zipfile.ZipFile(archive_path) as archive:
                names = set(archive.namelist())
                self.assertIn("manifest.json", names)
                self.assertIn("error.txt", names)
                self.assertIn("stdout.txt", names)
                self.assertIn("stderr.txt", names)
                self.assertIn("traceback.txt", names)
                self.assertIn("action_state.json", names)
                self.assertIn("config_redacted.json", names)
                self.assertIn("env_overrides_redacted.json", names)
                self.assertIn("usage_log_tail.json", names)
                self.assertIn("db_summary.json", names)

                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                self.assertEqual(manifest["action"]["key"], "run_on_flux")
                self.assertEqual(manifest["action"]["panel"], "equilibria")
                self.assertEqual(manifest["action"]["returncode"], 1)

                config_payload = json.loads(
                    archive.read("config_redacted.json").decode("utf-8")
                )
                self.assertEqual(config_payload["ssh_identity"], "[redacted]")
                self.assertEqual(config_payload["ssh_host"], "flux-login2")

                db_summary = json.loads(
                    archive.read("db_summary.json").decode("utf-8")
                )
                self.assertEqual(db_summary["row_counts"]["data_origin"], 1)
                self.assertEqual(db_summary["row_counts"]["gk_input"], 1)
