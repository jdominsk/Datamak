import importlib.util
import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = PROJECT_ROOT / "tools" / "datamak_hpc_acceptance.py"


def load_tool():
    spec = importlib.util.spec_from_file_location("datamak_hpc_acceptance", TOOL_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class DatamakHpcAcceptanceTests(unittest.TestCase):
    def test_core_acceptance_writes_profile_and_visibility_traces(self) -> None:
        tool = load_tool()
        with tempfile.TemporaryDirectory() as tmpdir:
            home = Path(tmpdir) / "datamak_home"
            rc = tool.main(
                [
                    "run",
                    "--machine",
                    "testmachine",
                    "--scheduler",
                    "none",
                    "--datamak-home",
                    str(home),
                    "--datamak-root",
                    str(PROJECT_ROOT),
                ]
            )
            self.assertEqual(rc, 0)

            latest = home / "hpc_acceptance" / "testmachine" / "latest.json"
            profile = home / "machine_profile_testmachine.json"
            self.assertTrue(latest.exists())
            self.assertTrue(profile.exists())

            report = json.loads(latest.read_text(encoding="utf-8"))
            self.assertTrue(report["successful"])
            statuses = {row["name"]: row["status"] for row in report["checks"]}
            self.assertEqual(statuses["database_claim_dependency"], "PASS")
            self.assertEqual(statuses["preclaim_failure_visibility"], "PASS")
            self.assertEqual(statuses["postclaim_failure_visibility"], "PASS")
            self.assertEqual(statuses["scheduler_smoke"], "SKIP")
            self.assertEqual(statuses["gx_smoke"], "SKIP")

            preclaim = next(
                row for row in report["checks"] if row["name"] == "preclaim_failure_visibility"
            )
            self.assertTrue(Path(preclaim["log"]).exists())
            profile_data = json.loads(profile.read_text(encoding="utf-8"))
            self.assertEqual(profile_data["machine"], "testmachine")
            self.assertTrue(profile_data["latest_acceptance"]["successful"])

    def test_scheduler_smoke_command_is_recorded(self) -> None:
        tool = load_tool()
        with tempfile.TemporaryDirectory() as tmpdir:
            home = Path(tmpdir) / "datamak_home"
            command = f"{shlex.quote(sys.executable)} -c \"print('scheduler-ok')\""
            rc = tool.main(
                [
                    "run",
                    "--machine",
                    "testmachine",
                    "--scheduler",
                    "none",
                    "--datamak-home",
                    str(home),
                    "--datamak-root",
                    str(PROJECT_ROOT),
                    "--scheduler-command",
                    command,
                ]
            )
            self.assertEqual(rc, 0)
            report = json.loads(
                (home / "hpc_acceptance" / "testmachine" / "latest.json").read_text(
                    encoding="utf-8"
                )
            )
            scheduler = next(row for row in report["checks"] if row["name"] == "scheduler_smoke")
            self.assertEqual(scheduler["status"], "PASS")
            log = Path(scheduler["log"])
            self.assertTrue(log.exists())
            self.assertIn("scheduler-ok", log.read_text(encoding="utf-8"))

    def test_missing_runtime_script_fails_profile_check(self) -> None:
        tool = load_tool()
        with tempfile.TemporaryDirectory() as tmpdir:
            home = Path(tmpdir) / "datamak_home"
            missing = Path(tmpdir) / "missing_module.sh"
            rc = tool.main(
                [
                    "run",
                    "--machine",
                    "testmachine",
                    "--scheduler",
                    "none",
                    "--datamak-home",
                    str(home),
                    "--datamak-root",
                    str(PROJECT_ROOT),
                    "--runtime-script",
                    str(missing),
                ]
            )
            self.assertEqual(rc, 1)
            report = json.loads(
                (home / "hpc_acceptance" / "testmachine" / "latest.json").read_text(
                    encoding="utf-8"
                )
            )
            checks = {row["name"]: row for row in report["checks"]}
            self.assertEqual(checks["runtime_script_exists"]["status"], "FAIL")
            self.assertFalse(report["successful"])

    def test_sqlite_connect_receives_string_path_for_python36(self) -> None:
        tool = load_tool()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pool.sqlite"
            real_connect = tool.sqlite3.connect

            def checked_connect(path, *args, **kwargs):
                self.assertIsInstance(path, str)
                return real_connect(path, *args, **kwargs)

            with mock.patch.object(tool.sqlite3, "connect", side_effect=checked_connect):
                with tool._connect(db_path) as conn:
                    conn.execute("CREATE TABLE smoke(id INTEGER PRIMARY KEY)")


if __name__ == "__main__":
    unittest.main()
