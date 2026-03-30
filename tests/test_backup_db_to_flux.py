import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import backup_db_to_flux


class BackupDbToFluxTests(unittest.TestCase):
    def test_default_remote_backup_dir_uses_flux_base_dir_parent(self) -> None:
        remote_dir = backup_db_to_flux._default_remote_backup_dir(
            {
                "user": "jdominsk",
                "base_dir": "/u/jdominsk/DTwin/transp_full_auto",
            }
        )
        self.assertEqual(remote_dir, "/u/jdominsk/DTwin/backup")

    def test_backup_database_runs_mkdir_rsync_and_latest_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "gyrokinetic_simulations.db"
            status_path = tmp_path / "backup_status.json"
            db_path.write_text("db", encoding="utf-8")

            run_calls: list[list[str]] = []

            def _fake_run(command, check):
                self.assertTrue(check)
                run_calls.append(list(command))
                return mock.Mock()

            with mock.patch.object(
                backup_db_to_flux,
                "resolve_flux_profile",
                return_value={
                    "user": "jdominsk",
                    "host": "flux",
                    "remote": "jdominsk@flux",
                    "base_dir": "/u/jdominsk/DTwin/transp_full_auto",
                    "duo_option": "1",
                },
            ), mock.patch.object(
                backup_db_to_flux,
                "_timestamp_utc",
                return_value="2026-03-29T09:00:00Z",
            ), mock.patch.object(
                backup_db_to_flux.time,
                "time",
                side_effect=[1000.0, 1002.5],
            ), mock.patch.object(
                backup_db_to_flux.subprocess,
                "run",
                side_effect=_fake_run,
            ):
                result = backup_db_to_flux.backup_database(
                    db_path=db_path,
                    remote_dir="",
                    status_path=status_path,
                    prime_ssh=False,
                )

            self.assertEqual(result["remote_host"], "jdominsk@flux")
            self.assertEqual(result["remote_dir"], "/u/jdominsk/DTwin/backup")
            self.assertEqual(len(run_calls), 2)
            self.assertEqual(run_calls[0][0], "ssh")
            self.assertIn("mkdir", run_calls[0])
            self.assertIn("/u/jdominsk/DTwin/backup", run_calls[0])
            self.assertEqual(run_calls[1][0], "rsync")
            self.assertIn(str(db_path), run_calls[1])
            self.assertIn(
                "jdominsk@flux:/u/jdominsk/DTwin/backup/gyrokinetic_simulations.db",
                run_calls[1],
            )

            status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual(status["state"], "ok")
            self.assertEqual(status["last_success_at"], "2026-03-29T09:00:00Z")
            self.assertEqual(status["remote_host"], "jdominsk@flux")
            self.assertEqual(status["remote_dir"], "/u/jdominsk/DTwin/backup")
            self.assertEqual(status["remote_name"], "gyrokinetic_simulations.db")
            self.assertEqual(
                status["remote_path"],
                "/u/jdominsk/DTwin/backup/gyrokinetic_simulations.db",
            )

    def test_check_backup_status_flags_stale_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            status_path = Path(tmpdir) / "backup_status.json"
            status_path.write_text(
                json.dumps(
                    {
                        "last_success_at": "2000-01-01T00:00:00Z",
                        "remote_host": "jdominsk@flux",
                        "remote_dir": "/u/jdominsk/DTwin/backup",
                        "remote_name": "gyrokinetic_simulations.db",
                    }
                ),
                encoding="utf-8",
            )

            exit_code = backup_db_to_flux.check_backup_status(
                status_path=status_path,
                max_age_hours=30.0,
            )

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
