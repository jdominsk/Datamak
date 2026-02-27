import contextlib
import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from batch.check_launched_batches import main as check_launched_batches_main
from batch.check_launched_batches import parse_remote


class BatchRemoteSyncWorkflowTests(unittest.TestCase):
    def test_parse_remote_formats(self) -> None:
        host, path = parse_remote("perlmutter:/pscratch/some/path", "")
        self.assertEqual(host, "perlmutter")
        self.assertEqual(path, "/pscratch/some/path")

        host2, path2 = parse_remote("/pscratch/some/path", "perlmutter")
        self.assertEqual(host2, "perlmutter")
        self.assertEqual(path2, "/pscratch/some/path")

        with self.assertRaises(ValueError):
            parse_remote("/pscratch/some/path", "")

    def test_remote_check_syncs_rows_and_marks_batch_synced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "main.db"
            batch_name = "batch_database_20260216_120000.db"
            remote_path = "/pscratch/sd/j/jdominsk/DTwin/newbatch/batch0001"
            self._create_main_db(
                db_path,
                batch_name=batch_name,
                remote_host="perlmutter.nersc.gov",
                remote_folder=remote_path,
            )

            fetched_row = {
                "id": 7,
                "gk_input_id": 42,
                "input_folder": remote_path,
                "job_folder": remote_path,
                "archive_folder": "",
                "input_name": "input_batchid1_gkinputid42_runid7.in",
                "nb_nodes": 1,
                "job_id": "12345",
                "status": "SUCCESS",
                "input_content": "test content",
                "t_max": 10.0,
                "nb_restart": 0,
                "ky_abs_mean": 0.3,
                "gamma_max": 0.5,
                "diffusion": 0.7,
            }

            first_stdout = (
                f"USING\t{batch_name}\t{remote_path.rstrip('/')}/{batch_name}\n"
                f"ROW\t{batch_name}\t{json.dumps(fetched_row)}\n"
            )
            mocked_calls = [
                subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout=first_stdout, stderr=""),
                subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout="", stderr=""),
            ]

            buf = io.StringIO()
            with mock.patch(
                "batch.check_launched_batches.subprocess.run",
                side_effect=mocked_calls,
            ) as run_mock, mock.patch.object(
                sys,
                "argv",
                [
                    "check_launched_batches.py",
                    "--db",
                    str(db_path),
                    "--remote-check",
                    "--timeout",
                    "2",
                ],
            ), contextlib.redirect_stdout(buf):
                check_launched_batches_main()

            self.assertEqual(run_mock.call_count, 2)

            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                batch_row = conn.execute(
                    "SELECT status FROM gk_batch WHERE batch_database_name = ?",
                    (batch_name,),
                ).fetchone()
                self.assertIsNotNone(batch_row)
                self.assertEqual(str(batch_row["status"]), "SYNCED")

                run_row = conn.execute(
                    "SELECT remote_id, gk_input_id, status, remote_host, remote_folder FROM gk_run"
                ).fetchone()
                self.assertIsNotNone(run_row)
                self.assertEqual(int(run_row["remote_id"]), 7)
                self.assertEqual(int(run_row["gk_input_id"]), 42)
                self.assertEqual(str(run_row["status"]), "SUCCESS")
                self.assertEqual(str(run_row["remote_host"]), "perlmutter.nersc.gov")
                self.assertEqual(str(run_row["remote_folder"]), remote_path)

            output = buf.getvalue()
            self.assertIn(f"{batch_name}: synchronizing 1 runs", output)

    @staticmethod
    def _create_main_db(
        db_path: Path,
        batch_name: str,
        remote_host: str,
        remote_folder: str,
    ) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_batch (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_database_name TEXT NOT NULL,
                    remote_folder TEXT NOT NULL,
                    remote_host TEXT,
                    status TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gk_batch (batch_database_name, remote_folder, remote_host, status)
                VALUES (?, ?, ?, 'LAUNCHED')
                """,
                (batch_name, remote_folder, remote_host),
            )
            conn.commit()


if __name__ == "__main__":
    unittest.main()
