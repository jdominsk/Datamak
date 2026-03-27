import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from batch.deploy_batch import main as deploy_batch_main


class _FakePopen:
    def __init__(self, args: list[str]) -> None:
        self.args = args
        self.stdout = io.StringIO("tar-stream")
        self.stderr = io.StringIO("")
        self.returncode = 0

    def wait(self) -> int:
        return self.returncode


class BatchDeployMultiUserTests(unittest.TestCase):
    def test_deploy_batch_uses_resolved_perlmutter_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "datamak-config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "perlmutter": {
                            "user": "operator",
                            "host": "perlmutter.example.org",
                            "base_dir": "/pscratch/sd/o/operator/DTwin",
                            "batch_dir": "/pscratch/sd/o/operator/DTwin/newbatch",
                            "gx_path": "/global/homes/o/operator/GX/custom",
                            "connect_timeout": 13,
                        }
                    }
                ),
                encoding="utf-8",
            )

            gk_db = root / "gyrokinetic.db"
            batch_new = root / "batch" / "new"
            batch_sent = root / "batch" / "sent"
            hpc_dir = root / "batch" / "hpc"
            batch_new.mkdir(parents=True)
            batch_sent.mkdir(parents=True)
            hpc_dir.mkdir(parents=True)

            batch_name = "batch_database_20260326_120000.db"
            batch_db = batch_new / batch_name

            self._create_main_db(gk_db, batch_name)
            self._create_batch_db(batch_db)
            for name in [
                "job_submit.sh",
                "job_execute.sh",
                "prepare_newbatch.sh",
                "gx_analyze.py",
                "linear_convergence.py",
                "ky_growth_rates.py",
                "job_interactive.sh",
            ]:
                (hpc_dir / name).write_text("#!/bin/sh\n", encoding="utf-8")

            submitted_run_dir = "/pscratch/sd/o/operator/DTwin/batch0001"
            mocked_run = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout=f"SUBMITTED\t{submitted_run_dir}\t{batch_name}\n",
                stderr="",
            )

            popen_calls: list[list[str]] = []

            def fake_popen(args: list[str], **_: object) -> _FakePopen:
                popen_calls.append(args)
                return _FakePopen(args)

            with mock.patch.dict(
                "os.environ",
                {"DTWIN_CONFIG": str(config_path)},
                clear=False,
            ), mock.patch(
                "batch.deploy_batch.subprocess.Popen",
                side_effect=fake_popen,
            ), mock.patch(
                "batch.deploy_batch.subprocess.run",
                return_value=mocked_run,
            ) as run_mock, mock.patch.object(
                sys,
                "argv",
                [
                    "deploy_batch.py",
                    "--gk-db",
                    str(gk_db),
                    "--batch-dir",
                    str(batch_new),
                ],
            ):
                deploy_batch_main()

            self.assertEqual(len(popen_calls), 1)
            self.assertEqual(popen_calls[0][:4], ["tar", "-cf", "-", "-C"])

            ssh_args = run_mock.call_args.args[0]
            self.assertIn("operator@perlmutter.example.org", ssh_args)
            remote_script = ssh_args[-1]
            self.assertIn("/pscratch/sd/o/operator/DTwin/newbatch", remote_script)
            self.assertIn("/pscratch/sd/o/operator/DTwin", remote_script)
            self.assertIn(
                "DTWIN_GX_PATH=/global/homes/o/operator/GX/custom",
                remote_script,
            )
            self.assertNotIn("jdominsk", " ".join(ssh_args))
            self.assertNotIn("jdominsk", remote_script)

            with sqlite3.connect(gk_db) as conn:
                row = conn.execute(
                    """
                    SELECT status, remote_host, remote_folder
                    FROM gk_batch
                    WHERE batch_database_name = ?
                    """,
                    (batch_name,),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "LAUNCHED")
            self.assertEqual(str(row[1]), "operator@perlmutter.example.org")
            self.assertEqual(
                str(row[2]),
                "operator@perlmutter.example.org:/pscratch/sd/o/operator/DTwin/batch0001",
            )
            self.assertFalse(batch_db.exists())
            self.assertTrue((batch_sent / batch_name).exists())

    @staticmethod
    def _create_main_db(db_path: Path, batch_name: str) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_batch (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_database_name TEXT NOT NULL,
                    remote_folder TEXT NOT NULL,
                    status TEXT NOT NULL,
                    remote_host TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gk_batch (batch_database_name, remote_folder, status, remote_host)
                VALUES (?, 'N/A', 'CREATED', '')
                """,
                (batch_name,),
            )
            conn.commit()

    @staticmethod
    def _create_batch_db(db_path: Path) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE gk_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT
                )
                """
            )
            conn.execute("INSERT INTO gk_run (status) VALUES ('TORUN')")
            conn.commit()
