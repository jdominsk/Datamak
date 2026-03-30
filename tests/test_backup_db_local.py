import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools import backup_db_local


class BackupDbLocalTests(unittest.TestCase):
    def test_backup_database_local_creates_sqlite_copy_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "gyrokinetic_simulations.db"
            backup_dir = tmp_path / "backup"
            status_path = tmp_path / "status.json"

            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
                conn.execute("INSERT INTO sample (value) VALUES ('ok')")
                conn.commit()

            result = backup_db_local.backup_database_local(
                db_path=db_path,
                backup_dir=backup_dir,
                status_path=status_path,
            )

            backup_path = backup_dir / "gyrokinetic_simulations.db"
            self.assertEqual(result["state"], "ok")
            self.assertTrue(backup_path.exists())
            with sqlite3.connect(backup_path) as conn:
                row = conn.execute("SELECT value FROM sample").fetchone()
            self.assertEqual(row[0], "ok")

            status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual(status["state"], "ok")
            self.assertEqual(status["backup_path"], str(backup_path))

    def test_check_backup_status_flags_missing_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            status_path = Path(tmpdir) / "status.json"
            status_path.write_text(json.dumps({"state": "error"}), encoding="utf-8")

            exit_code = backup_db_local.check_backup_status(
                status_path=status_path,
                max_age_hours=30.0,
            )

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
