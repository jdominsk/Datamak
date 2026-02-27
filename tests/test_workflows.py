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


if __name__ == "__main__":
    unittest.main()
