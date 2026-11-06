import sqlite3
import unittest

from datamak_lite.core.pool_runtime import (
    claim_next_run,
    classify_process_result,
    finish_run,
    recover_stale_running,
)


SCHEMA = """
CREATE TABLE runs (
  id INTEGER PRIMARY KEY,
  case_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  worker_id INTEGER,
  claimed_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  exit_code INTEGER,
  stdout_log TEXT,
  stderr_log TEXT,
  last_error TEXT
);
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER,
  event TEXT NOT NULL,
  status TEXT,
  worker_id INTEGER,
  message TEXT,
  created_at TEXT NOT NULL
);
"""


class PoolRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(SCHEMA)

    def tearDown(self):
        self.conn.close()

    def test_success_is_not_overridden_by_log_content(self):
        result = classify_process_result(0, walltime_stop_requested=False)
        self.assertEqual(result.status, "SUCCESS")

    def test_walltime_stop_has_priority_over_nonzero_exit(self):
        result = classify_process_result(6, walltime_stop_requested=True)
        self.assertEqual(result.status, "INTERRUPTED")

    def test_interrupted_rows_are_claimable_before_later_pending_rows(self):
        self.conn.execute(
            "INSERT INTO runs(id,case_id,status) VALUES (1,'restart','INTERRUPTED')"
        )
        self.conn.execute(
            "INSERT INTO runs(id,case_id,status) VALUES (2,'fresh','PENDING')"
        )
        self.conn.commit()
        row = claim_next_run(
            self.conn, worker_id=3, timestamp="2026-07-28T00:00:00Z"
        )
        self.assertEqual(row["case_id"], "restart")
        status = self.conn.execute(
            "SELECT status FROM runs WHERE id=1"
        ).fetchone()[0]
        self.assertEqual(status, "RUNNING")

    def test_finish_uses_exit_code_not_traceback_text(self):
        self.conn.execute(
            "INSERT INTO runs(id,case_id,status) VALUES (1,'case','RUNNING')"
        )
        self.conn.commit()
        result = finish_run(
            self.conn,
            run_id=1,
            worker_id=0,
            exit_code=0,
            walltime_stop_requested=False,
            stderr_log="contains-a-Traceback.err",
            timestamp="2026-07-28T00:10:00Z",
        )
        self.assertEqual(result.status, "SUCCESS")
        row = self.conn.execute(
            "SELECT status,exit_code,last_error FROM runs WHERE id=1"
        ).fetchone()
        self.assertEqual(row, ("SUCCESS", 0, None))

    def test_recover_running_preserves_files_by_only_updating_sql(self):
        self.conn.execute(
            "INSERT INTO runs(id,case_id,status) VALUES (1,'case','RUNNING')"
        )
        self.conn.commit()
        count = recover_stale_running(
            self.conn, timestamp="2026-07-28T00:20:00Z"
        )
        self.assertEqual(count, 1)
        status = self.conn.execute(
            "SELECT status FROM runs WHERE id=1"
        ).fetchone()[0]
        self.assertEqual(status, "INTERRUPTED")


if __name__ == "__main__":
    unittest.main()
