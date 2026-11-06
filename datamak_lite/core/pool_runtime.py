"""Reusable SQLite state transitions for allocation-local worker pools."""

from __future__ import annotations

import datetime as _datetime
import sqlite3
from dataclasses import dataclass
from typing import Optional, Sequence


CLAIMABLE_STATUSES = ("PENDING", "INTERRUPTED")


@dataclass(frozen=True)
class ProcessResult:
    status: str
    message: str


def utc_now() -> str:
    return _datetime.datetime.now(_datetime.timezone.utc).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")


def classify_process_result(
    exit_code: int, *, walltime_stop_requested: bool
) -> ProcessResult:
    """Classify only from the process result and explicit stop protocol.

    Log text is diagnostic evidence, not an exit-status authority. In
    particular, accumulated warnings or tracebacks must never overwrite a
    successful scheduler/application return code.
    """

    if walltime_stop_requested:
        return ProcessResult(
            "INTERRUPTED",
            "Walltime monitor requested a clean stop; restart state retained",
        )
    if int(exit_code) == 0:
        return ProcessResult("SUCCESS", "Application launcher exited successfully")
    return ProcessResult("CRASHED", "Application launcher exited nonzero")


def claim_next_run(
    conn: sqlite3.Connection,
    *,
    worker_id: int,
    claimable_statuses: Sequence[str] = CLAIMABLE_STATUSES,
    timestamp: Optional[str] = None,
) -> Optional[sqlite3.Row]:
    """Atomically claim the oldest pending or interrupted run."""

    if not claimable_statuses:
        raise ValueError("claimable_statuses must not be empty")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("BEGIN IMMEDIATE")
    placeholders = ",".join("?" for _ in claimable_statuses)
    row = conn.execute(
        "SELECT * FROM runs "
        f"WHERE status IN ({placeholders}) "
        "ORDER BY id LIMIT 1",
        tuple(claimable_statuses),
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    when = timestamp or utc_now()
    conn.execute(
        """
        UPDATE runs
        SET status='RUNNING', worker_id=?, claimed_at=?, started_at=?,
            finished_at=NULL, exit_code=NULL, last_error=NULL
        WHERE id=?
        """,
        (worker_id, when, when, row["id"]),
    )
    conn.execute(
        """
        INSERT INTO events(run_id,event,status,worker_id,message,created_at)
        VALUES (?, 'CLAIM', 'RUNNING', ?, ?, ?)
        """,
        (row["id"], worker_id, "Atomic worker claim", when),
    )
    conn.commit()
    return row


def finish_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    worker_id: int,
    exit_code: int,
    walltime_stop_requested: bool,
    stdout_log: Optional[str] = None,
    stderr_log: Optional[str] = None,
    timestamp: Optional[str] = None,
) -> ProcessResult:
    """Finish a run without deriving status from stdout or stderr content."""

    result = classify_process_result(
        exit_code, walltime_stop_requested=walltime_stop_requested
    )
    when = timestamp or utc_now()
    cursor = conn.execute(
        """
        UPDATE runs
        SET status=?, finished_at=?, exit_code=?, stdout_log=?,
            stderr_log=?, last_error=?
        WHERE id=?
        """,
        (
            result.status,
            when,
            int(exit_code),
            stdout_log,
            stderr_log,
            None if result.status == "SUCCESS" else result.message,
            run_id,
        ),
    )
    if cursor.rowcount != 1:
        raise ValueError(f"Unknown run id {run_id}")
    conn.execute(
        """
        INSERT INTO events(run_id,event,status,worker_id,message,created_at)
        VALUES (?, 'FINISH', ?, ?, ?, ?)
        """,
        (run_id, result.status, worker_id, result.message, when),
    )
    conn.commit()
    return result


def recover_stale_running(
    conn: sqlite3.Connection, *, timestamp: Optional[str] = None
) -> int:
    """Make stale RUNNING rows restart-claimable without touching run files."""

    when = timestamp or utc_now()
    rows = conn.execute("SELECT id FROM runs WHERE status='RUNNING'").fetchall()
    for row in rows:
        run_id = int(row[0])
        conn.execute(
            """
            UPDATE runs
            SET status='INTERRUPTED', worker_id=NULL, claimed_at=NULL,
                started_at=NULL, finished_at=?, exit_code=NULL,
                last_error='Recovered stale RUNNING row; restart files preserved'
            WHERE id=?
            """,
            (when, run_id),
        )
        conn.execute(
            """
            INSERT INTO events(run_id,event,status,message,created_at)
            VALUES (?, 'RECOVER', 'INTERRUPTED', ?, ?)
            """,
            (
                run_id,
                "Recovered stale RUNNING row; restart files preserved",
                when,
            ),
        )
    conn.commit()
    return len(rows)
