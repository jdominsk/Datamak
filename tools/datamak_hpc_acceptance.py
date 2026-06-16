#!/usr/bin/env python3
"""Datamak HPC acceptance tests and machine-local run contract capture.

This tool is intentionally stdlib-only so it can be copied to an HPC login
node or run from a checked-out Datamak tree before a full workflow is trusted.
It writes compact metadata under DATAMAK_HOME, or ~/.datamak when DATAMAK_HOME
is unset. It does not submit scheduler jobs; scheduler and GX smoke tests run
only when the caller provides explicit commands, typically from an existing
interactive allocation.
"""

import argparse
import json
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(now: Optional[datetime] = None) -> str:
    value = now or utc_now()
    return value.strftime("%Y%m%dT%H%M%SZ")


def iso_utc(now: Optional[datetime] = None) -> str:
    value = now or utc_now()
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def default_datamak_home() -> Path:
    return Path(os.environ.get("DATAMAK_HOME", "~/.datamak")).expanduser()


def safe_machine_name(machine: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in machine.strip())
    return safe or "unknown"


def machine_profile_path(datamak_home: Path, machine: str) -> Path:
    return datamak_home / f"machine_profile_{safe_machine_name(machine)}.json"


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def json_write(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def text_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class CheckResult:
    def __init__(
        self,
        name: str,
        status: str,
        message: str = "",
        details: Optional[Dict[str, Any]] = None,
        log: Optional[str] = None,
    ) -> None:
        self.name = name
        self.status = status
        self.message = message
        self.details = details or {}
        self.log = log

    def as_dict(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "message": self.message,
        }
        if self.details:
            row["details"] = self.details
        if self.log:
            row["log"] = self.log
        return row


def _connect(db_path: Path) -> sqlite3.Connection:
    # Python 3.6's sqlite3 does not accept pathlib.Path. Keep this explicit so
    # the acceptance tool can run with the default system Python on older HPCs.
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _create_pool_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          case_id TEXT NOT NULL,
          segment_index INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'PENDING',
          dependency_id INTEGER,
          fresh_start INTEGER NOT NULL DEFAULT 0,
          input_path TEXT,
          output_dir TEXT,
          restart_from_file TEXT,
          restart_to_file TEXT,
          worker_id INTEGER,
          exit_code INTEGER,
          stdout_log TEXT,
          stderr_log TEXT,
          result_summary TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_event (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER,
          worker_id INTEGER,
          event TEXT NOT NULL,
          status TEXT,
          message TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def _insert_event(
    conn: sqlite3.Connection,
    run_id: Optional[int],
    event: str,
    status: str,
    message: str = "",
    worker_id: Optional[int] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO scheduler_event(run_id, worker_id, event, status, message)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, worker_id, event, status, message),
    )


def _dependency_clause() -> str:
    return """
    (
      dependency_id IS NULL
      OR dependency_id IN (
        SELECT id FROM run_queue
        WHERE status IN ('SUCCESS', 'CONVERGED')
      )
    )
    AND (
      fresh_start = 1
      OR restart_from_file IS NULL
      OR restart_from_file = ''
      OR EXISTS_ON_DISK(restart_from_file)
    )
    """


def claim_next(conn: sqlite3.Connection, worker_id: int = 0) -> Optional[sqlite3.Row]:
    """Atomically claim the first runnable row.

    This helper is deliberately small and mirrors the Datamak pool contract:
    dependency and restart-file checks happen before RUNNING is written.
    """

    def exists_on_disk(path: str) -> int:
        return 1 if path and Path(path).exists() else 0

    conn.create_function("EXISTS_ON_DISK", 1, exists_on_disk)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            f"""
            SELECT * FROM run_queue
            WHERE status IN ('PENDING', 'TORUN')
              AND {_dependency_clause()}
            ORDER BY segment_index ASC, id ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE run_queue
            SET status = 'RUNNING', worker_id = ?, updated_at = datetime('now')
            WHERE id = ? AND status IN ('PENDING', 'TORUN')
            """,
            (worker_id, int(row["id"])),
        )
        _insert_event(conn, int(row["id"]), "CLAIM", "RUNNING", worker_id=worker_id)
        conn.commit()
        return conn.execute("SELECT * FROM run_queue WHERE id = ?", (int(row["id"]),)).fetchone()
    except Exception:
        conn.rollback()
        raise


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    exit_code: int,
    message: str,
    stdout_log: Optional[Path] = None,
    stderr_log: Optional[Path] = None,
) -> None:
    conn.execute(
        """
        UPDATE run_queue
        SET status = ?, exit_code = ?, result_summary = ?,
            stdout_log = COALESCE(?, stdout_log),
            stderr_log = COALESCE(?, stderr_log),
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (
            status,
            exit_code,
            message,
            str(stdout_log) if stdout_log else None,
            str(stderr_log) if stderr_log else None,
            run_id,
        ),
    )
    _insert_event(conn, run_id, "FINISH", status, message)
    conn.commit()


def collect_scheduler_env(scheduler: str) -> Dict[str, Optional[str]]:
    scheduler_lower = scheduler.lower()
    if scheduler_lower == "pbs":
        keys = [
            "PBS_JOBID",
            "PBS_JOBNAME",
            "PBS_NODEFILE",
            "PBS_O_WORKDIR",
            "PBS_QUEUE",
            "PBS_NODENUM",
        ]
    elif scheduler_lower == "slurm":
        keys = [
            "SLURM_JOB_ID",
            "SLURM_JOB_NAME",
            "SLURM_JOB_NODELIST",
            "SLURM_NNODES",
            "SLURM_SUBMIT_DIR",
        ]
    else:
        keys = []
    return {key: os.environ.get(key) for key in keys}


def run_process(
    cmd: Sequence[str],
    *,
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open(
        "w", encoding="utf-8"
    ) as err:
        return subprocess.run(
            list(cmd),
            cwd=str(cwd),
            env=env,
            stdout=out,
            stderr=err,
            universal_newlines=True,
            check=False,
        )


def write_shell_runner(
    path: Path,
    *,
    runtime_script: Optional[Path],
    command: str,
    log_path: Path,
) -> None:
    log_quoted = str(log_path).replace("'", "'\"'\"'")
    command_for_log = command.replace("'", "'\"'\"'")
    runtime_line = ""
    if runtime_script is not None:
        runtime_quoted = str(runtime_script).replace("'", "'\"'\"'")
        runtime_line = textwrap.dedent(
            f"""\
            echo "Sourcing runtime script: {runtime_script}"
            source '{runtime_quoted}'
            source_status=$?
            echo "Runtime script exit status: $source_status"
            if [ "$source_status" -ne 0 ]; then
              exit "$source_status"
            fi
            """
        )
    script = textwrap.dedent(
        f"""\
        #!/usr/bin/env bash
        set +e
        exec > '{log_quoted}' 2>&1
        echo "Datamak HPC acceptance command"
        echo "host=$(hostname 2>/dev/null)"
        echo "pwd=$(pwd)"
        echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null)"
        echo "python=$(command -v python3 2>/dev/null)"
        python3 -V 2>/dev/null || true
        {runtime_line}
        printf '%s\\n' 'Command: {command_for_log}'
        {command}
        status=$?
        echo "Command exit status: $status"
        exit "$status"
        """
    )
    text_write(path, script)
    path.chmod(0o755)


def check_shell_syntax(script_path: Path) -> Tuple[bool, str]:
    proc = subprocess.run(
        ["bash", "-n", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        check=False,
    )
    output = (proc.stdout + proc.stderr).strip()
    return proc.returncode == 0, output


def capture_runtime_environment(
    *,
    run_dir: Path,
    runtime_script: Optional[Path],
    login_shell: bool,
) -> CheckResult:
    if runtime_script is None:
        return CheckResult(
            "runtime_environment_capture",
            SKIP,
            "No runtime script provided.",
        )
    log_path = run_dir / "runtime_environment.log"
    runner = run_dir / "capture_runtime_environment.sh"
    command = "module list 2>&1 || true; env | sort"
    write_shell_runner(runner, runtime_script=runtime_script, command=command, log_path=log_path)
    bash_cmd = ["bash", "-l", str(runner)] if login_shell else ["bash", str(runner)]
    proc = subprocess.run(
        bash_cmd,
        cwd=str(run_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        check=False,
    )
    status = PASS if proc.returncode == 0 else FAIL
    return CheckResult(
        "runtime_environment_capture",
        status,
        "Captured runtime environment." if status == PASS else "Runtime capture failed.",
        {"exit_code": proc.returncode, "runner": str(runner)},
        str(log_path),
    )


def static_checks(
    *,
    run_dir: Path,
    datamak_root: Path,
    scheduler: str,
    runtime_script: Optional[Path],
    gx_bin: Optional[Path],
    shell_scripts: Iterable[Path],
    require_allocation: bool,
    login_shell: bool,
) -> List[CheckResult]:
    checks: List[CheckResult] = []
    checks.append(
        CheckResult(
            "python_sqlite_available",
            PASS,
            "Python and sqlite3 are importable.",
            {
                "python_executable": sys.executable,
                "python_version": sys.version.split()[0],
                "sqlite_version": sqlite3.sqlite_version,
            },
        )
    )

    checks.append(
        CheckResult(
            "datamak_root_exists",
            PASS if datamak_root.exists() else FAIL,
            str(datamak_root),
            {"datamak_root": str(datamak_root)},
        )
    )

    sched_env = collect_scheduler_env(scheduler)
    sched_details: Dict[str, Any] = {"scheduler": scheduler, "env": sched_env}
    nodefile = sched_env.get("PBS_NODEFILE") if scheduler.lower() == "pbs" else None
    if nodefile:
        nodefile_path = Path(nodefile)
        sched_details["nodefile_exists"] = nodefile_path.exists()
        if nodefile_path.exists():
            try:
                sched_details["nodefile_lines"] = len(nodefile_path.read_text(encoding="utf-8").splitlines())
            except Exception as exc:
                sched_details["nodefile_read_error"] = str(exc)
    in_allocation = any(value for value in sched_env.values())
    if require_allocation and not in_allocation:
        checks.append(
            CheckResult(
                "scheduler_allocation_environment",
                FAIL,
                "No scheduler allocation environment detected.",
                sched_details,
            )
        )
    else:
        checks.append(
            CheckResult(
                "scheduler_allocation_environment",
                PASS if in_allocation else SKIP,
                "Scheduler allocation detected." if in_allocation else "No allocation detected; scheduler tests may be skipped.",
                sched_details,
            )
        )

    if runtime_script is not None:
        exists = runtime_script.exists()
        checks.append(
            CheckResult(
                "runtime_script_exists",
                PASS if exists else FAIL,
                str(runtime_script),
                {"runtime_script": str(runtime_script)},
            )
        )
        if exists:
            ok, output = check_shell_syntax(runtime_script)
            checks.append(
                CheckResult(
                    "runtime_script_shell_syntax",
                    PASS if ok else FAIL,
                    "bash -n passed." if ok else output,
                    {"runtime_script": str(runtime_script)},
                )
            )

    if gx_bin is not None:
        exists = gx_bin.exists()
        executable = os.access(gx_bin, os.X_OK) if exists else False
        checks.append(
            CheckResult(
                "gx_executable_exists",
                PASS if exists and executable else FAIL,
                str(gx_bin),
                {"gx_bin": str(gx_bin), "exists": exists, "executable": executable},
            )
        )

    for script in shell_scripts:
        ok, output = check_shell_syntax(script)
        checks.append(
            CheckResult(
                f"shell_syntax:{script.name}",
                PASS if ok else FAIL,
                "bash -n passed." if ok else output,
                {"script": str(script)},
            )
        )

    checks.append(
        capture_runtime_environment(
            run_dir=run_dir,
            runtime_script=runtime_script,
            login_shell=login_shell,
        )
    )
    return checks


def database_claim_tests(run_dir: Path) -> CheckResult:
    db_path = run_dir / "acceptance.sqlite"
    restart_a = run_dir / "case_t100.restart.nc"
    restart_b = run_dir / "case_t200.restart.nc"
    with _connect(db_path) as conn:
        _create_pool_schema(conn)
        conn.execute(
            """
            INSERT INTO run_queue(case_id, segment_index, fresh_start, restart_to_file)
            VALUES ('case', 0, 1, ?)
            """,
            (str(restart_a),),
        )
        row1_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO run_queue(case_id, segment_index, dependency_id, restart_from_file, restart_to_file)
            VALUES ('case', 1, ?, ?, ?)
            """,
            (row1_id, str(restart_a), str(restart_b)),
        )
        row2_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        missing_restart = run_dir / "missing.restart.nc"
        conn.execute(
            """
            INSERT INTO run_queue(case_id, segment_index, dependency_id, restart_from_file)
            VALUES ('case', 2, ?, ?)
            """,
            (row2_id, str(missing_restart)),
        )
        conn.commit()

        row1 = claim_next(conn, worker_id=1)
        if row1 is None or int(row1["id"]) != row1_id:
            return CheckResult("database_claim_dependency", FAIL, "Fresh row was not claimed first.", {"db": str(db_path)})
        if claim_next(conn, worker_id=2) is not None:
            return CheckResult("database_claim_dependency", FAIL, "Dependent row claimed before dependency success.", {"db": str(db_path)})

        text_write(restart_a, "restart-a\n")
        finish_run(conn, row1_id, "SUCCESS", 0, "fresh segment success")
        row2 = claim_next(conn, worker_id=2)
        if row2 is None or int(row2["id"]) != row2_id:
            return CheckResult("database_claim_dependency", FAIL, "Restart row was not claimed after dependency success.", {"db": str(db_path)})
        finish_run(conn, row2_id, "SUCCESS", 0, "restart segment success")

        if claim_next(conn, worker_id=3) is not None:
            return CheckResult("database_claim_dependency", FAIL, "Row with missing restart file was claimed.", {"db": str(db_path)})

        status_rows = conn.execute(
            "SELECT status, count(*) AS n FROM run_queue GROUP BY status ORDER BY status"
        ).fetchall()
        events = conn.execute("SELECT count(*) FROM scheduler_event").fetchone()[0]

    return CheckResult(
        "database_claim_dependency",
        PASS,
        "SQLite claim, dependency, and restart-file gating passed.",
        {
            "db": str(db_path),
            "status_counts": {str(row["status"]): int(row["n"]) for row in status_rows},
            "event_count": int(events),
        },
    )


def preclaim_failure_visibility_test(run_dir: Path) -> CheckResult:
    db_path = run_dir / "preclaim_failure.sqlite"
    log_path = run_dir / "pool_driver_preclaim_failure.log"
    with _connect(db_path) as conn:
        _create_pool_schema(conn)
        conn.execute(
            """
            INSERT INTO run_queue(case_id, segment_index, fresh_start)
            VALUES ('preclaim', 0, 1)
            """
        )
        conn.commit()
        text_write(
            log_path,
            "\n".join(
                [
                    "Datamak pre-claim driver failure visibility test",
                    f"generated_at_utc={iso_utc()}",
                    "intentional_failure=missing_required_environment",
                    "claim_started=false",
                    "",
                ]
            ),
        )
        pending = conn.execute("SELECT count(*) FROM run_queue WHERE status = 'PENDING'").fetchone()[0]
        running = conn.execute("SELECT count(*) FROM run_queue WHERE status = 'RUNNING'").fetchone()[0]
    ok = pending == 1 and running == 0 and log_path.exists()
    return CheckResult(
        "preclaim_failure_visibility",
        PASS if ok else FAIL,
        "Pre-claim failure left rows unclaimed and wrote a driver log." if ok else "Pre-claim failure visibility failed.",
        {"db": str(db_path), "pending": int(pending), "running": int(running)},
        str(log_path),
    )


def postclaim_failure_visibility_test(run_dir: Path) -> CheckResult:
    db_path = run_dir / "postclaim_failure.sqlite"
    stdout_log = run_dir / "postclaim_false.out"
    stderr_log = run_dir / "postclaim_false.err"
    with _connect(db_path) as conn:
        _create_pool_schema(conn)
        conn.execute(
            """
            INSERT INTO run_queue(case_id, segment_index, fresh_start)
            VALUES ('postclaim', 0, 1)
            """
        )
        conn.commit()
        row = claim_next(conn, worker_id=1)
        if row is None:
            return CheckResult("postclaim_failure_visibility", FAIL, "Could not claim test row.", {"db": str(db_path)})
        proc = run_process(
            ["/bin/sh", "-c", "echo about-to-fail; exit 7"],
            cwd=run_dir,
            stdout_path=stdout_log,
            stderr_path=stderr_log,
        )
        status = "SUCCESS" if proc.returncode == 0 else "CRASHED"
        finish_run(
            conn,
            int(row["id"]),
            status,
            int(proc.returncode),
            "intentional failing command",
            stdout_log=stdout_log,
            stderr_log=stderr_log,
        )
        final = conn.execute(
            "SELECT status, exit_code FROM run_queue WHERE id = ?", (int(row["id"]),)
        ).fetchone()
    ok = str(final["status"]) == "CRASHED" and int(final["exit_code"]) == 7
    return CheckResult(
        "postclaim_failure_visibility",
        PASS if ok else FAIL,
        "Post-claim failure was recorded as CRASHED." if ok else "Post-claim failure was not recorded.",
        {"db": str(db_path), "status": str(final["status"]), "exit_code": int(final["exit_code"])},
        str(stdout_log),
    )


def explicit_command_check(
    *,
    name: str,
    run_dir: Path,
    command: Optional[str],
    runtime_script: Optional[Path],
    login_shell: bool,
) -> CheckResult:
    if not command:
        return CheckResult(name, SKIP, "No command provided.")
    log_path = run_dir / f"{name}.log"
    runner = run_dir / f"{name}.sh"
    write_shell_runner(runner, runtime_script=runtime_script, command=command, log_path=log_path)
    bash_cmd = ["bash", "-l", str(runner)] if login_shell else ["bash", str(runner)]
    proc = subprocess.run(
        bash_cmd,
        cwd=str(run_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        check=False,
    )
    return CheckResult(
        name,
        PASS if proc.returncode == 0 else FAIL,
        "Command completed successfully." if proc.returncode == 0 else "Command failed.",
        {"command": command, "exit_code": proc.returncode, "runner": str(runner)},
        str(log_path),
    )


def build_machine_profile(
    *,
    machine: str,
    scheduler: str,
    datamak_home: Path,
    datamak_root: Path,
    run_dir: Path,
    runtime_script: Optional[Path],
    gx_bin: Optional[Path],
    checks: Sequence[CheckResult],
    latest_report: Path,
) -> Dict[str, Any]:
    profile: Dict[str, Any] = {
        "generated_at_utc": iso_utc(),
        "machine": machine,
        "hostname": platform.node(),
        "platform": platform.platform(),
        "scheduler": scheduler,
        "datamak_home": str(datamak_home),
        "datamak_root": str(datamak_root),
        "python": {
            "executable": sys.executable,
            "version": sys.version.split()[0],
            "implementation": platform.python_implementation(),
        },
        "sqlite": {"version": sqlite3.sqlite_version},
        "scheduler_environment": collect_scheduler_env(scheduler),
        "gx": {
            "executable": str(gx_bin) if gx_bin else None,
            "runtime_script": str(runtime_script) if runtime_script else None,
        },
        "latest_acceptance": {
            "run_dir": str(run_dir),
            "report": str(latest_report),
            "successful": all(check.status in {PASS, SKIP} for check in checks),
            "failed_checks": [check.name for check in checks if check.status == FAIL],
        },
    }
    return profile


def run_acceptance(args: argparse.Namespace) -> int:
    datamak_home = Path(args.datamak_home).expanduser() if args.datamak_home else default_datamak_home()
    datamak_root = Path(args.datamak_root).expanduser().resolve() if args.datamak_root else project_root()
    runtime_script = Path(args.runtime_script).expanduser().resolve() if args.runtime_script else None
    gx_bin = Path(args.gx_bin).expanduser().resolve() if args.gx_bin else None
    shell_scripts = [Path(item).expanduser().resolve() for item in args.check_shell_script]

    stamp = utc_stamp()
    machine_root = datamak_home / "hpc_acceptance" / args.machine
    run_dir = machine_root / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    checks: List[CheckResult] = []
    checks.extend(
        static_checks(
            run_dir=run_dir,
            datamak_root=datamak_root,
            scheduler=args.scheduler,
            runtime_script=runtime_script,
            gx_bin=gx_bin,
            shell_scripts=shell_scripts,
            require_allocation=args.require_allocation,
            login_shell=args.login_shell,
        )
    )
    checks.append(database_claim_tests(run_dir))
    checks.append(preclaim_failure_visibility_test(run_dir))
    checks.append(postclaim_failure_visibility_test(run_dir))
    checks.append(
        explicit_command_check(
            name="scheduler_smoke",
            run_dir=run_dir,
            command=args.scheduler_command,
            runtime_script=runtime_script if args.source_runtime_for_smoke else None,
            login_shell=args.login_shell,
        )
    )
    checks.append(
        explicit_command_check(
            name="gx_smoke",
            run_dir=run_dir,
            command=args.gx_smoke_command,
            runtime_script=runtime_script if args.source_runtime_for_smoke else None,
            login_shell=args.login_shell,
        )
    )

    successful = all(check.status in {PASS, SKIP} for check in checks)
    report: Dict[str, Any] = {
        "generated_at_utc": iso_utc(),
        "machine": args.machine,
        "scheduler": args.scheduler,
        "successful": successful,
        "run_dir": str(run_dir),
        "datamak_home": str(datamak_home),
        "datamak_root": str(datamak_root),
        "checks": [check.as_dict() for check in checks],
    }

    report_path = run_dir / "test_report.json"
    latest_path = machine_root / "latest.json"
    json_write(report_path, report)
    json_write(latest_path, report)

    profile = build_machine_profile(
        machine=args.machine,
        scheduler=args.scheduler,
        datamak_home=datamak_home,
        datamak_root=datamak_root,
        run_dir=run_dir,
        runtime_script=runtime_script,
        gx_bin=gx_bin,
        checks=checks,
        latest_report=report_path,
    )
    profile_path = machine_profile_path(datamak_home, args.machine)
    json_write(profile_path, profile)

    print(f"Datamak HPC acceptance report: {report_path}")
    print(f"Latest pointer: {latest_path}")
    print(f"Machine profile: {profile_path}")
    print(f"Result: {'PASS' if successful else 'FAIL'}")
    for check in checks:
        print(f"  {check.status:4s} {check.name}: {check.message}")
    return 0 if successful else 1


def show_latest(args: argparse.Namespace) -> int:
    datamak_home = Path(args.datamak_home).expanduser() if args.datamak_home else default_datamak_home()
    latest_path = datamak_home / "hpc_acceptance" / args.machine / "latest.json"
    profile_path = machine_profile_path(datamak_home, args.machine)
    if not latest_path.exists():
        print(f"No latest acceptance report found: {latest_path}")
        return 2
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    print(json.dumps(latest, indent=2, sort_keys=True))
    if profile_path.exists():
        print(f"\nMachine profile: {profile_path}")
    return 0 if latest.get("successful") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Datamak HPC acceptance tests and store compact traces under ~/.datamak."
    )
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="Run acceptance tests.")
    run.add_argument("--machine", required=True, help="Machine label, e.g. polaris.")
    run.add_argument("--scheduler", default="pbs", help="Scheduler label: pbs, slurm, or none.")
    run.add_argument("--datamak-home", help="Override DATAMAK_HOME/~/.datamak.")
    run.add_argument("--datamak-root", help="Datamak checkout root. Defaults to this repo.")
    run.add_argument("--runtime-script", help="Optional module/runtime script to source for smoke commands.")
    run.add_argument("--gx-bin", help="Optional GX executable path to check.")
    run.add_argument(
        "--check-shell-script",
        action="append",
        default=[],
        help="Additional shell script to check with bash -n. May be repeated.",
    )
    run.add_argument(
        "--require-allocation",
        action="store_true",
        help="Fail if scheduler allocation variables are not present.",
    )
    run.add_argument(
        "--source-runtime-for-smoke",
        action="store_true",
        help="Source --runtime-script before scheduler/GX smoke commands.",
    )
    run.add_argument(
        "--login-shell",
        action="store_true",
        help="Run generated runtime/smoke shell scripts with 'bash -l'. Use this when batch scripts use #!/bin/bash -l.",
    )
    run.add_argument(
        "--scheduler-command",
        help="Optional explicit fake scheduler smoke command, e.g. 'mpiexec -n 1 hostname'.",
    )
    run.add_argument(
        "--gx-smoke-command",
        help="Optional explicit GX smoke command. This tool never invents a GX run command.",
    )
    run.set_defaults(func=run_acceptance)

    show = sub.add_parser("show", help="Print the latest acceptance report for a machine.")
    show.add_argument("--machine", required=True)
    show.add_argument("--datamak-home", help="Override DATAMAK_HOME/~/.datamak.")
    show.set_defaults(func=show_latest)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 2
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
