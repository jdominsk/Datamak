#!/usr/bin/env python3
import argparse
import os
import shlex
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple


ROOT_DIR = Path(
    os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[2])
).resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_flux_profile  # noqa: E402
from database.create_gyrokinetic_db import ensure_flux_action_log_schema  # noqa: E402


ACTIVE_QUEUE_STATES = {
    "CONFIGURING",
    "COMPLETING",
    "PENDING",
    "RUNNING",
    "SIGNALING",
    "SUSPENDED",
    "STAGE_OUT",
}
SSH_WITH_DUO = ROOT_DIR / "tools" / "ssh_with_duo.py"
FAILED_SACCT_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "TIMEOUT",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the saved Slurm job state for a full-auto Flux origin."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Main Datamak database.",
    )
    parser.add_argument("--origin-id", type=int, help="data_origin.id to inspect.")
    parser.add_argument("--origin-name", default="", help="Fallback data_origin.name.")
    return parser.parse_args()


def _ssh_control_options() -> list[str]:
    return [
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPersist=10m",
        "-o",
        f"ControlPath={Path.home() / '.ssh' / 'cm-%r@%h:%p'}",
    ]


def _ssh_command(flux_profile: Dict[str, str], *args: str) -> list[str]:
    duo_option = str(flux_profile.get("duo_option") or "").strip()
    if duo_option:
        return [
            sys.executable,
            str(SSH_WITH_DUO),
            "--duo-option",
            duo_option,
            "--",
            "ssh",
            *args,
        ]
    return ["ssh", *args]


def _prime_ssh(remote_host: str, flux_profile: Dict[str, str]) -> None:
    subprocess.run(
        _ssh_command(flux_profile, *_ssh_control_options(), "-t", remote_host, "true"),
        check=True,
    )


def load_origin(
    conn: sqlite3.Connection, origin_id: Optional[int], origin_name: str
) -> Dict[str, str]:
    if origin_id is not None:
        row = conn.execute(
            "SELECT id, name FROM data_origin WHERE id = ?",
            (origin_id,),
        ).fetchone()
        if row:
            return {"id": str(int(row[0])), "name": str(row[1] or "")}
    if origin_name.strip():
        row = conn.execute(
            "SELECT id, name FROM data_origin WHERE name = ?",
            (origin_name.strip(),),
        ).fetchone()
        if row:
            return {"id": str(int(row[0])), "name": str(row[1] or "")}
    raise SystemExit("Selected data_origin was not found in the main DB.")


def latest_flux_action(
    conn: sqlite3.Connection, origin_id: int, origin_name: str
) -> Optional[Dict[str, str]]:
    row = conn.execute(
        """
        SELECT id, flux_db_name, remote_host, remote_dir, status, slurm_job_id,
               status_detail, submitted_at, status_checked_at, synced_at, created_at
        FROM flux_action_log
        WHERE data_origin_id = ?
           OR data_origin_name = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (origin_id, origin_name),
    ).fetchone()
    if not row:
        return None
    return {
        "id": str(row[0] or ""),
        "flux_db_name": str(row[1] or ""),
        "remote_host": str(row[2] or ""),
        "remote_dir": str(row[3] or ""),
        "status": str(row[4] or ""),
        "slurm_job_id": str(row[5] or ""),
        "status_detail": str(row[6] or ""),
        "submitted_at": str(row[7] or ""),
        "status_checked_at": str(row[8] or ""),
        "synced_at": str(row[9] or ""),
        "created_at": str(row[10] or ""),
    }


def effective_remote_host(
    log_row: Optional[Dict[str, str]], flux_profile: Dict[str, str]
) -> str:
    configured_remote = str(flux_profile.get("remote") or "").strip()
    if configured_remote:
        return configured_remote
    return str((log_row or {}).get("remote_host") or "").strip()


def refresh_flux_action_endpoint(
    main_db: str,
    log_id: int,
    remote_host: str,
    remote_dir: str,
) -> None:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        conn.execute(
            """
            UPDATE flux_action_log
            SET remote_host = ?, remote_dir = ?
            WHERE id = ?
            """,
            (remote_host, remote_dir, log_id),
        )
        conn.commit()


def mark_flux_action_status(
    main_db: str,
    log_id: int,
    status: str,
    detail: str,
) -> None:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        conn.execute(
            """
            UPDATE flux_action_log
            SET status = ?,
                status_detail = ?,
                status_checked_at = datetime('now')
            WHERE id = ?
            """,
            (status, detail, log_id),
        )
        conn.commit()


def _remote_bootstrap_lines() -> list[str]:
    return [
        "set -euo pipefail",
        "if [[ -f /etc/profile ]]; then source /etc/profile; fi",
        "if [[ -f ~/.bash_profile ]]; then source ~/.bash_profile; fi",
        "if [[ -f ~/.bashrc ]]; then source ~/.bashrc; fi",
        "if command -v module >/dev/null 2>&1; then module load slurm >/dev/null 2>&1 || true; fi",
        "if ! command -v squeue >/dev/null 2>&1 || ! command -v sacct >/dev/null 2>&1; then",
        "  for candidate in /usr/bin /usr/local/bin /opt/slurm/bin /cm/shared/apps/slurm/current/bin; do",
        "    if [[ -d \"$candidate\" ]]; then export PATH=\"$candidate:$PATH\"; fi",
        "  done",
        "fi",
    ]


def _run_remote_probe(remote_host: str, body_lines: list[str], flux_profile: Dict[str, str]) -> str:
    command = [
        *_ssh_command(
            flux_profile,
            *_ssh_control_options(),
            "-t",
            remote_host,
            "bash",
            "-lc",
            "\n".join([*_remote_bootstrap_lines(), *body_lines]),
        ),
    ]
    completed = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    return completed.stdout or ""


def parse_job_token(slurm_job_id: str) -> str:
    token = (slurm_job_id or "").strip()
    if not token:
        return ""
    return token.split(";", 1)[0].strip()


def fetch_queue_state(remote_host: str, job_token: str, flux_profile: Dict[str, str]) -> Tuple[str, str]:
    stdout = _run_remote_probe(
        remote_host,
        [f"squeue -j {shlex.quote(job_token)} -h -o '%T|%M|%R' 2>/dev/null || true"],
        flux_profile,
    )
    for line in stdout.splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) == 3 and parts[0]:
            return parts[0].upper(), "|".join(parts)
    return "", ""


def fetch_sacct_state(remote_host: str, job_token: str, flux_profile: Dict[str, str]) -> Tuple[str, str]:
    stdout = _run_remote_probe(
        remote_host,
        [
            f"sacct -j {shlex.quote(job_token)} --parsable2 --noheader "
            "--format=JobIDRaw,State,ExitCode,Elapsed 2>/dev/null || true"
        ],
        flux_profile,
    )
    fallback = ""
    for line in stdout.splitlines():
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 4 or not parts[0]:
            continue
        job_id_raw = parts[0]
        state = parts[1].upper()
        if job_id_raw == job_token:
            return state, "|".join(parts[:4])
        if not fallback:
            fallback = "|".join(parts[:4])
    if fallback:
        return fallback.split("|", 2)[1].upper(), fallback
    return "", ""


def map_slurm_state(current_status: str, queue_state: str, sacct_state: str) -> Tuple[str, str]:
    queue_state = queue_state.upper().strip()
    sacct_state = sacct_state.upper().strip()
    if queue_state:
        if queue_state == "PENDING":
            return "SUBMITTED", queue_state
        if queue_state in ACTIVE_QUEUE_STATES:
            return "RUNNING", queue_state
        if queue_state == "COMPLETED":
            return "DONE", queue_state
        if queue_state in FAILED_SACCT_STATES:
            return "FAILED", queue_state
    if sacct_state:
        if sacct_state == "COMPLETED":
            return "DONE", sacct_state
        if sacct_state == "PENDING":
            return "SUBMITTED", sacct_state
        if sacct_state in ACTIVE_QUEUE_STATES:
            return "RUNNING", sacct_state
        if sacct_state in FAILED_SACCT_STATES:
            return "FAILED", sacct_state
        return current_status or "SUBMITTED", sacct_state
    return current_status or "SUBMITTED", current_status or "UNKNOWN"


def run_status_check(
    main_db: str, origin_id: Optional[int], origin_name: str
) -> Dict[str, str]:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        origin = load_origin(conn, origin_id, origin_name)
        log_row = latest_flux_action(conn, int(origin["id"]), origin["name"])
    if log_row is None:
        raise SystemExit("No Flux action is recorded for this origin.")

    log_id = int(log_row.get("id") or 0)
    current_status = str(log_row.get("status") or "").strip().upper()
    slurm_job_id = str(log_row.get("slurm_job_id") or "").strip()
    if current_status == "SYNCED":
        detail = str(log_row.get("status_detail") or "SYNCED").strip() or "SYNCED"
        return {
            "origin_name": origin["name"],
            "status": "SYNCED",
            "detail": detail,
            "slurm_job_id": slurm_job_id,
        }
    if not slurm_job_id:
        raise SystemExit("No Slurm job id is recorded for this origin.")

    flux_profile = resolve_flux_profile()
    remote_host = effective_remote_host(log_row, flux_profile)
    remote_dir = log_row.get("remote_dir") or str(flux_profile.get("base_dir") or "")
    if not remote_host:
        raise SystemExit("Flux runtime settings are incomplete for status check.")
    if log_id > 0 and remote_dir:
        refresh_flux_action_endpoint(main_db, log_id, remote_host, remote_dir)

    job_token = parse_job_token(slurm_job_id)
    if not job_token:
        raise SystemExit("Recorded Slurm job id is empty.")

    _prime_ssh(remote_host, flux_profile)
    queue_state, queue_detail = fetch_queue_state(remote_host, job_token, flux_profile)
    sacct_state = ""
    sacct_detail = ""
    if not queue_state:
        sacct_state, sacct_detail = fetch_sacct_state(remote_host, job_token, flux_profile)

    new_status, detail = map_slurm_state(current_status, queue_state, sacct_state)
    detail = queue_detail or sacct_detail or detail
    if log_id > 0:
        mark_flux_action_status(main_db, log_id, new_status, detail)
    return {
        "origin_name": origin["name"],
        "status": new_status,
        "detail": detail,
        "slurm_job_id": slurm_job_id,
    }


def main() -> None:
    args = parse_args()
    result = run_status_check(args.db, args.origin_id, args.origin_name)
    detail = f" ({result['detail']})" if result["detail"] else ""
    job = f" job {result['slurm_job_id']}" if result["slurm_job_id"] else ""
    print(
        f"Flux status for {result['origin_name']}: {result['status']}{detail}{job}"
    )


if __name__ == "__main__":
    main()
