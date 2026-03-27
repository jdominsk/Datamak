#!/usr/bin/env python3
import argparse
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional


ROOT_DIR = Path(
    os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[2])
).resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_flux_profile, split_remote_host  # noqa: E402
from database.create_gyrokinetic_db import ensure_flux_action_log_schema  # noqa: E402


SCRIPT_DIR = Path(__file__).resolve().parent
STEP3_SCRIPT = SCRIPT_DIR / "MainSteps_3_launch_on_laptop.sh"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync the latest staged Flux DB for a selected origin back into the main DB."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Main Datamak database.",
    )
    parser.add_argument("--origin-id", type=int, help="data_origin.id to sync.")
    parser.add_argument("--origin-name", default="", help="Fallback data_origin.name.")
    return parser.parse_args()


def load_origin(conn: sqlite3.Connection, origin_id: Optional[int], origin_name: str) -> Dict[str, str]:
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
    try:
        row = conn.execute(
            """
            SELECT id, flux_db_name, remote_host, remote_dir, status, slurm_job_id,
                   submitted_at, synced_at, created_at
            FROM flux_action_log
            WHERE data_origin_id = ?
               OR data_origin_name = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (origin_id, origin_name),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return {
        "id": str(row[0] or ""),
        "flux_db_name": str(row[1] or ""),
        "remote_host": str(row[2] or ""),
        "remote_dir": str(row[3] or ""),
        "status": str(row[4] or ""),
        "slurm_job_id": str(row[5] or ""),
        "submitted_at": str(row[6] or ""),
        "synced_at": str(row[7] or ""),
        "created_at": str(row[8] or ""),
    }


def effective_remote_host(log_row: Optional[Dict[str, str]], flux_profile: Dict[str, str]) -> str:
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


def mark_flux_action_synced(main_db: str, log_id: int) -> None:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        conn.execute(
            """
            UPDATE flux_action_log
            SET status = 'SYNCED',
                status_detail = 'SYNCED',
                status_checked_at = datetime('now'),
                synced_at = datetime('now')
            WHERE id = ?
            """,
            (log_id,),
        )
        conn.commit()


def run_sync(main_db: str, origin_id: Optional[int], origin_name: str) -> Dict[str, str]:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        origin = load_origin(conn, origin_id, origin_name)
        log_row = latest_flux_action(conn, int(origin["id"]), origin["name"])
    if log_row is None or not log_row.get("flux_db_name"):
        raise SystemExit("No staged Flux DB is recorded for this origin in flux_action_log.")

    flux_profile = resolve_flux_profile()
    remote_host = effective_remote_host(log_row, flux_profile)
    remote_dir = log_row.get("remote_dir") or str(flux_profile.get("base_dir") or "")
    remote_db_name = log_row.get("flux_db_name") or ""
    if not remote_host or not remote_dir or not remote_db_name:
        raise SystemExit("Flux action log entry is incomplete for this origin.")
    remote_db_path = str(Path(remote_dir) / remote_db_name)

    log_id = int(log_row.get("id") or 0)
    if log_id > 0:
        refresh_flux_action_endpoint(main_db, log_id, remote_host, remote_dir)

    flux_user, flux_host = split_remote_host(remote_host)
    env = os.environ.copy()
    if flux_user:
        env["DTWIN_FLUX_USER"] = flux_user
    if flux_host:
        env["DTWIN_FLUX_HOST"] = flux_host
    env["DTWIN_FLUX_BASE_DIR"] = remote_dir
    duo_option = str(flux_profile.get("duo_option") or "").strip()
    if duo_option:
        env["DTWIN_FLUX_DUO_OPTION"] = duo_option
    subprocess.run(
        ["bash", str(STEP3_SCRIPT), remote_db_path],
        check=True,
        env=env,
    )
    if log_id <= 0:
        raise SystemExit("Flux action log row is missing a valid id for sync tracking.")
    mark_flux_action_synced(main_db, log_id)
    return {
        "origin_name": origin["name"],
        "remote_host": remote_host,
        "remote_db_path": remote_db_path,
    }


def main() -> None:
    args = parse_args()
    result = run_sync(args.db, args.origin_id, args.origin_name)
    print(f"Synced back from {result['remote_host']}:{result['remote_db_path']}")


if __name__ == "__main__":
    main()
