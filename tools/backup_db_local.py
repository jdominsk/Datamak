#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "gyrokinetic_simulations.db"
DEFAULT_BACKUP_DIR = ROOT_DIR / "backup"
DEFAULT_STATUS_PATH = ROOT_DIR / "output" / "local_db_backup" / "backup_status.json"
DEFAULT_MAX_AGE_HOURS = 30.0


def _timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _load_status(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_status(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def backup_database_local(*, db_path: Path, backup_dir: Path, status_path: Path) -> Dict[str, Any]:
    if not db_path.is_file():
        raise SystemExit(f"Local database not found: {db_path}")

    backup_dir.mkdir(parents=True, exist_ok=True)
    target_path = backup_dir / db_path.name
    started_at = _timestamp_utc()
    started_ts = time.time()
    try:
        if target_path.exists():
            target_path.unlink()
        with sqlite3.connect(str(db_path)) as source_conn, sqlite3.connect(str(target_path)) as target_conn:
            source_conn.backup(target_conn)
    except Exception as exc:
        duration_sec = time.time() - started_ts
        _write_status(
            status_path,
            {
                "state": "error",
                "db_path": str(db_path),
                "backup_path": str(target_path),
                "last_attempt_at": started_at,
                "last_duration_sec": round(duration_sec, 3),
                "last_error": str(exc),
            },
        )
        raise

    duration_sec = time.time() - started_ts
    payload = {
        "state": "ok",
        "db_path": str(db_path),
        "backup_path": str(target_path),
        "last_attempt_at": started_at,
        "last_success_at": started_at,
        "last_duration_sec": round(duration_sec, 3),
        "last_error": "",
    }
    _write_status(status_path, payload)
    print(f"Local backup complete: {db_path} -> {target_path}")
    return payload


def check_backup_status(*, status_path: Path, max_age_hours: float) -> int:
    status = _load_status(status_path)
    last_success_at = str(status.get("last_success_at") or "").strip()
    if not last_success_at:
        print(f"STALE: no successful local backup recorded in {status_path}")
        return 1
    try:
        last_success = datetime.fromisoformat(last_success_at.replace("Z", "+00:00"))
    except ValueError:
        print(f"STALE: invalid last_success_at in {status_path}")
        return 1
    age = datetime.now(timezone.utc) - last_success
    max_age = timedelta(hours=max_age_hours)
    if age > max_age:
        hours = age.total_seconds() / 3600.0
        print(
            f"STALE: last successful local backup is {hours:.1f}h old, older than {max_age_hours:.1f}h"
        )
        return 1
    backup_path = str(status.get("backup_path") or "").strip()
    print(f"OK: last successful local backup at {last_success_at} -> {backup_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and monitor a local backup of gyrokinetic_simulations.db.")
    parser.add_argument(
        "command",
        nargs="?",
        choices=("backup", "check"),
        default="backup",
        help="backup: snapshot the database into the local backup folder; check: verify freshness.",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help="Local Datamak database to back up.",
    )
    parser.add_argument(
        "--backup-dir",
        default=str(DEFAULT_BACKUP_DIR),
        help="Directory where the local backup copy should be written.",
    )
    parser.add_argument(
        "--status-file",
        default=str(DEFAULT_STATUS_PATH),
        help="Local JSON file that records last backup status.",
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help="When running 'check', fail if the last successful backup is older than this many hours.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "check":
        return check_backup_status(
            status_path=Path(args.status_file).expanduser().resolve(),
            max_age_hours=float(args.max_age_hours),
        )
    backup_database_local(
        db_path=Path(args.db).expanduser().resolve(),
        backup_dir=Path(args.backup_dir).expanduser().resolve(),
        status_path=Path(args.status_file).expanduser().resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
