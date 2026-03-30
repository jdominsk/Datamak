#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional


ROOT_DIR = Path(
    os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1])
).resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_flux_profile  # noqa: E402


SSH_WITH_DUO = ROOT_DIR / "tools" / "ssh_with_duo.py"
DEFAULT_DB_PATH = ROOT_DIR / "gyrokinetic_simulations.db"
DEFAULT_STATE_DIR = ROOT_DIR / "output" / "flux_db_backup"
DEFAULT_STATUS_PATH = DEFAULT_STATE_DIR / "backup_status.json"
DEFAULT_MAX_AGE_HOURS = 30.0


def _timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _default_remote_backup_dir(flux_profile: Dict[str, str]) -> str:
    configured_base_dir = Path(str(flux_profile.get("base_dir") or "").strip())
    if str(configured_base_dir):
        return str(configured_base_dir.parent / "backup")
    user = str(flux_profile.get("user") or "").strip()
    return f"/u/{user}/DTwin/backup" if user else "/u/unknown/DTwin/backup"


def _control_path() -> str:
    return str(Path.home() / ".ssh" / "cm-%r@%h:%p")


def _ssh_control_options(*, batch_mode: bool) -> list[str]:
    options = [
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPersist=10m",
        "-o",
        f"ControlPath={_control_path()}",
    ]
    if batch_mode:
        options.extend(["-o", "BatchMode=yes"])
    return options


def _ssh_command(
    flux_profile: Dict[str, str],
    *args: str,
    batch_mode: bool,
) -> list[str]:
    duo_option = str(flux_profile.get("duo_option") or "").strip()
    if not batch_mode and duo_option:
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


def _rsync_ssh_command() -> str:
    return shlex.join(["ssh", *_ssh_control_options(batch_mode=True)])


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


def _update_status(
    path: Path,
    *,
    state: str,
    db_path: Path,
    remote_host: str,
    remote_dir: str,
    remote_name: str,
    started_at: str,
    duration_sec: float,
    error: str = "",
) -> None:
    status = _load_status(path)
    status.update(
        {
            "state": state,
            "db_path": str(db_path),
            "remote_host": remote_host,
            "remote_dir": remote_dir,
            "last_attempt_at": started_at,
            "last_duration_sec": round(duration_sec, 3),
            "remote_name": remote_name,
            "last_error": error,
        }
    )
    if state == "ok":
        status["last_success_at"] = started_at
        status["remote_path"] = f"{remote_dir}/{remote_name}"
    _write_status(path, status)


def _run(command: list[str]) -> None:
    print(shlex.join(command))
    subprocess.run(command, check=True)


def prime_ssh_control_connection(flux_profile: Dict[str, str], remote_host: str) -> None:
    _run(
        _ssh_command(
            flux_profile,
            *_ssh_control_options(batch_mode=False),
            "-t",
            remote_host,
            "true",
            batch_mode=False,
        )
    )


def backup_database(
    *,
    db_path: Path,
    remote_dir: str,
    status_path: Path,
    prime_ssh: bool,
) -> Dict[str, Any]:
    if not db_path.is_file():
        raise SystemExit(f"Local database not found: {db_path}")

    flux_profile = resolve_flux_profile()
    remote_host = str(flux_profile.get("remote") or "").strip()
    if not remote_host:
        raise SystemExit("Flux remote host is not configured. Save Flux settings in the GUI first.")

    remote_dir_value = (remote_dir or "").strip() or _default_remote_backup_dir(flux_profile)
    started_at = _timestamp_utc()
    started_ts = time.time()
    remote_name = db_path.name
    try:
        if prime_ssh:
            prime_ssh_control_connection(flux_profile, remote_host)

        _run(
            [
                "ssh",
                *_ssh_control_options(batch_mode=True),
                remote_host,
                "mkdir",
                "-p",
                remote_dir_value,
            ]
        )
        _run(
            [
                "rsync",
                "-av",
                "-e",
                _rsync_ssh_command(),
                str(db_path),
                f"{remote_host}:{remote_dir_value}/{remote_name}",
            ]
        )
    except subprocess.CalledProcessError as exc:
        duration_sec = time.time() - started_ts
        _update_status(
            status_path,
            state="error",
            db_path=db_path,
            remote_host=remote_host,
            remote_dir=remote_dir_value,
            remote_name=remote_name,
            started_at=started_at,
            duration_sec=duration_sec,
            error=str(exc),
        )
        raise

    duration_sec = time.time() - started_ts
    _update_status(
        status_path,
        state="ok",
        db_path=db_path,
        remote_host=remote_host,
        remote_dir=remote_dir_value,
        remote_name=remote_name,
        started_at=started_at,
        duration_sec=duration_sec,
    )
    result = {
        "state": "ok",
        "db_path": str(db_path),
        "remote_host": remote_host,
        "remote_dir": remote_dir_value,
        "remote_name": remote_name,
        "started_at": started_at,
        "duration_sec": round(duration_sec, 3),
    }
    print(
        f"Backup complete: {db_path.name} -> {remote_host}:{remote_dir_value}/{remote_name}"
    )
    return result


def check_backup_status(*, status_path: Path, max_age_hours: float) -> int:
    status = _load_status(status_path)
    last_success_at = str(status.get("last_success_at") or "").strip()
    if not last_success_at:
        print(f"STALE: no successful backup recorded in {status_path}")
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
            f"STALE: last successful backup is {hours:.1f}h old, older than {max_age_hours:.1f}h"
        )
        return 1
    remote_host = str(status.get("remote_host") or "").strip()
    remote_dir = str(status.get("remote_dir") or "").strip()
    remote_name = str(status.get("remote_name") or "").strip() or "gyrokinetic_simulations.db"
    print(
        f"OK: last successful backup at {last_success_at} -> {remote_host}:{remote_dir}/{remote_name}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Back up gyrokinetic_simulations.db to Flux and track daily status."
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("backup", "check"),
        default="backup",
        help="backup: run rsync to Flux; check: verify the last successful backup is fresh.",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help="Local Datamak database to back up.",
    )
    parser.add_argument(
        "--remote-dir",
        default="",
        help="Remote Flux backup directory. Defaults to a sibling 'backup' directory next to flux_base_dir.",
    )
    parser.add_argument(
        "--status-file",
        default=str(DEFAULT_STATUS_PATH),
        help="Local JSON file that records last backup status.",
    )
    parser.add_argument(
        "--prime-ssh",
        action="store_true",
        help="Open the control connection interactively first so Duo can complete once.",
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
    db_path = Path(args.db).expanduser().resolve()
    status_path = Path(args.status_file).expanduser().resolve()
    if args.command == "check":
        return check_backup_status(
            status_path=status_path,
            max_age_hours=float(args.max_age_hours),
        )
    backup_database(
        db_path=db_path,
        remote_dir=str(args.remote_dir or ""),
        status_path=status_path,
        prime_ssh=bool(args.prime_ssh),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
