#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from typing import List, Optional

ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_perlmutter_profile  # noqa: E402


def get_ssh_identity_file() -> Optional[str]:
    env_path = os.environ.get("DTWIN_SSH_IDENTITY")
    if env_path:
        return os.path.expanduser(env_path)
    configured = str(resolve_perlmutter_profile().get("identity") or "").strip()
    if configured:
        return os.path.expanduser(configured)
    return None


def get_ssh_control_path() -> Optional[str]:
    env_path = os.environ.get("DTWIN_SSH_CONTROL_PATH")
    if not env_path:
        configured = str(resolve_perlmutter_profile().get("control_path") or "").strip()
        return os.path.expanduser(configured) if configured else None
    return os.path.expanduser(env_path)


def get_ssh_control_persist() -> Optional[str]:
    env_value = os.environ.get("DTWIN_SSH_CONTROL_PERSIST")
    if env_value:
        return env_value
    configured = str(resolve_perlmutter_profile().get("control_persist") or "").strip()
    return configured or None


def get_ssh_connect_timeout(default_timeout: int) -> int:
    env_value = os.environ.get("DTWIN_SSH_CONNECT_TIMEOUT")
    if env_value and env_value.isdigit():
        return int(env_value)
    configured = resolve_perlmutter_profile().get("connect_timeout")
    try:
        return int(configured)
    except (TypeError, ValueError):
        return default_timeout


def get_default_remote_host() -> str:
    return str(resolve_perlmutter_profile().get("host") or "perlmutter.nersc.gov")


def get_default_remote_user() -> str:
    return str(resolve_perlmutter_profile().get("user") or "")


def build_ssh_base_args(host: str, connect_timeout: int) -> List[str]:
    args = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-o",
        "ConnectionAttempts=1",
        "-o",
        "NumberOfPasswordPrompts=0",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=1",
        host,
    ]
    identity = get_ssh_identity_file()
    if identity:
        args[1:1] = ["-o", "IdentitiesOnly=yes", "-i", identity]
    control_path = get_ssh_control_path()
    if control_path:
        persist = get_ssh_control_persist() or "10m"
        args[1:1] = [
            "-o",
            "ControlMaster=auto",
            "-o",
            f"ControlPath={control_path}",
            "-o",
            f"ControlPersist={persist}",
        ]
    return args
