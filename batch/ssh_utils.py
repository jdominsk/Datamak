#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List, Optional


def get_ssh_identity_file() -> Optional[str]:
    env_path = os.environ.get("DTWIN_SSH_IDENTITY")
    if env_path:
        return os.path.expanduser(env_path)
    candidate = Path.home() / ".ssh" / "nersc"
    if candidate.exists():
        return str(candidate)
    return None


def get_ssh_control_path() -> Optional[str]:
    env_path = os.environ.get("DTWIN_SSH_CONTROL_PATH")
    if not env_path:
        return None
    return os.path.expanduser(env_path)


def get_ssh_control_persist() -> Optional[str]:
    env_value = os.environ.get("DTWIN_SSH_CONTROL_PERSIST")
    if env_value:
        return env_value
    return None


def get_ssh_connect_timeout(default_timeout: int) -> int:
    env_value = os.environ.get("DTWIN_SSH_CONNECT_TIMEOUT")
    if env_value and env_value.isdigit():
        return int(env_value)
    return default_timeout


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
