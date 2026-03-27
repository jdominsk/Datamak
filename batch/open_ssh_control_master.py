#!/usr/bin/env python3
import argparse
import subprocess
from typing import List

try:
    from batch.ssh_utils import (
        build_ssh_base_args,
        get_default_remote_host,
        get_default_remote_user,
        get_ssh_connect_timeout,
        get_ssh_control_path,
    )
except ImportError:
    from ssh_utils import (
        build_ssh_base_args,
        get_default_remote_host,
        get_default_remote_user,
        get_ssh_connect_timeout,
        get_ssh_control_path,
    )


def _split_ssh_args(args: List[str]) -> tuple[List[str], str]:
    if not args:
        raise ValueError("Missing ssh arguments.")
    return args[:-1], args[-1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Open (or check) a persistent SSH ControlMaster connection."
    )
    parser.add_argument(
        "--host",
        default="",
        help="Remote SSH host (defaults to the Datamak Perlmutter host setting).",
    )
    parser.add_argument(
        "--user",
        default="",
        help="SSH username (defaults to the Datamak Perlmutter user setting).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="SSH connect timeout in seconds.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check if the control master is active.",
    )
    args = parser.parse_args()

    control_path = get_ssh_control_path()
    if not control_path:
        print("ControlPath is not configured; set DTWIN_SSH_CONTROL_PATH first.")
        return 2

    host = args.host.strip() or get_default_remote_host()
    user = args.user.strip() or get_default_remote_user()
    if user:
        if "@" not in host:
            host = f"{user}@{host}"
    connect_timeout = get_ssh_connect_timeout(max(1, args.timeout))
    ssh_args = build_ssh_base_args(host, connect_timeout)
    base_args, host_arg = _split_ssh_args(ssh_args)

    check_cmd = [*base_args, "-O", "check", host_arg]
    check = subprocess.run(check_cmd, capture_output=True, text=True)
    if check.returncode == 0:
        print("SSH control master already active.")
        return 0
    if args.check_only:
        err = (check.stderr or check.stdout or "").strip()
        print(err or "SSH control master not active.")
        return 1

    open_cmd = [*base_args, "-M", "-N", "-f", host_arg]
    result = subprocess.run(open_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(err or "Failed to open SSH control master.")
        return 3

    check = subprocess.run(check_cmd, capture_output=True, text=True)
    if check.returncode == 0:
        print("SSH control master opened.")
        return 0
    err = (check.stderr or check.stdout or "").strip()
    print(err or "SSH control master may not be active yet.")
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
