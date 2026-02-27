#!/usr/bin/env python3
import argparse
import subprocess
from typing import List

from ssh_utils import build_ssh_base_args, get_ssh_connect_timeout


def _split_ssh_args(args: List[str]) -> tuple[List[str], str]:
    if not args:
        raise ValueError("Missing ssh arguments.")
    return args[:-1], args[-1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test SSH connectivity by listing the remote home directory."
    )
    parser.add_argument(
        "--host",
        default="perlmutter.nersc.gov",
        help="Remote SSH host (default: perlmutter.nersc.gov).",
    )
    parser.add_argument(
        "--user",
        default="",
        help="SSH username (optional).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="SSH connect timeout in seconds.",
    )
    args = parser.parse_args()

    host = args.host.strip()
    if args.user and "@" not in host:
        host = f"{args.user}@{host}"
    connect_timeout = get_ssh_connect_timeout(max(1, args.timeout))
    ssh_args = build_ssh_base_args(host, connect_timeout)
    base_args, host_arg = _split_ssh_args(ssh_args)

    cmd = [*base_args, host_arg, "ls", "-lrt", "~"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(err or "SSH test failed.")
        return result.returncode or 1
    print((result.stdout or "").strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
