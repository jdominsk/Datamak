"""Fetch storage quota metadata from a known HPC login host over SSH."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import re
import subprocess


# An absolute host also matches SSH aliases that specify the same HostName.
HOSTS = {
    "aurora": "aurora.alcf.anl.gov",
    "perlmutter": "perlmutter.nersc.gov",
    "polaris": "polaris.alcf.anl.gov",
}
CONNECT_OPTIONS = ("-M", "-o", "ControlPath=~/.ssh/datamak-%C")
FALLBACK_CONTROL_PATH = CONNECT_OPTIONS[2]
SSH_OPTIONS = (
    "-n", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
    "-o", "ControlMaster=no", "-o", "StrictHostKeyChecking=yes",
    "-o", "UpdateHostKeys=no", "-o", "ClearAllForwardings=yes",
    "-o", "ForwardAgent=no", "-o", "ForwardX11=no",
)
QUOTA_COMMANDS = {
    "aurora": "LC_ALL=C myquota; LC_ALL=C myprojectquotas",
    "perlmutter": "LC_ALL=C showquota",
    "polaris": "LC_ALL=C myquota",
}
UNITS = {letter: 1024**power for power, letter in enumerate("BKMGTPE")}
SPACE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([BKMGTPE])(i?B)?\*?", re.IGNORECASE)
ALCF_ROW = re.compile(r"\s*(\S+)\s+(User|Project)\s+(\S+)\s+(\S+)\s+(\S+)\s+\S+\s*")


def storage_bytes(value: str) -> int | None:
    # ALCF's Lustre tools abbreviate binary units, e.g. '41G' or '1.65G*'.
    size = SPACE.fullmatch(value)
    if size is None:
        return 0 if value in {"0", "0*"} else None
    unit = size[2].upper()
    multiplier = 1000 ** "BKMGTPE".index(unit) if size[3] in {"B", "b"} else UNITS[unit]
    count = round(Decimal(size[1]) * multiplier)
    return count if count < 2**63 else None


def quota_row(scope: str, used: str, limit: str) -> dict | None:
    usage_bytes = storage_bytes(used)
    limit_bytes = storage_bytes(limit)
    if usage_bytes is None or limit_bytes is None:
        return None
    return {"scope": scope, "used_bytes": usage_bytes, "limit_bytes": limit_bytes}


def parse_alcf(stdout: str) -> list[dict]:
    quotas = {}
    for line in stdout.splitlines():
        match = ALCF_ROW.fullmatch(line)
        if match is None:
            continue
        name, quota_type, filesystem, used, limit = match.groups()
        scope_name = name
        if quota_type == "User":
            scope_name = "Home" if filesystem in {"/home", "/lus/agile"} else "User"
        scope = f"{scope_name} · {filesystem}"
        if scope not in quotas and (entry := quota_row(scope, used, limit)) is not None:
            quotas[scope] = entry
    return list(quotas.values())


def parse_nersc(stdout: str) -> list[dict]:
    filesystem_names = {"home": "Home", "pscratch": "SCRATCH"}
    quotas = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        columns = [column.strip() for column in line[1:-1].split("|")]
        if len(columns) < 3 or columns[0] not in filesystem_names:
            continue
        scope = filesystem_names[columns[0]]
        if (entry := quota_row(scope, columns[1], columns[2])) is not None:
            quotas.append(entry)
    return quotas


def ssh(arguments: tuple[str, ...], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["/usr/bin/ssh", *SSH_OPTIONS, *arguments],
        stdin=subprocess.DEVNULL, capture_output=True, text=True, errors="replace",
        timeout=timeout, check=False, env={**os.environ, "LC_ALL": "C"},
    )


def connection_options(host: str) -> tuple[str, ...]:
    # OpenSSH tries the configured socket by default. Also recognize the
    # dedicated socket created by the displayed Connect command.
    if ssh(("-O", "check", host), timeout=3).returncode == 0:
        return ()
    fallback = ("-o", FALLBACK_CONTROL_PATH)
    if ssh((*fallback, "-O", "check", host), timeout=3).returncode == 0:
        return fallback
    return ()


def connection_error(machine: str, stderr: str) -> tuple[str, str]:
    detail = stderr.lower()
    if "remote host identification has changed" in detail:
        return "unavailable", "The SSH host key changed. Verify the login host’s key before retrying."
    if any(phrase in detail for phrase in (
        "permission denied", "authentication failed", "authentication failures",
        "no supported authentication", "host key verification failed",
    )):
        message = "Connect in Terminal, then retry the quota refresh."
        if machine == "perlmutter":
            message += " Renew the NERSC SSH certificate with sshproxy if needed."
        return "connect_required", message
    diagnostic = next((line.strip() for line in reversed(stderr.splitlines()) if line.strip()), "")
    return "unavailable", diagnostic[:240] or "The SSH connection failed."


def refresh_quota(machine: str) -> dict:
    host = HOSTS[machine]
    response = {
        "state": "unavailable", "message": "", "ssh_host": host,
        "connect_args": [*CONNECT_OPTIONS, host], "quotas": [],
    }
    try:
        connection = ssh((*connection_options(host), host, QUOTA_COMMANDS[machine]), timeout=25)
    except subprocess.TimeoutExpired:
        return {**response, "message": "The SSH quota check timed out."}
    except OSError as error:
        return {**response, "message": f"Could not run SSH: {error}"}

    if connection.returncode == 255:
        state, message = connection_error(machine, connection.stderr)
        return {**response, "state": state, "message": message}
    parser = parse_nersc if machine == "perlmutter" else parse_alcf
    quotas = parser(connection.stdout)
    if not quotas:
        return {**response, "message": "The login node did not return usable quota figures."}

    observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return {
        **response, "state": "ok", "message": "Storage quotas refreshed.",
        "observed_at": observed_at, "quotas": quotas,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machine", required=True, choices=HOSTS)
    print(json.dumps(refresh_quota(parser.parse_args().machine)))


if __name__ == "__main__":
    main()
