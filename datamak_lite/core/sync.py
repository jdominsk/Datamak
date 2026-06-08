from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from .packet import import_packet
from .repository import LiteRepository
from .validate import format_issues, has_errors, validate_packet


class PacketSyncError(RuntimeError):
    """Raised when a remote sidecar packet cannot be copied."""


class PacketValidationError(RuntimeError):
    """Raised when a sidecar packet fails validation before import."""


def is_remote_packet_spec(packet: str) -> bool:
    """Return True for scp-style remote packet specs.

    Datamak Lite intentionally starts with the simple transport that is already
    used in the campaign workflow:

    ``user@host:/absolute/path/datamak_lite.json``

    Local absolute paths such as ``/tmp/datamak_lite.json`` return False.
    """

    if "://" in packet:
        return False
    if packet.startswith("/"):
        return False
    if ":" not in packet:
        return False
    host, remote_path = packet.split(":", 1)
    return bool(host and remote_path.startswith("/"))


def cached_packet_path(remote_packet: str, cache_dir: str | Path) -> Path:
    digest = hashlib.sha256(remote_packet.encode("utf-8")).hexdigest()[:12]
    basename = Path(remote_packet.rsplit(":", 1)[-1]).name or "datamak_lite.json"
    if not basename.endswith(".json"):
        basename = f"{basename}.json"
    return Path(cache_dir).expanduser().resolve() / f"{Path(basename).stem}_{digest}.json"


def sync_packet_to_cache(
    remote_packet: str,
    cache_dir: str | Path,
    *,
    dry_run: bool = False,
    timeout_seconds: int = 60,
) -> Path:
    """Copy a remote packet into a local cache with scp and return its path."""

    destination = cached_packet_path(remote_packet, cache_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "scp",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={min(timeout_seconds, 20)}",
        remote_packet,
        str(destination),
    ]
    if dry_run:
        return destination
    try:
        subprocess.run(command, check=True, timeout=timeout_seconds, text=True, capture_output=True)
    except subprocess.TimeoutExpired as exc:
        raise PacketSyncError(f"SCP timed out after {timeout_seconds} s while copying {remote_packet}") from exc
    except subprocess.CalledProcessError as exc:
        message = (exc.stderr or exc.stdout or str(exc)).strip()
        raise PacketSyncError(f"SCP failed while copying {remote_packet}: {message}") from exc
    return destination


def sync_and_import_packet(
    repo: LiteRepository,
    packet: str,
    *,
    cache_dir: str | Path | None = None,
    dry_run: bool = False,
    timeout_seconds: int = 60,
) -> tuple[str | None, Path]:
    """Import a local packet, or first sync a remote packet into a local cache.

    Returns ``(root_uid, local_packet_path)``.  In dry-run mode no import is
    performed and ``root_uid`` is ``None``.
    """

    if is_remote_packet_spec(packet):
        if cache_dir is None:
            cache_dir = repo.db_path.parent / "packets"
        local_packet = sync_packet_to_cache(
            packet,
            cache_dir,
            dry_run=dry_run,
            timeout_seconds=timeout_seconds,
        )
    else:
        local_packet = Path(packet).expanduser()

    if dry_run:
        return None, local_packet
    issues = validate_packet(local_packet)
    if has_errors(issues):
        raise PacketValidationError(f"Invalid Datamak Lite packet:\n{format_issues(issues)}")
    return import_packet(repo, local_packet), local_packet
