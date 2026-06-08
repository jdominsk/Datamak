from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from datamak_lite.adapters.campaign_registry import import_campaign_registry
from datamak_lite.adapters.figure_audit import discover_figure_audit_files, import_figure_audits
from datamak_lite.adapters.folder_inventory import import_folder_inventory, read_inventory_file

from .db import init_db
from .packet import import_packet
from .repository import LiteRepository
from .sync import PacketSyncError, PacketValidationError, sync_and_import_packet
from .validate import has_errors, validate_packet


PROFILE_SCHEMA_VERSION = 1


@dataclass
class RefreshSummary:
    database: Path
    packets: int = 0
    remote_packets: int = 0
    registries: int = 0
    folder_inventories: int = 0
    figure_audits: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    imported_roots: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def refresh_campaign(profile_path: str | Path, *, dry_run: bool = False) -> RefreshSummary:
    profile_file = Path(profile_path).expanduser()
    profile = _load_profile(profile_file)
    base_dir = profile_file.parent

    version = profile.get("schema_version")
    if version != PROFILE_SCHEMA_VERSION:
        raise ValueError(f"Unsupported profile schema_version={version!r}; expected {PROFILE_SCHEMA_VERSION}")

    campaign_uid = _required(profile, "campaign_uid")
    campaign_name = str(profile.get("campaign_name") or campaign_uid)
    db_path = _resolve_path(_required(profile, "database"), base_dir)
    summary = RefreshSummary(database=db_path)

    if dry_run:
        _collect_dry_run_summary(profile, base_dir, summary)
        return summary

    init_db(db_path)
    with LiteRepository(db_path) as repo:
        repo.upsert_entity(
            uid=campaign_uid,
            type="campaign",
            name=campaign_name,
            status="active",
            scientific_status="candidate",
            metadata={"source": "campaign_profile"},
        )

        for registry in _path_list(profile, "registries", base_dir):
            try:
                import_campaign_registry(repo, registry, campaign_uid=campaign_uid, campaign_name=campaign_name)
                summary.registries += 1
            except Exception as exc:  # noqa: BLE001 - keep refresh robust and report every failed source.
                summary.errors.append(f"Registry import failed for {registry}: {exc}")

        for inventory in _path_list(profile, "folder_inventories", base_dir):
            try:
                counts = import_folder_inventory(
                    repo,
                    read_inventory_file(inventory),
                    campaign_uid=campaign_uid,
                    discovery_source=str(inventory),
                )
                summary.folder_inventories += counts["added"]
            except Exception as exc:  # noqa: BLE001
                summary.errors.append(f"Folder inventory import failed for {inventory}: {exc}")

        figure_roots = _path_list(profile, "figure_audit_roots", base_dir)
        if figure_roots:
            try:
                audit_files = discover_figure_audit_files(figure_roots)
                counts = import_figure_audits(
                    repo,
                    audit_files,
                    campaign_uid=campaign_uid,
                    campaign_name=campaign_name,
                    create_source_entities=not bool(profile.get("no_figure_source_entities", False)),
                )
                summary.figure_audits = counts["figures"]
                if counts["warnings"]:
                    summary.warnings.append(f"Figure audit warnings: {counts['warnings']}")
            except Exception as exc:  # noqa: BLE001
                summary.errors.append(f"Figure audit import failed: {exc}")

        # Packets are authoritative curation receipts. Import them after broad
        # discovery adapters so sidecar metadata and parent links win over
        # shallow folder/figure-discovery placeholders.
        for packet in _discover_local_packets(profile, base_dir):
            try:
                issues = validate_packet(packet)
                if has_errors(issues):
                    summary.errors.append(f"Packet validation failed for {packet}")
                    continue
                root_uid = import_packet(repo, packet)
                summary.packets += 1
                summary.imported_roots.append(root_uid)
            except Exception as exc:  # noqa: BLE001
                summary.errors.append(f"Packet import failed for {packet}: {exc}")

        for remote_packet in _string_list(profile, "remote_packets"):
            try:
                root_uid, _local_packet = sync_and_import_packet(repo, remote_packet)
                if root_uid:
                    summary.remote_packets += 1
                    summary.imported_roots.append(root_uid)
            except (PacketSyncError, PacketValidationError) as exc:
                summary.errors.append(f"Remote packet import failed for {remote_packet}: {exc}")

    return summary


def format_refresh_summary(summary: RefreshSummary, *, dry_run: bool = False) -> str:
    prefix = "Dry run" if dry_run else "Refresh"
    lines = [
        f"{prefix} database: {summary.database}",
        f"  local packets:      {summary.packets}",
        f"  remote packets:     {summary.remote_packets}",
        f"  registries:         {summary.registries}",
        f"  folder entries:     {summary.folder_inventories}",
        f"  figure audits:      {summary.figure_audits}",
    ]
    if summary.imported_roots:
        lines.append("  imported roots:")
        lines.extend(f"    - {uid}" for uid in summary.imported_roots)
    if summary.warnings:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in summary.warnings)
    if summary.errors:
        lines.append("Errors:")
        lines.extend(f"  - {error}" for error in summary.errors)
    return "\n".join(lines) + "\n"


def _load_profile(profile_path: Path) -> dict[str, Any]:
    try:
        data = json.loads(profile_path.read_text())
    except FileNotFoundError as exc:
        raise ValueError(f"Campaign profile does not exist: {profile_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid campaign profile JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Campaign profile root must be a JSON object")
    return data


def _collect_dry_run_summary(profile: dict[str, Any], base_dir: Path, summary: RefreshSummary) -> None:
    summary.registries = len(_path_list(profile, "registries", base_dir))
    summary.packets = len(_discover_local_packets(profile, base_dir))
    summary.remote_packets = len(_string_list(profile, "remote_packets"))
    for inventory in _path_list(profile, "folder_inventories", base_dir):
        try:
            summary.folder_inventories += len(read_inventory_file(inventory))
        except Exception as exc:  # noqa: BLE001
            summary.errors.append(f"Folder inventory read failed for {inventory}: {exc}")
    figure_roots = _path_list(profile, "figure_audit_roots", base_dir)
    summary.figure_audits = len(discover_figure_audit_files(figure_roots)) if figure_roots else 0


def _discover_local_packets(profile: dict[str, Any], base_dir: Path) -> list[Path]:
    packets: list[Path] = []
    packets.extend(_path_list(profile, "packets", base_dir))
    for root in _path_list(profile, "packet_roots", base_dir):
        if root.is_file() and root.name == "datamak_lite.json":
            packets.append(root)
        elif root.is_dir():
            packets.extend(root.rglob("datamak_lite.json"))
            packets.extend(root.rglob("datamak_lite_*.json"))
    return sorted(set(path.resolve() for path in packets))


def _path_list(profile: dict[str, Any], key: str, base_dir: Path) -> list[Path]:
    return [_resolve_path(value, base_dir) for value in _string_list(profile, key)]


def _string_list(profile: dict[str, Any], key: str) -> list[str]:
    value = profile.get(key, [])
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError(f"Profile field {key!r} must be a string or list of strings")


def _resolve_path(path: str, base_dir: Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded
    return (base_dir / expanded).resolve()


def _required(profile: dict[str, Any], key: str) -> str:
    value = profile.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Campaign profile must define string field {key!r}")
    return value
