from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .packet import SUPPORTED_SCHEMA_VERSION


@dataclass(frozen=True)
class PacketIssue:
    severity: str
    message: str
    path: str = ""

    @property
    def is_error(self) -> bool:
        return self.severity == "error"


def validate_packet(packet_path: str | Path) -> list[PacketIssue]:
    """Validate a Datamak Lite sidecar packet without importing it."""

    packet_path = Path(packet_path)
    try:
        packet = json.loads(packet_path.read_text())
    except FileNotFoundError:
        return [PacketIssue("error", f"Packet does not exist: {packet_path}")]
    except json.JSONDecodeError as exc:
        return [PacketIssue("error", f"Invalid JSON: {exc}", path="$")]

    if not isinstance(packet, dict):
        return [PacketIssue("error", "Packet root must be a JSON object.", path="$")]

    return validate_packet_data(packet)


def validate_packet_data(packet: dict[str, Any]) -> list[PacketIssue]:
    issues: list[PacketIssue] = []

    version = packet.get("schema_version")
    if version != SUPPORTED_SCHEMA_VERSION:
        issues.append(
            PacketIssue(
                "error",
                f"Unsupported schema_version={version!r}; expected {SUPPORTED_SCHEMA_VERSION}.",
                "$.schema_version",
            )
        )

    entities = _expect_list(packet, "entities", issues)
    relations = _expect_list(packet, "relations", issues, default=[])
    artifacts = _expect_list(packet, "artifacts", issues, default=[])
    metrics = _expect_list(packet, "metrics", issues, default=[])
    notes = _expect_list(packet, "notes", issues, default=[])

    entity_uids: set[str] = set()
    entity_types: dict[str, str] = {}
    for index, entity in enumerate(entities):
        path = f"$.entities[{index}]"
        if not _expect_object(entity, issues, path):
            continue
        uid = _required_string(entity, "uid", issues, path)
        _required_string(entity, "type", issues, path)
        _required_string(entity, "name", issues, path)
        if uid:
            if uid in entity_uids:
                issues.append(PacketIssue("error", f"Duplicate entity uid: {uid}", f"{path}.uid"))
            entity_uids.add(uid)
            entity_types[uid] = str(entity.get("type", ""))

    root_uid = packet.get("root_uid") or (entities[0].get("uid") if entities and isinstance(entities[0], dict) else None)
    if not root_uid:
        issues.append(PacketIssue("error", "Packet must define root_uid or put uid on the first entity.", "$.root_uid"))
    elif root_uid not in entity_uids:
        issues.append(PacketIssue("error", f"root_uid={root_uid!r} is not defined in entities.", "$.root_uid"))

    relation_keys: set[tuple[str, str, str]] = set()
    plot_relation_sources: set[str] = set()
    for index, relation in enumerate(relations):
        path = f"$.relations[{index}]"
        if not _expect_object(relation, issues, path):
            continue
        source_uid = _required_string(relation, "source_uid", issues, path)
        relation_type = _required_string(relation, "relation_type", issues, path)
        target_uid = _required_string(relation, "target_uid", issues, path)
        metadata = relation.get("metadata") if isinstance(relation.get("metadata"), dict) else {}
        target_is_external = bool(metadata.get("external_target") or metadata.get("external"))
        if source_uid and source_uid not in entity_uids:
            issues.append(PacketIssue("error", f"Relation source_uid is not defined: {source_uid}", f"{path}.source_uid"))
        if target_uid and target_uid not in entity_uids and not target_is_external:
            issues.append(
                PacketIssue(
                    "error",
                    f"Relation target_uid is not defined: {target_uid}. Add the entity or set metadata.external_target=true.",
                    f"{path}.target_uid",
                )
            )
        if source_uid and relation_type and target_uid:
            key = (source_uid, relation_type, target_uid)
            if key in relation_keys:
                issues.append(PacketIssue("warning", f"Duplicate relation: {source_uid} --{relation_type}--> {target_uid}", path))
            relation_keys.add(key)
        if relation_type == "plots" and source_uid:
            plot_relation_sources.add(source_uid)

    artifact_entities: set[str] = set()
    for index, artifact in enumerate(artifacts):
        path = f"$.artifacts[{index}]"
        if not _expect_object(artifact, issues, path):
            continue
        entity_uid = _required_string(artifact, "entity_uid", issues, path)
        _required_string(artifact, "kind", issues, path)
        _required_string(artifact, "path", issues, path)
        if entity_uid:
            artifact_entities.add(entity_uid)
            if entity_uid not in entity_uids:
                issues.append(PacketIssue("error", f"Artifact entity_uid is not defined: {entity_uid}", f"{path}.entity_uid"))

    for index, metric in enumerate(metrics):
        path = f"$.metrics[{index}]"
        if not _expect_object(metric, issues, path):
            continue
        entity_uid = _required_string(metric, "entity_uid", issues, path)
        _required_string(metric, "name", issues, path)
        value = metric.get("value")
        if entity_uid and entity_uid not in entity_uids:
            issues.append(PacketIssue("error", f"Metric entity_uid is not defined: {entity_uid}", f"{path}.entity_uid"))
        if isinstance(value, (list, dict)):
            issues.append(PacketIssue("error", "Metric value must be scalar; store arrays in an artifact.", f"{path}.value"))

    for index, note in enumerate(notes):
        path = f"$.notes[{index}]"
        if not _expect_object(note, issues, path):
            continue
        _required_string(note, "markdown_text", issues, path)
        entity_uid = note.get("entity_uid")
        relation_id = note.get("relation_id")
        if not entity_uid and relation_id in (None, ""):
            issues.append(PacketIssue("error", "Note must define entity_uid or relation_id.", path))
        if entity_uid and entity_uid not in entity_uids:
            issues.append(PacketIssue("error", f"Note entity_uid is not defined: {entity_uid}", f"{path}.entity_uid"))
        if relation_id not in (None, ""):
            issues.append(
                PacketIssue(
                    "warning",
                    "relation_id notes cannot be fully validated before import; prefer entity_uid or relation note_markdown in sidecars.",
                    f"{path}.relation_id",
                )
            )

    for uid, entity_type in entity_types.items():
        if entity_type == "figure":
            if uid not in artifact_entities:
                issues.append(PacketIssue("warning", f"Figure entity has no output artifact: {uid}", f"entity:{uid}"))
            if uid not in plot_relation_sources:
                issues.append(PacketIssue("warning", f"Figure entity has no plots relation: {uid}", f"entity:{uid}"))

    issues.extend(_large_inline_value_issues(packet))
    return issues


def has_errors(issues: list[PacketIssue]) -> bool:
    return any(issue.is_error for issue in issues)


def issues_as_json(issues: list[PacketIssue]) -> str:
    return json.dumps([asdict(issue) for issue in issues], indent=2)


def format_issues(issues: list[PacketIssue]) -> str:
    if not issues:
        return "OK: packet is valid.\n"
    lines = []
    for issue in issues:
        location = f" {issue.path}" if issue.path else ""
        lines.append(f"{issue.severity.upper()}:{location}: {issue.message}")
    return "\n".join(lines) + "\n"


def _expect_list(
    packet: dict[str, Any],
    key: str,
    issues: list[PacketIssue],
    *,
    default: list[Any] | None = None,
) -> list[Any]:
    value = packet.get(key, default)
    if not isinstance(value, list):
        issues.append(PacketIssue("error", f"{key!r} must be a list.", f"$.{key}"))
        return []
    return value


def _expect_object(value: Any, issues: list[PacketIssue], path: str) -> bool:
    if isinstance(value, dict):
        return True
    issues.append(PacketIssue("error", "Expected JSON object.", path))
    return False


def _required_string(data: dict[str, Any], key: str, issues: list[PacketIssue], path: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or value == "":
        issues.append(PacketIssue("error", f"Missing required string field {key!r}.", f"{path}.{key}"))
        return ""
    return value


def _large_inline_value_issues(packet: dict[str, Any]) -> list[PacketIssue]:
    issues: list[PacketIssue] = []
    for path, value in _walk(packet):
        key = path.rsplit(".", 1)[-1].strip("[]0123456789")
        if key in {"data", "values", "array", "arrays", "samples", "series"} and isinstance(value, list) and len(value) > 100:
            issues.append(
                PacketIssue(
                    "warning",
                    "Large inline array found; store arrays in a summary artifact and keep only the path in the packet.",
                    path,
                )
            )
        if isinstance(value, str) and len(value) > 100_000:
            issues.append(
                PacketIssue(
                    "warning",
                    "Large inline string found; store large content in an artifact and keep only the path in the packet.",
                    path,
                )
            )
    return issues


def _walk(value: Any, path: str = "$") -> list[tuple[str, Any]]:
    items = [(path, value)]
    if isinstance(value, dict):
        for key, child in value.items():
            items.extend(_walk(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            items.extend(_walk(child, f"{path}[{index}]"))
    return items
