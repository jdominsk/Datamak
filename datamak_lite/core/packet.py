from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .repository import LiteRepository


SUPPORTED_SCHEMA_VERSION = 1


def import_packet(repo: LiteRepository, packet_path: str | Path) -> str:
    """Import a Datamak Lite sidecar JSON packet.

    Packet format v1 is intentionally close to the generic database model:

    - `entities`: list of entity dictionaries;
    - `relations`: list of relation dictionaries;
    - `artifacts`: list of artifact dictionaries;
    - `metrics`: list of metric dictionaries;
    - `notes`: list of note dictionaries.

    The function returns the packet's `root_uid`.
    """

    packet_path = Path(packet_path)
    packet = json.loads(packet_path.read_text())
    packet_dir = packet_path.parent

    version = packet.get("schema_version")
    if version != SUPPORTED_SCHEMA_VERSION:
        raise ValueError(f"Unsupported packet schema_version={version!r}; expected {SUPPORTED_SCHEMA_VERSION}")

    entities = packet.get("entities", [])
    if not entities:
        raise ValueError("Packet must contain at least one entity")

    root_uid = packet.get("root_uid") or entities[0].get("uid")
    if not root_uid:
        raise ValueError("Packet must define root_uid or put uid on the first entity")

    entity_uids: set[str] = set()
    for entity in entities:
        uid = _required(entity, "uid", "entity")
        entity_uids.add(uid)
        repo.upsert_entity(
            uid=uid,
            type=_required(entity, "type", f"entity {uid}"),
            name=_required(entity, "name", f"entity {uid}"),
            path=_resolve_optional_path(entity.get("path"), packet_dir),
            status=entity.get("status", "unknown"),
            scientific_status=entity.get("scientific_status", "candidate"),
            description=entity.get("description", ""),
            metadata=entity.get("metadata", {}),
        )

    if root_uid not in entity_uids:
        raise ValueError(f"root_uid={root_uid!r} is not defined in entities")

    for relation in packet.get("relations", []):
        relation_id = repo.add_relation(
            source_uid=_required(relation, "source_uid", "relation"),
            relation_type=_required(relation, "relation_type", "relation"),
            target_uid=_required(relation, "target_uid", "relation"),
            note=relation.get("note", ""),
            metadata=relation.get("metadata", {}),
        )
        if relation.get("note_markdown"):
            repo.add_note(
                relation_id=relation_id,
                note_type=relation.get("note_type", "comment"),
                author=relation.get("author", ""),
                markdown_text=relation["note_markdown"],
            )

    for artifact in packet.get("artifacts", []):
        repo.add_artifact(
            entity_uid=_required(artifact, "entity_uid", "artifact"),
            kind=_required(artifact, "kind", "artifact"),
            path=_resolve_path(_required(artifact, "path", "artifact"), packet_dir),
            format=artifact.get("format"),
            description=artifact.get("description", ""),
            metadata=artifact.get("metadata", {}),
        )

    for metric in packet.get("metrics", []):
        repo.add_metric(
            entity_uid=_required(metric, "entity_uid", "metric"),
            name=_required(metric, "name", "metric"),
            value=metric.get("value"),
            unit=metric.get("unit"),
            context=metric.get("context", {}),
        )

    for note in packet.get("notes", []):
        repo.add_note(
            entity_uid=note.get("entity_uid"),
            relation_id=note.get("relation_id"),
            note_type=note.get("note_type", "comment"),
            author=note.get("author", ""),
            markdown_text=_required(note, "markdown_text", "note"),
        )

    return str(root_uid)


def _required(data: dict[str, Any], key: str, context: str) -> Any:
    value = data.get(key)
    if value in (None, ""):
        raise ValueError(f"Missing required field {key!r} in {context}")
    return value


def _resolve_optional_path(path: str | None, base_dir: Path) -> str | None:
    if path in (None, ""):
        return None
    return _resolve_path(str(path), base_dir)


def _resolve_path(path: str, base_dir: Path) -> str:
    if _is_external_or_absolute(path):
        return path
    return str((base_dir / path).resolve())


def _is_external_or_absolute(path: str) -> bool:
    return (
        path.startswith("/")
        or path.startswith("$")
        or "://" in path
    )
