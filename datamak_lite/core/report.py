from __future__ import annotations

import sqlite3

from .repository import LiteRepository


PRODUCER_RELATIONS = {"produces", "generated"}
SKIP_TREE_RELATIONS = {"member_of"}


def render_entity_report(repo: LiteRepository, entity_uid: str, *, max_depth: int = 6) -> str:
    entity = repo.get_entity(entity_uid)
    lines = [
        f"# Datamak Lite Entity Report: {entity['name']}",
        "",
        f"- uid: `{entity['uid']}`",
        f"- type: `{entity['type']}`",
        f"- operational status: `{entity['status']}`",
        f"- scientific status: `{entity['scientific_status']}`",
    ]
    if entity["path"]:
        lines.append(f"- path: `{entity['path']}`")
    if entity["description"]:
        lines.extend(["", entity["description"]])

    notes = repo.notes_for_entity(int(entity["id"]))
    if notes:
        lines.extend(["", "## Notes"])
        for note in notes:
            prefix = f"{note['note_type']}"
            if note["author"]:
                prefix += f" by {note['author']}"
            lines.extend([f"- **{prefix}:** {note['markdown_text']}"])

    artifacts = repo.artifacts_for_entity(int(entity["id"]))
    if artifacts:
        lines.extend(["", "## Artifacts"])
        for artifact in artifacts:
            exists = _artifact_location_label(str(artifact["path"]), bool(artifact["exists_on_disk"]))
            lines.append(f"- `{artifact['kind']}`: `{artifact['path']}` ({exists})")

    metrics = repo.metrics_for_entity(int(entity["id"]))
    if metrics:
        lines.extend(["", "## Metrics"])
        for metric in metrics:
            value = "" if metric["value"] is None else f" = {metric['value']:g}"
            unit = "" if not metric["unit"] else f" {metric['unit']}"
            lines.append(f"- `{metric['name']}`{value}{unit}")

    lines.extend(["", "## Direct Relations"])
    outgoing = repo.outgoing_relations(int(entity["id"]))
    incoming = repo.incoming_relations(int(entity["id"]))
    if not outgoing and not incoming:
        lines.append("- none")
    for rel in outgoing:
        lines.append(f"- `{entity['uid']}` --{rel['relation_type']}--> `{rel['target_uid']}`")
    for rel in incoming:
        lines.append(f"- `{rel['source_uid']}` --{rel['relation_type']}--> `{entity['uid']}`")

    lines.extend(["", "## Dependency View"])
    tree_lines = _render_dependency_tree(repo, entity, max_depth=max_depth)
    lines.extend(tree_lines or ["- no upstream dependencies found"])

    return "\n".join(lines) + "\n"


def _artifact_location_label(path: str, exists_on_disk: bool) -> str:
    if exists_on_disk:
        return "exists locally"
    if _looks_like_remote_hpc_path(path):
        return "remote path; not checked locally"
    return "not found locally"


def _looks_like_remote_hpc_path(path: str) -> bool:
    return path.startswith((
        "/pscratch/",
        "/global/",
        "/grand/",
        "/flare/",
        "/eagle/",
        "/lus/",
    ))


def _render_dependency_tree(
    repo: LiteRepository,
    entity: sqlite3.Row,
    *,
    max_depth: int,
    depth: int = 0,
    visited: set[int] | None = None,
) -> list[str]:
    if visited is None:
        visited = set()
    entity_id = int(entity["id"])
    if entity_id in visited or depth >= max_depth:
        return []
    visited.add(entity_id)

    lines: list[str] = []
    indent = "  " * depth
    for rel in repo.outgoing_relations(entity_id):
        if rel["relation_type"] in SKIP_TREE_RELATIONS:
            continue
        target = repo.get_entity(rel["target_uid"])
        if int(target["id"]) in visited:
            lines.append(
                f"{indent}- `{entity['uid']}` --{rel['relation_type']}--> "
                f"`{target['uid']}` ({target['type']}: {target['name']}; already shown)"
            )
            continue
        lines.append(
            f"{indent}- `{entity['uid']}` --{rel['relation_type']}--> "
            f"`{target['uid']}` ({target['type']}: {target['name']})"
        )
        lines.extend(_render_dependency_tree(repo, target, max_depth=max_depth, depth=depth + 1, visited=visited))

    # If the current entity is a file/dataset, include its producers.  This is
    # what makes "figure -> pool -> history file <- simulation" readable.
    for rel in repo.incoming_relations(entity_id):
        if rel["relation_type"] not in PRODUCER_RELATIONS:
            continue
        source = repo.get_entity(rel["source_uid"])
        if int(source["id"]) in visited:
            continue
        lines.append(
            f"{indent}- `{source['uid']}` --{rel['relation_type']}--> "
            f"`{entity['uid']}` ({source['type']}: {source['name']})"
        )
    return lines
