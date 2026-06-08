from __future__ import annotations

import hashlib
from pathlib import Path

from datamak_lite.core.repository import LiteRepository


def import_folder_inventory(
    repo: LiteRepository,
    paths: list[str],
    *,
    campaign_uid: str = "campaign_default",
    discovery_source: str = "folder_inventory",
) -> dict[str, int]:
    """Import a shallow folder inventory as candidate entities.

    This is intentionally weaker than curated sidecar or registry import.  It is
    useful to make missing campaign roots visible without pretending that their
    full provenance is known.
    """

    counts = {"added": 0, "skipped_existing_path": 0}
    for raw in paths:
        path = str(raw).strip()
        if not path:
            continue
        if _entity_uid_for_path(repo, path):
            counts["skipped_existing_path"] += 1
            continue

        name = Path(path).name or path
        uid = _unique_uid(repo, f"discovered_{name}_{_short_hash(path)}")
        repo.upsert_entity(
            uid=uid,
            type=_infer_type(name),
            name=name,
            path=path,
            status="unknown",
            scientific_status="candidate",
            description="Auto-discovered campaign folder. Detailed provenance has not been imported yet.",
            metadata={"discovery_source": discovery_source},
        )
        repo.add_relation(source_uid=uid, relation_type="member_of", target_uid=campaign_uid)
        repo.add_artifact(
            entity_uid=uid,
            kind="discovered_root",
            path=path,
            description="Auto-discovered folder root.",
        )
        repo.add_note(
            entity_uid=uid,
            note_type="todo",
            markdown_text="Auto-discovered folder. Add a sidecar or curated registry entry to capture dependencies.",
        )
        counts["added"] += 1
    return counts


def read_inventory_file(path: str | Path) -> list[str]:
    return [
        line.strip()
        for line in Path(path).read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _entity_uid_for_path(repo: LiteRepository, path: str) -> str | None:
    row = repo.conn.execute("SELECT uid FROM entity WHERE path=?", (path,)).fetchone()
    return str(row["uid"]) if row else None


def _unique_uid(repo: LiteRepository, base: str) -> str:
    uid = _slug(base)
    if repo.conn.execute("SELECT 1 FROM entity WHERE uid=?", (uid,)).fetchone() is None:
        return uid
    index = 2
    while True:
        candidate = f"{uid}_{index}"
        if repo.conn.execute("SELECT 1 FROM entity WHERE uid=?", (candidate,)).fetchone() is None:
            return candidate
        index += 1


def _infer_type(name: str) -> str:
    lower = name.lower()
    if "analysis" in lower or "diagnostic" in lower or "postprocess" in lower:
        return "analysis"
    if "history" in lower or lower.startswith("source_"):
        return "simulation"
    if "pool" in lower or "scan" in lower or "replay" in lower:
        return "pool"
    return "folder"


def _short_hash(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()[:8]


def _slug(text: str) -> str:
    import re

    slug = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "datamak_lite_object"
