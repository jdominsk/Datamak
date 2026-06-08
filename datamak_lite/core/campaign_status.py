from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .repository import LiteRepository


@dataclass(frozen=True)
class EntityRef:
    uid: str
    name: str
    type: str
    status: str = ""
    scientific_status: str = ""


@dataclass(frozen=True)
class RecentNote:
    note_type: str
    markdown_text: str
    created_at: str
    entity: EntityRef | None = None


@dataclass(frozen=True)
class AttentionItem:
    key: str
    label: str
    count: int
    sample_entities: list[EntityRef] = field(default_factory=list)


@dataclass(frozen=True)
class CampaignStatus:
    campaign: EntityRef | None
    entity_total: int
    counts_by_type: dict[str, int]
    counts_by_status: dict[str, int]
    counts_by_scientific_status: dict[str, int]
    note_counts: dict[str, int]
    attention: list[AttentionItem]
    recent_notes: list[RecentNote]


def resolve_database_path(path: str | Path) -> Path:
    """Resolve either a Lite SQLite path or a campaign profile JSON path."""
    candidate = Path(path).expanduser()
    if candidate.suffix.lower() != ".json":
        return candidate

    data = json.loads(candidate.read_text())
    database = data.get("database")
    if not isinstance(database, str) or not database:
        raise ValueError(f"Campaign profile does not define a database path: {candidate}")
    db_path = Path(database).expanduser()
    if db_path.is_absolute():
        return db_path
    return (candidate.parent / db_path).resolve()


def build_campaign_status(repo: LiteRepository, *, sample_limit: int = 6, recent_limit: int = 8) -> CampaignStatus:
    campaign = _load_campaign(repo)
    counts_by_type = _count_by(repo, "type")
    counts_by_status = _count_by(repo, "status")
    counts_by_scientific_status = _count_by(repo, "scientific_status")
    note_counts = _note_counts(repo)
    entity_total = sum(counts_by_type.values())

    missing_marker_count, missing_marker_entities = _pools_missing_datamak_marker(repo, sample_limit)
    disconnected_count, disconnected_entities = _disconnected_entities(repo, sample_limit)
    figure_warning_count, figure_warning_entities = _figures_with_warnings(repo, sample_limit)
    discovered_count, discovered_entities = _discovered_entities(repo, sample_limit)

    attention = [
        AttentionItem(
            key="warnings",
            label="Warnings to review",
            count=note_counts.get("warning", 0),
            sample_entities=_entities_with_note_type(repo, "warning", sample_limit),
        ),
        AttentionItem(
            key="todos",
            label="Open metadata tasks",
            count=note_counts.get("todo", 0),
            sample_entities=_entities_with_note_type(repo, "todo", sample_limit),
        ),
        AttentionItem(
            key="missing_datamak_marker",
            label="Pools missing Datamak receipt",
            count=missing_marker_count,
            sample_entities=missing_marker_entities,
        ),
        AttentionItem(
            key="disconnected",
            label="Objects without dependency links",
            count=disconnected_count,
            sample_entities=disconnected_entities,
        ),
        AttentionItem(
            key="figure_warnings",
            label="Figures needing verification",
            count=figure_warning_count,
            sample_entities=figure_warning_entities,
        ),
        AttentionItem(
            key="discovered_candidates",
            label="Auto-discovered folders to curate",
            count=discovered_count,
            sample_entities=discovered_entities,
        ),
    ]

    return CampaignStatus(
        campaign=campaign,
        entity_total=entity_total,
        counts_by_type=counts_by_type,
        counts_by_status=counts_by_status,
        counts_by_scientific_status=counts_by_scientific_status,
        note_counts=note_counts,
        attention=attention,
        recent_notes=_recent_notes(repo, recent_limit),
    )


def build_campaign_status_from_path(path: str | Path) -> CampaignStatus:
    db_path = resolve_database_path(path)
    with LiteRepository(db_path) as repo:
        return build_campaign_status(repo)


def format_campaign_status(status: CampaignStatus) -> str:
    campaign_name = status.campaign.name if status.campaign else "Unspecified campaign"
    lines = [
        f"Campaign status: {campaign_name}",
        f"Entities: {status.entity_total}",
        "",
        "Entity types:",
    ]
    lines.extend(f"  {key}: {value}" for key, value in sorted(status.counts_by_type.items()))
    lines.append("")
    lines.append("Operational status:")
    lines.extend(f"  {key}: {value}" for key, value in sorted(status.counts_by_status.items()))
    lines.append("")
    lines.append("Scientific status:")
    lines.extend(f"  {key}: {value}" for key, value in sorted(status.counts_by_scientific_status.items()))
    lines.append("")
    lines.append("Attention:")
    lines.extend(f"  {item.label}: {item.count}" for item in status.attention)
    if status.recent_notes:
        lines.append("")
        lines.append("Recent warning/todo notes:")
        for note in status.recent_notes:
            target = f" [{note.entity.uid}]" if note.entity else ""
            lines.append(f"  - {note.note_type}{target}: {_one_line(note.markdown_text)}")
    return "\n".join(lines) + "\n"


def _count_by(repo: LiteRepository, column: str) -> dict[str, int]:
    if column not in {"type", "status", "scientific_status"}:
        raise ValueError(f"Unsupported count column: {column}")
    rows = repo.conn.execute(
        f"""
        SELECT {column} AS key, COUNT(*) AS n
        FROM entity
        GROUP BY {column}
        ORDER BY {column}
        """
    ).fetchall()
    return {str(row["key"]): int(row["n"]) for row in rows}


def _note_counts(repo: LiteRepository) -> dict[str, int]:
    rows = repo.conn.execute(
        """
        SELECT note_type, COUNT(*) AS n
        FROM note
        GROUP BY note_type
        ORDER BY note_type
        """
    ).fetchall()
    return {str(row["note_type"]): int(row["n"]) for row in rows}


def _load_campaign(repo: LiteRepository) -> EntityRef | None:
    row = repo.conn.execute(
        """
        SELECT uid, name, type, status, scientific_status
        FROM entity
        WHERE type='campaign'
        ORDER BY name, uid
        LIMIT 1
        """
    ).fetchone()
    return _entity_ref(row) if row else None


def _pools_missing_datamak_marker(repo: LiteRepository, limit: int) -> tuple[int, list[EntityRef]]:
    where = """
        e.type='pool'
        AND NOT EXISTS (
            SELECT 1 FROM artifact a
            WHERE a.entity_id=e.id AND a.kind='datamak_pool_marker'
        )
    """
    return _count_and_sample(repo, where, limit)


def _disconnected_entities(repo: LiteRepository, limit: int) -> tuple[int, list[EntityRef]]:
    where = """
        e.type != 'campaign'
        AND NOT EXISTS (
            SELECT 1 FROM relation r
            WHERE r.source_entity_id=e.id OR r.target_entity_id=e.id
        )
    """
    return _count_and_sample(repo, where, limit)


def _figures_with_warnings(repo: LiteRepository, limit: int) -> tuple[int, list[EntityRef]]:
    where = """
        e.type='figure'
        AND EXISTS (
            SELECT 1 FROM note n
            WHERE n.entity_id=e.id AND n.note_type='warning'
        )
    """
    return _count_and_sample(repo, where, limit)


def _discovered_entities(repo: LiteRepository, limit: int) -> tuple[int, list[EntityRef]]:
    rows = repo.conn.execute(
        """
        SELECT uid, name, type, status, scientific_status
        FROM entity
        WHERE metadata_json LIKE '%"discovery_source"%'
        ORDER BY updated_at DESC, name
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    count_row = repo.conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM entity
        WHERE metadata_json LIKE '%"discovery_source"%'
        """
    ).fetchone()
    return int(count_row["n"]), [_entity_ref(row) for row in rows]


def _entities_with_note_type(repo: LiteRepository, note_type: str, limit: int) -> list[EntityRef]:
    rows = repo.conn.execute(
        """
        SELECT DISTINCT e.uid, e.name, e.type, e.status, e.scientific_status
        FROM note n
        JOIN entity e ON e.id=n.entity_id
        WHERE n.note_type=?
        ORDER BY e.type, e.name
        LIMIT ?
        """,
        (note_type, limit),
    ).fetchall()
    return [_entity_ref(row) for row in rows]


def _count_and_sample(repo: LiteRepository, where_sql: str, limit: int) -> tuple[int, list[EntityRef]]:
    count_row = repo.conn.execute(f"SELECT COUNT(*) AS n FROM entity e WHERE {where_sql}").fetchone()
    rows = repo.conn.execute(
        f"""
        SELECT e.uid, e.name, e.type, e.status, e.scientific_status
        FROM entity e
        WHERE {where_sql}
        ORDER BY e.type, e.name
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return int(count_row["n"]), [_entity_ref(row) for row in rows]


def _recent_notes(repo: LiteRepository, limit: int) -> list[RecentNote]:
    rows = repo.conn.execute(
        """
        SELECT
            n.note_type,
            n.markdown_text,
            n.created_at,
            e.uid,
            e.name,
            e.type,
            e.status,
            e.scientific_status
        FROM note n
        LEFT JOIN entity e ON e.id=n.entity_id
        WHERE n.note_type IN ('warning', 'todo')
        ORDER BY n.created_at DESC, n.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    notes: list[RecentNote] = []
    for row in rows:
        entity = _entity_ref(row) if row["uid"] else None
        notes.append(
            RecentNote(
                note_type=str(row["note_type"]),
                markdown_text=str(row["markdown_text"]),
                created_at=str(row["created_at"]),
                entity=entity,
            )
        )
    return notes


def _entity_ref(row: Any) -> EntityRef:
    return EntityRef(
        uid=str(row["uid"]),
        name=str(row["name"]),
        type=str(row["type"]),
        status=str(row["status"] or ""),
        scientific_status=str(row["scientific_status"] or ""),
    )


def _one_line(text: str, max_len: int = 120) -> str:
    compact = " ".join(text.split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1] + "..."
