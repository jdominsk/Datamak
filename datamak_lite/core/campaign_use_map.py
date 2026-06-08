from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .campaign_status import EntityRef
from .repository import LiteRepository


@dataclass(frozen=True)
class UseMapObject:
    entity: EntityRef
    metadata: dict[str, Any] = field(default_factory=dict)
    path: str = ""


@dataclass(frozen=True)
class HistoryUseGroup:
    label: str
    objects: list[UseMapObject]


@dataclass(frozen=True)
class HistoryMap:
    history: UseMapObject
    parents: list[UseMapObject]
    use_groups: list[HistoryUseGroup]


@dataclass(frozen=True)
class CampaignUseMap:
    histories: list[HistoryMap]
    standalone_simulations: list[UseMapObject]
    standalone_downstream: list[UseMapObject]


def build_campaign_use_map(repo: LiteRepository, *, limit_histories: int = 10, limit_objects: int = 12) -> CampaignUseMap:
    histories = [_object_from_row(row) for row in _history_rows(repo)]
    history_maps: list[HistoryMap] = []
    for history in histories:
        parents = _relation_neighbors(repo, history.entity.uid, relation_type="produces", direction="incoming")
        users = _relation_neighbors(repo, history.entity.uid, relation_type="uses_history", direction="incoming")
        history_maps.append(
            HistoryMap(
                history=history,
                parents=parents[:limit_objects],
                use_groups=_group_downstream_objects(users[:limit_objects]),
            )
        )

    standalone_simulations = [
        _object_from_row(row)
        for row in repo.conn.execute(
            """
            SELECT e.*
            FROM entity e
            WHERE e.type='simulation'
              AND NOT EXISTS (
                  SELECT 1 FROM relation r
                  WHERE r.source_entity_id=e.id AND r.relation_type='produces'
              )
            ORDER BY e.name
            LIMIT ?
            """,
            (limit_objects,),
        ).fetchall()
    ]
    standalone_downstream = [
        _object_from_row(row)
        for row in repo.conn.execute(
            """
            SELECT e.*
            FROM entity e
            WHERE e.type IN ('pool', 'analysis')
              AND NOT EXISTS (
                  SELECT 1 FROM relation r
                  WHERE r.source_entity_id=e.id AND r.relation_type='uses_history'
              )
            ORDER BY e.type, e.name
            LIMIT ?
            """,
            (limit_objects,),
        ).fetchall()
    ]
    return CampaignUseMap(
        histories=history_maps[:limit_histories],
        standalone_simulations=standalone_simulations,
        standalone_downstream=standalone_downstream,
    )


def metadata_summary(metadata: dict[str, Any]) -> list[tuple[str, str]]:
    preferred = [
        "category",
        "code",
        "model",
        "models",
        "source_window",
        "time_start",
        "time_end",
        "fields",
        "saved_stride",
        "history_stride",
        "step_record_stride",
        "effective_stride",
        "field_time_interpolation",
        "n_cases",
        "nhermite",
        "nlaguerre",
        "nspecies",
    ]
    items: list[tuple[str, str]] = []
    for key in preferred:
        if key in metadata and metadata[key] not in (None, "", []):
            items.append((key, _compact_value(metadata[key])))
    return items


def object_summary_label(obj: UseMapObject) -> str:
    pieces = []
    for key in ("category", "code", "model", "field_time_interpolation", "effective_stride", "n_cases"):
        value = obj.metadata.get(key)
        if value not in (None, "", []):
            pieces.append(f"{key}={_compact_value(value)}")
    return "; ".join(pieces)


def _history_rows(repo: LiteRepository) -> list[Any]:
    return repo.conn.execute(
        """
        SELECT e.*
        FROM entity e
        WHERE e.type='history_file'
        ORDER BY
          CASE
            WHEN EXISTS (SELECT 1 FROM relation r WHERE r.target_entity_id=e.id AND r.relation_type='uses_history')
            THEN 0 ELSE 1
          END,
          e.name
        """
    ).fetchall()


def _relation_neighbors(
    repo: LiteRepository,
    uid: str,
    *,
    relation_type: str,
    direction: str,
) -> list[UseMapObject]:
    entity = repo.get_entity(uid)
    entity_id = int(entity["id"])
    if direction == "incoming":
        rows = repo.conn.execute(
            """
            SELECT e.*
            FROM relation r
            JOIN entity e ON e.id=r.source_entity_id
            WHERE r.target_entity_id=? AND r.relation_type=?
            ORDER BY e.type, e.name
            """,
            (entity_id, relation_type),
        ).fetchall()
    elif direction == "outgoing":
        rows = repo.conn.execute(
            """
            SELECT e.*
            FROM relation r
            JOIN entity e ON e.id=r.target_entity_id
            WHERE r.source_entity_id=? AND r.relation_type=?
            ORDER BY e.type, e.name
            """,
            (entity_id, relation_type),
        ).fetchall()
    else:
        raise ValueError(f"Unknown relation direction: {direction}")
    return [_object_from_row(row) for row in rows]


def _group_downstream_objects(objects: list[UseMapObject]) -> list[HistoryUseGroup]:
    groups: dict[str, list[UseMapObject]] = {}
    for obj in objects:
        label = str(obj.metadata.get("category") or obj.metadata.get("code") or obj.entity.type)
        groups.setdefault(label, []).append(obj)
    return [HistoryUseGroup(label=label, objects=groups[label]) for label in sorted(groups)]


def _object_from_row(row: Any) -> UseMapObject:
    return UseMapObject(
        entity=EntityRef(
            uid=str(row["uid"]),
            name=str(row["name"]),
            type=str(row["type"]),
            status=str(row["status"] or ""),
            scientific_status=str(row["scientific_status"] or ""),
        ),
        metadata=_metadata(row["metadata_json"]),
        path=str(row["path"] or ""),
    )


def _metadata(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _compact_value(value: Any, *, max_len: int = 56) -> str:
    if isinstance(value, list):
        text = ", ".join(str(item) for item in value[:6])
        if len(value) > 6:
            text += ", ..."
    elif isinstance(value, dict):
        text = ", ".join(f"{key}={value[key]}" for key in list(value)[:4])
        if len(value) > 4:
            text += ", ..."
    else:
        text = str(value)
    text = " ".join(text.split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "..."
