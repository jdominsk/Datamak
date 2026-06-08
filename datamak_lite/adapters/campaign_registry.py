from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from datamak_lite.core.repository import LiteRepository


DEFAULT_CAMPAIGN_UID = "campaign_default"
DEFAULT_CAMPAIGN_NAME = "Default campaign"


def import_campaign_registry(
    repo: LiteRepository,
    registry_db: str | Path,
    *,
    campaign_uid: str = DEFAULT_CAMPAIGN_UID,
    campaign_name: str = DEFAULT_CAMPAIGN_NAME,
) -> dict[str, int]:
    """Import a campaign-level SQLite registry into Datamak Lite.

    The source registry must provide the campaign, campaign_job, and
    campaign_event tables used by the lightweight Datamak-style registry
    pattern.  Domain-specific meaning remains in row metadata, not in Lite.
    """

    registry_db = Path(registry_db)
    source = sqlite3.connect(registry_db)
    source.row_factory = sqlite3.Row

    repo.upsert_entity(
        uid=campaign_uid,
        type="campaign",
        name=campaign_name,
        status="active",
        scientific_status="candidate",
        description="Imported campaign view from an external SQLite registry.",
        metadata={"source_registry": str(registry_db.resolve())},
    )

    counts = {"campaigns": 0, "histories": 0, "jobs": 0, "relations": 0}
    for row in source.execute("SELECT * FROM campaign ORDER BY campaign_key"):
        _import_campaign_row(repo, row, campaign_uid, counts)

    for row in source.execute("SELECT * FROM campaign_job ORDER BY campaign_key, id"):
        _import_job_row(repo, row, campaign_uid, counts)

    for row in source.execute("SELECT * FROM campaign_event ORDER BY campaign_key, id"):
        _import_event_row(repo, row)

    return counts


def _import_campaign_row(repo: LiteRepository, row: sqlite3.Row, campaign_uid: str, counts: dict[str, int]) -> None:
    key = str(row["campaign_key"])
    uid = _campaign_uid(key)
    metadata = _load_json(row["metadata_json"])
    metadata.update(
        {
            "category": row["category"],
            "source_registry_key": key,
            "source_window": row["source_window"],
        }
    )
    entity_type = _entity_type_from_category(row["category"], row["pool_db_path"])
    repo.upsert_entity(
        uid=uid,
        type=entity_type,
        name=row["title"],
        path=row["remote_path"] or row["local_path"],
        status=_status(row["status"]),
        scientific_status="candidate",
        description=row["purpose"] or "",
        metadata=metadata,
    )
    counts["campaigns"] += 1

    repo.add_relation(source_uid=uid, relation_type="member_of", target_uid=campaign_uid)
    counts["relations"] += 1

    _add_artifact_if_present(repo, uid, "remote_root", row["remote_path"], description="Remote campaign or pool root.")
    _add_artifact_if_present(repo, uid, "local_root", row["local_path"], description="Local campaign preparation folder.")
    _add_artifact_if_present(repo, uid, "pool_db", row["pool_db_path"], format="sqlite", description="Datamak-style pool database.")

    if row["purpose"]:
        repo.add_note(entity_uid=uid, note_type="comment", markdown_text=row["purpose"])
    if row["notes"]:
        repo.add_note(entity_uid=uid, note_type="comment", markdown_text=row["notes"])
    if row["source_window"]:
        repo.add_metric(entity_uid=uid, name="source_window", value=None, unit=row["source_window"], context={})

    _add_metadata_metrics(repo, uid, metadata)
    _import_source_history(repo, uid, row, campaign_uid, counts)


def _import_source_history(
    repo: LiteRepository,
    entity_uid: str,
    row: sqlite3.Row,
    campaign_uid: str,
    counts: dict[str, int],
) -> None:
    source_history = str(row["source_history"] or "").strip()
    if not source_history:
        return
    paths = _split_source_history(source_history)
    relation_type = "produces" if "source-history" in str(row["category"]) else "uses_history"
    for index, path in enumerate(paths, start=1):
        if not _looks_like_path(path):
            repo.add_note(
                entity_uid=entity_uid,
                note_type="comment",
                markdown_text=f"Source/history note: {path}",
            )
            continue
        history_uid = _history_uid(path, fallback=f"{entity_uid}_history_{index}")
        history_name = _history_name(path, row["source_window"])
        repo.upsert_entity(
            uid=history_uid,
            type="history_file",
            name=history_name,
            path=path,
            status="available" if not any(ch in path for ch in "*?") else "pattern",
            scientific_status="candidate",
            description="Input or history file referenced by imported registry.",
            metadata={"source_window": row["source_window"]},
        )
        repo.add_relation(source_uid=history_uid, relation_type="member_of", target_uid=campaign_uid)
        repo.add_relation(source_uid=entity_uid, relation_type=relation_type, target_uid=history_uid)
        _add_artifact_if_present(repo, history_uid, "history_path", path, format=_guess_format(path), description="Referenced history file or pattern.")
        counts["histories"] += 1
        counts["relations"] += 2


def _import_job_row(repo: LiteRepository, row: sqlite3.Row, campaign_uid: str, counts: dict[str, int]) -> None:
    campaign_key = str(row["campaign_key"])
    parent_uid = _campaign_uid(campaign_key)
    job_id = str(row["job_id"] or row["id"])
    job_uid = _slug(f"job_{campaign_key}_{job_id}")
    metadata = {
        "scheduler": row["scheduler"],
        "job_id": row["job_id"],
        "queue": row["queue"],
        "nodes": row["nodes"],
        "time_limit": row["time_limit"],
        "elapsed": row["elapsed"],
        "exit_code": row["exit_code"],
        "submitted_at": row["submitted_at"],
        "last_seen_at": row["last_seen_at"],
    }
    repo.upsert_entity(
        uid=job_uid,
        type="job",
        name=row["job_name"] or f"Job {job_id}",
        status=_status(row["state"]),
        scientific_status="candidate",
        description=row["notes"] or "",
        metadata={k: v for k, v in metadata.items() if v not in (None, "")},
    )
    repo.add_relation(source_uid=job_uid, relation_type="member_of", target_uid=campaign_uid)
    repo.add_relation(source_uid=parent_uid, relation_type="has_job", target_uid=job_uid)
    if row["notes"]:
        repo.add_note(entity_uid=job_uid, note_type="comment", markdown_text=row["notes"])
    counts["jobs"] += 1
    counts["relations"] += 2


def _import_event_row(repo: LiteRepository, row: sqlite3.Row) -> None:
    parent_uid = _campaign_uid(str(row["campaign_key"]))
    text = str(row["event"] or "")
    if row["event_time"]:
        text = f"{row['event_time']}: {text}"
    if row["notes"]:
        text = f"{text}\n\n{row['notes']}" if text else row["notes"]
    if text:
        repo.add_note(entity_uid=parent_uid, note_type="comment", markdown_text=text)


def _add_metadata_metrics(repo: LiteRepository, uid: str, metadata: dict[str, Any]) -> None:
    for key, value in metadata.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            repo.add_metric(entity_uid=uid, name=key, value=float(value), unit=None, context={})
        elif isinstance(value, list) and value and all(isinstance(item, (int, float)) for item in value):
            repo.add_metric(entity_uid=uid, name=f"n_{key}", value=float(len(value)), unit="count", context={})
        elif isinstance(value, list):
            repo.add_metric(entity_uid=uid, name=f"n_{key}", value=float(len(value)), unit="count", context={})


def _add_artifact_if_present(
    repo: LiteRepository,
    entity_uid: str,
    kind: str,
    path: str | None,
    *,
    format: str | None = None,
    description: str = "",
) -> None:
    if not path:
        return
    repo.add_artifact(
        entity_uid=entity_uid,
        kind=kind,
        path=str(path),
        format=format or _guess_format(str(path)),
        description=description,
    )


def _entity_type_from_category(category: str | None, pool_db_path: str | None) -> str:
    text = str(category or "").lower()
    if "source-history" in text:
        return "simulation"
    if "analysis" in text or "diagnostic" in text or "postprocess" in text:
        return "analysis"
    if pool_db_path:
        return "pool"
    return "simulation"


def _status(value: str | None) -> str:
    if not value:
        return "unknown"
    normalized = str(value).strip().lower()
    mapping = {
        "completed": "success",
        "complete": "success",
        "prepared": "prepared",
        "pending": "pending",
        "submitted_pending": "pending",
        "running": "running",
        "running_or_pending": "running",
        "partially_completed": "partial",
        "failed_launch": "crashed",
    }
    return mapping.get(normalized, normalized)


def _split_source_history(source_history: str) -> list[str]:
    parts = []
    for raw in source_history.split(";"):
        item = raw.strip()
        if item:
            parts.append(item)
    return parts


def _looks_like_path(value: str) -> bool:
    return value.startswith("/") or value.startswith("$") or "/" in value


def _history_uid(path: str, *, fallback: str) -> str:
    name = Path(path.replace("*", "star").replace("?", "qmark")).name
    if not name:
        return fallback
    return _slug(f"history_{name}")


def _history_name(path: str, source_window: str | None) -> str:
    name = Path(path).name or path
    if source_window:
        return f"{name} ({source_window})"
    return name


def _campaign_uid(key: str) -> str:
    return _slug(f"campaign_object_{key}")


def _guess_format(path: str) -> str | None:
    suffix = Path(path.replace("*", "")).suffix.lower()
    return {
        ".db": "sqlite",
        ".sqlite": "sqlite",
        ".json": "json",
        ".csv": "csv",
        ".nc": "netcdf",
        ".md": "markdown",
        ".py": "python",
        ".sh": "shell",
    }.get(suffix)


def _load_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "datamak_lite_object"
