"""Read-only Datamak Lite snapshot for the macOS menu bar."""
from __future__ import annotations

import argparse
import json
import math
import posixpath
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from datamak_lite.core.repository import LiteRepository
from datamak_lite.core.user_config import load_user_index


RUN_TYPES = {"run", "simulation", "simulation_segment"}
POOL_TYPES = {"pool", "simulation_pool"}
MACHINE_NAMES = {"perlmutter": "Perlmutter", "polaris": "Polaris", "aurora": "Aurora", "frontier": "Frontier"}
RUN_STATES = {
    "running": {"running", "running_from_pbs", "r"},
    "pending": {"pending", "queued", "queued_from_pbs", "submitted", "submitted_pending",
                "torun", "wait", "waiting", "pd", "q"},
    "completed": {"success", "complete", "completed", "finished", "converged", "done"},
    "attention": {"error", "failed", "crashed", "interrupted", "stopped_walltime_limit",
                  "crashed_partial_output", "timeout", "cancelled", "canceled", "hold",
                  "started_then_stopped_before_f3d", "walltime_stopped_medium_debug_variant",
                  "walltime_stopped_xgca_outputs_present", "restart_available_output_missing"},
}
NO_REMOTE_DIRECTORY = {"alias_symlink", "no_run_directory"}
FOLLOW_UP_AFTER = timedelta(days=1)
TIB = 1024**4
GX_ARCHIVE_ROOT = "/pscratch/sd/j/jdominsk/GX"
GX_FINGERPRINT_METHOD = "recursive_path_size_mtime_ns_symlink_sha256_v1"


class ReadOnlyRepository(LiteRepository):
    """Reuse Lite's campaign scoping without its create-on-open behavior."""

    def __init__(self, path: Path):
        self.db_path = path
        self.conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only = ON")
        self.conn.execute("BEGIN")


def object_json(value: str) -> dict[str, Any]:
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def utc_stamp(value: Any, *, sqlite: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        if not sqlite:
            return None
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def latest(values: list[str | None]) -> str | None:
    return max((value for value in values if value), default=None)


def run_state(status: str) -> str:
    normalized = status.strip().lower()
    return next((state for state, keys in RUN_STATES.items() if normalized in keys), "other")


def observation(metadata: dict[str, Any]) -> str | None:
    dates = [metadata.get(key) for key in (
        "checked_at", "last_status_check_utc", "last_progress_check_utc", "status_observed_at",
        "status_observed_at_utc", "last_observed_at", "last_seen_at",
    )]
    last_check = record(metadata.get("last_status_check"))
    scheduler = record(metadata.get("scheduler"))
    dates.extend([last_check.get("observed_at_utc"), scheduler.get("checked_at")])
    return latest([utc_stamp(value) for value in dates])


def machine_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    host = value.rsplit("@", 1)[-1].strip().lower().split(".", 1)[0]
    return host if host in MACHINE_NAMES else None


def remote_location(path: Any, metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    """Recognize explicitly recorded remote paths and NERSC's distinctive roots."""
    host = next(
        (machine for key in ("machine", "remote_host", "host", "source_host")
         if (machine := machine_id(metadata.get(key)))),
        None,
    )
    if not isinstance(path, str):
        return host, None
    if path.startswith("ssh://"):
        try:
            url = urlsplit(path)
            host = machine_id(url.hostname) or host
            path = unquote(url.path)
        except ValueError:
            return host, None
    elif match := re.fullmatch(r"([^/]+):(/.*)", path):
        host = machine_id(match[1]) or host
        path = match[2]
    if path.startswith(("/pscratch/", "/global/")):
        host = host or "perlmutter"
    if not host or not path.startswith("/") or path.startswith(("/Users/", "/private/", "/tmp/")):
        return host, None
    return host, posixpath.normpath(path)


def entity_location(row: sqlite3.Row, metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    return remote_location(metadata.get("remote_path") or metadata.get("remote_root") or row["path"], metadata)


def campaign_details(
    entry: dict[str, Any],
    rows: list[sqlite3.Row],
    metadata: dict[int, dict[str, Any]],
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    counted = [row for row in rows if row["type"] in RUN_TYPES]
    kind = "runs"
    if not counted:
        counted = [row for row in rows if row["type"] == "job"]
        kind = "jobs"
    if not counted:
        counted = [row for row in rows if row["type"] in POOL_TYPES]
        kind = "pools"

    counts = Counter(run_state(row["status"]) for row in counted)
    machines = sorted({
        MACHINE_NAMES[machine] for row in rows if row["type"] in RUN_TYPES | POOL_TYPES | {"job"}
        if (machine := entity_location(row, metadata[row["id"]])[0])
    })
    state = next((key for key in ("running", "pending", "attention") if counts[key]), "idle")
    cutoff = now - FOLLOW_UP_AFTER
    stale, active_dates, dates = 0, [], []
    for row in counted:
        info = metadata[row["id"]]
        observed_at = observation(info)
        dates.append(observed_at)
        status = row["status"].strip().lower()
        queued = status in {"queued", "queued_from_pbs", "submitted", "submitted_pending", "pd", "q"}
        has_remote_context = entity_location(row, info)[0] or observed_at or kind == "jobs"
        if has_remote_context and (run_state(status) == "running" or queued):
            active_dates.append(observed_at)
            stale += int(observed_at is None or datetime.fromisoformat(observed_at[:-1] + "+00:00") < cutoff)

    alert = None
    if stale:
        noun = {"runs": "run", "jobs": "job", "pools": "pool"}[kind]
        label = f"{stale} {noun}s" if stale != 1 else f"1 {noun}"
        verb = "have" if stale != 1 else "has"
        alert = {
            "id": f"{entry['uid']}:check-status",
            "title": "Check recorded activity",
            "detail": f"{label} recorded as running or queued {verb} no observation in the last 24 hours.",
            "campaign_uid": entry["uid"],
        }
    result = {
        **entry,
        **{key: counts[key] if counted else None for key in RUN_STATES},
        "machines": machines,
        "updated_at": latest([utc_stamp(row["updated_at"], sqlite=True) for row in counted]),
        "checked_at": latest(active_dates or dates),
        "state": state,
        "count_kind": kind if counted else None,
        "message": alert["detail"] if alert else None,
    }
    return result, alert


def nonnegative_bytes(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return int(value) if value >= 0 and value == int(value) else None


def storage_item(
    machine: str, kind: str, title: str, path: str,
    *, size: int | None = None, observed_at: str | None = None,
    archive_path: str | None = None, note: str | None = None,
    archive_state: str | None = None, archive_detail: str | None = None,
    archive_plan_eligible: bool = False,
) -> tuple[str, dict[str, Any]]:
    item = {
        "id": f"{kind}:{path}",
        "name": title,
        "kind": kind,
        "path": path,
        "size_bytes": size,
        "observed_at": observed_at,
        "archive_path": archive_path,
        "note": note,
    }
    if kind == "remote":
        item.update(
            archive_state=archive_state or "unarchived",
            archive_detail=archive_detail or "No archive is recorded for this run.",
            archive_plan_eligible=archive_plan_eligible,
        )
    return machine, item


def directory_fingerprint(metadata: dict[str, Any]) -> str | None:
    fingerprint = metadata.get("directory_fingerprint")
    if (metadata.get("fingerprint_method") == GX_FINGERPRINT_METHOD
            and isinstance(fingerprint, str) and re.fullmatch(r"[0-9a-f]{64}", fingerprint)):
        return fingerprint
    return None


def run_archive_evidence(
    row: sqlite3.Row,
    source: dict[str, Any],
    relation: dict[str, Any],
    archive: dict[str, Any],
    artifact: dict[str, Any],
    archive_type: str,
) -> tuple[int, str, str]:
    machine, path = entity_location(row, source)
    recorded_paths = {
        remote_location(value, {"machine": archive.get("source_host") or machine})
        for value in (archive.get("source_path"), archive.get("source_resolved_path"),
                      relation.get("source_path"), artifact.get("source_path"))
        if value
    }
    if not machine or not path or (machine, path) not in recorded_paths:
        return 0, "review", "A linked archive exists, but its source directory match is unverified."
    if archive.get("source_entity_uid") and archive["source_entity_uid"] != row["uid"]:
        return 0, "review", "The archive revision identifies a different source entity."

    fingerprint = directory_fingerprint(archive)
    verified_at = utc_stamp(archive.get("verification_completed_at_utc"))
    full_revision = (
        archive_type == "hpss_archive_revision"
        and archive.get("archive_policy") == "directory_snapshot"
        and archive.get("verification_level") == "FULL_INVENTORY"
    )
    if not full_revision:
        if archive.get("exclude_restart_files"):
            return 1, "review", "The historical archive excluded restart files by policy."
        return 1, "review", "An archive is recorded. Full directory verification is unavailable."

    # A source observation uses the planner inventory fields for this exact path.
    inventory = record(source.get("latest_source_inventory"))
    observed_at = utc_stamp(inventory.get("observed_at_utc"))
    current_fingerprint = directory_fingerprint(inventory)
    observed_path = remote_location(inventory.get("source_path"), {"machine": machine})
    if fingerprint and current_fingerprint and verified_at and observed_at and observed_path == (machine, path):
        if current_fingerprint == fingerprint and observed_at >= verified_at:
            return 4, "archived", "The verified archive matches the last recorded source inventory."
        if current_fingerprint != fingerprint and observed_at > verified_at:
            return 3, "review", "The recorded source inventory changed after this archive was verified."
    return 2, "review", "A full archive was verified. The latest source inventory has not been matched."


def gx_archive_plan_eligible(
    machine: str, path: str, metadata: dict[str, Any], has_gx_input: bool
) -> bool:
    code = metadata.get("code") or metadata.get("code_family")
    is_gx = code.lower() in {"gx", "gx-r", "gxr"} if isinstance(code, str) else has_gx_input
    return machine == "perlmutter" and path.startswith(GX_ARCHIVE_ROOT + "/") and is_gx


def archived_sources(
    repo: ReadOnlyRepository, rows: dict[int, sqlite3.Row], metadata: dict[int, dict[str, Any]],
) -> tuple[list[tuple[str, dict[str, Any]]], dict[int, dict[str, str]]]:
    records = []
    best_evidence = {}
    archive_artifacts = {
        (item["entity_id"], item["path"]): object_json(item["metadata_json"])
        for item in repo.conn.execute(
            "SELECT entity_id, path, metadata_json FROM artifact WHERE kind='hpss_archive_directory'"
        )
        if item["entity_id"] in rows
    }
    links = repo.conn.execute(
        """
        SELECT r.source_entity_id, r.metadata_json, a.path AS archive_path,
               a.type AS archive_type, a.metadata_json AS archive_metadata
        FROM relation r
        JOIN entity a ON a.id=r.target_entity_id
        WHERE r.relation_type='archived_in'
          AND a.type IN ('hpss_archive_collection', 'hpss_archive_revision')
          AND lower(a.status) IN ('available', 'complete', 'completed', 'verified')
        ORDER BY a.id
        """
    )
    for link in links:
        row = rows.get(link["source_entity_id"])
        if row is None or row["type"] not in RUN_TYPES | POOL_TYPES | {"collection", "simulation_history_directory"}:
            continue
        relation = object_json(link["metadata_json"])
        archive = object_json(link["archive_metadata"])
        machine, path = entity_location(row, {**archive, **metadata[row["id"]], **relation})
        archive_path = relation.get("hpss_path") or link["archive_path"]
        if not machine or not path or not isinstance(archive_path, str):
            continue
        archived_at = latest([utc_stamp(value) for value in (
            relation.get("archive_recorded_at_utc"), archive.get("archive_recorded_at_utc"),
            archive.get("verification_completed_at_utc"),
        )])
        note = "Restart files excluded" if archive.get("exclude_restart_files") else "Archive recorded"
        records.append(storage_item(
            machine, "archive", row["name"], path, archive_path=archive_path,
            observed_at=archived_at, note=note,
        ))
        if row["type"] in RUN_TYPES:
            artifact = archive_artifacts.get((row["id"], archive_path), {})
            rank, state, detail = run_archive_evidence(
                row, metadata[row["id"]], relation, archive, artifact, link["archive_type"],
            )
            priority = (rank, archived_at or "")
            previous = best_evidence.get(row["id"])
            if previous is None or priority > previous[0]:
                best_evidence[row["id"]] = (priority, {
                    "archive_state": state,
                    "archive_detail": detail,
                    "archive_path": archive_path,
                })
    return records, {entity_id: entry[1] for entity_id, entry in best_evidence.items()}


def measured_directories(
    repo: ReadOnlyRepository, rows: dict[int, sqlite3.Row], metadata: dict[int, dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    records = []
    metrics = repo.conn.execute(
        """
        SELECT entity_id, name, value, context_json FROM metric
        WHERE name IN ('storage_allocated_bytes', 'whole_pool_allocated_bytes')
          AND unit IN ('byte', 'bytes')
        """
    )
    for metric in metrics:
        row = rows.get(metric["entity_id"])
        context = object_json(metric["context_json"])
        size = nonnegative_bytes(metric["value"])
        if row is None or size is None:
            continue
        machine, path = remote_location(
            context.get("observed_path") or context.get("source_root"), metadata[row["id"]],
        )
        if not machine or not path:
            continue
        records.append(storage_item(
            machine, "disk", row["name"] if metric["name"] == "storage_allocated_bytes"
            else posixpath.basename(path), path, size=size,
            observed_at=utc_stamp(context.get("observed_at_utc")), note="Observed allocated bytes",
        ))
    return records


def quota_observations(
    repo: ReadOnlyRepository,
) -> list[tuple[str, dict[str, Any]]]:
    """Read machine-wide audits that may belong to a project instead of a campaign."""
    records = []
    summaries = repo.conn.execute(
        """
        SELECT a.path, e.metadata_json
        FROM artifact a JOIN entity e ON e.id=a.entity_id
        WHERE a.kind='storage_audit_final_summary' AND a.format='json'
          AND e.type='storage_audit' AND lower(e.status) IN ('complete', 'completed')
        """
    )
    for summary in summaries:
        file = Path(summary["path"])
        try:
            if not file.is_absolute() or not file.is_file() or file.stat().st_size > 1024**2:
                continue
            report = object_json(file.read_text())
        except (OSError, UnicodeError):
            continue
        info = object_json(summary["metadata_json"])
        machine, path = remote_location(report.get("root"), info)
        if machine != "perlmutter" or not path or path != info.get("scratch_root") or not path.startswith("/pscratch/"):
            continue
        if not (observed_at := utc_stamp(report.get("observed_at_utc"))):
            continue
        used_tib = report.get("quota_reported_used_tib")
        used = None
        if isinstance(used_tib, (int, float)) and not isinstance(used_tib, bool):
            used_bytes = used_tib * TIB
            if isinstance(used_bytes, int) or math.isfinite(used_bytes):
                used = nonnegative_bytes(round(used_bytes))
        records.append((machine, {
            "known_bytes": nonnegative_bytes(report.get("allocated_bytes")),
            "quota_limit_bytes": nonnegative_bytes(report.get("quota_bytes")),
            "quota_used_bytes": used,
            "storage_scope": "SCRATCH",
            "updated_at": observed_at,
        }))
    return records


def machine_summary(
    seen_machines: set[str],
    storage: list[tuple[str, dict[str, Any]]],
    quotas: list[tuple[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    machines = {}
    for machine in seen_machines | {host for host, _ in storage + quotas}:
        entries = {}
        for host, item in storage:
            if host != machine:
                continue
            item_id = item["id"]
            previous = entries.get(item_id)
            if not previous or (previous["observed_at"] or "") < (item["observed_at"] or ""):
                entries[item_id] = item
        items = sorted(entries.values(), key=lambda item: (item["kind"], item["name"].casefold(), item["id"]))
        counts = Counter(item["kind"] for item in items)
        defaults = dict.fromkeys(("known_bytes", "quota_used_bytes", "quota_limit_bytes", "updated_at", "storage_scope"))
        latest_quota = max((quota for host, quota in quotas if host == machine),
                           key=lambda quota: quota["updated_at"], default={})
        machines[machine] = {
            "id": machine, "name": MACHINE_NAMES[machine], "remote_runs": counts["remote"],
            "archived_runs": counts["archive"], "measured_runs": counts["disk"], "storage": items,
            **defaults, **latest_quota,
        }
    return sorted(machines.values(), key=lambda item: item["name"])


def local_snapshot(now: datetime, config_dir: Path | None) -> dict[str, Any]:
    snapshot = {
        "generated_at": utc_stamp(now.isoformat()), "mode": "local",
        "campaigns": [], "machines": [], "alerts": [], "hpc_status": "unchecked",
    }
    alerts = snapshot["alerts"]
    try:
        registry = load_user_index(config_dir)["campaigns"]
    except (OSError, ValueError) as error:
        alerts.append({"id": "registry", "title": "Campaign registry unavailable", "detail": str(error)})
        return snapshot

    entries = {}
    unregistered = 0
    for entry in registry:
        if not isinstance(entry, dict) or not isinstance(entry.get("uid"), str) or not entry["uid"].strip():
            unregistered += 1
            continue
        entries[entry["uid"]] = {
            "uid": entry["uid"], "name": str(entry.get("name") or entry["uid"]),
            "profile_path": entry.get("profile") if isinstance(entry.get("profile"), str) else None,
            "database_path": entry.get("database") if isinstance(entry.get("database"), str) else None,
        }
    if unregistered:
        alerts.append({
            "id": "registry-entries", "title": "Incomplete campaign registry",
            "detail": f"{unregistered} registry entries have no campaign identifier.",
        })
    if not entries and not unregistered:
        alerts.append({
            "id": "registry-empty", "title": "No registered campaigns",
            "detail": "Datamak Lite uses the campaign registry to find local campaign databases.",
        })

    by_database: dict[Path | None, list[dict[str, Any]]] = {}
    for entry in entries.values():
        database = entry["database_path"]
        path = Path(database).expanduser() if database else None
        if path and not path.is_absolute():
            profile = entry["profile_path"]
            path = (Path(profile).expanduser().parent / path).absolute() if profile else None
        by_database.setdefault(path, []).append({**entry, "database_path": str(path) if path else database})

    campaigns, storage, quotas = [], [], []
    seen_machines = set()
    for path, registered in by_database.items():
        remaining = registered.copy()
        try:
            if path is None or not path.is_file():
                raise FileNotFoundError(path)
            with ReadOnlyRepository(path) as repo:
                all_rows, info = {}, {}
                for entry in registered:
                    try:
                        entity = repo.get_entity(entry["uid"])
                    except KeyError:
                        entity = None
                    if entity is None or entity["type"] != "campaign":
                        add_unavailable(campaigns, alerts, entry, "Registered campaign is missing from its database.")
                        remaining.remove(entry)
                        continue
                    rows = repo.list_entities(campaign_uid=entry["uid"])
                    for row in rows:
                        row_id = row["id"]
                        if row_id not in all_rows:
                            all_rows[row_id] = row
                            info[row_id] = object_json(row["metadata_json"])
                    campaign, alert = campaign_details(entry, rows, info, now)
                    campaigns.append(campaign)
                    if alert:
                        alerts.append(alert)
                    remaining.remove(entry)
                archives, archive_evidence = archived_sources(repo, all_rows, info)
                gx_input_entities = {
                    row["entity_id"]
                    for row in repo.conn.execute("SELECT DISTINCT entity_id FROM artifact WHERE kind='gx_input'")
                }
                for row_id, row in all_rows.items():
                    if row["type"] not in RUN_TYPES | POOL_TYPES | {"job"}:
                        continue
                    metadata = info[row_id]
                    machine, remote_path = entity_location(row, metadata)
                    if machine:
                        seen_machines.add(machine)
                    if (machine and remote_path and row["type"] in RUN_TYPES
                            and row["status"].lower() not in NO_REMOTE_DIRECTORY
                            and metadata.get("run_directory_exists") is not False
                            and record(metadata.get("remote_metadata")).get("exists") is not False):
                        storage.append(storage_item(
                            machine, "remote", row["name"], remote_path,
                            observed_at=observation(metadata), note="Recorded remote path",
                            archive_plan_eligible=gx_archive_plan_eligible(
                                machine, remote_path, metadata, row_id in gx_input_entities,
                            ),
                            **archive_evidence.get(row_id, {}),
                        ))
                storage.extend(archives)
                storage.extend(measured_directories(repo, all_rows, info))
                quotas.extend(quota_observations(repo))
        except (OSError, ValueError, sqlite3.Error) as error:
            message = (
                "Registered database file is missing." if isinstance(error, FileNotFoundError)
                else "Datamak Lite database could not be read."
            )
            if not remaining:
                alerts.append({"id": f"storage:{path}", "title": "Storage records unavailable", "detail": message})
            for entry in remaining:
                add_unavailable(campaigns, alerts, entry, message)

    state_order = {"running": 0, "pending": 1, "attention": 2, "idle": 3, "unavailable": 4}
    snapshot["campaigns"] = sorted(campaigns, key=lambda item: (state_order[item["state"]], item["name"].casefold()))
    alerts.sort(key=lambda alert: not alert["id"].endswith(":check-status"))
    snapshot["machines"] = machine_summary(seen_machines, storage, quotas)
    if any(campaign["checked_at"] for campaign in campaigns):
        snapshot["hpc_status"] = "cached"
    return snapshot


def add_unavailable(
    campaigns: list[dict[str, Any]], alerts: list[dict[str, Any]], entry: dict[str, Any], message: str,
) -> None:
    campaigns.append({
        **entry, **dict.fromkeys((*RUN_STATES, "updated_at", "checked_at", "count_kind")),
        "machines": [], "state": "unavailable", "message": message,
    })
    alerts.append({
        "id": f"{entry['uid']}:database", "title": "Campaign database unavailable",
        "detail": message, "campaign_uid": entry["uid"],
    })


def demo_snapshot(now: datetime) -> dict[str, Any]:
    generated = utc_stamp(now.isoformat())
    observed = utc_stamp((now - timedelta(days=2)).date().isoformat() + "T15:00:00Z")
    updated = utc_stamp((now - timedelta(days=1)).date().isoformat() + "T15:00:00Z")
    campaigns = []
    for uid, name, host, running, pending, completed, attention, state in [
        ("demo_replay", "Tungsten transport · GX replay", "Perlmutter", 8, 12, 36, 0, "running"),
        ("demo_nstxu", "NSTX-U · XGC edge scan", "Polaris", 0, 4, 18, 0, "pending"),
        ("demo_pedestal", "Pedestal · resolution scan", "Aurora", 0, 0, 24, 0, "idle"),
    ]:
        campaigns.append({
            "uid": uid, "name": name, "profile_path": None, "database_path": None,
            "machines": [host], "running": running, "pending": pending, "completed": completed,
            "attention": attention, "state": state, "count_kind": "runs", "updated_at": updated,
            "checked_at": observed,
            "message": None,
        })
    _, replay_disk = storage_item(
        "perlmutter", "disk", "Replay campaign data", "/scratch/demo/gx/replay",
        size=7 * TIB, observed_at=observed, note="Observed allocated bytes",
    )
    _, replay_archive = storage_item(
        "perlmutter", "archive", "Baseline replay", "/scratch/demo/gx/replay/baseline",
        observed_at=observed, archive_path="hpss://nersc/demo/gx/baseline", note="Verified directory revision",
    )
    _, replay_remote = storage_item(
        "perlmutter", "remote", "Replay scan", "/scratch/demo/gx/replay/scan", observed_at=observed,
        archive_plan_eligible=True,
    )
    _, queued_remote = storage_item(
        "perlmutter", "remote", "Queued replay", "/scratch/demo/gx/replay/queued", observed_at=observed,
        archive_plan_eligible=True,
    )
    _, baseline_remote = storage_item(
        "perlmutter", "remote", "Baseline replay", "/scratch/demo/gx/replay/baseline", observed_at=observed,
        archive_path=replay_archive["archive_path"], archive_state="archived",
        archive_detail="The verified archive matches the last recorded source inventory.",
        archive_plan_eligible=True,
    )
    _, older_archive = storage_item(
        "perlmutter", "archive", "Continued replay", "/scratch/demo/gx/replay/continued",
        observed_at=observed, archive_path="hpss://nersc/demo/gx/continued",
        note="Verified earlier directory revision",
    )
    _, continued_remote = storage_item(
        "perlmutter", "remote", "Continued replay", "/scratch/demo/gx/replay/continued", observed_at=updated,
        archive_path=older_archive["archive_path"], archive_state="review",
        archive_detail="The recorded source inventory changed after this archive was verified.",
        archive_plan_eligible=True,
    )
    _, xgc_remote = storage_item("polaris", "remote", "NSTX-U edge scan", "/scratch/demo/xgc/edge_scan")
    _, pedestal_remote = storage_item("aurora", "remote", "Pedestal resolution scan", "/scratch/demo/gx/pedestal")
    machines = [
        {
            "id": "perlmutter", "name": "Perlmutter", "remote_runs": 4, "archived_runs": 2,
            "known_bytes": 18 * TIB, "measured_runs": 1, "quota_used_bytes": 18 * TIB,
            "quota_limit_bytes": 20 * TIB, "updated_at": observed, "storage_scope": "SCRATCH",
            "storage": [
                replay_remote, queued_remote, baseline_remote, continued_remote,
                replay_disk, replay_archive, older_archive,
            ],
        },
        {
            "id": "polaris", "name": "Polaris", "remote_runs": 1, "archived_runs": 0,
            "known_bytes": None, "measured_runs": 0, "quota_used_bytes": None,
            "quota_limit_bytes": None, "updated_at": None, "storage_scope": None, "storage": [xgc_remote],
        },
        {
            "id": "aurora", "name": "Aurora", "remote_runs": 1, "archived_runs": 0,
            "known_bytes": None, "measured_runs": 0, "quota_used_bytes": None,
            "quota_limit_bytes": None, "updated_at": None, "storage_scope": None, "storage": [pedestal_remote],
        },
    ]
    return {
        "generated_at": generated, "mode": "demo", "campaigns": campaigns, "machines": machines,
        "alerts": [{
            "id": "demo_replay:check-status", "title": "Scheduler mismatch",
            "detail": "Slurm job 4821071 reached its time limit. Datamak still shows 8 cases running.",
            "campaign_uid": "demo_replay",
        }],
        "hpc_status": "cached",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true", help="Show example campaigns.")
    parser.add_argument("--config-dir", type=Path, help="Read campaigns.json from this directory.")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    snapshot = demo_snapshot(now) if args.demo else local_snapshot(now, args.config_dir)
    print(json.dumps(snapshot, ensure_ascii=False))


if __name__ == "__main__":
    main()
