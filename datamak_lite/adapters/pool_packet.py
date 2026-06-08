from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def build_pool_packet(
    pool_root: str | Path,
    *,
    uid: str | None = None,
    name: str | None = None,
    campaign_uid: str | None = None,
    campaign_name: str | None = None,
    uses_dataset_uid: str | None = None,
    dataset_name: str | None = None,
    dataset_path: str | None = None,
    dataset_type: str = "dataset",
    relation_type: str = "uses_input",
    status: str = "prepared",
    scientific_status: str = "candidate",
    note: str | None = None,
    author: str = "",
) -> dict[str, Any]:
    """Build a sidecar packet for a generic pool directory.

    This adapter is intentionally conservative.  It records the pool root,
    obvious local artifacts, simple metadata parsed from the folder name, and
    explicit upstream data supplied by the caller.  It should be cheap enough
    to run whenever an agent prepares or updates a pool.
    """

    root = Path(pool_root).expanduser()
    root_path = str(root.resolve())
    pool_uid = uid or _slug(f"pool_{root.name}")
    pool_name = name or _humanize_pool_name(root.name)
    manifest_path = root / "pool_manifest.json"
    manifest = _read_json_if_present(manifest_path)
    inferred = _infer_metadata_from_name(root.name)
    metadata = {
        "adapter": "pool_packet",
        **inferred,
        **_compact_manifest_metadata(manifest),
    }

    packet: dict[str, Any] = {
        "schema_version": 1,
        "root_uid": pool_uid,
        "entities": [
            {
                "uid": pool_uid,
                "type": "pool",
                "name": pool_name,
                "path": root_path,
                "status": status,
                "scientific_status": scientific_status,
                "description": "Pool registered by Datamak Lite.",
                "metadata": metadata,
            }
        ],
        "relations": [],
        "artifacts": _pool_artifacts(root, pool_uid),
        "metrics": _pool_metrics(pool_uid, metadata),
        "notes": [
            {
                "entity_uid": pool_uid,
                "note_type": "comment",
                "author": author,
                "markdown_text": note or "Auto-generated Datamak Lite sidecar for a pool.",
            }
        ],
    }

    if campaign_uid:
        packet["entities"].append(
            {
                "uid": campaign_uid,
                "type": "campaign",
                "name": campaign_name or campaign_uid,
                "status": "active",
                "scientific_status": "candidate",
                "description": "Campaign containing this pool.",
                "metadata": {},
            }
        )
        packet["relations"].append(
            {
                "source_uid": pool_uid,
                "relation_type": "member_of",
                "target_uid": campaign_uid,
            }
        )

    dataset_uid = uses_dataset_uid or (_slug(f"{dataset_type}_{Path(dataset_path).stem}") if dataset_path else None)
    if dataset_uid:
        packet["entities"].append(
            {
                "uid": dataset_uid,
                "type": dataset_type,
                "name": dataset_name or (Path(str(dataset_path)).name if dataset_path else dataset_uid),
                "path": dataset_path,
                "status": "available" if dataset_path else "unknown",
                "scientific_status": "candidate",
                "description": "Input dataset used by this pool.",
                "metadata": {},
            }
        )
        packet["relations"].append(
            {
                "source_uid": pool_uid,
                "relation_type": relation_type,
                "target_uid": dataset_uid,
                "note": "Input dataset for the pool.",
            }
        )

    return packet


def write_pool_packet(packet: dict[str, Any], pool_root: str | Path, output: str | Path | None = None) -> Path:
    output_path = Path(output).expanduser() if output else Path(pool_root).expanduser() / "datamak_lite.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(packet, indent=2, sort_keys=False) + "\n")
    return output_path


def _read_json_if_present(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _compact_manifest_metadata(manifest: dict[str, Any]) -> dict[str, Any]:
    if not manifest:
        return {}
    selected: dict[str, Any] = {"has_pool_manifest": True}
    for key in (
        "run_root",
        "history_file",
        "field_history",
        "source_history",
        "time_start",
        "time_end",
        "t_start",
        "t_end",
        "effective_stride",
        "step_stride",
        "saved_stride",
        "model",
        "variant",
    ):
        if key in manifest and _is_scalar(manifest[key]):
            selected[key] = manifest[key]
    return selected


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _pool_artifacts(root: Path, pool_uid: str) -> list[dict[str, Any]]:
    artifacts = [
        {
            "entity_uid": pool_uid,
            "kind": "run_root",
            "path": str(root.resolve()),
            "description": "Pool root.",
        }
    ]
    candidates = [
        ("pool_db", "pool.db", "sqlite", "SQLite pool database."),
        ("pool_db", "replay_pool.db", "sqlite", "SQLite pool database."),
        ("datamak_pool_marker", "datamak_pool.json", "json", "Datamak pool marker."),
        ("pool_manifest", "pool_manifest.json", "json", "Pool manifest."),
        ("manifest", "manifest.json", "json", "Manifest."),
        ("readme", "README.md", "markdown", "Pool README."),
        ("status_script", "pool_status.py", "python", "Pool status script."),
        ("setup_script", "setup_pool.py", "python", "Pool setup script."),
        ("setup_script", "setup_replay_pool.py", "python", "Pool setup script."),
        ("interactive_launcher", "run_pool_interactive.sh", "shell", "Interactive launcher."),
        ("interactive_launcher", "run_replay_pool_interactive.sh", "shell", "Interactive launcher."),
        ("analysis_dir", "analysis", "directory", "Analysis output directory."),
        ("cases_dir", "cases", "directory", "Case input/output directory."),
    ]
    for kind, relative, fmt, description in candidates:
        path = root / relative
        if path.exists():
            artifacts.append(
                {
                    "entity_uid": pool_uid,
                    "kind": kind,
                    "path": str(path.resolve()),
                    "format": fmt,
                    "description": description,
                }
            )
    return artifacts


def _pool_metrics(pool_uid: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    metric_map = {
        "time_start": "time",
        "time_end": "time",
        "effective_stride": "step",
        "step_stride": "step",
        "saved_stride": "step",
        "history_saved_stride": "step",
    }
    metrics: list[dict[str, Any]] = []
    for key, unit in metric_map.items():
        value = metadata.get(key)
        if isinstance(value, (int, float)):
            metrics.append({"entity_uid": pool_uid, "name": key, "value": value, "unit": unit})
    return metrics


def _infer_metadata_from_name(name: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    time_match = re.search(r"t(\d+(?:p\d+)?)_t(\d+(?:p\d+)?)", name)
    if time_match:
        metadata["time_start"] = _number_from_token(time_match.group(1))
        metadata["time_end"] = _number_from_token(time_match.group(2))
    eff_stride = re.search(r"effstride(\d+)", name)
    if eff_stride:
        metadata["effective_stride"] = int(eff_stride.group(1))
    hist_stride = re.search(r"stride(\d+)hist", name)
    if hist_stride:
        metadata["history_saved_stride"] = int(hist_stride.group(1))
    elif "stride1hist" in name:
        metadata["history_saved_stride"] = 1
    return metadata


def _number_from_token(token: str) -> float | int:
    value = float(token.replace("p", "."))
    return int(value) if value.is_integer() else value


def _slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "datamak_lite_object"


def _humanize_pool_name(name: str) -> str:
    return name.replace("_", " ").replace("-", " ")
