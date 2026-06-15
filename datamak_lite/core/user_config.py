from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


USER_CONFIG_SCHEMA_VERSION = 1
DATAMAK_HOME_ENV = "DATAMAK_HOME"
INDEX_FILENAME = "campaigns.json"


def default_config_dir() -> Path:
    """Return the machine-level Datamak config directory."""
    configured = os.environ.get(DATAMAK_HOME_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".datamak"


def user_index_path(config_dir: str | Path | None = None) -> Path:
    return _config_dir(config_dir) / INDEX_FILENAME


def load_user_index(config_dir: str | Path | None = None) -> dict[str, Any]:
    path = user_index_path(config_dir)
    if not path.exists():
        return {"schema_version": USER_CONFIG_SCHEMA_VERSION, "campaigns": []}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Datamak user campaign index: {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Datamak user campaign index root must be an object: {path}")
    version = data.get("schema_version")
    if version != USER_CONFIG_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported Datamak user campaign index schema_version={version!r}; "
            f"expected {USER_CONFIG_SCHEMA_VERSION}: {path}"
        )
    campaigns = data.get("campaigns", [])
    if not isinstance(campaigns, list):
        raise ValueError(f"Datamak user campaign index field 'campaigns' must be a list: {path}")
    return data


def save_user_index(index: dict[str, Any], config_dir: str | Path | None = None) -> Path:
    path = user_index_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(index, indent=2, sort_keys=True) + "\n"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(payload)
    tmp_path.replace(path)
    return path


def list_registered_campaigns(config_dir: str | Path | None = None) -> list[dict[str, Any]]:
    index = load_user_index(config_dir)
    campaigns = index.get("campaigns", [])
    return sorted((dict(item) for item in campaigns), key=lambda item: (item.get("name", ""), item.get("uid", "")))


def register_campaign_profile(
    profile_path: str | Path,
    *,
    config_dir: str | Path | None = None,
    workspace: str | Path | None = None,
    set_default: bool = False,
    note: str = "",
) -> dict[str, Any]:
    """Register one campaign profile in the machine-level Datamak index."""
    profile_file = Path(profile_path).expanduser().resolve()
    profile = _load_profile(profile_file)
    campaign_uid = _required_string(profile, "campaign_uid", profile_file)
    campaign_name = str(profile.get("campaign_name") or campaign_uid)
    now = _now_iso()

    index = load_user_index(config_dir)
    campaigns = [item for item in index.get("campaigns", []) if isinstance(item, dict)]
    existing = next((item for item in campaigns if item.get("uid") == campaign_uid), None)
    registered_at = str(existing.get("registered_at")) if existing and existing.get("registered_at") else now

    entry = {
        "uid": campaign_uid,
        "name": campaign_name,
        "campaign_type": str(profile.get("campaign_type") or ""),
        "profile": str(profile_file),
        "database": str(_resolve_profile_path(_required_string(profile, "database", profile_file), profile_file.parent)),
        "workspace": str(_resolve_workspace(profile_file, workspace)),
        "overview": str(profile.get("overview") or ""),
        "local_only": bool(profile.get("local_only", False)),
        "packet_roots": [
            str(_resolve_profile_path(value, profile_file.parent))
            for value in _string_list(profile, "packet_roots", profile_file)
        ],
        "source_catalogs": [
            str(_resolve_profile_path(value, profile_file.parent))
            for value in _string_list(profile, "source_catalogs", profile_file)
        ],
        "note": note,
        "registered_at": registered_at,
        "updated_at": now,
    }
    campaigns = [item for item in campaigns if item.get("uid") != campaign_uid]
    campaigns.append(entry)
    index["schema_version"] = USER_CONFIG_SCHEMA_VERSION
    index["campaigns"] = sorted(campaigns, key=lambda item: (str(item.get("name", "")), str(item.get("uid", ""))))
    if set_default or not index.get("default_campaign_uid"):
        index["default_campaign_uid"] = campaign_uid
    save_user_index(index, config_dir)
    return entry


def resolve_registered_campaign(
    selector: str | None = None,
    *,
    config_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Resolve a campaign by uid, name, profile stem, or the configured default."""
    index = load_user_index(config_dir)
    campaigns = [item for item in index.get("campaigns", []) if isinstance(item, dict)]
    if not campaigns:
        raise ValueError(f"No Datamak campaigns are registered in {user_index_path(config_dir)}")

    key = selector or index.get("default_campaign_uid")
    if not isinstance(key, str) or not key:
        raise ValueError("No campaign selector was provided and no default campaign is configured")

    exact = [item for item in campaigns if item.get("uid") == key]
    if not exact:
        exact = [item for item in campaigns if item.get("name") == key]
    if not exact:
        exact = [
            item
            for item in campaigns
            if Path(str(item.get("profile", ""))).stem == key
        ]
    if len(exact) == 1:
        return dict(exact[0])
    if len(exact) > 1:
        matches = ", ".join(str(item.get("uid")) for item in exact)
        raise ValueError(f"Campaign selector {key!r} is ambiguous: {matches}")
    raise ValueError(f"Campaign selector {key!r} was not found in {user_index_path(config_dir)}")


def format_campaigns(campaigns: list[dict[str, Any]]) -> str:
    if not campaigns:
        return "No Datamak campaigns are registered.\n"
    lines = []
    for item in campaigns:
        uid = str(item.get("uid", ""))
        name = str(item.get("name", ""))
        profile = str(item.get("profile", ""))
        database = str(item.get("database", ""))
        lines.append(f"{uid:32s} {name}")
        lines.append(f"{'':32s} profile:  {profile}")
        lines.append(f"{'':32s} database: {database}")
    return "\n".join(lines) + "\n"


def _config_dir(config_dir: str | Path | None) -> Path:
    return Path(config_dir).expanduser() if config_dir is not None else default_config_dir()


def _load_profile(profile_file: Path) -> dict[str, Any]:
    try:
        data = json.loads(profile_file.read_text())
    except FileNotFoundError as exc:
        raise ValueError(f"Campaign profile does not exist: {profile_file}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid campaign profile JSON: {profile_file}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Campaign profile root must be an object: {profile_file}")
    return data


def _required_string(profile: dict[str, Any], key: str, profile_file: Path) -> str:
    value = profile.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Campaign profile must define string field {key!r}: {profile_file}")
    return value


def _string_list(profile: dict[str, Any], key: str, profile_file: Path) -> list[str]:
    value = profile.get(key, [])
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError(f"Campaign profile field {key!r} must be a string or list of strings: {profile_file}")


def _resolve_profile_path(path: str, base_dir: Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()


def _resolve_workspace(profile_file: Path, workspace: str | Path | None) -> Path:
    if workspace is not None:
        return Path(workspace).expanduser().resolve()
    for parent in profile_file.parents:
        if parent.name == ".datamak_lite":
            return parent.parent.resolve()
    for parent in profile_file.parents:
        if (parent / ".git").exists() or (parent / "AGENTS.md").exists():
            return parent.resolve()
    return profile_file.parent.resolve()


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
