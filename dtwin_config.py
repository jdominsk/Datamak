#!/usr/bin/env python3
import json
import os
import shlex
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional


REPO_ROOT = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parent))

DEFAULT_CONFIG: Dict[str, Any] = {
    "version": 1,
    "perlmutter": {
        "user": "",
        "host": "perlmutter.nersc.gov",
        "identity": "",
        "control_path": "/tmp/datamak_ssh_%r@%h_%p",
        "control_persist": "10m",
        "connect_timeout": 10,
        "monitor_timeout": 120,
        "base_dir": "",
        "batch_dir": "",
        "gx_path": "",
    },
    "flux": {
        "user": "",
        "host": "flux",
        "base_dir": "",
        "python_bin": "",
        "duo_option": "",
    },
}


def _deepcopy_default() -> Dict[str, Any]:
    return deepcopy(DEFAULT_CONFIG)


def _merge_nested(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_nested(merged[key], value)
        else:
            merged[key] = value
    return merged


def _repo_root(root: Optional[str | Path] = None) -> Path:
    if root:
        return Path(root).expanduser().resolve()
    return REPO_ROOT.resolve()


def get_user_config_path() -> Path:
    override = (os.environ.get("DTWIN_CONFIG") or "").strip()
    if override:
        return Path(override).expanduser()
    xdg = (os.environ.get("XDG_CONFIG_HOME") or "").strip()
    if xdg:
        return Path(xdg).expanduser() / "datamak" / "config.json"
    return Path.home() / ".config" / "datamak" / "config.json"


def get_legacy_config_path(root: Optional[str | Path] = None) -> Path:
    return _repo_root(root) / "db_analysis" / "hpc_config.json"


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        return {}
    return data


def _legacy_to_current(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "version": 1,
        "perlmutter": {
            "user": str(data.get("ssh_user") or ""),
            "host": str(data.get("ssh_host") or "perlmutter.nersc.gov"),
            "identity": str(data.get("ssh_identity") or ""),
            "control_path": str(data.get("ssh_control_path") or ""),
            "control_persist": str(data.get("ssh_control_persist") or ""),
            "connect_timeout": data.get("ssh_connect_timeout") or 10,
            "monitor_timeout": data.get("monitor_timeout") or 120,
            "base_dir": "",
            "batch_dir": "",
            "gx_path": "",
        },
    }


def load_runtime_config(root: Optional[str | Path] = None) -> Dict[str, Any]:
    config = _deepcopy_default()
    user_path = get_user_config_path()
    if user_path.exists():
        try:
            return _merge_nested(config, _load_json(user_path))
        except Exception:
            return config
    legacy_path = get_legacy_config_path(root)
    if legacy_path.exists():
        try:
            return _merge_nested(config, _legacy_to_current(_load_json(legacy_path)))
        except Exception:
            return config
    return config


def save_runtime_config(config: Dict[str, Any]) -> Path:
    path = get_user_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _merge_nested(_deepcopy_default(), config)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return path


def _env_or_none(name: str) -> Optional[str]:
    value = (os.environ.get(name) or "").strip()
    return value or None


def _pick_str(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _pick_int(*values: object, default: int) -> int:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            return int(text)
        except ValueError:
            continue
    return default


def _current_user() -> str:
    return _pick_str(os.environ.get("USER"), Path.home().name)


def _first_letter(user: str) -> str:
    user = (user or "").strip()
    return user[:1].lower() if user else ""


def compose_remote_host(user: str, host: str) -> str:
    host = (host or "").strip()
    user = (user or "").strip()
    if not host:
        return ""
    if "@" in host or not user:
        return host
    return f"{user}@{host}"


def split_remote_host(remote: str) -> tuple[str, str]:
    remote = (remote or "").strip()
    if "@" in remote:
        user, host = remote.split("@", 1)
        return user.strip(), host.strip()
    return "", remote


def _derived_perlmutter(user: str, host: str) -> Dict[str, Any]:
    letter = _first_letter(user)
    base_dir = ""
    gx_path = ""
    if user and letter:
        base_dir = f"/pscratch/sd/{letter}/{user}/DTwin"
        gx_path = f"/global/homes/{letter}/{user}/GX/gx_next6"
    return {
        "user": user,
        "host": host or "perlmutter.nersc.gov",
        "remote": compose_remote_host(user, host or "perlmutter.nersc.gov"),
        "identity": "",
        "control_path": "/tmp/datamak_ssh_%r@%h_%p",
        "control_persist": "10m",
        "connect_timeout": 10,
        "monitor_timeout": 120,
        "base_dir": base_dir,
        "batch_dir": f"{base_dir}/newbatch" if base_dir else "",
        "gx_path": gx_path,
    }


def _derived_flux(user: str, host: str) -> Dict[str, str]:
    base_dir = f"/u/{user}/DTwin/transp_full_auto" if user else ""
    python_bin = f"/u/{user}/pyrokinetics/.venv/bin/python" if user else ""
    return {
        "user": user,
        "host": host or "flux",
        "remote": compose_remote_host(user, host or "flux"),
        "base_dir": base_dir,
        "python_bin": python_bin,
        "duo_option": "",
    }


def resolve_perlmutter_profile(overrides: Optional[Dict[str, object]] = None) -> Dict[str, Any]:
    overrides = overrides or {}
    config = load_runtime_config()
    cfg = config.get("perlmutter", {})
    remote_override = _pick_str(overrides.get("remote"))
    parsed_user, parsed_host = split_remote_host(remote_override)

    user = _pick_str(
        overrides.get("user"),
        parsed_user,
        _env_or_none("DTWIN_PERLMUTTER_USER"),
        cfg.get("user"),
        _current_user(),
    )
    host = _pick_str(
        overrides.get("host"),
        parsed_host,
        _env_or_none("DTWIN_PERLMUTTER_HOST"),
        cfg.get("host"),
        "perlmutter.nersc.gov",
    )
    derived = _derived_perlmutter(user, host)
    remote = compose_remote_host(user, host)
    return {
        "user": user,
        "host": host,
        "remote": _pick_str(remote_override, remote, derived["remote"]),
        "identity": _pick_str(
            overrides.get("identity"),
            _env_or_none("DTWIN_SSH_IDENTITY"),
            cfg.get("identity"),
            derived["identity"],
        ),
        "control_path": _pick_str(
            overrides.get("control_path"),
            _env_or_none("DTWIN_SSH_CONTROL_PATH"),
            cfg.get("control_path"),
            derived["control_path"],
        ),
        "control_persist": _pick_str(
            overrides.get("control_persist"),
            _env_or_none("DTWIN_SSH_CONTROL_PERSIST"),
            cfg.get("control_persist"),
            derived["control_persist"],
        ),
        "connect_timeout": _pick_int(
            overrides.get("connect_timeout"),
            _env_or_none("DTWIN_SSH_CONNECT_TIMEOUT"),
            cfg.get("connect_timeout"),
            default=int(derived["connect_timeout"]),
        ),
        "monitor_timeout": _pick_int(
            overrides.get("monitor_timeout"),
            _env_or_none("DTWIN_MONITOR_TIMEOUT"),
            cfg.get("monitor_timeout"),
            default=int(derived["monitor_timeout"]),
        ),
        "base_dir": _pick_str(
            overrides.get("base_dir"),
            _env_or_none("DTWIN_PERLMUTTER_BASE_DIR"),
            cfg.get("base_dir"),
            derived["base_dir"],
        ),
        "batch_dir": _pick_str(
            overrides.get("batch_dir"),
            _env_or_none("DTWIN_PERLMUTTER_BATCH_DIR"),
            cfg.get("batch_dir"),
            derived["batch_dir"],
        ),
        "gx_path": _pick_str(
            overrides.get("gx_path"),
            _env_or_none("DTWIN_GX_PATH"),
            cfg.get("gx_path"),
            derived["gx_path"],
        ),
    }


def resolve_flux_profile(overrides: Optional[Dict[str, object]] = None) -> Dict[str, str]:
    overrides = overrides or {}
    config = load_runtime_config()
    cfg = config.get("flux", {})
    remote_override = _pick_str(overrides.get("remote"))
    parsed_user, parsed_host = split_remote_host(remote_override)
    user = _pick_str(
        overrides.get("user"),
        parsed_user,
        _env_or_none("DTWIN_FLUX_USER"),
        cfg.get("user"),
        _current_user(),
    )
    host = _pick_str(
        overrides.get("host"),
        parsed_host,
        _env_or_none("DTWIN_FLUX_HOST"),
        cfg.get("host"),
        "flux",
    )
    derived = _derived_flux(user, host)
    return {
        "user": user,
        "host": host,
        "remote": _pick_str(remote_override, derived["remote"]),
        "base_dir": _pick_str(
            overrides.get("base_dir"),
            _env_or_none("DTWIN_FLUX_BASE_DIR"),
            cfg.get("base_dir"),
            derived["base_dir"],
        ),
        "python_bin": _pick_str(
            overrides.get("python_bin"),
            _env_or_none("DTWIN_FLUX_PYTHON"),
            cfg.get("python_bin"),
            derived["python_bin"],
        ),
        "duo_option": _pick_str(
            overrides.get("duo_option"),
            _env_or_none("DTWIN_FLUX_DUO_OPTION"),
            cfg.get("duo_option"),
            derived["duo_option"],
        ),
    }


def resolve_sources_profile(overrides: Optional[Dict[str, object]] = None) -> Dict[str, str]:
    overrides = overrides or {}
    config = load_runtime_config()
    cfg = config.get("sources", {})
    return {
        "mate_root": _pick_str(
            overrides.get("mate_root"),
            _env_or_none("DTWIN_MATE_ROOT"),
            cfg.get("mate_root"),
        ),
        "transp_copy_root_09": _pick_str(
            overrides.get("transp_copy_root_09"),
            _env_or_none("DTWIN_TRANSP_COPY_ROOT_09"),
            cfg.get("transp_copy_root_09"),
        ),
        "transp_copy_root_10": _pick_str(
            overrides.get("transp_copy_root_10"),
            _env_or_none("DTWIN_TRANSP_COPY_ROOT_10"),
            cfg.get("transp_copy_root_10"),
        ),
    }


def require_source_path(key: str, explicit: Optional[str] = None) -> str:
    profile = resolve_sources_profile({key: explicit} if explicit is not None else None)
    value = _pick_str(profile.get(key))
    if value:
        return value
    env_hint = {
        "mate_root": "DTWIN_MATE_ROOT",
        "transp_copy_root_09": "DTWIN_TRANSP_COPY_ROOT_09",
        "transp_copy_root_10": "DTWIN_TRANSP_COPY_ROOT_10",
    }.get(key, key.upper())
    raise SystemExit(
        f"Missing required source path '{key}'. Provide CLI argument or set {env_hint}. "
        "Source locations are not managed from the GUI workflow settings."
    )


def load_gui_workflow_config() -> Dict[str, str]:
    perlmutter = resolve_perlmutter_profile()
    flux = resolve_flux_profile()
    return {
        "config_path": str(get_user_config_path()),
        "ssh_user": perlmutter["user"],
        "ssh_host": perlmutter["host"],
        "ssh_identity": perlmutter["identity"],
        "ssh_control_path": perlmutter["control_path"],
        "ssh_control_persist": perlmutter["control_persist"],
        "ssh_connect_timeout": str(perlmutter["connect_timeout"]),
        "monitor_timeout": str(perlmutter["monitor_timeout"]),
        "perlmutter_base_dir": perlmutter["base_dir"],
        "perlmutter_batch_dir": perlmutter["batch_dir"],
        "gx_path": perlmutter["gx_path"],
        "flux_user": flux["user"],
        "flux_host": flux["host"],
        "flux_base_dir": flux["base_dir"],
        "flux_python_bin": flux["python_bin"],
        "flux_duo_option": flux["duo_option"],
    }


def _int_or_blank(value: object) -> object:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return int(text)
    except ValueError:
        return text


def save_gui_workflow_config(payload: Dict[str, str]) -> Path:
    config = load_runtime_config()
    config["perlmutter"] = {
        "user": _pick_str(payload.get("ssh_user")),
        "host": _pick_str(payload.get("ssh_host")),
        "identity": _pick_str(payload.get("ssh_identity")),
        "control_path": _pick_str(payload.get("ssh_control_path")),
        "control_persist": _pick_str(payload.get("ssh_control_persist")),
        "connect_timeout": _int_or_blank(payload.get("ssh_connect_timeout")),
        "monitor_timeout": _int_or_blank(payload.get("monitor_timeout")),
        "base_dir": _pick_str(payload.get("perlmutter_base_dir")),
        "batch_dir": _pick_str(payload.get("perlmutter_batch_dir")),
        "gx_path": _pick_str(payload.get("gx_path")),
    }
    config["flux"] = {
        "user": _pick_str(payload.get("flux_user")),
        "host": _pick_str(payload.get("flux_host")),
        "base_dir": _pick_str(payload.get("flux_base_dir")),
        "python_bin": _pick_str(payload.get("flux_python_bin")),
        "duo_option": _pick_str(payload.get("flux_duo_option")),
    }
    return save_runtime_config(config)


def format_shell_exports(values: Dict[str, Any]) -> str:
    lines = []
    for key in sorted(values.keys()):
        value = values[key]
        if value is None:
            value = ""
        lines.append(f"export {key}={shlex.quote(str(value))}")
    return "\n".join(lines)
