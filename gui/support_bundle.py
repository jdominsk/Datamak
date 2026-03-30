import json
import os
import platform
import shlex
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


KEY_TABLES = (
    "data_origin",
    "data_equil",
    "transp_timeseries",
    "gk_study",
    "gk_model",
    "gk_input",
    "gk_batch",
    "gk_run",
    "gk_surrogate",
    "sg_estimate",
    "flux_action_log",
)

REDACTED_KEY_PARTS = ("password", "secret", "token", "identity")


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in (value or "").strip().lower())
    cleaned = cleaned.strip("_")
    return cleaned or "action"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _redact_mapping(payload: Optional[Mapping[str, object]]) -> Dict[str, object]:
    if not payload:
        return {}
    redacted: Dict[str, object] = {}
    for key, value in payload.items():
        lowered = str(key or "").strip().lower()
        if any(part in lowered for part in REDACTED_KEY_PARTS):
            redacted[str(key)] = "[redacted]"
        else:
            redacted[str(key)] = _json_safe(value)
    return redacted


def _load_usage_log_tail(usage_log_path: str, limit: int = 50) -> Dict[str, object]:
    if not usage_log_path or not os.path.exists(usage_log_path):
        return {"events": []}
    try:
        with open(usage_log_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        return {"events": [], "error": str(exc)}
    events = payload.get("events", []) if isinstance(payload, dict) else []
    if not isinstance(events, list):
        events = []
    return {"events": events[-limit:]}


def _collect_db_summary(db_path: Optional[str]) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "db_path": str(db_path or ""),
        "exists": bool(db_path and os.path.exists(db_path)),
        "is_file": bool(db_path and os.path.isfile(db_path)),
        "tables": [],
        "row_counts": {},
    }
    if not db_path or not os.path.isfile(db_path):
        return summary
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        summary["error"] = str(exc)
        return summary
    try:
        table_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = [str(row[0]) for row in table_rows]
        summary["tables"] = tables
        row_counts: Dict[str, int] = {}
        for table in KEY_TABLES:
            if table not in tables:
                continue
            try:
                row_counts[table] = int(
                    conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
            except sqlite3.Error as exc:
                row_counts[table] = -1
                summary.setdefault("count_errors", {})[table] = str(exc)
        summary["row_counts"] = row_counts
    except sqlite3.Error as exc:
        summary["error"] = str(exc)
    finally:
        conn.close()
    return summary


def _prune_old_bundles(bundle_dir: Path, keep_last: int) -> None:
    try:
        bundles = sorted(
            bundle_dir.glob("*.zip"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return
    for path in bundles[keep_last:]:
        try:
            path.unlink()
        except OSError:
            continue


def _relative_to_project(path: Path, project_dir: str) -> str:
    try:
        return str(path.resolve().relative_to(Path(project_dir).resolve()))
    except (OSError, ValueError):
        return str(path)


def create_support_bundle(
    *,
    bundle_dir: str,
    project_dir: str,
    usage_log_path: str,
    action_key: str,
    action_label: str,
    script_path: str,
    command: Sequence[str],
    db_path: Optional[str],
    panel: Optional[str],
    redirect_params: Optional[Mapping[str, object]],
    env_overrides: Optional[Mapping[str, str]],
    failure_kind: str,
    returncode: Optional[int],
    stdout: str,
    stderr: str,
    traceback_text: str,
    action_state: Optional[Mapping[str, object]],
    hpc_config: Optional[Mapping[str, object]],
    keep_last: int = 20,
) -> Optional[str]:
    try:
        bundle_root = Path(bundle_dir)
        bundle_root.mkdir(parents=True, exist_ok=True)
        created_at = datetime.now(timezone.utc)
        created_at_text = created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        filename = (
            f"{created_at.strftime('%Y-%m-%dT%H-%M-%SZ')}_action_"
            f"{_safe_name(action_key or action_label)}_failed.zip"
        )
        bundle_path = bundle_root / filename
        error_text = "\n".join(
            part
            for part in (
                f"Action: {action_label}",
                f"Action key: {action_key}",
                f"Failure kind: {failure_kind}",
                f"Return code: {returncode}" if returncode is not None else "",
                f"Script: {script_path}",
                f"DB: {db_path or ''}",
                f"Panel: {panel or ''}",
            )
            if part
        )
        manifest = {
            "bundle_version": 1,
            "created_at": created_at_text,
            "project_dir": project_dir,
            "cwd": os.getcwd(),
            "python": sys.version,
            "platform": platform.platform(),
            "action": {
                "key": action_key,
                "label": action_label,
                "script_path": script_path,
                "command": [str(item) for item in command],
                "command_text": shlex.join([str(item) for item in command]),
                "db_path": db_path or "",
                "panel": panel or "",
                "redirect_params": _json_safe(dict(redirect_params or {})),
                "failure_kind": failure_kind,
                "returncode": returncode,
            },
            "artifacts": [
                "manifest.json",
                "error.txt",
                "stdout.txt",
                "stderr.txt",
                "traceback.txt",
                "action_state.json",
                "config_redacted.json",
                "env_overrides_redacted.json",
                "usage_log_tail.json",
                "db_summary.json",
            ],
        }
        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest, indent=2))
            archive.writestr("error.txt", error_text + "\n")
            archive.writestr("stdout.txt", (stdout or "") + ("\n" if stdout else ""))
            archive.writestr("stderr.txt", (stderr or "") + ("\n" if stderr else ""))
            archive.writestr(
                "traceback.txt",
                (traceback_text or "") + ("\n" if traceback_text else ""),
            )
            archive.writestr(
                "action_state.json",
                json.dumps(_json_safe(dict(action_state or {})), indent=2),
            )
            archive.writestr(
                "config_redacted.json",
                json.dumps(_redact_mapping(hpc_config), indent=2),
            )
            archive.writestr(
                "env_overrides_redacted.json",
                json.dumps(_redact_mapping(env_overrides), indent=2),
            )
            archive.writestr(
                "usage_log_tail.json",
                json.dumps(_load_usage_log_tail(usage_log_path), indent=2),
            )
            archive.writestr(
                "db_summary.json",
                json.dumps(_collect_db_summary(db_path), indent=2),
            )
        _prune_old_bundles(bundle_root, keep_last=keep_last)
        return _relative_to_project(bundle_path, project_dir)
    except Exception:
        return None
