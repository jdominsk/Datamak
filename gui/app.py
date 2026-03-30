#!/usr/bin/env python3
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import json
import glob
import math
import random
import time
import traceback
import zipfile
from collections import OrderedDict
from urllib.parse import quote
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from flask import Flask, jsonify, redirect, render_template, request, send_from_directory, url_for
from werkzeug.serving import WSGIRequestHandler
try:
    import numpy as np
    HAVE_NUMPY = True
except Exception:
    np = None
    HAVE_NUMPY = False

APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(APP_DIR))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from dtwin_config import load_gui_workflow_config, resolve_perlmutter_profile, save_gui_workflow_config  # noqa: E402
from gui.actions import (  # noqa: E402
    ACTIONS,
    ActionValidationError,
    ActionSpec,
    ResolvedAction,
    resolve_action_request,
    with_redirect_params,
)
from gui.panels.workflow import build_workflow_panel_context  # noqa: E402
from gui.support_bundle import create_support_bundle  # noqa: E402

DEFAULT_DB = os.path.join(PROJECT_DIR, "gyrokinetic_simulations.db")
DB_UPDATE_DIR = os.path.join(PROJECT_DIR, "db_update")
DOCS_DIR = os.path.join(PROJECT_DIR, "docs")
GUI_STATIC_DIR = os.path.join(APP_DIR, "static")
BATCH_BASE_DIR = os.path.join(PROJECT_DIR, "batch")
BATCH_NEW_DIR = os.path.join(BATCH_BASE_DIR, "new")
BATCH_SENT_DIR = os.path.join(BATCH_BASE_DIR, "sent")
ANALYSIS_DIR = os.path.join(PROJECT_DIR, "db_analysis")
SURROGATE_DIR = os.path.join(PROJECT_DIR, "db_surrogate")
SURROGATE_MODEL_DIR = os.path.join(SURROGATE_DIR, "models")
AI_FEEDBACK_PATH = os.path.join(APP_DIR, "ai_feedback.json")
AI_FEEDBACK_LOCK = threading.Lock()
MONITOR_REPORT_PATH = os.path.join(ANALYSIS_DIR, "remote_monitor_report.json")
HPC_TEST_PATH = os.path.join(ANALYSIS_DIR, "hpc_test_result.json")
USAGE_LOG_PATH = os.path.join(ANALYSIS_DIR, "monitor_feedback.json")
SUPPORT_BUNDLE_DIR = os.path.join(PROJECT_DIR, "output", "support_bundles")
USAGE_LOG_LOCK = threading.Lock()
DEMO_DATABASE_URL = (
    "https://drive.google.com/file/d/1XS1jpbqQICJNDR6AU_wXeYG7BM_nt4yM/view"
    "?usp=share_link"
)
DEMO_DATABASE_GLOB = "gyrokinetic_simulations*.db"
SUPPORT_EMAIL_RECIPIENT = (
    os.environ.get("DATAMAK_SUPPORT_EMAIL", "jdominsk@pppl.gov").strip()
    or "jdominsk@pppl.gov"
)
GUI_DATA_CACHE_LOCK = threading.Lock()
GUI_DATA_CACHE_MAX_ENTRIES = 128
GUI_DATA_CACHE: "OrderedDict[tuple, object]" = OrderedDict()
LAZY_TABLE_COUNT_TABLES = {
    "gk_input",
    "gk_run",
    "sg_estimate",
    "gk_convergence_timeseries",
    "transp_timeseries",
}

ACTION_LOCK = threading.Lock()
ACTION_MESSAGE_TTL_SECONDS = 20
ACTION_STATE: Dict[str, object] = {
    "running": False,
    "name": None,
    "message": None,
    "technical_message": None,
    "is_error": False,
    "key": None,
    "support_bundle": None,
    "token": 0,
    "completed_at": None,
}

QUIET_REQUEST_PATH_PREFIXES = (
    "/action_status",
    "/usage",
)


def _parse_action_completed_at(value: object) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _prune_action_message_locked() -> None:
    if ACTION_STATE.get("running") or not ACTION_STATE.get("message"):
        return
    completed_at = _parse_action_completed_at(ACTION_STATE.get("completed_at"))
    if completed_at is None:
        return
    expires_at = completed_at + timedelta(seconds=ACTION_MESSAGE_TTL_SECONDS)
    if datetime.now(timezone.utc) < expires_at:
        return
    ACTION_STATE["message"] = None
    ACTION_STATE["technical_message"] = None
    ACTION_STATE["is_error"] = False
    ACTION_STATE["completed_at"] = None
    ACTION_STATE["key"] = None
    ACTION_STATE["support_bundle"] = None


def _should_suppress_request_log(path: str) -> bool:
    normalized = str(path or "").strip()
    return any(normalized.startswith(prefix) for prefix in QUIET_REQUEST_PATH_PREFIXES)


class DatamakRequestHandler(WSGIRequestHandler):
    def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
        if _should_suppress_request_log(getattr(self, "path", "")):
            return
        super().log_request(code, size)

app = Flask(__name__, static_folder="logo")


@app.route("/docs/<path:filename>")
def docs_file(filename: str):
    return send_from_directory(DOCS_DIR, filename)


@app.route("/gui-static/<path:filename>")
def gui_static_file(filename: str):
    return send_from_directory(GUI_STATIC_DIR, filename)


@app.route("/plots/<path:filename>")
def plots_file(filename: str):
    return send_from_directory(os.path.join(BATCH_BASE_DIR, "plots"), filename)


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _db_cache_signature(conn: sqlite3.Connection) -> Tuple[str, int, int]:
    db_path = ""
    try:
        for row in conn.execute("PRAGMA database_list").fetchall():
            if str(row["name"] or "") == "main":
                db_path = str(row["file"] or "")
                break
    except sqlite3.Error:
        return db_path, 0, 0
    if not db_path:
        return db_path, 0, 0
    try:
        stat = os.stat(db_path)
    except OSError:
        return db_path, 0, 0
    return db_path, int(stat.st_mtime_ns), int(stat.st_size)


def _get_cached_gui_value(cache_key: tuple) -> Optional[object]:
    with GUI_DATA_CACHE_LOCK:
        cached = GUI_DATA_CACHE.get(cache_key)
        if cached is None:
            return None
        GUI_DATA_CACHE.move_to_end(cache_key)
        return cached


def _set_cached_gui_value(cache_key: tuple, value: object) -> object:
    with GUI_DATA_CACHE_LOCK:
        GUI_DATA_CACHE[cache_key] = value
        GUI_DATA_CACHE.move_to_end(cache_key)
        while len(GUI_DATA_CACHE) > GUI_DATA_CACHE_MAX_ENTRIES:
            GUI_DATA_CACHE.popitem(last=False)
    return value


def _should_compute_table_counts(table: Optional[str], explicit_request: bool) -> bool:
    if explicit_request:
        return True
    if not table:
        return False
    return table not in LAZY_TABLE_COUNT_TABLES


def load_hpc_config() -> Dict[str, str]:
    return load_gui_workflow_config()


def save_hpc_config(payload: Dict[str, str]) -> None:
    save_gui_workflow_config(payload)


def load_hpc_test_result() -> Optional[Dict[str, object]]:
    try:
        with open(HPC_TEST_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return None


def save_hpc_test_result(payload: Dict[str, object]) -> None:
    Path(HPC_TEST_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(HPC_TEST_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _snapshot_action_state() -> Dict[str, object]:
    with ACTION_LOCK:
        state = dict(ACTION_STATE)
    state["message"] = _sanitize_action_message_text(state.get("message"))
    state["is_error"] = bool(state.get("is_error"))
    return state


def _sanitize_action_message_text(message: object) -> Optional[str]:
    if message is None:
        return None
    cleaned = str(message).replace(
        " Technical details are available below if you need them.", ""
    ).replace(
        "\nTechnical details are available below if you need them.", ""
    )
    return cleaned


def _parse_batch_db_date_label(db_name: str) -> str:
    match = re.match(r"batch_database_(\d{4})(\d{2})(\d{2})_\d{6}\.db$", db_name)
    if not match:
        return db_name
    year, month, day = match.groups()
    return f"{month}/{day}/{year}"


def _parse_status_counts_fragment(fragment: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for part in str(fragment or "").split(","):
        if "=" not in part:
            continue
        status, raw_count = part.split("=", 1)
        status_key = status.strip()
        count_text = raw_count.strip()
        if not status_key:
            continue
        try:
            counts[status_key] = int(count_text)
        except ValueError:
            continue
    return counts


def _format_status_counts_summary(counts: Dict[str, int]) -> str:
    if not counts:
        return ""
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{status} {count}" for status, count in ordered)


def _build_batch_sync_success_summary(raw_message: str) -> Optional[str]:
    lines = [line.strip() for line in str(raw_message or "").splitlines() if line.strip()]
    if not lines:
        return None

    host = ""
    checked_count: Optional[int] = None
    header_match = re.search(r"Checking (\d+) batch DB\(s\) on (.+?)\.\.\.", raw_message)
    if header_match:
        checked_count = int(header_match.group(1))
        host = header_match.group(2).strip()

    batch_info: Dict[str, Dict[str, object]] = {}
    for line in lines:
        fetched_match = re.match(
            r"^(?P<db>batch_database_\d{8}_\d{6}\.db): "
            r"fetched_unsynced=(?P<unsynced>\d+), "
            r"finished\([^)]+\)=(?P<finished>\d+), "
            r"status_counts: (?P<statuses>.+)$",
            line,
        )
        if fetched_match:
            db_name = fetched_match.group("db")
            batch_info.setdefault(db_name, {})
            batch_info[db_name]["unsynced"] = int(fetched_match.group("unsynced"))
            batch_info[db_name]["finished"] = int(fetched_match.group("finished"))
            batch_info[db_name]["status_counts"] = _parse_status_counts_fragment(
                fetched_match.group("statuses")
            )
            continue

        png_match = re.match(
            r"^(?P<db>batch_database_\d{8}_\d{6}\.db): rsynced (?P<count>\d+) png files?\.$",
            line,
        )
        if png_match:
            db_name = png_match.group("db")
            batch_info.setdefault(db_name, {})
            batch_info[db_name]["png_files"] = int(png_match.group("count"))
            continue

        sync_match = re.match(
            r"^(?P<db>batch_database_\d{8}_\d{6}\.db): synchronizing (?P<count>\d+) runs?$",
            line,
        )
        if sync_match:
            db_name = sync_match.group("db")
            batch_info.setdefault(db_name, {})
            batch_info[db_name]["sync_runs"] = int(sync_match.group("count"))
            continue

    if not batch_info:
        return None

    intro_bits: List[str] = []
    if checked_count is not None:
        intro_bits.append(
            f"Checked {checked_count} batch DB{'s' if checked_count != 1 else ''}"
        )
    if host:
        intro_bits.append(f"on {host}")
    intro = " ".join(intro_bits).strip()

    if len(batch_info) == 1:
        db_name, info = next(iter(batch_info.items()))
        batch_label = _parse_batch_db_date_label(db_name)
        run_count = int(info.get("sync_runs") or info.get("unsynced") or 0)
        png_count = int(info.get("png_files") or 0)
        status_counts = info.get("status_counts") or {}
        pieces: List[str] = []
        if intro:
            pieces.append(f"{intro}.")
        if run_count > 0:
            run_phrase = f"Synchronizing {run_count} run{'s' if run_count != 1 else ''}"
            if png_count > 0:
                run_phrase += f", including {png_count} png file{'s' if png_count != 1 else ''}"
            pieces.append(f"Batch {batch_label}: {run_phrase}.")
        status_summary = _format_status_counts_summary(status_counts)
        if status_summary:
            pieces.append(f"Current statuses: {status_summary}.")
        return " ".join(piece.strip() for piece in pieces if piece).strip() or None

    total_runs = sum(int(info.get("sync_runs") or info.get("unsynced") or 0) for info in batch_info.values())
    total_png = sum(int(info.get("png_files") or 0) for info in batch_info.values())
    batches_with_sync = sum(
        1 for info in batch_info.values() if int(info.get("sync_runs") or info.get("unsynced") or 0) > 0
    )
    pieces = [f"{intro}." if intro else ""]
    if batches_with_sync > 0:
        sync_summary = (
            f"Synchronizing {total_runs} run{'s' if total_runs != 1 else ''} "
            f"across {batches_with_sync} batch{'es' if batches_with_sync != 1 else ''}"
        )
        if total_png > 0:
            sync_summary += f", including {total_png} png file{'s' if total_png != 1 else ''}"
        pieces.append(sync_summary + ".")
    return " ".join(piece.strip() for piece in pieces if piece).strip() or None


def _build_human_friendly_success_message(
    action_key: str,
    action_label: str,
    raw_message: str,
) -> Tuple[str, Optional[str]]:
    raw_text = str(raw_message or "").strip()
    if not raw_text:
        return "", None

    summary: Optional[str] = None
    if action_key == "check_launched_batches":
        summary = _build_batch_sync_success_summary(raw_text)

    if summary and summary != raw_text:
        return summary, raw_text
    return raw_text, None


def _append_support_bundle_note(message: str, bundle_path: Optional[str]) -> str:
    if not bundle_path:
        return message
    if "Support bundle:" in message:
        return message
    return f"{message} Support bundle: {bundle_path}"


def _build_human_friendly_failure_message(
    action_label: str,
    technical_message: str,
    *,
    stdout_text: str = "",
    stderr_text: str = "",
    traceback_text: str = "",
    returncode: Optional[int] = None,
) -> str:
    diagnostic_text = "\n".join(
        part
        for part in (
            action_label,
            technical_message,
            stdout_text,
            stderr_text,
            traceback_text,
        )
        if part
    )
    lowered = diagnostic_text.lower()
    action_prefix = f"{action_label} could not complete."
    if (
        "permission denied (publickey" in lowered
        or "keyboard-interactive" in lowered
        or "duo two-factor login" in lowered
    ):
        return (
            f"{action_prefix} Datamak could not log into the remote system. "
            "Check the saved username, SSH access, and Duo setup, then try again."
        )
    if "modulenotfounderror" in lowered or "no module named" in lowered:
        return (
            f"{action_prefix} A required Python module or staged Datamak file is missing "
            "on the execution side. Refresh the staged scripts or environment, then retry."
        )
    if "unable to open database file" in lowered:
        return (
            f"{action_prefix} Datamak could not open the selected database file. "
            "Check that the path exists and points to a readable SQLite database."
        )
    if "foreign key constraint failed" in lowered:
        return (
            f"{action_prefix} Datamak found inconsistent related records while writing "
            "database updates, so it stopped to avoid corrupting the data."
        )
    if "check constraint failed" in lowered:
        return (
            f"{action_prefix} The database rejected a value because it did not match "
            "the allowed workflow states or schema rules."
        )
    if "integrityerror" in lowered:
        return (
            f"{action_prefix} Datamak hit a database integrity check and stopped safely "
            "before applying an invalid update."
        )
    if "permissionerror" in lowered or "[errno 13]" in lowered:
        return (
            f"{action_prefix} Datamak does not have permission to read or write one of "
            "the required files."
        )
    if "file not found" in lowered or "no such file or directory" in lowered:
        return (
            f"{action_prefix} One of the required files or folders could not be found."
        )
    if "returned non-zero exit status" in lowered or returncode not in (None, 0):
        return f"{action_prefix} The underlying command stopped before finishing."
    return f"{action_prefix} Datamak stopped because of an unexpected error."


def _resolve_support_bundle_path(bundle_path: str) -> Optional[Path]:
    if not bundle_path:
        return None
    try:
        candidate = Path(bundle_path).expanduser()
        if not candidate.is_absolute():
            candidate = (Path(PROJECT_DIR) / candidate).resolve()
        else:
            candidate = candidate.resolve()
        bundle_root = Path(SUPPORT_BUNDLE_DIR).resolve()
    except OSError:
        return None
    try:
        candidate.relative_to(bundle_root)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


def _set_support_bundle_feedback(message: str) -> None:
    with ACTION_LOCK:
        current_message = str(ACTION_STATE.get("message") or "").strip()
        if current_message:
            ACTION_STATE["message"] = f"{current_message}\n{message}"
        else:
            ACTION_STATE["message"] = message
        ACTION_STATE["technical_message"] = None
        ACTION_STATE["completed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _escape_applescript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _compose_support_bundle_mail(
    bundle_path: Path, recipient: str, compose_mode: str = "gmail"
) -> Tuple[bool, str]:
    def _read_bundle_member(name: str, max_chars: int = 3000) -> str:
        try:
            with zipfile.ZipFile(bundle_path, "r") as archive:
                with archive.open(name, "r") as handle:
                    content = handle.read().decode("utf-8", errors="replace").strip()
        except Exception:
            return ""
        if len(content) <= max_chars:
            return content
        return f"...\n{content[-max_chars:]}"

    subject = f"Datamak support bundle: {bundle_path.name}"
    error_summary = _read_bundle_member("error.txt", max_chars=1200)
    stderr_excerpt = _read_bundle_member("stderr.txt", max_chars=2400)
    traceback_excerpt = _read_bundle_member("traceback.txt", max_chars=2400)
    details = error_summary
    if stderr_excerpt:
        details = (
            f"{details}\n\nStderr excerpt:\n{stderr_excerpt}"
            if details
            else f"Stderr excerpt:\n{stderr_excerpt}"
        )
    elif traceback_excerpt:
        details = (
            f"{details}\n\nTraceback excerpt:\n{traceback_excerpt}"
            if details
            else f"Traceback excerpt:\n{traceback_excerpt}"
        )
    body = (
        "Datamak generated a support bundle for a failed action.\n\n"
        f"Bundle path: {bundle_path}\n\n"
        f"{details}\n\n"
        "You can still attach the generated zip file if more debugging context is needed.\n"
    )
    normalized_mode = str(compose_mode or "gmail").strip().lower()
    if normalized_mode not in {"gmail", "mail"}:
        normalized_mode = "gmail"
    if normalized_mode == "mail":
        if shutil.which("osascript"):
            applescript = f'''
tell application "Mail"
  set newMessage to make new outgoing message with properties {{visible:true, subject:"{_escape_applescript(subject)}", content:"{_escape_applescript(body)}"}}
  tell newMessage
    make new to recipient at end of to recipients with properties {{address:"{_escape_applescript(recipient)}"}}
    try
      make new attachment with properties {{file name:POSIX file "{_escape_applescript(str(bundle_path))}"}} at after the last paragraph
    end try
  end tell
  activate
end tell
'''.strip()
            try:
                subprocess.run(
                    ["osascript", "-e", applescript],
                    check=True,
                    text=True,
                    capture_output=True,
                )
                return True, ""
            except subprocess.CalledProcessError as exc:
                error_text = (exc.stderr or exc.stdout or str(exc)).strip()
                return False, error_text or "Could not open Apple Mail."
        return False, "Apple Mail integration is not available on this system."
    if shutil.which("open"):
        gmail_url = (
            "https://mail.google.com/mail/?view=cm&fs=1"
            f"&to={quote(recipient)}"
            f"&su={quote(subject)}"
            f"&body={quote(body)}"
        )
        try:
            subprocess.run(
                ["open", gmail_url],
                check=True,
                text=True,
                capture_output=True,
            )
            return True, ""
        except subprocess.CalledProcessError as exc:
            error_text = (exc.stderr or exc.stdout or str(exc)).strip()
            mailto = (
                f"mailto:{quote(recipient)}?subject={quote(subject)}&body="
                f"{quote(body)}"
            )
            try:
                subprocess.run(
                    ["open", mailto],
                    check=True,
                    text=True,
                    capture_output=True,
                )
                return True, ""
            except subprocess.CalledProcessError:
                return False, error_text or "Could not open Gmail or the default mail client."
    return False, "No mail-compose integration is available on this system."


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [row["name"] for row in rows]


def get_table_schema_rows(
    conn: sqlite3.Connection, table: str
) -> List[Dict[str, str]]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    schema_rows: List[Dict[str, str]] = []
    for row in rows:
        schema_rows.append(
            {
                "name": str(row["name"] or ""),
                "type": str(row["type"] or ""),
                "pk": "Yes" if bool(row["pk"]) else "",
                "notnull": "Yes" if bool(row["notnull"]) else "",
                "default": str(row["dflt_value"] or ""),
            }
        )
    return schema_rows


def get_demo_database_search_dirs() -> List[Path]:
    roots = [
        Path.home() / "Downloads",
        Path.home() / "Desktop",
        Path(PROJECT_DIR),
        Path(PROJECT_DIR) / "demo",
    ]
    deduped: List[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(root)
    return deduped


def find_demo_database_candidates() -> List[Dict[str, str]]:
    matches: List[tuple[float, Dict[str, str]]] = []
    seen: set[str] = set()
    for root in get_demo_database_search_dirs():
        if not root.is_dir():
            continue
        for path in root.glob(DEMO_DATABASE_GLOB):
            if not path.is_file():
                continue
            try:
                resolved = path.resolve()
                stat = path.stat()
            except OSError:
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                (
                    stat.st_mtime,
                    {
                        "path": key,
                        "name": path.name,
                        "directory": str(path.parent),
                        "modified": datetime.fromtimestamp(stat.st_mtime).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    },
                )
            )
    matches.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in matches]


def resolve_demo_database_candidate(source_path: str) -> Optional[Path]:
    if not source_path:
        return None
    try:
        requested = Path(source_path).expanduser().resolve()
    except OSError:
        return None
    for candidate in find_demo_database_candidates():
        try:
            candidate_path = Path(candidate["path"]).resolve()
        except OSError:
            continue
        if str(candidate_path) == str(requested):
            return candidate_path
    return None


def get_demo_copy_target_path(source_path: Path) -> Path:
    preferred = Path(PROJECT_DIR) / source_path.name
    try:
        if source_path.resolve() == preferred.resolve():
            return preferred
    except OSError:
        pass
    if not preferred.exists():
        return preferred
    stem = source_path.stem
    suffix = source_path.suffix
    index = 1
    while True:
        candidate = Path(PROJECT_DIR) / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def list_batch_databases(batch_dir: str) -> List[str]:
    if not os.path.isdir(batch_dir):
        return []
    batch_dbs = [
        name
        for name in os.listdir(batch_dir)
        if name.endswith(".db") and os.path.isfile(os.path.join(batch_dir, name))
    ]
    return sorted(batch_dbs)


def list_sampling_reports(analysis_dir: str) -> List[str]:
    if not os.path.isdir(analysis_dir):
        return []
    reports = sorted(
        [
            os.path.basename(path)
            for path in glob.glob(os.path.join(analysis_dir, "sampling_results_*.json"))
        ]
    )
    return reports


def list_surrogate_models(model_dir: str) -> List[Dict[str, object]]:
    if not os.path.isdir(model_dir):
        return []
    model_files = sorted(glob.glob(os.path.join(model_dir, "*.json")))
    models: List[Dict[str, object]] = []
    for path in model_files:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                meta = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(meta, dict):
            continue
        name = meta.get("name") or os.path.splitext(os.path.basename(path))[0]
        status_filter = meta.get("status_filter")
        if isinstance(status_filter, (list, tuple)):
            status_filter = ",".join(str(item) for item in status_filter)
        models.append(
            {
                "name": str(name),
                "created_at": meta.get("created_at"),
                "train_rows": meta.get("train_rows"),
                "test_rows": meta.get("test_rows"),
                "status_filter": status_filter,
                "metrics": meta.get("metrics"),
                "model_path": meta.get("model_path"),
                "meta_path": path,
            }
        )
    models.sort(
        key=lambda item: (
            str(item.get("created_at") or ""),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )
    return models


def list_surrogate_models_db(conn: sqlite3.Connection) -> List[Dict[str, object]]:
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_surrogate)")}
    except sqlite3.OperationalError:
        return []
    sg_label_expr = "sg_label" if "sg_label" in columns else "NULL AS sg_label"
    mapsto_expr = "mapsto" if "mapsto" in columns else "NULL AS mapsto"
    try:
        rows = conn.execute(
            f"""
            SELECT id, name, {sg_label_expr}, {mapsto_expr}, status_filter, origin_id, origin_name, test_size,
                   n_estimators, max_depth, min_samples_leaf, log1p_target,
                   model_path, meta_path, created_at, train_rows, test_rows, metrics_json
            FROM gk_surrogate
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    models: List[Dict[str, object]] = []
    for row in rows:
        metrics = None
        raw_metrics = row["metrics_json"]
        if raw_metrics:
            try:
                metrics = json.loads(raw_metrics)
            except json.JSONDecodeError:
                metrics = None
        models.append(
            {
                "id": row["id"],
                "name": row["name"],
                "sg_label": row["sg_label"],
                "mapsto": row["mapsto"],
                "created_at": row["created_at"],
                "train_rows": row["train_rows"],
                "test_rows": row["test_rows"],
                "status_filter": row["status_filter"],
                "metrics": metrics,
                "model_path": row["model_path"],
                "meta_path": row["meta_path"],
                "origin_id": row["origin_id"],
                "origin_name": row["origin_name"],
                "test_size": row["test_size"],
                "n_estimators": row["n_estimators"],
                "max_depth": row["max_depth"],
                "min_samples_leaf": row["min_samples_leaf"],
                "log1p_target": row["log1p_target"],
            }
        )
    return models


def ensure_gk_surrogate_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_surrogate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sg_label TEXT,
            mapsto TEXT,
            status_filter TEXT,
            origin_id INTEGER,
            origin_name TEXT,
            test_size REAL,
            n_estimators INTEGER,
            max_depth INTEGER,
            min_samples_leaf INTEGER,
            log1p_target INTEGER,
            model_path TEXT,
            meta_path TEXT,
            created_at TEXT,
            train_rows INTEGER,
            test_rows INTEGER,
            metrics_json TEXT
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_surrogate)")}
    if "sg_label" not in columns:
        conn.execute("ALTER TABLE gk_surrogate ADD COLUMN sg_label TEXT")
    if "mapsto" not in columns:
        conn.execute("ALTER TABLE gk_surrogate ADD COLUMN mapsto TEXT")
    conn.execute(
        """
        UPDATE gk_surrogate
        SET sg_label = COALESCE(sg_label, 'gamma_max'),
            mapsto = COALESCE(mapsto, 'gk_run.gamma_max')
        WHERE sg_label IS NULL OR mapsto IS NULL
        """
    )
    conn.commit()


def surrogate_commentary(summary: Dict[str, object]) -> Optional[str]:
    count = int(summary.get("count") or 0)
    if count <= 0:
        return None

    eval_count = int(summary.get("eval_count") or 0)
    test_rows_raw = summary.get("test_rows")
    test_rows = (
        int(test_rows_raw)
        if isinstance(test_rows_raw, (int, float)) and not isinstance(test_rows_raw, bool)
        else None
    )
    rel_unc_pct_raw = summary.get("eval_relative_uncertainty_pct")
    rel_unc_pct = (
        float(rel_unc_pct_raw)
        if isinstance(rel_unc_pct_raw, (int, float)) and math.isfinite(float(rel_unc_pct_raw))
        else None
    )
    coverage_raw = summary.get("eval_coverage")
    coverage = (
        float(coverage_raw)
        if isinstance(coverage_raw, (int, float)) and math.isfinite(float(coverage_raw))
        else None
    )
    r2_raw = summary.get("eval_r2")
    r2 = (
        float(r2_raw)
        if isinstance(r2_raw, (int, float)) and math.isfinite(float(r2_raw))
        else None
    )

    sentences: List[str] = []
    validation_rows = test_rows if test_rows is not None and test_rows > 0 else eval_count
    if validation_rows > 0 and validation_rows < 20:
        sentences.append(
            f"Validation remains limited because only {validation_rows} held-out or truth-matched points are currently available."
        )
    elif validation_rows >= 20 and validation_rows < 50:
        sentences.append(
            f"Validation is based on a modest sample of {validation_rows} held-out or truth-matched points."
        )

    if rel_unc_pct is not None:
        rel_unc_scale = rel_unc_pct / 100.0
        rel_unc_scale_text = f"{rel_unc_scale:.2f}".rstrip("0").rstrip(".")
        sentences.append(
            f"The median relative uncertainty is ~{rel_unc_pct:.0f}%, meaning the typical model-reported uncertainty is about {rel_unc_scale_text}x the magnitude of the true value."
        )
        if rel_unc_pct >= 100:
            sentences.append(
                "This places the surrogate in a low-confidence regime, so the estimates should be interpreted with caution."
            )
        elif rel_unc_pct >= 50:
            sentences.append(
                "This indicates substantial uncertainty, so the surrogate is better suited to coarse ranking than to precise quantitative prediction."
            )
        elif rel_unc_pct >= 25:
            sentences.append(
                "This indicates moderate uncertainty, so the surrogate may be useful for screening, but it still requires caution."
            )
        else:
            sentences.append(
                "This indicates a relatively tight uncertainty scale, consistent with higher-confidence estimates."
            )
    elif eval_count <= 0:
        sentences.append(
            f"Predictions are available for {count:,} rows, but no ground-truth points are currently available to validate this surrogate."
        )

    if coverage is not None:
        coverage_pct = coverage * 100.0
        if coverage <= 0.3:
            sentences.append(
                f"Only ~{coverage_pct:.0f}% of validation points fall within sg_quality, suggesting that the reported uncertainty is not yet well calibrated."
            )
        elif coverage >= 0.7:
            sentences.append(
                f"About ~{coverage_pct:.0f}% of validation points fall within sg_quality, suggesting that the reported uncertainty is reasonably calibrated."
            )

    if r2 is not None:
        if r2 < 0.2:
            sentences.append(
                "The low R² value indicates limited predictive skill relative to a simple mean predictor."
            )
        elif r2 >= 0.8:
            sentences.append(
                "The high R² value indicates strong predictive agreement on the available validation set."
            )

    if not sentences:
        return None
    return " ".join(sentences)


def surrogate_relative_uncertainty_label(summary: Dict[str, object]) -> Optional[str]:
    raw_value = summary.get("eval_relative_uncertainty_pct")
    source_suffix = ""
    if not isinstance(raw_value, (int, float)) or not math.isfinite(float(raw_value)):
        raw_value = summary.get("relative_uncertainty_pct")
        source_suffix = " (mean)"
    if not isinstance(raw_value, (int, float)) or not math.isfinite(float(raw_value)):
        return None

    rel_unc_pct = float(raw_value)
    if rel_unc_pct >= 100:
        level = "Very high"
    elif rel_unc_pct >= 50:
        level = "High"
    elif rel_unc_pct >= 25:
        level = "Moderate"
    else:
        level = "Low"
    return f"{level} relative uncertainty (~{rel_unc_pct:.0f}%){source_suffix}"


def get_sg_estimate_summary(
    conn: sqlite3.Connection, surrogate_id: int, sample_limit: int = 20
) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "count": 0,
        "train_rows": None,
        "test_rows": None,
        "estimate_min": None,
        "estimate_max": None,
        "estimate_mean": None,
        "estimate_median": None,
        "quality_min": None,
        "quality_max": None,
        "quality_mean": None,
        "quality_median": None,
        "sample_rows": [],
    }
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt,
               MIN(sg_estimate) AS est_min,
               MAX(sg_estimate) AS est_max,
               AVG(sg_estimate) AS est_mean,
               MIN(sg_quality) AS q_min,
               MAX(sg_quality) AS q_max,
               AVG(sg_quality) AS q_mean
        FROM sg_estimate
        WHERE gk_surrogate_id = ?
        """,
        (surrogate_id,),
    ).fetchone()
    if row is None or row["cnt"] == 0:
        return summary
    summary["count"] = int(row["cnt"])
    summary["estimate_min"] = row["est_min"]
    summary["estimate_max"] = row["est_max"]
    summary["estimate_mean"] = row["est_mean"]
    summary["quality_min"] = row["q_min"]
    summary["quality_max"] = row["q_max"]
    summary["quality_mean"] = row["q_mean"]
    try:
        est_mean = float(row["est_mean"]) if row["est_mean"] is not None else None
        q_mean = float(row["q_mean"]) if row["q_mean"] is not None else None
        if est_mean is not None and q_mean is not None and est_mean != 0:
            summary["relative_uncertainty_pct"] = abs(q_mean / est_mean) * 100.0
    except (TypeError, ValueError):
        pass

    surrogate_row = conn.execute(
        "SELECT train_rows, test_rows FROM gk_surrogate WHERE id = ?",
        (surrogate_id,),
    ).fetchone()
    if surrogate_row is not None:
        summary["train_rows"] = surrogate_row["train_rows"]
        summary["test_rows"] = surrogate_row["test_rows"]

    # Evaluate estimates against available ground truth for this surrogate.
    eval_summary = evaluate_surrogate_estimates(conn, surrogate_id)
    summary.update(eval_summary)

    est_rows = conn.execute(
        """
        SELECT sg_estimate, sg_quality
        FROM sg_estimate
        WHERE gk_surrogate_id = ?
        """,
        (surrogate_id,),
    ).fetchall()
    if est_rows:
        est_vals = sorted(
            float(row["sg_estimate"]) for row in est_rows if row["sg_estimate"] is not None
        )
        if est_vals:
            mid = len(est_vals) // 2
            summary["estimate_median"] = (
                est_vals[mid]
                if len(est_vals) % 2 == 1
                else 0.5 * (est_vals[mid - 1] + est_vals[mid])
            )
        qual_vals = sorted(
            float(row["sg_quality"]) for row in est_rows if row["sg_quality"] is not None
        )
        if qual_vals:
            mid = len(qual_vals) // 2
            summary["quality_median"] = (
                qual_vals[mid]
                if len(qual_vals) % 2 == 1
                else 0.5 * (qual_vals[mid - 1] + qual_vals[mid])
            )

    sample_rows = conn.execute(
        """
        SELECT gk_input_id, sg_estimate, sg_quality
        FROM sg_estimate
        WHERE gk_surrogate_id = ?
        ORDER BY gk_input_id
        LIMIT ?
        """,
        (surrogate_id, sample_limit),
    ).fetchall()
    summary["sample_rows"] = [
        {
            "gk_input_id": row["gk_input_id"],
            "sg_estimate": row["sg_estimate"],
            "sg_quality": row["sg_quality"],
        }
        for row in sample_rows
    ]
    summary["relative_uncertainty_label"] = surrogate_relative_uncertainty_label(summary)
    summary["commentary"] = surrogate_commentary(summary)
    return summary


def evaluate_surrogate_estimates(
    conn: sqlite3.Connection, surrogate_id: int
) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "eval_count": 0,
        "eval_mae": None,
        "eval_rmse": None,
        "eval_r2": None,
        "eval_bias": None,
        "eval_median_relative_error": None,
        "eval_coverage": None,
        "eval_relative_uncertainty_pct": None,
        "eval_mean_truth": None,
        "eval_mae_relative_pct": None,
        "eval_accuracy_label": None,
        "eval_confidence_label": None,
        "eval_verdict": None,
        "eval_assessment": None,
    }
    row = conn.execute(
        "SELECT mapsto, status_filter FROM gk_surrogate WHERE id = ?",
        (surrogate_id,),
    ).fetchone()
    if row is None:
        return summary
    mapsto = str(row["mapsto"] or "")
    if not mapsto.startswith("gk_run."):
        return summary
    target_col = mapsto.split(".", 1)[1]
    if not target_col:
        return summary
    run_columns = {
        col["name"] for col in conn.execute("PRAGMA table_info(gk_run)").fetchall()
    }
    if target_col not in run_columns:
        return summary
    status_filter = str(row["status_filter"] or "").strip()
    statuses: List[str] = []
    if status_filter and status_filter.upper() != "ALL":
        for token in re.split(r"[,\s]+", status_filter):
            token = token.strip()
            if token:
                statuses.append(token)

    params: List[object] = [surrogate_id]
    status_sql = ""
    if statuses:
        placeholders = ", ".join(["?"] * len(statuses))
        status_sql = f" AND r.status IN ({placeholders})"
        params.extend(statuses)

    query = f"""
        SELECT sg.sg_estimate AS est_val,
               sg.sg_quality AS quality_val,
               r.{target_col} AS truth_val
        FROM sg_estimate sg
        JOIN gk_run r ON r.id = (
            SELECT r2.id
            FROM gk_run r2
            WHERE r2.gk_input_id = sg.gk_input_id
              AND r2.{target_col} IS NOT NULL
            ORDER BY r2.id DESC
            LIMIT 1
        )
        WHERE sg.gk_surrogate_id = ?
        {status_sql}
    """

    errors: List[float] = []
    truths: List[float] = []
    rel_errors: List[float] = []
    rel_uncertainties: List[float] = []
    quality_hits = 0
    quality_total = 0
    for est_val, quality_val, truth_val in conn.execute(query, params).fetchall():
        try:
            est = float(est_val)
            truth = float(truth_val)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(est) or not math.isfinite(truth):
            continue
        err = est - truth
        errors.append(err)
        truths.append(truth)
        if truth != 0:
            rel_errors.append(abs(err) / abs(truth))
        if quality_val is not None:
            try:
                quality = float(quality_val)
            except (TypeError, ValueError):
                quality = None
            if quality is not None and math.isfinite(quality):
                quality_total += 1
                if abs(err) <= quality:
                    quality_hits += 1
                if truth != 0:
                    rel_uncertainties.append(abs(quality) / abs(truth))

    if not errors:
        return summary

    import statistics

    count = len(errors)
    summary["eval_count"] = count
    mae = sum(abs(err) for err in errors) / count
    rmse = math.sqrt(sum(err * err for err in errors) / count)
    bias = sum(errors) / count
    summary["eval_mae"] = mae
    summary["eval_rmse"] = rmse
    summary["eval_bias"] = bias
    if rel_errors:
        summary["eval_median_relative_error"] = statistics.median(rel_errors)
    if quality_total:
        summary["eval_coverage"] = quality_hits / quality_total
    if rel_uncertainties:
        summary["eval_relative_uncertainty_pct"] = (
            statistics.median(rel_uncertainties) * 100.0
        )

    truth_mean = sum(truths) / count
    summary["eval_mean_truth"] = truth_mean
    if truth_mean:
        summary["eval_mae_relative_pct"] = abs(mae / truth_mean) * 100.0
    denom = sum((t - truth_mean) ** 2 for t in truths)
    if denom > 0:
        summary["eval_r2"] = 1.0 - sum(err * err for err in errors) / denom

    assessment = None
    accuracy_label = None
    confidence_label = None
    if count < 20:
        assessment = f"Too few truth points to judge (N={count})."
    else:
        r2 = summary.get("eval_r2")
        mre = summary.get("eval_median_relative_error")
        score = 0
        if isinstance(r2, (int, float)) and math.isfinite(float(r2)):
            r2_val = float(r2)
            if r2_val >= 0.8:
                score += 2
            elif r2_val >= 0.5:
                score += 1
            elif r2_val < 0.2:
                score -= 1
        if isinstance(mre, (int, float)):
            mre_val = float(mre)
            if mre_val <= 0.15:
                score += 2
            elif mre_val <= 0.3:
                score += 1
            elif mre_val >= 0.6:
                score -= 1
        if score >= 3:
            accuracy_label = "High accuracy"
            accuracy_reason = "R²≥0.5 and median rel. error≤15%"
        elif score >= 1:
            accuracy_label = "Moderate accuracy"
            accuracy_reason = "R²≥0.2 or median rel. error≤30%"
        elif score >= 0:
            accuracy_label = "Low accuracy"
            accuracy_reason = "R²<0.2 or median rel. error>30%"
        else:
            accuracy_label = "Poor accuracy"
            accuracy_reason = "R²<0.2 and median rel. error≥60%"

        confidence_score = 0
        rel_unc_pct = summary.get("eval_relative_uncertainty_pct")
        coverage = summary.get("eval_coverage")
        if isinstance(rel_unc_pct, (int, float)):
            if rel_unc_pct <= 25:
                confidence_score += 2
            elif rel_unc_pct <= 50:
                confidence_score += 1
            elif rel_unc_pct >= 100:
                confidence_score -= 1
        if isinstance(coverage, (int, float)):
            if coverage >= 0.7:
                confidence_score += 2
            elif coverage >= 0.5:
                confidence_score += 1
            elif coverage <= 0.3:
                confidence_score -= 1
        if confidence_score >= 3:
            confidence_label = "High confidence"
            confidence_reason = "median uncertainty≤25% and coverage≥70%"
        elif confidence_score >= 1:
            confidence_label = "Medium confidence"
            confidence_reason = "median uncertainty≤50% or coverage≥50%"
        else:
            confidence_label = "Low confidence"
            confidence_reason = "median uncertainty>50% or coverage<50%"

        if accuracy_label and isinstance(mre, (int, float)):
            assessment = f"{accuracy_label}; median relative error ~{mre * 100:.0f}%."
        elif accuracy_label:
            assessment = f"{accuracy_label}."
        elif isinstance(mre, (int, float)):
            assessment = f"Median relative error ~{mre * 100:.0f}%."
    summary["eval_assessment"] = assessment
    summary["eval_accuracy_label"] = accuracy_label
    summary["eval_confidence_label"] = confidence_label
    summary["eval_accuracy_reason"] = accuracy_reason if "accuracy_reason" in locals() else None
    summary["eval_confidence_reason"] = confidence_reason if "confidence_reason" in locals() else None
    verdict_parts = []
    if accuracy_label:
        if summary["eval_accuracy_reason"]:
            verdict_parts.append(f"{accuracy_label} ({summary['eval_accuracy_reason']})")
        else:
            verdict_parts.append(accuracy_label)
    if confidence_label:
        confidence_text = confidence_label.lower()
        if summary["eval_confidence_reason"]:
            verdict_parts.append(
                f"{confidence_text} ({summary['eval_confidence_reason']})"
            )
        else:
            verdict_parts.append(confidence_text)
    if verdict_parts:
        summary["eval_verdict"] = ", ".join(verdict_parts)
    assessment_detail = assessment
    if assessment and accuracy_label:
        prefix = f"{accuracy_label}; "
        if assessment.startswith(prefix):
            assessment_detail = assessment[len(prefix) :]
        else:
            prefix = f"{accuracy_label}."
            if assessment.startswith(prefix):
                assessment_detail = assessment[len(prefix) :].lstrip()
    summary["eval_assessment_detail"] = assessment_detail
    return summary


def _parse_surrogate_args(extra_args: Optional[List[str]]) -> Dict[str, object]:
    if not extra_args:
        return {}
    args: Dict[str, object] = {}
    idx = 0
    while idx < len(extra_args):
        token = extra_args[idx]
        if token == "--log1p-target":
            args["log1p_target"] = 1
            idx += 1
            continue
        if token == "--mapsto-all":
            args["mapsto_all"] = True
            idx += 1
            continue
        if token.startswith("--") and idx + 1 < len(extra_args):
            value = extra_args[idx + 1]
            if token == "--name":
                args["name"] = value
            elif token == "--mapsto":
                args["mapsto"] = value
            elif token == "--statuses":
                args["status_filter"] = value
            elif token == "--origin-id":
                if value.isdigit():
                    args["origin_id"] = int(value)
            elif token == "--origin":
                args["origin_name"] = value
            elif token == "--test-size":
                try:
                    args["test_size"] = float(value)
                except ValueError:
                    pass
            elif token == "--n-estimators":
                if value.isdigit():
                    args["n_estimators"] = int(value)
            elif token == "--max-depth":
                if value.isdigit():
                    args["max_depth"] = int(value)
            elif token == "--min-samples-leaf":
                if value.isdigit():
                    args["min_samples_leaf"] = int(value)
            idx += 2
            continue
        idx += 1
    return args


def _safe_surrogate_name(name: str) -> Optional[str]:
    if not name:
        return None
    if any(sep in name for sep in ("/", "\\", os.path.sep)):
        return None
    safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in name).strip("_")
    return safe or None


def _record_surrogate_model(db_path: str, meta_path: Optional[str], params: Dict[str, object]) -> None:
    if not db_path:
        return
    meta = {}
    if meta_path:
        try:
            with open(meta_path, "r", encoding="utf-8") as handle:
                meta = json.load(handle)
        except (OSError, json.JSONDecodeError):
            meta = {}
    name = params.get("name") or meta.get("name")
    if not name:
        return
    status_filter = params.get("status_filter") or meta.get("status_filter")
    if isinstance(status_filter, (list, tuple)):
        status_filter = ",".join(str(item) for item in status_filter)
    target_label = meta.get("target") or params.get("sg_label")
    mapsto = params.get("mapsto") or meta.get("mapsto")
    if not mapsto and target_label:
        mapsto = f"gk_run.{target_label}"
    payload = {
        "name": name,
        "sg_label": target_label,
        "mapsto": mapsto,
        "status_filter": status_filter,
        "origin_id": params.get("origin_id") or meta.get("origin_id"),
        "origin_name": params.get("origin_name") or meta.get("origin_name"),
        "test_size": params.get("test_size"),
        "n_estimators": params.get("n_estimators"),
        "max_depth": params.get("max_depth"),
        "min_samples_leaf": params.get("min_samples_leaf"),
        "log1p_target": params.get("log1p_target") or (1 if meta.get("target_transform") == "log1p" else 0),
        "model_path": meta.get("model_path"),
        "meta_path": meta_path,
        "created_at": meta.get("created_at"),
        "train_rows": meta.get("train_rows"),
        "test_rows": meta.get("test_rows"),
        "metrics_json": json.dumps(meta.get("metrics")) if meta.get("metrics") is not None else None,
    }
    conn = get_connection(db_path)
    try:
        ensure_gk_surrogate_table(conn)
        conn.execute(
            """
            INSERT INTO gk_surrogate (
                name, sg_label, mapsto, status_filter, origin_id, origin_name, test_size,
                n_estimators, max_depth, min_samples_leaf, log1p_target,
                model_path, meta_path, created_at, train_rows, test_rows, metrics_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["name"],
                payload["sg_label"],
                payload["mapsto"],
                payload["status_filter"],
                payload["origin_id"],
                payload["origin_name"],
                payload["test_size"],
                payload["n_estimators"],
                payload["max_depth"],
                payload["min_samples_leaf"],
                payload["log1p_target"],
                payload["model_path"],
                payload["meta_path"],
                payload["created_at"],
                payload["train_rows"],
                payload["test_rows"],
                payload["metrics_json"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_sampling_report(path: str) -> Optional[Dict[str, object]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def load_monitor_report(path: str) -> Optional[Dict[str, object]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def filter_monitor_report_for_origin(
    report: Optional[Dict[str, object]], origin_name: Optional[str]
) -> Optional[Dict[str, object]]:
    if not report or not origin_name:
        return report
    target = canonical_origin_name(str(origin_name or ""))
    filtered_batches: List[Dict[str, object]] = []
    for batch in report.get("batches", []):
        if not isinstance(batch, dict):
            continue
        origins = batch.get("origin_names")
        if not isinstance(origins, list):
            continue
        canonical_origins = {
            canonical_origin_name(str(name or ""))
            for name in origins
            if str(name or "").strip()
        }
        if target in canonical_origins:
            filtered_batches.append(batch)
    filtered_report = dict(report)
    filtered_report["batches"] = filtered_batches
    return filtered_report


def _load_usage_log() -> Dict[str, object]:
    if not os.path.exists(USAGE_LOG_PATH):
        return {"events": []}
    try:
        with open(USAGE_LOG_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return payload
    except (OSError, json.JSONDecodeError):
        pass
    return {"events": []}


def log_usage(event: str, details: Optional[Dict[str, object]] = None) -> None:
    if not event:
        return
    entry = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "event": event,
        "details": details or {},
    }
    with USAGE_LOG_LOCK:
        payload = _load_usage_log()
        events = payload.get("events", [])
        if not isinstance(events, list):
            events = []
        events.append(entry)
        if len(events) > 5000:
            events = events[-5000:]
        payload["events"] = events
        try:
            os.makedirs(os.path.dirname(USAGE_LOG_PATH), exist_ok=True)
            with open(USAGE_LOG_PATH, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except OSError:
            pass


def get_data_origins(conn: sqlite3.Connection) -> List[Tuple[int, str, Optional[str]]]:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(data_origin)")}
    if "color" in columns:
        rows = conn.execute(
            "SELECT id, name, color FROM data_origin ORDER BY id"
        ).fetchall()
        return [
            (int(row["id"]), str(row["name"]), row["color"] if row["color"] else None)
            for row in rows
        ]
    rows = conn.execute("SELECT id, name FROM data_origin ORDER BY id").fetchall()
    return [(int(row["id"]), str(row["name"]), None) for row in rows]


def canonical_origin_name(name: str) -> str:
    value = (name or "").strip()
    if value == "Mate Kinetic EFIT":
        return "Kinetic EFIT (Mate)"
    if value.startswith("Alexei "):
        return value[len("Alexei ") :]
    return value


def get_data_origin_details(conn: sqlite3.Connection) -> List[Dict[str, object]]:
    columns = get_table_columns(conn, "data_origin")
    select_columns = [
        column
        for column in ("id", "name", "file_type", "tokamak", "origin", "copy", "color", "creation_date")
        if column in columns
    ]
    if not select_columns:
        return []
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM data_origin ORDER BY id"
    ).fetchall()
    details: List[Dict[str, object]] = []
    for row in rows:
        item = {column: row[column] for column in select_columns}
        item.setdefault("file_type", "")
        item.setdefault("tokamak", "")
        item.setdefault("origin", "")
        item.setdefault("copy", "")
        item.setdefault("color", None)
        details.append(item)
    return details


def get_latest_flux_action_state(
    conn: sqlite3.Connection, origin_id: int, origin_name: str
) -> Optional[Dict[str, str]]:
    try:
        columns = get_table_columns(conn, "flux_action_log")
    except sqlite3.OperationalError:
        return None
    if not columns:
        return None
    select_parts = [
        "id",
        "flux_db_name",
        "created_at",
        "status" if "status" in columns else "'STAGED' AS status",
        "slurm_job_id" if "slurm_job_id" in columns else "'' AS slurm_job_id",
        "status_detail" if "status_detail" in columns else "'' AS status_detail",
        "status_checked_at" if "status_checked_at" in columns else "'' AS status_checked_at",
    ]
    row = conn.execute(
        f"""
        SELECT {', '.join(select_parts)}
        FROM flux_action_log
        WHERE data_origin_id = ?
           OR data_origin_name = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (origin_id, origin_name),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": str(row["id"] or ""),
        "flux_db_name": str(row["flux_db_name"] or ""),
        "created_at": str(row["created_at"] or ""),
        "status": str(row["status"] or ""),
        "slurm_job_id": str(row["slurm_job_id"] or ""),
        "status_detail": str(row["status_detail"] or ""),
        "status_checked_at": str(row["status_checked_at"] or ""),
    }


def get_equilibria_origin_actions(
    origin_name: str,
    file_type: Optional[str],
    flux_action_state: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, str]], List[str]]:
    actions: List[Dict[str, str]] = []
    notes: List[str] = []
    name = (origin_name or "").strip()
    canonical_name = canonical_origin_name(name)
    file_type_value = (file_type or "").strip().upper()
    lowered = canonical_name.lower()
    flux_status = str((flux_action_state or {}).get("status") or "").strip().upper()
    flux_job_id = str((flux_action_state or {}).get("slurm_job_id") or "").strip()
    has_flux_job = bool(flux_job_id)
    if canonical_name == "Kinetic EFIT (Mate)":
        actions.append({"title": "Populate equilibria", "key": "populate_mate"})
        actions.append({"title": "Generate GK inputs", "key": "create_inputs_mate"})
        return actions, notes
    if canonical_name == "Transp 09 (semi-auto)":
        actions.append({"title": "Populate equilibria", "key": "populate_alexei"})
        actions.append({"title": "Generate GK inputs", "key": "create_inputs_transp"})
        return actions, notes
    if "transp 09" in lowered and "full-auto" in lowered:
        if flux_status in {"SUBMITTED", "RUNNING"}:
            actions.append({"title": "Check Flux Status", "key": "check_flux_status"})
            actions.append({"title": "Sync Back From Flux", "key": "sync_back_from_flux"})
            job_fragment = f" (job {flux_job_id})" if flux_job_id else ""
            notes.append(
                "A Flux run is already active for this origin"
                f"{job_fragment}. Do not launch a second parallel job; sync back first."
            )
            return actions, notes
        actions.append({"title": "Run On Flux", "key": "run_on_flux"})
        if has_flux_job:
            actions.append({"title": "Check Flux Status", "key": "check_flux_status"})
        actions.append({"title": "Sync Back From Flux", "key": "sync_back_from_flux"})
        notes.append(
            "Run On Flux reuses an existing staged Flux DB for this origin when one is already logged, otherwise it stages once and submits step 2 through Slurm."
        )
        return actions, notes
    if "transp 10" in lowered and "full-auto" in lowered:
        if flux_status in {"SUBMITTED", "RUNNING"}:
            actions.append({"title": "Check Flux Status", "key": "check_flux_status"})
            actions.append({"title": "Sync Back From Flux", "key": "sync_back_from_flux"})
            job_fragment = f" (job {flux_job_id})" if flux_job_id else ""
            notes.append(
                "A Flux run is already active for this origin"
                f"{job_fragment}. Do not launch a second parallel job; sync back first."
            )
            return actions, notes
        actions.append({"title": "Run On Flux", "key": "run_on_flux"})
        if has_flux_job:
            actions.append({"title": "Check Flux Status", "key": "check_flux_status"})
        actions.append({"title": "Sync Back From Flux", "key": "sync_back_from_flux"})
        notes.append(
            "Run On Flux reuses an existing staged Flux DB for this origin when one is already logged, otherwise it stages once and submits step 2 through Slurm."
        )
        return actions, notes
    if file_type_value == "EFIT":
        notes.append(
            "This EFIT origin is not wired to a dedicated GUI action yet. The current EFIT create-input script still targets the built-in Mate workflow."
        )
    elif file_type_value == "TRANSP":
        notes.append(
            "This TRANSP origin is not wired to a dedicated GUI action yet. Current GUI actions cover only the known semi-auto and full-auto origins."
        )
    else:
        notes.append("No GUI action mapping is available for this origin yet.")
    return actions, notes


def get_equilibria_origin_summary(
    conn: sqlite3.Connection, origin_id: int, tables: List[str]
) -> Dict[str, int]:
    summary = {
        "equilibria_total": 0,
        "equilibria_active": 0,
        "gk_input_total": 0,
        "transp_timeseries_total": 0,
    }
    if "data_equil" in tables:
        summary["equilibria_total"] = int(
            conn.execute(
                "SELECT COUNT(*) FROM data_equil WHERE data_origin_id = ?",
                (origin_id,),
            ).fetchone()[0]
        )
        data_equil_columns = get_table_columns(conn, "data_equil")
        if "active" in data_equil_columns:
            summary["equilibria_active"] = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM data_equil
                    WHERE data_origin_id = ? AND active = 1
                    """,
                    (origin_id,),
                ).fetchone()[0]
            )
    if "gk_input" in tables and "gk_study" in tables and "data_equil" in tables:
        gk_input_columns = get_table_columns(conn, "gk_input")
        gk_study_columns = get_table_columns(conn, "gk_study")
        if "gk_study_id" in gk_input_columns and "data_equil_id" in gk_study_columns:
            summary["gk_input_total"] = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM gk_input AS gi
                    JOIN gk_study AS gs ON gs.id = gi.gk_study_id
                    JOIN data_equil AS de ON de.id = gs.data_equil_id
                    WHERE de.data_origin_id = ?
                    """,
                    (origin_id,),
                ).fetchone()[0]
            )
    if "transp_timeseries" in tables:
        transp_columns = get_table_columns(conn, "transp_timeseries")
        if "data_origin_id" in transp_columns:
            summary["transp_timeseries_total"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM transp_timeseries WHERE data_origin_id = ?",
                    (origin_id,),
                ).fetchone()[0]
            )
    return summary


def _format_count_breakdown(
    counts: Dict[str, int], preferred_order: Tuple[str, ...]
) -> str:
    ordered_keys: List[str] = [
        key for key in preferred_order if int(counts.get(key, 0)) > 0
    ]
    ordered_keys.extend(
        sorted(
            key
            for key, value in counts.items()
            if int(value) > 0 and key not in ordered_keys
        )
    )
    return ", ".join(f"{counts[key]} {key}" for key in ordered_keys)


def _workflow_state_row(label: str, state: str, detail: str) -> Dict[str, str]:
    normalized_state = (state or "UNKNOWN").strip().upper()
    state_class = re.sub(r"[^a-z0-9]+", "-", normalized_state.lower()).strip("-")
    return {
        "label": label,
        "state": normalized_state,
        "state_class": state_class or "unknown",
        "detail": detail,
    }


def _gk_model_label(row: sqlite3.Row) -> str:
    code = str(row["gk_code_name"] or "GK").strip().upper()
    linearity = "linear" if int(row["is_linear"] or 0) == 1 else "nonlinear"
    adiabatic_value = int(row["is_adiabatic"] or 0)
    if adiabatic_value == 1:
        species = "adiabatic"
    elif adiabatic_value == 0:
        species = "kinetic"
    else:
        species = "mixed"
    field_type = "electrostatic" if int(row["is_electrostatic"] or 0) == 1 else "electromagnetic"
    template_name = os.path.basename(str(row["input_template"] or "").strip())
    label = f"#{int(row['id'])} {code} {linearity} {species} {field_type}"
    if template_name:
        label = f"{label} ({template_name})"
    return label


def get_equilibria_origin_workflow_status(
    conn: sqlite3.Connection,
    origin_id: int,
    origin_name: str,
    file_type: str,
    tables: List[str],
    flux_action_state: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    summary = get_equilibria_origin_summary(conn, origin_id, tables)
    gk_study_total = 0
    gk_input_status_counts: Dict[str, int] = {}
    gk_batch_status_counts: Dict[str, int] = {}
    gk_run_status_counts: Dict[str, int] = {}
    gk_model_usage: List[Tuple[str, int]] = []
    gk_model_active_labels: List[str] = []

    if "gk_study" in tables and "data_equil" in tables:
        gk_study_total = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM gk_study AS gs
                JOIN data_equil AS de ON de.id = gs.data_equil_id
                WHERE de.data_origin_id = ?
                """,
                (origin_id,),
            ).fetchone()[0]
        )

    if "gk_input" in tables and "gk_study" in tables and "data_equil" in tables:
        for row in conn.execute(
            """
            SELECT gi.status, COUNT(*)
            FROM gk_input AS gi
            JOIN gk_study AS gs ON gs.id = gi.gk_study_id
            JOIN data_equil AS de ON de.id = gs.data_equil_id
            WHERE de.data_origin_id = ?
            GROUP BY gi.status
            """,
            (origin_id,),
        ).fetchall():
            status_key = str(row[0] or "").strip().upper()
            if status_key:
                gk_input_status_counts[status_key] = int(row[1])

        if "gk_model" in tables:
            for row in conn.execute(
                """
                SELECT gm.id, gc.name AS gk_code_name, gm.is_linear, gm.is_adiabatic,
                       gm.is_electrostatic, gm.input_template, COUNT(*) AS row_count
                FROM gk_input AS gi
                JOIN gk_model AS gm ON gm.id = gi.gk_model_id
                LEFT JOIN gk_code AS gc ON gc.id = gm.gk_code_id
                JOIN gk_study AS gs ON gs.id = gi.gk_study_id
                JOIN data_equil AS de ON de.id = gs.data_equil_id
                WHERE de.data_origin_id = ?
                GROUP BY gm.id, gc.name, gm.is_linear, gm.is_adiabatic, gm.is_electrostatic, gm.input_template
                ORDER BY gm.id
                """,
                (origin_id,),
            ).fetchall():
                gk_model_usage.append((_gk_model_label(row), int(row["row_count"])))

    if "gk_model" in tables:
        seen_active_labels: set[str] = set()
        for row in conn.execute(
            """
            SELECT gm.id, gc.name AS gk_code_name, gm.is_linear, gm.is_adiabatic,
                   gm.is_electrostatic, gm.input_template
            FROM gk_model AS gm
            LEFT JOIN gk_code AS gc ON gc.id = gm.gk_code_id
            WHERE gm.active = 1
            ORDER BY gm.id
            """
        ).fetchall():
            label = _gk_model_label(row)
            if label not in seen_active_labels:
                seen_active_labels.add(label)
                gk_model_active_labels.append(label)

    if (
        "gk_batch" in tables
        and "gk_run" in tables
        and "gk_input" in tables
        and "gk_study" in tables
        and "data_equil" in tables
    ):
        for row in conn.execute(
            """
            SELECT b.status, COUNT(DISTINCT b.id)
            FROM gk_batch AS b
            JOIN gk_run AS r ON r.gk_batch_id = b.id
            JOIN gk_input AS gi ON gi.id = r.gk_input_id
            JOIN gk_study AS gs ON gs.id = gi.gk_study_id
            JOIN data_equil AS de ON de.id = gs.data_equil_id
            WHERE de.data_origin_id = ?
            GROUP BY b.status
            """,
            (origin_id,),
        ).fetchall():
            status_key = str(row[0] or "").strip().upper()
            if status_key:
                gk_batch_status_counts[status_key] = int(row[1])

    if "gk_run" in tables and "gk_input" in tables and "gk_study" in tables and "data_equil" in tables:
        for row in conn.execute(
            """
            SELECT r.status, COUNT(*)
            FROM gk_run AS r
            JOIN gk_input AS gi ON gi.id = r.gk_input_id
            JOIN gk_study AS gs ON gs.id = gi.gk_study_id
            JOIN data_equil AS de ON de.id = gs.data_equil_id
            WHERE de.data_origin_id = ?
            GROUP BY r.status
            """,
            (origin_id,),
        ).fetchall():
            status_key = str(row[0] or "").strip().upper()
            if status_key:
                gk_run_status_counts[status_key] = int(row[1])

    stages: List[Dict[str, str]] = []
    notes: List[str] = []

    flux_status = str((flux_action_state or {}).get("status") or "").strip().upper()
    flux_detail_parts: List[str] = []
    status_detail = str((flux_action_state or {}).get("status_detail") or "").strip()
    if status_detail and status_detail.upper() != flux_status:
        flux_detail_parts.append(status_detail)
    slurm_job_id = str((flux_action_state or {}).get("slurm_job_id") or "").strip()
    if slurm_job_id:
        flux_detail_parts.append(f"job {slurm_job_id}")
    flux_db_name = str((flux_action_state or {}).get("flux_db_name") or "").strip()
    if flux_db_name:
        flux_detail_parts.append(flux_db_name)
    status_checked_at = str((flux_action_state or {}).get("status_checked_at") or "").strip()
    if status_checked_at:
        flux_detail_parts.append(f"checked {status_checked_at}")
    if flux_status or flux_detail_parts:
        stages.append(
            _workflow_state_row(
                "Flux workflow",
                flux_status or "STAGED",
                " | ".join(part for part in flux_detail_parts if part) or "recorded in flux_action_log",
            )
        )

    equilibria_state = "READY" if summary["equilibria_total"] > 0 else "EMPTY"
    stages.append(
        _workflow_state_row(
            "data_equil",
            equilibria_state,
            f"{summary['equilibria_total']} rows, {summary['equilibria_active']} active",
        )
    )

    if (file_type or "").strip().upper() == "TRANSP":
        transp_state = "READY" if summary["transp_timeseries_total"] > 0 else "EMPTY"
        stages.append(
            _workflow_state_row(
                "transp_timeseries",
                transp_state,
                f"{summary['transp_timeseries_total']} rows",
            )
        )

    studies_state = (
        "READY"
        if gk_study_total > 0
        else ("PENDING" if summary["equilibria_total"] > 0 else "EMPTY")
    )
    stages.append(
        _workflow_state_row("gk_study", studies_state, f"{gk_study_total} rows")
    )

    if gk_model_usage:
        model_detail = ", ".join(f"{label}: {count}" for label, count in gk_model_usage)
        model_state = "USED"
    elif gk_model_active_labels:
        model_detail = ", ".join(gk_model_active_labels)
        model_state = "CONFIGURED"
    else:
        model_detail = "no gk_model rows configured"
        model_state = "EMPTY"
    stages.append(_workflow_state_row("gk_model", model_state, model_detail))

    gk_input_total = summary["gk_input_total"]
    input_detail = f"{gk_input_total} rows"
    input_breakdown = _format_count_breakdown(
        gk_input_status_counts,
        ("NEW", "WAIT", "TORUN", "BATCH", "RUNNING", "SUCCESS", "CRASHED", "ERROR"),
    )
    if input_breakdown:
        input_detail = f"{input_detail} | {input_breakdown}"
    if gk_input_total > 0:
        if any(gk_input_status_counts.get(key, 0) > 0 for key in ("TORUN", "BATCH", "RUNNING", "WAIT", "NEW")):
            input_state = "ACTIVE"
        elif any(gk_input_status_counts.get(key, 0) > 0 for key in ("CRASHED", "ERROR")):
            input_state = "FAILED"
        else:
            input_state = "READY"
    else:
        input_state = "PENDING" if gk_study_total > 0 or summary["equilibria_total"] > 0 else "EMPTY"
    stages.append(_workflow_state_row("gk_input", input_state, input_detail))

    batch_total = sum(gk_batch_status_counts.values())
    batch_detail = (
        _format_count_breakdown(gk_batch_status_counts, ("CREATED", "SENT", "LAUNCHED", "SYNCED"))
        or "no batch DB linked to this origin"
    )
    if batch_total == 0:
        batch_state = "NONE"
    elif any(gk_batch_status_counts.get(key, 0) > 0 for key in ("CREATED", "SENT", "LAUNCHED")):
        batch_state = "ACTIVE"
    elif gk_batch_status_counts.get("SYNCED", 0) > 0:
        batch_state = "SYNCED"
    else:
        batch_state = "READY"
    stages.append(_workflow_state_row("gk_batch", batch_state, batch_detail))

    run_total = sum(gk_run_status_counts.values())
    run_detail = (
        _format_count_breakdown(
            gk_run_status_counts,
            ("TORUN", "RUNNING", "RESTART", "CONVERGED", "SUCCESS", "CRASHED", "ERROR", "INTERRUPTED"),
        )
        or "no remote runs linked to this origin"
    )
    if run_total == 0:
        run_state = "NONE"
    elif any(gk_run_status_counts.get(key, 0) > 0 for key in ("TORUN", "RUNNING", "RESTART")):
        run_state = "ACTIVE"
    elif any(gk_run_status_counts.get(key, 0) > 0 for key in ("CRASHED", "ERROR", "INTERRUPTED")):
        run_state = "MIXED"
    else:
        run_state = "DONE"
    stages.append(_workflow_state_row("gk_run", run_state, run_detail))

    if flux_status == "SYNCED" and summary["equilibria_total"] > 0 and gk_study_total == 0:
        notes.append(
            "Flux sync completed, but this origin still has no gk_study rows in the main DB."
        )
    if gk_study_total > 0 and gk_input_total == 0:
        notes.append(
            "gk_study rows exist, but gk_input creation has not produced any rows yet."
        )
    if gk_input_total > 0 and batch_total == 0 and run_total == 0:
        notes.append(
            "gk_input rows exist, but no batch DB or remote run is linked to this origin yet."
        )

    return {"stages": stages, "notes": notes}


def get_equilibria_preview(
    conn: sqlite3.Connection,
    origin_id: int,
    valid_only: bool,
    limit: int = 40,
) -> Tuple[List[str], List[sqlite3.Row], int]:
    if "data_equil" not in list_tables(conn):
        return [], [], 0
    columns = get_table_columns(conn, "data_equil")
    select_columns = [
        column
        for column in (
            "id",
            "shot_number",
            "shot_variant",
            "shot_time",
            "active",
            "transpfile",
            "gfile",
            "pfile",
            "comment",
            "creation_date",
        )
        if column in columns
    ]
    if not select_columns:
        return [], [], 0
    where_clauses = ["data_origin_id = ?"]
    params: List[object] = [origin_id]
    if valid_only and "active" in columns:
        where_clauses.append("active = 1")
    where_sql = " AND ".join(where_clauses)
    total = int(
        conn.execute(
            f"SELECT COUNT(*) FROM data_equil WHERE {where_sql}",
            params,
        ).fetchone()[0]
    )
    rows = conn.execute(
        f"""
        SELECT {', '.join(select_columns)}
        FROM data_equil
        WHERE {where_sql}
        ORDER BY id DESC
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return select_columns, rows, total


def data_origin_color(origin_name: str, color: Optional[str] = None) -> str:
    if color:
        return str(color)
    name = canonical_origin_name(str(origin_name))
    if name == "Transp 09 (full-auto)":
        return "#2ca02c"
    if name.startswith("Transp 09"):
        return "#d62728"
    return "#1f77b4"


def get_table_rows(
    conn: sqlite3.Connection,
    table: str,
    only_active: bool,
    limit: int = 100,
    origin_filter: Optional[int] = None,
    transpfile_regex: Optional[str] = None,
    include_counts: bool = True,
) -> Tuple[List[str], List[sqlite3.Row], Optional[int], Optional[int]]:
    columns = [
        row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    ]
    if table == "data_equil":
        if "shot_number" in columns and "shot_variant" in columns:
            columns = [col for col in columns if col != "shot_variant"]
            shot_idx = columns.index("shot_number") + 1
            columns.insert(shot_idx, "shot_variant")
        if "shot_time" in columns and "comment" in columns:
            columns = [col for col in columns if col != "comment"]
            time_idx = columns.index("shot_time") + 1
            columns.insert(time_idx, "comment")
        if "creation_date" in columns:
            columns = [col for col in columns if col != "creation_date"] + [
                "creation_date"
            ]
    where_clauses = []
    params: List[object] = []
    if only_active and "active" in columns:
        where_clauses.append("active = 1")
    if table == "data_equil" and origin_filter is not None:
        where_clauses.append("data_origin_id = ?")
        params.append(origin_filter)
    if table == "gk_input" and origin_filter is not None:
        where_clauses.append(
            "gk_study_id IN (SELECT id FROM gk_study WHERE data_equil_id IN "
            "(SELECT id FROM data_equil WHERE data_origin_id = ?))"
        )
        params.append(origin_filter)
    if table == "data_equil" and transpfile_regex:
        where_clauses.append("transpfile REGEXP ?")
        params.append(transpfile_regex)
    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
    total_count: Optional[int] = None
    filtered_count: Optional[int] = None
    if include_counts:
        total_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if where_sql:
            filtered_count = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {table}{where_sql}",
                    params,
                ).fetchone()[0]
            )
        else:
            filtered_count = total_count
    rows = conn.execute(
        f"SELECT * FROM {table}{where_sql} LIMIT {limit}",
        params,
    ).fetchall()
    return columns, rows, total_count, filtered_count


def load_ai_feedback() -> Dict[str, Dict[str, int]]:
    with AI_FEEDBACK_LOCK:
        if not os.path.exists(AI_FEEDBACK_PATH):
            return {"counts": {}}
        try:
            with open(AI_FEEDBACK_PATH, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {"counts": {}}
    if not isinstance(data, dict):
        return {"counts": {}}
    counts = data.get("counts")
    if not isinstance(counts, dict):
        counts = {}
    return {"counts": {str(k): int(v) for k, v in counts.items()}}


def record_ai_feedback(suggestion_id: str, action_name: str) -> None:
    payload = load_ai_feedback()
    counts = payload.get("counts", {})
    counts[suggestion_id] = counts.get(suggestion_id, 0) + 1
    payload["counts"] = counts
    payload["last_action"] = {
        "suggestion_id": suggestion_id,
        "action": action_name,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
    }
    with AI_FEEDBACK_LOCK:
        with open(AI_FEEDBACK_PATH, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)


def get_ai_suggestions(
    conn: sqlite3.Connection, feedback: Dict[str, Dict[str, int]]
) -> List[Dict[str, str]]:
    suggestions: List[Dict[str, str]] = []
    tables = set(list_tables(conn))
    if "gk_input" in tables:
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM gk_input GROUP BY status"
        ).fetchall()
        counts = {str(row[0]): int(row[1]) for row in rows if row[0] is not None}
        wait_count = counts.get("WAIT", 0)
        torun_count = counts.get("TORUN", 0)
        empty_wait_row = conn.execute(
            "SELECT COUNT(*) FROM gk_input WHERE status = 'WAIT' AND (content IS NULL OR TRIM(content) = '')"
        ).fetchone()
        empty_wait_count = int(empty_wait_row[0]) if empty_wait_row else 0
        if wait_count > 0:
            suggestions.append(
                {
                    "id": "gk_input_wait",
                    "text": (
                        f"{wait_count} gk_input rows are WAIT. "
                        "Open Equilibria & Input to review and mark TORUN, then batch/launch runs."
                    ),
                }
            )
        if empty_wait_count > 0:
            suggestions.append(
                {
                    "id": "gk_input_empty_wait",
                    "text": (
                        f"{empty_wait_count} gk_input rows are WAIT with empty content. "
                        "Consider marking them ERROR."
                    ),
                    "action": "mark_empty_gk_input_error",
                }
            )
        if torun_count > 0:
            suggestions.append(
                {
                    "id": "gk_input_torun",
                    "text": (
                        f"{torun_count} gk_input rows are TORUN. "
                        "Consider creating a batch DB."
                    ),
                    "action": "create_batch_db",
                }
            )
    if "gk_batch" in tables:
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM gk_batch GROUP BY status"
        ).fetchall()
        batch_counts = {str(row[0]): int(row[1]) for row in rows if row[0] is not None}
        created = batch_counts.get("CREATED", 0)
        sent = batch_counts.get("SENT", 0)
        launched = batch_counts.get("LAUNCHED", 0)
        synced = batch_counts.get("SYNCED", 0)
        if created > 0:
            suggestions.append(
                {
                    "id": "batch_created",
                    "text": (
                        f"{created} batch DB(s) are CREATED. "
                        "Consider deploying them to the remote cluster."
                    ),
                    "action": "deploy_batch_db",
                }
            )
            suggestions.append(
                {
                    "id": "batch_created_large",
                    "text": (
                        f"{created} batch DB(s) are CREATED. "
                        "Consider deploying them with the large batch workflow."
                    ),
                    "action": "deploy_batch_db_large",
                }
            )
        if sent > 0:
            suggestions.append(
                {
                    "id": "batch_sent",
                    "text": (
                        f"{sent} batch DB(s) are SENT. "
                        "Consider preparing new batch scripts remotely."
                    ),
                }
            )
        if launched > 0 or synced > 0:
            total_active = launched + synced
            suggestions.append(
                {
                    "id": "batch_launched",
                    "text": f"{total_active} batch DB(s) are LAUNCHED/SYNCED. Consider:",
                    "items": [
                        "Check status with check_launched_batches.py.",
                        "Run the Remote Monitor and open the Monitor tab.",
                    ],
                    "actions": [
                        {"action": "check_launched_batches"},
                        {"action": "monitor_remote_runs", "panel": "monitor"},
                    ],
                }
            )
    remote_bundle_dir = os.path.join(PROJECT_DIR, "remote_gk_inputs")
    if os.path.isdir(remote_bundle_dir):
        bundle_paths = glob.glob(
            os.path.join(remote_bundle_dir, "remote_gk_inputs_*.db")
        )
        if bundle_paths:
            latest_bundle = max(bundle_paths, key=os.path.getmtime)
            suggestions.append(
                {
                    "id": "sync_remote_gk_inputs",
                    "text": (
                        f"Remote GK input bundle detected ({os.path.basename(latest_bundle)}). "
                        "Consider syncing results from Flux with "
                        "sync_remote_gk_inputs_from_flux.py."
                    ),
                }
            )
    if not suggestions:
        suggestions.append(
            {
                "id": "none",
                "text": "No immediate workflow actions detected. Database looks steady.",
            }
        )
    counts = feedback.get("counts", {}) if isinstance(feedback, dict) else {}
    suggestions.sort(key=lambda s: counts.get(s.get("id", ""), 0), reverse=True)
    return suggestions


def parse_numeric_fields(content: str) -> Dict[str, float]:
    pattern = re.compile(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)\s*$"
    )
    values: Dict[str, float] = {}
    for line in content.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        key = match.group(1)
        try:
            values[key] = float(match.group(2))
        except ValueError:
            continue
    return values


def parse_list_values(raw: str) -> List[float]:
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return []
    items: List[float] = []
    for item in raw[start + 1 : end].split(","):
        item = item.strip().strip('"').strip("'")
        if not item:
            continue
        try:
            items.append(float(item))
        except ValueError:
            # Keep string entries such as species type labels.
            try:
                items.append(str(item))
            except Exception:
                continue
    return items


def parse_species_fields(content: str) -> Dict[str, float]:
    lines = content.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "[species]":
            start = idx + 1
            break
    if start is None:
        return {}
    end = len(lines)
    for idx in range(start, len(lines)):
        if lines[idx].strip().startswith("[") and idx != start:
            end = idx
            break
    fields: Dict[str, List[float]] = {}
    for line in lines[start:end]:
        if "=" not in line:
            continue
        key, raw_val = line.split("=", 1)
        key = key.strip().lower().replace("_", "")
        fields[key] = parse_list_values(raw_val)
    types = [str(val).lower() for val in fields.get("type", [])]
    densities = fields.get("dens", [])
    electron_idx = None
    for idx, tval in enumerate(types):
        if tval == "electron":
            electron_idx = idx
            break
    ion_indices = [idx for idx, tval in enumerate(types) if tval == "ion"]
    main_ion_idx = None
    if ion_indices and densities:
        max_idx = ion_indices[0]
        max_val = densities[max_idx] if max_idx < len(densities) else None
        for idx in ion_indices[1:]:
            if idx >= len(densities):
                continue
            if max_val is None or densities[idx] > max_val:
                max_val = densities[idx]
                max_idx = idx
        main_ion_idx = max_idx
    result: Dict[str, float] = {}
    for label, idx in (("electron", electron_idx), ("ion", main_ion_idx)):
        if idx is None:
            continue
        for key in ("z", "mass", "dens", "temp", "tprim", "fprim", "vnewk"):
            values = fields.get(key, [])
            if idx < len(values):
                try:
                    result[f"{label}_{key}"] = float(values[idx])
                except (TypeError, ValueError):
                    continue
    return result


ALLOWED_STATS_COLUMNS = [
    "rhoc",
    "Rmaj",
    "R_geo",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
    "beta",
    "electron_z",
    "electron_mass",
    "electron_dens",
    "electron_temp",
    "electron_tprim",
    "electron_fprim",
    "electron_vnewk",
    "ion_z",
    "ion_mass",
    "ion_dens",
    "ion_temp",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
]

ALLOWED_RESULTS_COLUMNS = [
    "id",
    "gk_input_id",
    "gk_batch_id",
    "nb_nodes",
    "synced",
    "t_max_initial",
    "t_max",
    "nb_restart",
    "ky_abs_mean",
    "gamma_max",
    "diffusion",
]

SPECIES_COLUMNS = [
    "electron_z",
    "electron_mass",
    "electron_dens",
    "electron_temp",
    "electron_tprim",
    "electron_fprim",
    "electron_vnewk",
    "ion_z",
    "ion_mass",
    "ion_dens",
    "ion_temp",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
]

MHD_COLUMNS = [
    "rhoc",
    "Rmaj",
    "R_geo",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
    "beta",
]
MHD_ID_COLUMN = "id"
MHD_REGIME_DEFAULTS = ["qinp", "shat", "beta"]
EQUIL_PLASMA_COLUMNS = [
    "temp_ratio",
    "dens_ratio",
    "electron_tprim",
    "electron_fprim",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
    "electron_vnewk",
    "mass_ratio",
    "Rmaj",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
]
PLASMA_COLUMNS = [
    "electron_z",
    "electron_mass",
    "electron_dens",
    "electron_temp",
    "electron_tprim",
    "electron_fprim",
    "electron_vnewk",
    "ion_z",
    "ion_mass",
    "ion_dens",
    "ion_temp",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
]
PLASMA_REGIME_DEFAULTS = ["electron_dens", "electron_temp", "ion_temp"]


def get_gk_input_points(
    conn: sqlite3.Connection, x_col: str, y_col: str, origin_id: Optional[int]
) -> List[dict]:
    cache_key = ("gk_input_points", *_db_cache_signature(conn), x_col, y_col, origin_id)
    cached = _get_cached_gui_value(cache_key)
    if cached is None:
        base_query = f"""
            SELECT {x_col}, {y_col}, do.name, do.color
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            JOIN data_origin AS do ON do.id = data_equil.data_origin_id
            WHERE {x_col} IS NOT NULL AND {y_col} IS NOT NULL
        """
        params = []
        if origin_id is not None:
            base_query += " AND data_equil.data_origin_id = ?"
            params.append(origin_id)
        try:
            rows = conn.execute(base_query, params).fetchall()
        except sqlite3.OperationalError:
            return []
        cached = _set_cached_gui_value(
            cache_key,
            tuple(
                (
                    float(x_val),
                    float(y_val),
                    data_origin_color(origin_name, origin_color),
                )
                for x_val, y_val, origin_name, origin_color in rows
            ),
        )
    return [{"x": x, "y": y, "color": color} for x, y, color in cached]


def _as_finite_float(value: object) -> Optional[float]:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def get_table_columns(conn: sqlite3.Connection, table: str) -> set:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _percentile(sorted_values: List[float], pct: float) -> Optional[float]:
    if not sorted_values:
        return None
    idx = int(round((len(sorted_values) - 1) * pct))
    return sorted_values[max(0, min(idx, len(sorted_values) - 1))]


def _column_basic_stats(values: List[float], total_rows: int) -> Dict[str, object]:
    if not values:
        return {
            "count": 0,
            "missing": total_rows,
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
            "median": None,
        }
    count = len(values)
    mean_val = sum(values) / count
    if count > 1:
        variance = sum((val - mean_val) ** 2 for val in values) / (count - 1)
        std_val = math.sqrt(variance)
    else:
        std_val = 0.0
    sorted_vals = sorted(values)
    median_val = _percentile(sorted_vals, 0.5)
    return {
        "count": count,
        "missing": total_rows - count,
        "mean": mean_val,
        "std": std_val,
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
        "median": median_val,
    }


def get_sampling_dataset(
    conn: sqlite3.Connection, origin_id: Optional[int], columns: List[str]
) -> Tuple[List[Dict[str, Optional[float]]], int]:
    cache_key = (
        "sampling_dataset",
        *_db_cache_signature(conn),
        origin_id,
        tuple(columns),
    )
    rows = _get_cached_gui_value(cache_key)
    if rows is None:
        base_query = f"""
            SELECT gk_input.id, {", ".join(columns)}
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        """
        params: List[object] = []
        if origin_id is not None:
            base_query += " WHERE data_equil.data_origin_id = ?"
            params.append(origin_id)
        rows = _set_cached_gui_value(
            cache_key,
            tuple(tuple(row) for row in conn.execute(base_query, params).fetchall()),
        )
    dataset: List[Dict[str, Optional[float]]] = []
    for row in rows:
        item: Dict[str, Optional[float]] = {}
        item[MHD_ID_COLUMN] = _as_finite_float(row[0])
        for idx, col in enumerate(columns, start=1):
            item[col] = _as_finite_float(row[idx])
        dataset.append(item)
    return dataset, len(rows)


def get_equil_plasma_status_counts(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    ion_tprim_min: Optional[float] = None,
) -> Dict[str, int]:
    counts = {"WAIT": 0, "TORUN": 0, "BATCH": 0, "SUCCESS": 0}
    tables = set(list_tables(conn))
    if "gk_input" in tables:
        base_query = """
            SELECT gk_input.status, COUNT(*) AS cnt
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        """
        params: List[object] = []
        if origin_id is not None:
            base_query += " WHERE data_equil.data_origin_id = ?"
            params.append(origin_id)
        if ion_tprim_min is not None:
            if "WHERE" in base_query:
                base_query += " AND gk_input.ion_tprim >= ?"
            else:
                base_query += " WHERE gk_input.ion_tprim >= ?"
            params.append(ion_tprim_min)
        base_query += " GROUP BY gk_input.status"
        rows = conn.execute(base_query, params).fetchall()
        for row in rows:
            status = str(row["status"])
            if status in counts:
                counts[status] = int(row["cnt"])
    if "gk_run" in tables:
        base_query = """
            SELECT COUNT(*) AS cnt
            FROM gk_run
            JOIN gk_input ON gk_input.id = gk_run.gk_input_id
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            WHERE gk_run.status = 'SUCCESS'
        """
        params = []
        if origin_id is not None:
            base_query += " AND data_equil.data_origin_id = ?"
            params.append(origin_id)
        if ion_tprim_min is not None:
            base_query += " AND gk_input.ion_tprim >= ?"
            params.append(ion_tprim_min)
        row = conn.execute(base_query, params).fetchone()
        if row is not None:
            counts["SUCCESS"] = int(row["cnt"])
    return counts


def build_sampling_report(
    dataset: List[Dict[str, Optional[float]]], total_rows: int, columns: List[str]
) -> Dict[str, object]:
    stats: Dict[str, Dict[str, object]] = {}
    for col in columns:
        values = [row[col] for row in dataset if row[col] is not None]
        stats[col] = _column_basic_stats(values, total_rows)
    return {
        "total_rows": total_rows,
        "stats": stats,
    }


def _filter_complete_rows(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    id_column: Optional[str] = None,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for row in dataset:
        if any(row[col] is None for col in columns):
            continue
        complete = {col: float(row[col]) for col in columns}
        if id_column and row.get(id_column) is not None:
            complete[id_column] = float(row[id_column])
        rows.append(complete)
    return rows


def _standardize_rows(
    rows: List[Dict[str, float]],
    columns: List[str],
) -> Tuple[List[List[float]], Dict[str, float], Dict[str, float]]:
    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    for col in columns:
        values = [row[col] for row in rows]
        if not values:
            means[col] = 0.0
            stds[col] = 1.0
            continue
        mean_val = sum(values) / len(values)
        if len(values) > 1:
            variance = sum((val - mean_val) ** 2 for val in values) / (len(values) - 1)
            std_val = math.sqrt(variance)
        else:
            std_val = 1.0
        if std_val == 0.0:
            std_val = 1.0
        means[col] = mean_val
        stds[col] = std_val
    vectors: List[List[float]] = []
    for row in rows:
        vectors.append([(row[col] - means[col]) / stds[col] for col in columns])
    return vectors, means, stds


def _standardize_rows_with(
    rows: List[Dict[str, float]],
    columns: List[str],
    means: Dict[str, float],
    stds: Dict[str, float],
) -> List[List[float]]:
    vectors: List[List[float]] = []
    for row in rows:
        vectors.append([(row[col] - means[col]) / stds[col] for col in columns])
    return vectors


def _farthest_point_indices(vectors: List[List[float]], target: int) -> Tuple[List[int], List[float]]:
    n = len(vectors)
    if n == 0:
        return [], []
    target = max(1, min(target, n))
    rng = random.Random(0)
    selected = [rng.randrange(n)]
    min_dists = [float("inf") for _ in range(n)]
    for _ in range(1, target):
        best_idx = None
        best_dist = -1.0
        last_vec = vectors[selected[-1]]
        for idx, vec in enumerate(vectors):
            dist = _euclidean(vec, last_vec)
            if dist < min_dists[idx]:
                min_dists[idx] = dist
            if min_dists[idx] > best_dist:
                best_dist = min_dists[idx]
                best_idx = idx
        if best_idx is None:
            break
        selected.append(best_idx)
    last_vec = vectors[selected[-1]]
    for idx, vec in enumerate(vectors):
        dist = _euclidean(vec, last_vec)
        if dist < min_dists[idx]:
            min_dists[idx] = dist
    return selected, min_dists


def _local_farthest_selection(args: Tuple[List[List[float]], int]) -> List[int]:
    chunk_vectors, target = args
    if not chunk_vectors:
        return []
    selected, _ = _farthest_point_indices(chunk_vectors, target)
    return selected


def _min_dists_to_selected(vectors: List[List[float]], selected_indices: List[int]) -> List[float]:
    if not vectors:
        return []
    if not selected_indices:
        return [0.0 for _ in range(len(vectors))]
    selected_vectors = [vectors[idx] for idx in selected_indices]
    min_dists: List[float] = []
    for vec in vectors:
        best = None
        for svec in selected_vectors:
            dist = _euclidean(vec, svec)
            if best is None or dist < best:
                best = dist
        min_dists.append(best if best is not None else 0.0)
    return min_dists


def build_sampling_coverage(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    max_points: int = 1500,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns)
    total = len(rows)
    if total < 2:
        return {
            "total": total,
            "used": total,
            "sampled": False,
            "pairwise": None,
            "nearest": None,
        }
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    vectors, _, _ = _standardize_rows(rows, columns)
    pairwise: List[float] = []
    nearest: List[float] = []
    n = len(vectors)
    for i in range(n):
        min_dist = None
        vi = vectors[i]
        for j in range(i + 1, n):
            vj = vectors[j]
            dist_sq = 0.0
            for k in range(len(columns)):
                diff = vi[k] - vj[k]
                dist_sq += diff * diff
            dist = math.sqrt(dist_sq)
            pairwise.append(dist)
            if min_dist is None or dist < min_dist:
                min_dist = dist
        if min_dist is not None:
            nearest.append(min_dist)
    pairwise.sort()
    nearest.sort()
    return {
        "total": total,
        "used": len(rows),
        "sampled": sampled,
        "pairwise": {
            "min": pairwise[0] if pairwise else None,
            "median": _percentile(pairwise, 0.5),
            "p95": _percentile(pairwise, 0.95),
            "max": pairwise[-1] if pairwise else None,
        },
        "nearest": {
            "min": nearest[0] if nearest else None,
            "median": _percentile(nearest, 0.5),
            "p95": _percentile(nearest, 0.95),
            "max": nearest[-1] if nearest else None,
        },
    }


def _quantile_edges(values: List[float], bins: int) -> Optional[List[float]]:
    if not values:
        return None
    sorted_vals = sorted(values)
    edges = []
    for i in range(1, bins):
        edges.append(_percentile(sorted_vals, i / bins))
    return edges


def _assign_bin(value: float, edges: List[float]) -> int:
    for idx, edge in enumerate(edges):
        if value <= edge:
            return idx
    return len(edges)


def build_sampling_regimes(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    params: Optional[List[str]] = None,
    bins: int = 3,
) -> Dict[str, object]:
    if params is None:
        params = columns[:3]
    available = [p for p in params if p in columns]
    if len(available) != len(params):
        missing = [p for p in params if p not in available]
        return {"params": available, "missing": missing, "coverage": None, "bins": None}
    values_by_param = {p: [row[p] for row in dataset if row[p] is not None] for p in params}
    edges_by_param = {}
    for p in params:
        edges = _quantile_edges(values_by_param[p], bins)
        if not edges:
            return {"params": params, "missing": [], "coverage": None, "bins": None}
        edges_by_param[p] = edges
    counts: Dict[Tuple[int, ...], int] = {}
    for row in dataset:
        if any(row[p] is None for p in params):
            continue
        bin_key = tuple(_assign_bin(row[p], edges_by_param[p]) for p in params)
        counts[bin_key] = counts.get(bin_key, 0) + 1
    total_bins = bins ** len(params)
    non_empty = len(counts)
    bin_counts = sorted(counts.values())
    return {
        "params": params,
        "missing": [],
        "coverage": {
            "non_empty": non_empty,
            "total_bins": total_bins,
            "coverage_pct": (non_empty / total_bins * 100.0) if total_bins else 0.0,
            "min": bin_counts[0] if bin_counts else None,
            "median": _percentile(bin_counts, 0.5) if bin_counts else None,
            "max": bin_counts[-1] if bin_counts else None,
        },
        "bins": sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10],
        "edges": edges_by_param,
    }


def _euclidean(vec_a: List[float], vec_b: List[float]) -> float:
    dist_sq = 0.0
    for a, b in zip(vec_a, vec_b):
        diff = a - b
        dist_sq += diff * diff
    return math.sqrt(dist_sq)


def _mean_vector(vectors: List[List[float]]) -> List[float]:
    if not vectors:
        return []
    dim = len(vectors[0])
    totals = [0.0 for _ in range(dim)]
    for vec in vectors:
        for idx in range(dim):
            totals[idx] += vec[idx]
    return [val / len(vectors) for val in totals]


def _kmeans_numpy(
    vectors: List[List[float]],
    k: int,
    max_iter: int = 25,
    seed: int = 0,
) -> Tuple[List[int], List[List[float]]]:
    if not HAVE_NUMPY:
        return _kmeans(vectors, k, max_iter=max_iter, seed=seed)
    x = np.asarray(vectors, dtype=np.float64)
    n = x.shape[0]
    if n == 0:
        return [], []
    if k >= n:
        assignments = list(range(n))
        return assignments, x.tolist()
    rng = np.random.default_rng(seed)
    init_indices = rng.choice(n, size=k, replace=False)
    centroids = x[init_indices].copy()
    assignments = np.zeros(n, dtype=np.int32)
    for _ in range(max_iter):
        diffs = x[:, None, :] - centroids[None, :, :]
        dists = np.einsum("ijk,ijk->ij", diffs, diffs)
        new_assignments = np.argmin(dists, axis=1).astype(np.int32)
        if np.array_equal(assignments, new_assignments):
            break
        assignments = new_assignments
        for idx in range(k):
            mask = assignments == idx
            if np.any(mask):
                centroids[idx] = x[mask].mean(axis=0)
            else:
                centroids[idx] = x[rng.integers(0, n)]
    return assignments.tolist(), centroids.tolist()


def _kmeans(
    vectors: List[List[float]],
    k: int,
    max_iter: int = 25,
    seed: int = 0,
) -> Tuple[List[int], List[List[float]]]:
    rng = random.Random(seed)
    n = len(vectors)
    if k >= n:
        assignments = list(range(n))
        centroids = [vec[:] for vec in vectors]
        return assignments, centroids
    init_indices = rng.sample(range(n), k)
    centroids = [vectors[idx][:] for idx in init_indices]
    assignments = [0 for _ in range(n)]
    for _ in range(max_iter):
        changed = False
        clusters: List[List[List[float]]] = [[] for _ in range(k)]
        for i, vec in enumerate(vectors):
            best = 0
            best_dist = None
            for c_idx, centroid in enumerate(centroids):
                dist = _euclidean(vec, centroid)
                if best_dist is None or dist < best_dist:
                    best_dist = dist
                    best = c_idx
            if assignments[i] != best:
                changed = True
            assignments[i] = best
            clusters[best].append(vec)
        for idx in range(k):
            if clusters[idx]:
                centroids[idx] = _mean_vector(clusters[idx])
            else:
                centroids[idx] = vectors[rng.randrange(n)][:]  # re-seed empty cluster
        if not changed:
            break
    return assignments, centroids


def _silhouette_score(
    vectors: List[List[float]],
    assignments: List[int],
    sample_indices: Optional[List[int]] = None,
) -> Optional[float]:
    n = len(vectors)
    if n < 2:
        return None
    if sample_indices is None:
        sample_indices = list(range(n))
    clusters: Dict[int, List[int]] = {}
    for idx in sample_indices:
        clusters.setdefault(assignments[idx], []).append(idx)
    if len(clusters) < 2:
        return None
    scores = []
    for idx in sample_indices:
        own = assignments[idx]
        own_cluster = clusters.get(own, [])
        if len(own_cluster) <= 1:
            continue
        a = sum(_euclidean(vectors[idx], vectors[j]) for j in own_cluster if j != idx)
        a /= max(1, len(own_cluster) - 1)
        b = None
        for cluster_id, members in clusters.items():
            if cluster_id == own or not members:
                continue
            dist = sum(_euclidean(vectors[idx], vectors[j]) for j in members)
            dist /= len(members)
            if b is None or dist < b:
                b = dist
        if b is None:
            continue
        denom = max(a, b)
        if denom == 0.0:
            continue
        scores.append((b - a) / denom)
    if not scores:
        return None
    return sum(scores) / len(scores)


def _davies_bouldin_index(
    vectors: List[List[float]],
    assignments: List[int],
    centroids: List[List[float]],
) -> Optional[float]:
    k = len(centroids)
    if k < 2:
        return None
    clusters: Dict[int, List[int]] = {}
    for idx, cluster_id in enumerate(assignments):
        clusters.setdefault(cluster_id, []).append(idx)
    scatters: List[float] = []
    for cluster_id in range(k):
        members = clusters.get(cluster_id, [])
        if not members:
            scatters.append(0.0)
            continue
        dist = sum(_euclidean(vectors[i], centroids[cluster_id]) for i in members)
        scatters.append(dist / len(members))
    db_values = []
    for i in range(k):
        worst = None
        for j in range(k):
            if i == j:
                continue
            denom = _euclidean(centroids[i], centroids[j])
            if denom == 0.0:
                continue
            val = (scatters[i] + scatters[j]) / denom
            if worst is None or val > worst:
                worst = val
        if worst is not None:
            db_values.append(worst)
    if not db_values:
        return None
    return sum(db_values) / len(db_values)


def build_sampling_clustering(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    k: int,
    max_points: int = 1200,
    sample_for_metrics: int = 400,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns)
    total = len(rows)
    if total < 2:
        return {"total": total, "used": total, "sampled": False, "k": k, "metrics": None}
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    vectors, _, _ = _standardize_rows(rows, columns)
    if k < 2 or k > len(vectors):
        return {
            "total": total,
            "used": len(vectors),
            "sampled": sampled,
            "k": k,
            "metrics": None,
        }
    assignments, centroids = _kmeans(vectors, k)
    sample_indices = None
    if len(vectors) > sample_for_metrics:
        rng = random.Random(1)
        sample_indices = rng.sample(range(len(vectors)), sample_for_metrics)
    silhouette = _silhouette_score(vectors, assignments, sample_indices)
    dbi = _davies_bouldin_index(vectors, assignments, centroids)
    sizes: Dict[int, int] = {}
    for cluster_id in assignments:
        sizes[cluster_id] = sizes.get(cluster_id, 0) + 1
    counts = sorted(sizes.values())
    return {
        "total": total,
        "used": len(vectors),
        "sampled": sampled,
        "k": k,
        "metrics": {
            "silhouette": silhouette,
            "davies_bouldin": dbi,
            "min_size": counts[0] if counts else None,
            "median_size": _percentile(counts, 0.5) if counts else None,
            "max_size": counts[-1] if counts else None,
        },
    }


def _mat_vec_mul(matrix: List[List[float]], vector: List[float]) -> List[float]:
    result = []
    for row in matrix:
        total = 0.0
        for a, b in zip(row, vector):
            total += a * b
        result.append(total)
    return result


def _vec_norm(vector: List[float]) -> float:
    return math.sqrt(sum(val * val for val in vector))


def _normalize(vector: List[float]) -> List[float]:
    norm = _vec_norm(vector)
    if norm == 0.0:
        return vector[:]
    return [val / norm for val in vector]


def _outer(vector: List[float]) -> List[List[float]]:
    return [[a * b for b in vector] for a in vector]


def _deflate(matrix: List[List[float]], eigenvalue: float, eigenvector: List[float]) -> None:
    outer = _outer(eigenvector)
    for i in range(len(matrix)):
        for j in range(len(matrix)):
            matrix[i][j] -= eigenvalue * outer[i][j]


def _power_iteration(matrix: List[List[float]], iters: int = 80) -> Tuple[float, List[float]]:
    rng = random.Random(0)
    vec = [rng.random() for _ in range(len(matrix))]
    vec = _normalize(vec)
    for _ in range(iters):
        vec = _mat_vec_mul(matrix, vec)
        vec = _normalize(vec)
    eig_val = sum(a * b for a, b in zip(vec, _mat_vec_mul(matrix, vec)))
    return eig_val, vec


def build_sampling_pca(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    components: int = 3,
    max_points: int = 2000,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns)
    total = len(rows)
    if total < 2:
        return {"total": total, "used": total, "sampled": False, "components": []}
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    vectors, _, _ = _standardize_rows(rows, columns)
    n = len(vectors)
    dim = len(columns)
    cov = [[0.0 for _ in range(dim)] for _ in range(dim)]
    for vec in vectors:
        for i in range(dim):
            for j in range(dim):
                cov[i][j] += vec[i] * vec[j]
    denom = max(1, n - 1)
    for i in range(dim):
        for j in range(dim):
            cov[i][j] /= denom
    total_variance = sum(cov[i][i] for i in range(dim))
    components = min(components, dim)
    eigs: List[Tuple[float, List[float]]] = []
    work = [row[:] for row in cov]
    for _ in range(components):
        eig_val, eig_vec = _power_iteration(work)
        if eig_val <= 0.0:
            break
        eigs.append((eig_val, eig_vec))
        _deflate(work, eig_val, eig_vec)
    comp_rows = []
    cumulative = 0.0
    for idx, (eig_val, _) in enumerate(eigs, start=1):
        ratio = eig_val / total_variance if total_variance else 0.0
        cumulative += ratio
        comp_rows.append(
            {"component": idx, "eigenvalue": eig_val, "ratio": ratio, "cumulative": cumulative}
        )
    return {
        "total": total,
        "used": len(vectors),
        "sampled": sampled,
        "components": comp_rows,
    }


def build_sampling_selection(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    target: int = 50,
    max_points: int = 2000,
    id_column: str = MHD_ID_COLUMN,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns, id_column)
    total = len(rows)
    if total == 0:
        return {"total": total, "used": 0, "sampled": False, "target": target, "metrics": None}
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    vectors, _, _ = _standardize_rows(rows, columns)
    n = len(vectors)
    selected, min_dists = _farthest_point_indices(vectors, target)
    avg_dist = sum(min_dists) / len(min_dists) if min_dists else None
    min_dist = min(min_dists) if min_dists else None
    max_dist = max(min_dists) if min_dists else None
    selected_ids = []
    for idx in selected[:50]:
        row_id = rows[idx].get(id_column)
        if row_id is not None:
            selected_ids.append(int(row_id))
    sorted_dists = sorted(min_dists) if min_dists else []
    return {
        "total": total,
        "used": n,
        "sampled": sampled,
        "target": target,
        "metrics": {
            "min_nearest": min_dist,
            "avg_nearest": avg_dist,
            "p95_nearest": _percentile(sorted_dists, 0.95) if sorted_dists else None,
            "max_nearest": max_dist,
        },
        "selected_ids": selected_ids,
    }


def build_two_stage_selection(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    target: int,
    max_points: int,
    workers: int,
    id_column: str = MHD_ID_COLUMN,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns, id_column)
    total = len(rows)
    if total == 0:
        return {"total": total, "used": 0, "sampled": False, "target": target, "metrics": None}
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    workers = max(1, min(workers, len(rows)))
    vectors, means, stds = _standardize_rows(rows, columns)
    n = len(vectors)
    if workers == 1:
        selected, min_dists = _farthest_point_indices(vectors, target)
    else:
        chunk_size = int(math.ceil(n / workers))
        local_target = max(1, int(math.ceil(target / workers)))
        merged_indices: List[int] = []
        chunk_vectors_list: List[List[List[float]]] = []
        chunk_offsets: List[int] = []
        for start in range(0, n, chunk_size):
            end = min(start + chunk_size, n)
            chunk_offsets.append(start)
            chunk_vectors_list.append(vectors[start:end])
        try:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                local_results = list(
                    executor.map(
                        _local_farthest_selection,
                        [(chunk, min(local_target, len(chunk))) for chunk in chunk_vectors_list],
                    )
                )
        except Exception:
            local_results = []
            for chunk in chunk_vectors_list:
                local_results.append(
                    _local_farthest_selection((chunk, min(local_target, len(chunk))))
                )
        for offset, local_selected in zip(chunk_offsets, local_results):
            merged_indices.extend([offset + i for i in local_selected])
        merged_indices = list(dict.fromkeys(merged_indices))
        merged_vectors = [vectors[i] for i in merged_indices]
        final_selected, _ = _farthest_point_indices(
            merged_vectors, min(target, len(merged_vectors))
        )
        selected = [merged_indices[i] for i in final_selected]
        min_dists = _min_dists_to_selected(vectors, selected)
    avg_dist = sum(min_dists) / len(min_dists) if min_dists else None
    min_dist = min(min_dists) if min_dists else None
    max_dist = max(min_dists) if min_dists else None
    sorted_dists = sorted(min_dists) if min_dists else []
    selected_ids = []
    for idx in selected[:50]:
        row_id = rows[idx].get(id_column)
        if row_id is not None:
            selected_ids.append(int(row_id))
    return {
        "total": total,
        "used": n,
        "sampled": sampled,
        "target": target,
        "metrics": {
            "min_nearest": min_dist,
            "avg_nearest": avg_dist,
            "p95_nearest": _percentile(sorted_dists, 0.95) if sorted_dists else None,
            "max_nearest": max_dist,
        },
        "selected_ids": selected_ids,
    }


def build_kmeans_selection(
    dataset: List[Dict[str, Optional[float]]],
    columns: List[str],
    k: int,
    max_points: int = 2000,
    id_column: str = MHD_ID_COLUMN,
) -> Dict[str, object]:
    rows = _filter_complete_rows(dataset, columns, id_column)
    total = len(rows)
    if total == 0:
        return {"total": total, "used": 0, "sampled": False, "k": k, "metrics": None}
    sampled = False
    if total > max_points:
        rng = random.Random(0)
        rows = rng.sample(rows, max_points)
        sampled = True
    vectors, _, _ = _standardize_rows(rows, columns)
    n = len(vectors)
    k = max(1, min(k, n))
    if HAVE_NUMPY:
        x = np.asarray(vectors, dtype=np.float64)
        if k == 1:
            selected = [0]
            assignments = [0 for _ in range(n)]
            centroids = [vectors[0][:]]
        else:
            assignments, centroids = _kmeans_numpy(vectors, k)
            selected = []
            assignments_arr = np.asarray(assignments, dtype=np.int32)
            centroids_arr = np.asarray(centroids, dtype=np.float64)
            for cluster_id in range(centroids_arr.shape[0]):
                mask = assignments_arr == cluster_id
                if not np.any(mask):
                    continue
                cluster_indices = np.nonzero(mask)[0]
                cluster_vecs = x[cluster_indices]
                diffs = cluster_vecs - centroids_arr[cluster_id]
                dists = np.einsum("ij,ij->i", diffs, diffs)
                best_local = int(np.argmin(dists))
                selected.append(int(cluster_indices[best_local]))
        if selected:
            s = x[selected]
            diffs = x[:, None, :] - s[None, :, :]
            dists = np.einsum("ijk,ijk->ij", diffs, diffs)
            min_dists = np.sqrt(np.min(dists, axis=1)).tolist()
        else:
            min_dists = [0.0 for _ in range(n)]
    else:
        if k == 1:
            selected = [0]
            centroids = [vectors[0][:]]
            assignments = [0 for _ in range(n)]
        else:
            assignments, centroids = _kmeans(vectors, k)
            selected = []
            for cluster_id, centroid in enumerate(centroids):
                best_idx = None
                best_dist = None
                for idx, vec in enumerate(vectors):
                    if assignments[idx] != cluster_id:
                        continue
                    dist = _euclidean(vec, centroid)
                    if best_dist is None or dist < best_dist:
                        best_dist = dist
                        best_idx = idx
                if best_idx is not None:
                    selected.append(best_idx)
        selected_set = set(selected)
        min_dists = []
        for idx, vec in enumerate(vectors):
            if idx in selected_set:
                min_dists.append(0.0)
                continue
            min_dist = None
            for sidx in selected:
                dist = _euclidean(vec, vectors[sidx])
                if min_dist is None or dist < min_dist:
                    min_dist = dist
            min_dists.append(min_dist if min_dist is not None else 0.0)
    avg_dist = sum(min_dists) / len(min_dists) if min_dists else None
    min_dist = min(min_dists) if min_dists else None
    max_dist = max(min_dists) if min_dists else None
    sorted_dists = sorted(min_dists) if min_dists else []
    selected_ids = []
    for idx in selected[:50]:
        row_id = rows[idx].get(id_column)
        if row_id is not None:
            selected_ids.append(int(row_id))
    return {
        "total": total,
        "used": n,
        "sampled": sampled,
        "k": k,
        "metrics": {
            "min_nearest": min_dist,
            "avg_nearest": avg_dist,
            "p95_nearest": _percentile(sorted_dists, 0.95) if sorted_dists else None,
            "max_nearest": max_dist,
        },
        "selected_ids": selected_ids,
    }


def get_equil_plasma_dataset(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    status_filter: Optional[str] = None,
    ion_tprim_min: Optional[float] = None,
) -> Tuple[List[Dict[str, Optional[float]]], int]:
    columns = [
        "gk_input.id",
        "gk_input.electron_temp",
        "gk_input.ion_temp",
        "gk_input.electron_dens",
        "gk_input.ion_dens",
        "gk_input.electron_tprim",
        "gk_input.electron_fprim",
        "gk_input.ion_tprim",
        "gk_input.ion_fprim",
        "gk_input.ion_vnewk",
        "gk_input.electron_vnewk",
        "gk_input.ion_mass",
        "gk_input.electron_mass",
        "gk_input.Rmaj",
        "gk_input.qinp",
        "gk_input.shat",
        "gk_input.shift",
        "gk_input.akappa",
        "gk_input.akappri",
        "gk_input.tri",
        "gk_input.tripri",
        "gk_input.betaprim",
    ]
    cache_key = (
        "equil_plasma_dataset",
        *_db_cache_signature(conn),
        origin_id,
        status_filter,
        ion_tprim_min,
    )
    rows = _get_cached_gui_value(cache_key)
    if rows is None:
        base_query = f"""
            SELECT {", ".join(columns)}
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        """
        params: List[object] = []
        if origin_id is not None:
            base_query += " WHERE data_equil.data_origin_id = ?"
            params.append(origin_id)
        if status_filter is not None:
            if "WHERE" in base_query:
                base_query += " AND gk_input.status = ?"
            else:
                base_query += " WHERE gk_input.status = ?"
            params.append(status_filter)
        if ion_tprim_min is not None:
            if "WHERE" in base_query:
                base_query += " AND gk_input.ion_tprim >= ?"
            else:
                base_query += " WHERE gk_input.ion_tprim >= ?"
            params.append(ion_tprim_min)
        rows = _set_cached_gui_value(
            cache_key,
            tuple(tuple(row) for row in conn.execute(base_query, params).fetchall()),
        )
    dataset: List[Dict[str, Optional[float]]] = []
    for row in rows:
        (
            gid,
            electron_temp,
            ion_temp,
            electron_dens,
            ion_dens,
            electron_tprim,
            electron_fprim,
            ion_tprim,
            ion_fprim,
            ion_vnewk,
            electron_vnewk,
            ion_mass,
            electron_mass,
            rmaj,
            qinp,
            shat,
            shift,
            akappa,
            akappri,
            tri,
            tripri,
            betaprim,
        ) = row
        def safe_ratio(num: Optional[float], denom: Optional[float]) -> Optional[float]:
            if num is None or denom is None:
                return None
            try:
                denom_val = float(denom)
                if denom_val == 0.0:
                    return None
                return float(num) / denom_val
            except (TypeError, ValueError):
                return None
        temp_ratio = safe_ratio(ion_temp, electron_temp)
        dens_ratio = safe_ratio(ion_dens, electron_dens)
        mass_ratio = safe_ratio(ion_mass, None if electron_mass is None else float(electron_mass) * 1836.0)
        item: Dict[str, Optional[float]] = {
            MHD_ID_COLUMN: _as_finite_float(gid),
            "temp_ratio": _as_finite_float(temp_ratio),
            "dens_ratio": _as_finite_float(dens_ratio),
            "electron_tprim": _as_finite_float(electron_tprim),
            "electron_fprim": _as_finite_float(electron_fprim),
            "ion_tprim": _as_finite_float(ion_tprim),
            "ion_fprim": _as_finite_float(ion_fprim),
            "ion_vnewk": _as_finite_float(ion_vnewk),
            "electron_vnewk": _as_finite_float(electron_vnewk),
            "mass_ratio": _as_finite_float(mass_ratio),
            "Rmaj": _as_finite_float(rmaj),
            "qinp": _as_finite_float(qinp),
            "shat": _as_finite_float(shat),
            "shift": _as_finite_float(shift),
            "akappa": _as_finite_float(akappa),
            "akappri": _as_finite_float(akappri),
            "tri": _as_finite_float(tri),
            "tripri": _as_finite_float(tripri),
            "betaprim": _as_finite_float(betaprim),
        }
        dataset.append(item)
    return dataset, len(rows)


def build_results_columns(
    run_columns: Optional[set] = None,
    surrogate_models: Optional[List[Dict[str, object]]] = None,
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for col in ALLOWED_STATS_COLUMNS:
        items.append({"value": f"gk_input.{col}", "label": f"gk_input.{col}"})
    for col in ALLOWED_RESULTS_COLUMNS:
        if run_columns is not None and col not in run_columns:
            continue
        items.append({"value": f"gk_run.{col}", "label": f"gk_run.{col}"})
    if surrogate_models:
        for model in surrogate_models:
            try:
                surrogate_id = int(model.get("id"))
            except (TypeError, ValueError):
                continue
            label = str(model.get("sg_label") or "surrogate")
            items.append(
                {
                    "value": f"sg_estimate:{surrogate_id}",
                    "label": f"SG {surrogate_id} {label} - estimate",
                }
            )
            items.append(
                {
                    "value": f"sg_error:{surrogate_id}",
                    "label": f"SG {surrogate_id} {label} - error",
                }
            )
            items.append(
                {
                    "value": f"sg_diff:{surrogate_id}",
                    "label": f"SG {surrogate_id} {label} - diff",
                }
            )
    return items


def get_gk_run_results_points(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
    return get_results_points_any(conn, x_col, y_col, results_filter, origin_id)


def _normalize_axis_spec(spec: str) -> str:
    if not spec:
        return spec
    if spec in ALLOWED_STATS_COLUMNS:
        return f"gk_input.{spec}"
    return spec


def _axis_info(
    conn: sqlite3.Connection,
    spec: str,
    axis_label: str,
) -> Optional[Dict[str, object]]:
    spec = _normalize_axis_spec(spec)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    gk_input_columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_input)")} if "gk_input" in tables else set()
    gk_run_columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")} if "gk_run" in tables else set()
    if spec.startswith("gk_input."):
        col = spec.split(".", 1)[1]
        if col not in gk_input_columns:
            return None
        return {
            "expr": f"gk_input.{col}",
            "joins": [],
            "where": [f"gk_input.{col} IS NOT NULL"],
            "params": [],
            "needs_gk_run": False,
        }
    if spec.startswith("gk_run."):
        col = spec.split(".", 1)[1]
        if col not in gk_run_columns:
            return None
        return {
            "expr": f"gr.{col}",
            "joins": [],
            "where": [f"gr.{col} IS NOT NULL"],
            "params": [],
            "needs_gk_run": True,
        }
    if spec.startswith("sg_estimate:"):
        if "sg_estimate" not in tables:
            return None
        try:
            surrogate_id = int(spec.split(":", 1)[1])
        except (IndexError, ValueError):
            return None
        alias = f"sg_{axis_label}"
        join = (
            f"JOIN sg_estimate AS {alias} "
            f"ON {alias}.gk_input_id = gk_input.id AND {alias}.gk_surrogate_id = ?"
        )
        return {
            "expr": f"{alias}.sg_estimate",
            "joins": [join],
            "where": [f"{alias}.sg_estimate IS NOT NULL"],
            "params": [surrogate_id],
            "needs_gk_run": False,
        }
    if spec.startswith("sg_error:") or spec.startswith("sg_diff:"):
        if "sg_estimate" not in tables or "gk_surrogate" not in tables:
            return None
        try:
            surrogate_id = int(spec.split(":", 1)[1])
        except (IndexError, ValueError):
            return None
        row = conn.execute(
            "SELECT mapsto FROM gk_surrogate WHERE id = ?",
            (surrogate_id,),
        ).fetchone()
        if row is None:
            return None
        mapsto = str(row["mapsto"] or "")
        truth_expr = ""
        needs_gk_run = False
        if mapsto.startswith("gk_run."):
            truth_col = mapsto.split(".", 1)[1]
            if truth_col not in gk_run_columns:
                return None
            truth_expr = f"gr.{truth_col}"
            needs_gk_run = True
        elif mapsto.startswith("gk_input."):
            truth_col = mapsto.split(".", 1)[1]
            if truth_col not in gk_input_columns:
                return None
            truth_expr = f"gk_input.{truth_col}"
        else:
            return None
        alias = f"sg_{axis_label}"
        join = (
            f"JOIN sg_estimate AS {alias} "
            f"ON {alias}.gk_input_id = gk_input.id AND {alias}.gk_surrogate_id = ?"
        )
        expr = f"{alias}.sg_estimate - {truth_expr}"
        where = [
            f"{alias}.sg_estimate IS NOT NULL",
            f"{truth_expr} IS NOT NULL",
        ]
        if spec.startswith("sg_error:"):
            expr = f"ABS(({alias}.sg_estimate - {truth_expr}) / {truth_expr}) * 100.0"
            where.append(f"{truth_expr} != 0")
        return {
            "expr": expr,
            "joins": [join],
            "where": where,
            "params": [surrogate_id],
            "needs_gk_run": needs_gk_run,
        }
    return None


def get_results_points_any(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
    cache_key = (
        "results_points_any",
        *_db_cache_signature(conn),
        x_col,
        y_col,
        results_filter,
        origin_id,
    )
    cached = _get_cached_gui_value(cache_key)
    if cached is None:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        axis_x = _axis_info(conn, x_col, "x")
        axis_y = _axis_info(conn, y_col, "y")
        if axis_x is None or axis_y is None:
            return [], False
        joins = []
        params: List[object] = []
        needs_gk_run = axis_x["needs_gk_run"] or axis_y["needs_gk_run"]
        joins.extend(axis_x["joins"])
        joins.extend(axis_y["joins"])
        params.extend(axis_x["params"])
        params.extend(axis_y["params"])
        if results_filter in {"finished", "growth"}:
            needs_gk_run = True
        if needs_gk_run and "gk_run" not in tables:
            return [], False
        if needs_gk_run:
            joins.append("JOIN gk_run AS gr ON gr.gk_input_id = gk_input.id")
        joins.append("JOIN gk_study ON gk_study.id = gk_input.gk_study_id")
        joins.append("JOIN data_equil ON data_equil.id = gk_study.data_equil_id")
        joins.append("JOIN data_origin AS do ON do.id = data_equil.data_origin_id")
        where_clauses = []
        where_clauses.extend(axis_x["where"])
        where_clauses.extend(axis_y["where"])
        if results_filter == "finished":
            where_clauses.append("gr.status IN ('SUCCESS', 'CONVERGED')")
        if results_filter == "growth":
            where_clauses.append("gr.gamma_max IS NOT NULL AND gr.gamma_max != 0")
        if origin_id is not None:
            where_clauses.append("data_equil.data_origin_id = ?")
            params.append(origin_id)
        base_query = (
            f"SELECT DISTINCT {axis_x['expr']}, {axis_y['expr']}, do.name, do.color "
            "FROM gk_input "
            + " ".join(joins)
        )
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
        rows = conn.execute(base_query, params).fetchall()
        points = []
        has_non_finite = False
        for x_val, y_val, origin_name, origin_color in rows:
            try:
                x_float = float(x_val)
                y_float = float(y_val)
            except (TypeError, ValueError):
                has_non_finite = True
                continue
            if not math.isfinite(x_float) or not math.isfinite(y_float):
                has_non_finite = True
                continue
            color = data_origin_color(origin_name, origin_color)
            points.append((x_float, y_float, color))
        cached = _set_cached_gui_value(cache_key, (tuple(points), has_non_finite))
    point_rows, has_non_finite = cached
    return (
        [{"x": x_float, "y": y_float, "color": color} for x_float, y_float, color in point_rows],
        has_non_finite,
    )
    use_gk_run = True
    if y_col.startswith("gk_run."):
        y_field = y_col.split(".", 1)[1]
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
        }
        if y_field not in columns:
            return [], False
        y_expr = f"gk_run.{y_field}"
    elif y_col.startswith("gk_input."):
        y_field = y_col.split(".", 1)[1]
        if y_field not in ALLOWED_STATS_COLUMNS:
            return [], False
        y_expr = f"gk_input.{y_field}"
        use_gk_run = False
    else:
        return [], False
    if use_gk_run:
        base_query = f"""
            SELECT gk_input.{x_col}, {y_expr}, do.name, do.color
            FROM gk_run
            JOIN gk_input ON gk_input.id = gk_run.gk_input_id
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            JOIN data_origin AS do ON do.id = data_equil.data_origin_id
            WHERE gk_input.{x_col} IS NOT NULL AND {y_expr} IS NOT NULL
        """
    else:
        base_query = f"""
            SELECT DISTINCT gk_input.{x_col}, {y_expr}, do.name, do.color
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            JOIN data_origin AS do ON do.id = data_equil.data_origin_id
            WHERE gk_input.{x_col} IS NOT NULL AND {y_expr} IS NOT NULL
        """
        if results_filter in {"finished", "growth"}:
            base_query = base_query.replace(
                "FROM gk_input",
                "FROM gk_input JOIN gk_run ON gk_run.gk_input_id = gk_input.id",
            )
    params = []
    if results_filter == "finished":
        base_query += " AND gk_run.status IN ('SUCCESS', 'CONVERGED')"
    if results_filter == "growth":
        base_query += " AND gk_run.gamma_max IS NOT NULL AND gk_run.gamma_max != 0"
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    points = []
    has_non_finite = False
    for x_val, y_val, origin_name, origin_color in rows:
        if not math.isfinite(float(x_val)) or not math.isfinite(float(y_val)):
            has_non_finite = True
            continue
        color = data_origin_color(origin_name, origin_color)
        points.append({"x": float(x_val), "y": float(y_val), "color": color})
    return points, has_non_finite


def get_surrogate_error_points(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "sg_estimate" not in tables or "gk_surrogate" not in tables:
        return [], False
    if x_col.startswith("sg_estimate:") or x_col.startswith("sg_error:") or x_col.startswith("sg_diff:"):
        return [], False
    if x_col not in ALLOWED_STATS_COLUMNS:
        return [], False
    try:
        surrogate_id = int(y_col.split(":", 1)[1])
    except (IndexError, ValueError):
        return [], False
    row = conn.execute(
        "SELECT mapsto FROM gk_surrogate WHERE id = ?",
        (surrogate_id,),
    ).fetchone()
    if row is None:
        return [], False
    mapsto = str(row["mapsto"] or "")
    truth_expr = ""
    join_gk_run = False
    if mapsto.startswith("gk_run."):
        truth_col = mapsto.split(".", 1)[1]
        truth_expr = f"gr.{truth_col}"
        join_gk_run = True
    elif mapsto.startswith("gk_input."):
        truth_col = mapsto.split(".", 1)[1]
        truth_expr = f"gk_input.{truth_col}"
    else:
        return [], False

    base_query = f"""
        SELECT DISTINCT gk_input.{x_col}, sg.sg_estimate, {truth_expr}, do.name, do.color
        FROM gk_input
        JOIN sg_estimate AS sg ON sg.gk_input_id = gk_input.id
    """
    if join_gk_run or results_filter in {"finished", "growth"}:
        base_query += " JOIN gk_run AS gr ON gr.gk_input_id = gk_input.id"
    base_query += """
        JOIN gk_study ON gk_study.id = gk_input.gk_study_id
        JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        JOIN data_origin AS do ON do.id = data_equil.data_origin_id
        WHERE gk_input.{x_col} IS NOT NULL
          AND sg.sg_estimate IS NOT NULL
          AND {truth_expr} IS NOT NULL
          AND {truth_expr} != 0
          AND sg.gk_surrogate_id = ?
    """.format(x_col=x_col, truth_expr=truth_expr)
    params: List[object] = [surrogate_id]
    if results_filter == "finished":
        base_query += " AND gr.status IN ('SUCCESS', 'CONVERGED')"
    if results_filter == "growth":
        base_query += " AND gr.gamma_max IS NOT NULL AND gr.gamma_max != 0"
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    points = []
    has_non_finite = False
    for x_val, est_val, truth_val, origin_name, origin_color in rows:
        try:
            x_float = float(x_val)
            est_float = float(est_val)
            truth_float = float(truth_val)
        except (TypeError, ValueError):
            has_non_finite = True
            continue
        if not math.isfinite(x_float) or not math.isfinite(est_float) or not math.isfinite(truth_float):
            has_non_finite = True
            continue
        if truth_float == 0.0:
            continue
        err = abs((est_float - truth_float) / truth_float) * 100.0
        if not math.isfinite(err):
            has_non_finite = True
            continue
        color = data_origin_color(origin_name, origin_color)
        points.append({"x": x_float, "y": err, "color": color})
    return points, has_non_finite


def get_surrogate_diff_points(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "sg_estimate" not in tables or "gk_surrogate" not in tables:
        return [], False
    if x_col not in ALLOWED_STATS_COLUMNS:
        return [], False
    try:
        surrogate_id = int(y_col.split(":", 1)[1])
    except (IndexError, ValueError):
        return [], False
    row = conn.execute(
        "SELECT mapsto FROM gk_surrogate WHERE id = ?",
        (surrogate_id,),
    ).fetchone()
    if row is None:
        return [], False
    mapsto = str(row["mapsto"] or "")
    truth_expr = ""
    join_gk_run = False
    if mapsto.startswith("gk_run."):
        truth_col = mapsto.split(".", 1)[1]
        truth_expr = f"gr.{truth_col}"
        join_gk_run = True
    elif mapsto.startswith("gk_input."):
        truth_col = mapsto.split(".", 1)[1]
        truth_expr = f"gk_input.{truth_col}"
    else:
        return [], False

    base_query = f"""
        SELECT DISTINCT gk_input.{x_col}, sg.sg_estimate, {truth_expr}, do.name, do.color
        FROM gk_input
        JOIN sg_estimate AS sg ON sg.gk_input_id = gk_input.id
    """
    if join_gk_run or results_filter in {"finished", "growth"}:
        base_query += " JOIN gk_run AS gr ON gr.gk_input_id = gk_input.id"
    base_query += """
        JOIN gk_study ON gk_study.id = gk_input.gk_study_id
        JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        JOIN data_origin AS do ON do.id = data_equil.data_origin_id
        WHERE gk_input.{x_col} IS NOT NULL
          AND sg.sg_estimate IS NOT NULL
          AND {truth_expr} IS NOT NULL
          AND sg.gk_surrogate_id = ?
    """.format(x_col=x_col, truth_expr=truth_expr)
    params: List[object] = [surrogate_id]
    if results_filter == "finished":
        base_query += " AND gr.status IN ('SUCCESS', 'CONVERGED')"
    if results_filter == "growth":
        base_query += " AND gr.gamma_max IS NOT NULL AND gr.gamma_max != 0"
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    points = []
    has_non_finite = False
    for x_val, est_val, truth_val, origin_name, origin_color in rows:
        try:
            x_float = float(x_val)
            est_float = float(est_val)
            truth_float = float(truth_val)
        except (TypeError, ValueError):
            has_non_finite = True
            continue
        if not math.isfinite(x_float) or not math.isfinite(est_float) or not math.isfinite(truth_float):
            has_non_finite = True
            continue
        diff = est_float - truth_float
        if not math.isfinite(diff):
            has_non_finite = True
            continue
        color = data_origin_color(origin_name, origin_color)
        points.append({"x": x_float, "y": diff, "color": color})
    return points, has_non_finite


def get_surrogate_results_points(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "sg_estimate" not in tables:
        return [], False
    if x_col not in ALLOWED_STATS_COLUMNS:
        return [], False
    try:
        surrogate_id = int(y_col.split(":", 1)[1])
    except (IndexError, ValueError):
        return [], False
    base_query = f"""
        SELECT DISTINCT gk_input.{x_col}, sg.sg_estimate, do.name, do.color
        FROM gk_input
        JOIN sg_estimate AS sg ON sg.gk_input_id = gk_input.id
        JOIN gk_study ON gk_study.id = gk_input.gk_study_id
        JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        JOIN data_origin AS do ON do.id = data_equil.data_origin_id
        WHERE gk_input.{x_col} IS NOT NULL
          AND sg.sg_estimate IS NOT NULL
          AND sg.gk_surrogate_id = ?
    """
    params: List[object] = [surrogate_id]
    if results_filter in {"finished", "growth"}:
        base_query = base_query.replace(
            "FROM gk_input",
            "FROM gk_input JOIN gk_run ON gk_run.gk_input_id = gk_input.id",
        )
    if results_filter == "finished":
        base_query += " AND gk_run.status IN ('SUCCESS', 'CONVERGED')"
    if results_filter == "growth":
        base_query += " AND gk_run.gamma_max IS NOT NULL AND gk_run.gamma_max != 0"
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    points = []
    has_non_finite = False
    for x_val, y_val, origin_name, origin_color in rows:
        if not math.isfinite(float(x_val)) or not math.isfinite(float(y_val)):
            has_non_finite = True
            continue
        color = data_origin_color(origin_name, origin_color)
        points.append({"x": float(x_val), "y": float(y_val), "color": color})
    return points, has_non_finite


def get_action_state() -> Dict[str, Optional[str]]:
    with ACTION_LOCK:
        _prune_action_message_locked()
        state = dict(ACTION_STATE)
    state["message"] = _sanitize_action_message_text(state.get("message"))
    return state


@app.route("/action_status")
def action_status():
    response = jsonify(get_action_state())
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def get_gamma_max_status_report(
    conn: sqlite3.Connection, origin_id: Optional[int]
) -> Optional[str]:
    tables = set(list_tables(conn))
    if "gk_run" not in tables or "gk_input" not in tables:
        return None
    base_query = """
        SELECT gk_run.status, COUNT(*) AS cnt
        FROM gk_run
        JOIN gk_input ON gk_input.id = gk_run.gk_input_id
        JOIN gk_study ON gk_study.id = gk_input.gk_study_id
        JOIN data_equil ON data_equil.id = gk_study.data_equil_id
        WHERE gk_run.gamma_max IS NOT NULL AND gk_run.gamma_max != 0
    """
    params = []
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    base_query += " GROUP BY gk_run.status ORDER BY cnt DESC"
    rows = conn.execute(base_query, params).fetchall()
    if not rows:
        return "Zero runs have results."
    total = sum(int(row["cnt"]) for row in rows)
    status_map = {str(row["status"]): int(row["cnt"]) for row in rows}
    order = ["CONVERGED", "RUNNING", "TORUN", "RESTART", "CRASHED"]
    labels = {
        "CONVERGED": "are CONVERGED",
        "RUNNING": "is RUNNING",
        "TORUN": "needs TORUN",
        "RESTART": "needs a RESTART",
        "CRASHED": "have CRASHED",
    }
    parts = []
    for status in order:
        count = status_map.get(status)
        if count:
            parts.append(f"{count} {labels.get(status, f'are {status}')}")
    if not parts:
        parts = [f"{total} have status in other categories"]
    return (
        f"{total} runs have a non zero growth rate. Here is their status: "
        f"{', '.join(parts)}"
    )


def _run_action(
    spec: ActionSpec,
    db_path: Optional[str],
    extra_args: Optional[List[str]] = None,
    env_overrides: Optional[Dict[str, str]] = None,
    failure_context: Optional[Dict[str, object]] = None,
) -> None:
    action_key = spec.key
    action_label = spec.label
    script_path = spec.script
    script_args: List[str] = list(spec.args)
    use_db = spec.use_db
    db_arg = spec.db_arg
    capture_output = spec.capture_output
    message: Optional[str] = None
    technical_message: Optional[str] = None
    support_bundle_path: Optional[str] = None
    is_error = False

    if extra_args:
        script_args = [*script_args, *extra_args]
    if use_db and db_path:
        if db_arg:
            script_args = [*script_args, db_arg, db_path]
        else:
            script_args = [*script_args, db_path]
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    cmd = [sys.executable, script_path, *script_args]
    panel_name = str((failure_context or {}).get("panel") or "action")
    redirect_params = dict((failure_context or {}).get("redirect_params") or {})

    def _create_failure_bundle(
        *,
        failure_kind: str,
        returncode: Optional[int],
        stdout_text: str,
        stderr_text: str,
        traceback_text: str,
    ) -> Optional[str]:
        try:
            hpc_config = load_hpc_config()
        except Exception:
            hpc_config = {}
        action_state_snapshot = _snapshot_action_state()
        action_state_snapshot["running"] = False
        action_state_snapshot["name"] = None
        action_state_snapshot["key"] = action_key
        return create_support_bundle(
            bundle_dir=SUPPORT_BUNDLE_DIR,
            project_dir=PROJECT_DIR,
            usage_log_path=USAGE_LOG_PATH,
            action_key=action_key,
            action_label=action_label,
            script_path=script_path,
            command=cmd,
            db_path=db_path,
            panel=panel_name,
            redirect_params=redirect_params,
            env_overrides=env_overrides,
            failure_kind=failure_kind,
            returncode=returncode,
            stdout=stdout_text,
            stderr=stderr_text,
            traceback_text=traceback_text,
            action_state=action_state_snapshot,
            hpc_config=hpc_config,
        )

    try:
        if capture_output:
            if action_key == "train_gamma_surrogate":
                def _run_surrogate_once(
                    run_args: List[str],
                    params: Dict[str, object],
                    env_local: Dict[str, str],
                ) -> str:
                    cmd_local = [sys.executable, script_path, *run_args]
                    print("Running:", " ".join(cmd_local))
                    process = subprocess.Popen(
                        cmd_local,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        env=env_local,
                    )
                    combined_lines: List[str] = []
                    meta_path = None
                    if process.stdout is not None:
                        for line in process.stdout:
                            line = line.rstrip()
                            if line:
                                print(line)
                                combined_lines.append(line)
                                if line.startswith("Metadata saved:"):
                                    meta_path = line.split("Metadata saved:", 1)[-1].strip() or None
                    returncode = process.wait()
                    if returncode != 0:
                        raise subprocess.CalledProcessError(returncode, cmd_local)
                    if not meta_path:
                        safe_name = _safe_surrogate_name(str(params.get("name") or ""))
                        if safe_name:
                            candidate = os.path.join(
                                SURROGATE_MODEL_DIR, f"{safe_name}.pkl.json"
                            )
                            if os.path.exists(candidate):
                                meta_path = candidate
                        if not meta_path and os.path.isdir(SURROGATE_MODEL_DIR):
                            json_paths = sorted(
                                glob.glob(os.path.join(SURROGATE_MODEL_DIR, "*.pkl.json")),
                                key=os.path.getmtime,
                                reverse=True,
                            )
                            if json_paths:
                                meta_path = json_paths[0]
                    if meta_path:
                        _record_surrogate_model(db_path or "", meta_path, params)
                    return "\n".join(combined_lines).strip()

                params = _parse_surrogate_args(extra_args)
                mapsto_all = bool(params.get("mapsto_all"))
                combined_messages: List[str] = []
                if mapsto_all:
                    base_name = str(params.get("name") or "").strip()
                    mapsto_targets = [
                        "gk_run.gamma_max",
                        "gk_run.ky_abs_mean",
                        "gk_run.diffusion",
                    ]
                    def _strip_args(args: List[str]) -> List[str]:
                        stripped: List[str] = []
                        skip_next = False
                        for token in args:
                            if skip_next:
                                skip_next = False
                                continue
                            if token in {"--name", "--mapsto"}:
                                skip_next = True
                                continue
                            if token == "--mapsto-all":
                                continue
                            stripped.append(token)
                        return stripped
                    base_args = _strip_args(script_args)
                    for mapsto in mapsto_targets:
                        suffix = mapsto
                        name = f"{base_name} - {suffix}".strip()
                        run_args = [*base_args, "--name", name, "--mapsto", mapsto]
                        run_params = dict(params)
                        run_params["name"] = name
                        run_params["mapsto"] = mapsto
                        run_params.pop("mapsto_all", None)
                        combined = _run_surrogate_once(run_args, run_params, env)
                        if combined:
                            combined_messages.append(
                                f"[{mapsto}]\n{combined}"
                            )
                    message = "\n\n".join(combined_messages).strip() if combined_messages else None
                else:
                    combined = _run_surrogate_once(script_args, params, env)
                    if combined:
                        if len(combined) > 2000:
                            combined = combined[:2000].rstrip() + "\n... (truncated)"
                        message = combined
                    else:
                        message = None
            elif action_key == "test_hpc_connection":
                result = subprocess.run(
                    cmd,
                    check=False,
                    text=True,
                    capture_output=True,
                    env=env,
                )
                stdout = (result.stdout or "").strip()
                stderr = (result.stderr or "").strip()
                save_hpc_test_result(
                    {
                        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "success": result.returncode == 0,
                        "output": stdout,
                        "error": stderr,
                    }
                )
                combined = "\n".join([chunk for chunk in (stdout, stderr) if chunk])
                if combined:
                    print(combined)
                    if len(combined) > 2000:
                        combined = combined[:2000].rstrip() + "\n... (truncated)"
                if result.returncode != 0:
                    is_error = True
                    technical_message = (
                        f"Action '{action_label}' failed:\n{combined}"
                        if combined
                        else f"Action '{action_label}' failed."
                    )
                    support_bundle_path = _create_failure_bundle(
                        failure_kind="nonzero_return",
                        returncode=result.returncode,
                        stdout_text=stdout,
                        stderr_text=stderr,
                        traceback_text="",
                    )
                    technical_message = _append_support_bundle_note(
                        technical_message, support_bundle_path
                    )
                    message = _build_human_friendly_failure_message(
                        action_label,
                        technical_message,
                        stdout_text=stdout,
                        stderr_text=stderr,
                        returncode=result.returncode,
                    )
                else:
                    if combined:
                        message, technical_message = _build_human_friendly_success_message(
                            action_key,
                            action_label,
                            combined,
                        )
                    else:
                        message = None
                        technical_message = None
            else:
                result = subprocess.run(
                    cmd,
                    check=True,
                    text=True,
                    capture_output=True,
                    env=env,
                )
                stdout = result.stdout.strip()
                stderr = result.stderr.strip()
                combined = "\n".join(
                    [chunk for chunk in (stdout, stderr) if chunk]
                )
                if combined:
                    print(combined)
                    if len(combined) > 2000:
                        combined = combined[:2000].rstrip() + "\n... (truncated)"
                    message, technical_message = _build_human_friendly_success_message(
                        action_key,
                        action_label,
                        combined,
                    )
                else:
                    message = None
                    technical_message = None
        else:
            subprocess.run(cmd, check=True, env=env)
            message = None
            technical_message = None
    except subprocess.CalledProcessError as exc:
        is_error = True
        stdout = (exc.stdout or "").strip() if capture_output else ""
        stderr = (exc.stderr or "").strip() if capture_output else ""
        combined = "\n".join([chunk for chunk in (stdout, stderr) if chunk])
        traceback_text = traceback.format_exc()
        if combined:
            print(combined)
            if len(combined) > 2000:
                combined = combined[:2000].rstrip() + "\n... (truncated)"
            technical_message = f"Action '{action_label}' failed:\n{combined}"
        else:
            technical_message = f"Action '{action_label}' failed: {exc}"
        support_bundle_path = _create_failure_bundle(
            failure_kind="called_process_error",
            returncode=exc.returncode,
            stdout_text=stdout,
            stderr_text=stderr,
            traceback_text=traceback_text,
        )
        technical_message = _append_support_bundle_note(
            technical_message, support_bundle_path
        )
        message = _build_human_friendly_failure_message(
            action_label,
            technical_message,
            stdout_text=stdout,
            stderr_text=stderr,
            traceback_text=traceback_text,
            returncode=exc.returncode,
        )
        if action_key == "test_hpc_connection":
            save_hpc_test_result(
                {
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "success": False,
                    "output": stdout,
                    "error": stderr or str(exc),
                }
            )
    except Exception as exc:
        is_error = True
        technical_message = f"Action '{action_label}' failed: {exc}"
        support_bundle_path = _create_failure_bundle(
            failure_kind="exception",
            returncode=None,
            stdout_text="",
            stderr_text=str(exc),
            traceback_text=traceback.format_exc(),
        )
        technical_message = _append_support_bundle_note(
            technical_message, support_bundle_path
        )
        message = _build_human_friendly_failure_message(
            action_label,
            technical_message,
            stderr_text=str(exc),
            traceback_text=traceback.format_exc(),
        )
    if message:
        print(message)
    with ACTION_LOCK:
        ACTION_STATE["running"] = False
        ACTION_STATE["name"] = None
        ACTION_STATE["message"] = message
        ACTION_STATE["technical_message"] = technical_message
        ACTION_STATE["is_error"] = is_error
        ACTION_STATE["support_bundle"] = support_bundle_path
        ACTION_STATE["completed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _redirect_to_index(**kwargs):
    return redirect(url_for("index", **kwargs))


def _support_bundle_redirect_kwargs(form_data: object) -> Dict[str, object]:
    kwargs: Dict[str, object] = {
        "panel": request.form.get("panel", "action"),
        "db": request.form.get("db", DEFAULT_DB),
    }
    origin_id = request.form.get("origin_id", "").strip()
    if origin_id.isdigit():
        kwargs["origin_id"] = origin_id
    surrogate_tab = request.form.get("surrogate_tab", "").strip().lower()
    if surrogate_tab in {"models", "train"}:
        kwargs["surrogate_tab"] = surrogate_tab
    if request.form.get("equilibria_valid_only", "").strip().lower() in {"1", "true", "on", "yes"}:
        kwargs["equilibria_valid_only"] = "1"
    table_name = request.form.get("table", "").strip()
    if table_name:
        kwargs["table"] = table_name
    return kwargs


@app.route("/send_support_bundle", methods=["POST"])
def send_support_bundle():
    redirect_kwargs = _support_bundle_redirect_kwargs(request.form)
    bundle_path = _resolve_support_bundle_path(request.form.get("bundle_path", ""))
    if bundle_path is None:
        _set_support_bundle_feedback(
            "Support bundle could not be found anymore. Please rerun the failed action if needed."
        )
        return _redirect_to_index(**redirect_kwargs)
    compose_mode = request.form.get("compose_mode", "gmail").strip().lower()
    ok, error = _compose_support_bundle_mail(
        bundle_path,
        SUPPORT_EMAIL_RECIPIENT,
        compose_mode=compose_mode,
    )
    if not ok:
        _set_support_bundle_feedback(f"Could not open support-bundle email draft: {error}")
    return _redirect_to_index(**redirect_kwargs)


@app.route("/copy_demo_database", methods=["POST"])
def copy_demo_database():
    original_db = request.form.get("db", DEFAULT_DB)
    source_path = request.form.get("source_path", "")
    source = resolve_demo_database_candidate(source_path)
    if source is None:
        return _redirect_to_index(
            db=original_db,
            panel="tables",
            db_recovery_error=(
                "Detected demo database not found anymore. Re-scan and try again."
            ),
        )
    try:
        target = get_demo_copy_target_path(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        if source != target:
            shutil.copy2(source, target)
    except OSError as exc:
        return _redirect_to_index(
            db=original_db,
            panel="tables",
            db_recovery_error=f"Could not copy demo database: {exc}",
        )
    return _redirect_to_index(
        db=str(target),
        panel="tables",
        db_recovery_message=f"Using copied demo database: {target}",
    )


def _redirect_after_validation_error(
    db_path: str,
    default_panel: str,
    redirect_params: Optional[Dict[str, object]],
    exc: ActionValidationError,
):
    with ACTION_LOCK:
        ACTION_STATE["running"] = False
        ACTION_STATE["name"] = None
        ACTION_STATE["message"] = exc.message
        ACTION_STATE["technical_message"] = None
        ACTION_STATE["is_error"] = True
        ACTION_STATE["key"] = None
        ACTION_STATE["support_bundle"] = None
        ACTION_STATE["completed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    merged_redirect_params = dict(redirect_params or {})
    if exc.redirect_params:
        merged_redirect_params.update(exc.redirect_params)
    return _redirect_to_index(
        panel=exc.panel or default_panel,
        db=db_path,
        **merged_redirect_params,
    )


def _start_action(
    resolved: ResolvedAction,
):
    spec = resolved.spec
    db_path = resolved.db_path
    panel = resolved.panel
    with ACTION_LOCK:
        if ACTION_STATE["running"]:
            current = ACTION_STATE["name"] or "another action"
            ACTION_STATE["message"] = f"Action '{current}' is already running."
            ACTION_STATE["is_error"] = False
            return _redirect_to_index(panel=panel, db=db_path)
        ACTION_STATE["token"] = int(ACTION_STATE.get("token") or 0) + 1
        ACTION_STATE["running"] = True
        ACTION_STATE["name"] = spec.label
        ACTION_STATE["message"] = f"Action '{spec.label}' is running."
        ACTION_STATE["technical_message"] = None
        ACTION_STATE["is_error"] = False
        ACTION_STATE["key"] = spec.key
        ACTION_STATE["support_bundle"] = None
        ACTION_STATE["completed_at"] = None
    thread = threading.Thread(
        target=_run_action,
        args=(
            spec,
            db_path,
            resolved.extra_args or None,
            resolved.env_overrides,
            {
                "panel": panel,
                "redirect_params": dict(resolved.redirect_params or {}),
            },
        ),
        daemon=True,
    )
    thread.start()
    redirect_kwargs: Dict[str, object] = {"panel": panel, "db": db_path}
    if resolved.redirect_params:
        redirect_kwargs.update(resolved.redirect_params)
    return _redirect_to_index(**redirect_kwargs)


@app.route("/action/<action_name>", methods=["POST"])
def run_action(action_name: str):
    db_path = request.form.get("db", DEFAULT_DB)
    panel = request.form.get("panel", "action")
    if panel == "hpc":
        panel = "action"
    log_usage("action_click", {"action": action_name, "panel": panel, "db": db_path})
    try:
        resolved = resolve_action_request(
            action_name,
            request.form,
            default_db=DEFAULT_DB,
            load_hpc_config_fn=load_hpc_config,
            resolve_perlmutter_profile_fn=resolve_perlmutter_profile,
        )
    except ActionValidationError as exc:
        return _redirect_after_validation_error(db_path, panel, None, exc)
    return _start_action(resolved)


@app.route("/save_hpc_config", methods=["POST"])
def save_hpc_config_route():
    db_path = request.form.get("db", DEFAULT_DB)
    panel = (request.form.get("panel") or "action").strip() or "action"
    if panel == "hpc":
        panel = "action"
    hpc_tab = (request.form.get("hpc_tab") or "").strip().lower()
    payload = {
        "ssh_user": (request.form.get("ssh_user") or "").strip(),
        "ssh_host": (request.form.get("ssh_host") or "").strip(),
        "ssh_identity": (request.form.get("ssh_identity") or "").strip(),
        "ssh_control_path": (request.form.get("ssh_control_path") or "").strip(),
        "ssh_control_persist": (request.form.get("ssh_control_persist") or "").strip(),
        "ssh_connect_timeout": (request.form.get("ssh_connect_timeout") or "").strip(),
        "monitor_timeout": (request.form.get("monitor_timeout") or "").strip(),
        "perlmutter_base_dir": (request.form.get("perlmutter_base_dir") or "").strip(),
        "perlmutter_batch_dir": (request.form.get("perlmutter_batch_dir") or "").strip(),
        "gx_path": (request.form.get("gx_path") or "").strip(),
        "flux_user": (request.form.get("flux_user") or "").strip(),
        "flux_host": (request.form.get("flux_host") or "").strip(),
        "flux_base_dir": (request.form.get("flux_base_dir") or "").strip(),
        "flux_python_bin": (request.form.get("flux_python_bin") or "").strip(),
        "flux_duo_option": (request.form.get("flux_duo_option") or "").strip(),
    }
    save_hpc_config(payload)
    redirect_params = {"panel": panel, "db": db_path, "hpc": "1"}
    if hpc_tab in {"perlmutter", "flux"}:
        redirect_params["hpc_tab"] = hpc_tab
    return _redirect_to_index(**redirect_params)


@app.route("/suggestion_action", methods=["POST"])
def suggestion_action():
    db_path = request.form.get("db", DEFAULT_DB)
    action_name = request.form.get("action_name", "")
    suggestion_id = request.form.get("suggestion_id", "")
    panel = request.form.get("panel", "action")
    if panel == "hpc":
        panel = "action"
    redirect_params: Dict[str, object] = {}
    if (request.form.get("suggestions_open") or "").strip():
        redirect_params["suggestions"] = "1"
    if suggestion_id and action_name:
        record_ai_feedback(suggestion_id, action_name)
        log_usage(
            "suggestion_click",
            {"action": action_name, "panel": panel, "suggestion_id": suggestion_id},
        )
    try:
        resolved = resolve_action_request(
            action_name,
            request.form,
            default_db=DEFAULT_DB,
            load_hpc_config_fn=load_hpc_config,
            resolve_perlmutter_profile_fn=resolve_perlmutter_profile,
            base_redirect_params=redirect_params or None,
            panel_override=panel,
        )
    except ActionValidationError as exc:
        return _redirect_after_validation_error(db_path, panel, redirect_params, exc)
    return _start_action(with_redirect_params(resolved, redirect_params or None))


@app.route("/update_status", methods=["GET", "POST"])
def update_status():
    if request.method != "POST":
        return _redirect_to_index()
    db_path = request.form.get("db", DEFAULT_DB)
    table = request.form.get("table")
    row_id = request.form.get("row_id")
    panel = request.form.get("panel", "tables")
    if table != "gk_input" or not row_id or not row_id.isdigit():
        return _redirect_to_index(panel=panel, db=db_path, table=table)
    conn = get_connection(db_path)
    try:
        conn.execute(
            "UPDATE gk_input SET status = 'TORUN' WHERE id = ? AND status = 'WAIT'",
            (int(row_id),),
        )
        conn.commit()
    finally:
        conn.close()
    log_usage("status_update", {"table": table, "row_id": int(row_id), "panel": panel})
    return _redirect_to_index(panel=panel, db=db_path, table=table)


@app.route("/update_status_bulk", methods=["POST"])
def update_status_bulk():
    db_path = request.form.get("db", DEFAULT_DB)
    panel = request.form.get("panel", "tables")
    redirect_params: Dict[str, object] = {}
    origin_id = (request.form.get("origin_id") or "").strip()
    if origin_id.isdigit():
        redirect_params["origin_id"] = origin_id
    row_ids_raw = request.form.get("row_ids", "")
    row_ids: List[int] = []
    for part in row_ids_raw.split(","):
        part = part.strip()
        if part.isdigit():
            row_ids.append(int(part))
    if not row_ids:
        return _redirect_to_index(panel=panel, db=db_path, **redirect_params)
    placeholders = ",".join(["?"] * len(row_ids))
    conn = get_connection(db_path)
    try:
        conn.execute(
            f"UPDATE gk_input SET status = 'TORUN' WHERE status = 'WAIT' AND id IN ({placeholders})",
            row_ids,
        )
        conn.commit()
    finally:
        conn.close()
    log_usage(
        "status_update_bulk",
        {"table": "gk_input", "count": len(row_ids), "panel": panel},
    )
    return _redirect_to_index(panel=panel, db=db_path, **redirect_params)


@app.route("/usage", methods=["POST"])
def usage_event():
    payload = request.get_json(silent=True) or {}
    event = str(payload.get("event") or "").strip()
    details = payload.get("details")
    if isinstance(details, dict):
        log_usage(event, details)
    else:
        log_usage(event, {})
    return jsonify({"ok": True})


@app.route("/edit_gk_input", methods=["POST"])
def edit_gk_input():
    db_path = request.form.get("db", DEFAULT_DB)
    gk_input_id = request.form.get("gk_input_id", "").strip()
    action = request.form.get("action", "load")
    panel = request.form.get("panel", "tables")
    log_usage(
        "edit_gk_input",
        {"action": action, "gk_input_id": gk_input_id, "db": db_path},
    )
    if not gk_input_id.isdigit():
        return _redirect_to_index(
            panel=panel,
            db=db_path,
            gk_input_id=gk_input_id,
            edit_error="Enter a numeric gk_input id.",
        )
    if action == "save":
        content = request.form.get("gk_input_content", "")
        conn = get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM gk_input WHERE id = ?",
                (int(gk_input_id),),
            ).fetchone()
            if row is None:
                return _redirect_to_index(
                    panel=panel,
                    db=db_path,
                    gk_input_id=gk_input_id,
                    edit_error="No gk_input row found for that id.",
                )
            if str(row["status"]) != "WAIT":
                return _redirect_to_index(
                    panel=panel,
                    db=db_path,
                    gk_input_id=gk_input_id,
                    edit_error="Edits allowed only when status is WAIT.",
                    edit_status=str(row["status"]),
                )
            allowed_keys = set(ALLOWED_STATS_COLUMNS + SPECIES_COLUMNS + ["psin"])
            parsed = parse_numeric_fields(content)
            species_updates = parse_species_fields(content)
            updates: Dict[str, float] = {}
            for key, value in {**parsed, **species_updates}.items():
                if key not in allowed_keys:
                    continue
                if key not in row.keys():
                    continue
                current = row[key]
                if current is None or abs(float(current) - value) > 1e-12:
                    updates[key] = value
            if updates:
                set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
                params = [content, *updates.values(), int(gk_input_id)]
                conn.execute(
                    f"UPDATE gk_input SET content = ?, {set_clause} WHERE id = ?",
                    params,
                )
            else:
                conn.execute(
                    "UPDATE gk_input SET content = ? WHERE id = ?",
                    (content, int(gk_input_id)),
                )
            conn.commit()
        finally:
            conn.close()
        warning = None
        if updates:
            keys = ", ".join(sorted(updates.keys()))
            warning = f"Updated columns based on content: {keys}"
        return _redirect_to_index(
            panel=panel,
            db=db_path,
            gk_input_id=gk_input_id,
            edit_message="Saved.",
            edit_warning=warning,
        )
    return _redirect_to_index(
        panel=panel,
        db=db_path,
        gk_input_id=gk_input_id,
    )


@app.route("/app2", methods=["GET"])
def legacy_app2_redirect():
    target = url_for("index")
    if request.query_string:
        query = request.query_string.decode("utf-8", errors="ignore")
        target = f"{target}?{query}"
    return redirect(target)


@app.route("/", methods=["GET"])
def index():
    db_path = request.args.get("db", DEFAULT_DB)
    selected_table = request.args.get("table")
    panel_param = request.args.get("panel", "results")
    db_recovery_message = request.args.get("db_recovery_message")
    db_recovery_error = request.args.get("db_recovery_error")
    selected_panel = panel_param
    if selected_panel in {"hpc", "overview"}:
        selected_panel = "results"
    log_usage(
        "page_view",
        {
            "panel": selected_panel,
            "table": selected_table,
            "db": db_path,
            "query": request.query_string.decode("utf-8", errors="ignore"),
        },
    )
    only_active = request.args.get("only_active") == "1"
    table_origin_raw = request.args.get("table_origin_id")
    table_origin_id = (
        int(table_origin_raw) if table_origin_raw and table_origin_raw.isdigit() else None
    )
    table_transpfile_regex = request.args.get("table_transpfile_regex", "").strip()
    table_counts_requested = request.args.get("table_counts") in {"1", "true", "on", "yes"}
    table_limit_raw = request.args.get("table_limit", "100")
    if table_limit_raw.isdigit():
        table_limit = int(table_limit_raw)
    else:
        table_limit = 100
    if table_limit not in {10, 20, 50, 100, 200, 500}:
        table_limit = 100
    results_filter = (request.args.get("results_filter") or "").strip().lower()
    if not results_filter:
        finished_flags = request.args.getlist("results_only_finished")
        if finished_flags:
            results_filter = "finished" if "1" in finished_flags else "all"
        else:
            results_filter = "all"
    if results_filter not in {"all", "finished", "growth"}:
        results_filter = "all"
    selected_batch_db = request.args.get("batch_db")
    batch_view = request.args.get("batch_view", "new")
    edit_gk_input_id = request.args.get("gk_input_id")
    edit_message = request.args.get("edit_message")
    edit_error = request.args.get("edit_error")
    edit_warning = request.args.get("edit_warning")
    edit_status = request.args.get("edit_status")
    sampling_origin_raw = request.args.get("sampling_origin_id")
    sampling_origin_id = (
        int(sampling_origin_raw)
        if sampling_origin_raw and sampling_origin_raw.isdigit()
        else None
    )
    plasma_origin_raw = request.args.get("plasma_origin_id")
    plasma_origin_id = (
        int(plasma_origin_raw)
        if plasma_origin_raw and plasma_origin_raw.isdigit()
        else None
    )
    sampling_report_file = request.args.get("sampling_report_file", "")
    sampling_tab = (request.args.get("sampling_tab") or "equil").strip().lower()
    surrogate_tab = (request.args.get("surrogate_tab") or "models").strip().lower()
    if panel_param == "equil-plasma-sampling":
        selected_panel = "equilibria"
    elif panel_param == "monitor":
        selected_panel = "equilibria"
    elif panel_param == "action":
        selected_panel = "equilibria"
    elif panel_param == "schema":
        selected_panel = "tables"
    elif panel_param == "plasma-sampling":
        sampling_tab = "plasma"
        selected_panel = "sampling"
    elif panel_param == "sampling-batch":
        sampling_tab = "batch"
        selected_panel = "sampling"
    if sampling_tab not in {"equil", "plasma", "batch"}:
        sampling_tab = "equil"
    if surrogate_tab not in {"models", "train"}:
        surrogate_tab = "models"
    sampling_batch_origin_raw = request.args.get("sampling_batch_origin_id")
    sampling_batch_origin_id = (
        int(sampling_batch_origin_raw)
        if sampling_batch_origin_raw and sampling_batch_origin_raw.isdigit()
        else None
    )
    equilibria_valid_only = request.args.get("equilibria_valid_only") in {
        "1",
        "true",
        "on",
        "yes",
    }
    sampling_k_raw = request.args.get("sampling_k", "6")
    sampling_k = int(sampling_k_raw) if sampling_k_raw.isdigit() else 6
    plasma_k_raw = request.args.get("plasma_k", "6")
    plasma_k = int(plasma_k_raw) if plasma_k_raw.isdigit() else 6
    eqp_method = request.args.get("eqp_method", "farthest").strip().lower()
    if eqp_method not in {"farthest", "kmeans"}:
        eqp_method = "farthest"
    eqp_analyze = request.args.get("eqp_analyze") in {"1", "true", "on", "yes"}
    sampling_target_raw = request.args.get("sampling_target", "50")
    sampling_target = (
        int(sampling_target_raw) if sampling_target_raw.isdigit() else 50
    )
    plasma_target_raw = request.args.get("plasma_target", "50")
    plasma_target = int(plasma_target_raw) if plasma_target_raw.isdigit() else 50
    eqp_target_raw = request.args.get("eqp_target", "50")
    eqp_target = int(eqp_target_raw) if eqp_target_raw.isdigit() else 50
    eqp_coverage_enabled = request.args.get("eqp_coverage") in {"1", "true", "on", "yes"}
    sampling_max_raw = request.args.get("sampling_max", "1500")
    sampling_max = int(sampling_max_raw) if sampling_max_raw.isdigit() else 1500
    plasma_max_raw = request.args.get("plasma_max", "1500")
    plasma_max = int(plasma_max_raw) if plasma_max_raw.isdigit() else 1500
    eqp_max_raw = request.args.get("eqp_max", "1500")
    eqp_max = int(eqp_max_raw) if eqp_max_raw.isdigit() else 1500
    if sampling_max < 200:
        sampling_max = 200
    if sampling_max > 20000:
        sampling_max = 20000
    if plasma_max < 200:
        plasma_max = 200
    if plasma_max > 20000:
        plasma_max = 20000
    if eqp_max < 200:
        eqp_max = 200
    if eqp_coverage_enabled and eqp_max > 20000:
        eqp_max = 20000
    origin_id_raw = request.args.get("origin_id")
    origin_id = int(origin_id_raw) if origin_id_raw and origin_id_raw.isdigit() else None
    x_col = request.args.get("x_col", "rhoc")
    y_col = request.args.get("y_col", "ion_vnewk")
    x2_col = request.args.get("x2_col", "rhoc")
    y2_col = request.args.get("y2_col", "ion_vnewk")
    x3_col = request.args.get("x3_col", "rhoc")
    y3_col = request.args.get("y3_col", "ion_vnewk")
    x4_col = request.args.get("x4_col", "rhoc")
    y4_col = request.args.get("y4_col", "ion_vnewk")
    results_x_col = request.args.get("results_x_col")
    results_x2_col = request.args.get("results_x2_col")
    results_x3_col = request.args.get("results_x3_col")
    results_x4_col = request.args.get("results_x4_col")
    results_y_col = request.args.get("results_y_col", "gk_input.ion_tprim")
    results_y2_col = request.args.get("results_y2_col", "gk_run.gamma_max")
    results_y3_col = request.args.get("results_y3_col", "gk_run.ky_abs_mean")
    results_y4_col = request.args.get("results_y4_col", "gk_run.diffusion")
    results_plot_run_raw = request.args.get("results_plot_run")
    results_plot_run_id = (
        int(results_plot_run_raw)
        if results_plot_run_raw and results_plot_run_raw.isdigit()
        else None
    )
    def _parse_limit(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    results_x_min = _parse_limit(request.args.get("results_x_min"))
    results_x_max = _parse_limit(request.args.get("results_x_max"))
    results_y_min = _parse_limit(request.args.get("results_y_min"))
    results_y_max = _parse_limit(request.args.get("results_y_max"))
    eqp_ion_tprim_min = _parse_limit(request.args.get("eqp_ion_tprim_min"))
    if eqp_ion_tprim_min is None:
        eqp_ion_tprim_min = 0.1
    results_x_scale = (request.args.get("results_x_scale") or "linear").strip().lower()
    results_y_scale = (request.args.get("results_y_scale") or "linear").strip().lower()
    results_x2_min = _parse_limit(request.args.get("results_x2_min"))
    results_x2_max = _parse_limit(request.args.get("results_x2_max"))
    results_y2_min = _parse_limit(request.args.get("results_y2_min"))
    results_y2_max = _parse_limit(request.args.get("results_y2_max"))
    results_x2_scale = (request.args.get("results_x2_scale") or "linear").strip().lower()
    results_y2_scale = (request.args.get("results_y2_scale") or "linear").strip().lower()
    results_x3_min = _parse_limit(request.args.get("results_x3_min"))
    results_x3_max = _parse_limit(request.args.get("results_x3_max"))
    results_y3_min = _parse_limit(request.args.get("results_y3_min"))
    results_y3_max = _parse_limit(request.args.get("results_y3_max"))
    results_x3_scale = (request.args.get("results_x3_scale") or "linear").strip().lower()
    results_y3_scale = (request.args.get("results_y3_scale") or "linear").strip().lower()
    results_x4_min = _parse_limit(request.args.get("results_x4_min"))
    results_x4_max = _parse_limit(request.args.get("results_x4_max"))
    results_y4_min = _parse_limit(request.args.get("results_y4_min"))
    results_y4_max = _parse_limit(request.args.get("results_y4_max"))
    results_x4_scale = (request.args.get("results_x4_scale") or "linear").strip().lower()
    results_y4_scale = (request.args.get("results_y4_scale") or "linear").strip().lower()
    valid_scales = {"linear", "log", "symlog"}
    if results_x_scale not in valid_scales:
        results_x_scale = "linear"
    if results_y_scale not in valid_scales:
        results_y_scale = "linear"
    if results_x2_scale not in valid_scales:
        results_x2_scale = "linear"
    if results_y2_scale not in valid_scales:
        results_y2_scale = "linear"
    if results_x3_scale not in valid_scales:
        results_x3_scale = "linear"
    if results_y3_scale not in valid_scales:
        results_y3_scale = "linear"
    if results_x4_scale not in valid_scales:
        results_x4_scale = "linear"
    if results_y4_scale not in valid_scales:
        results_y4_scale = "linear"
    if x_col not in ALLOWED_STATS_COLUMNS:
        x_col = "qinp"
    if y_col not in ALLOWED_STATS_COLUMNS:
        y_col = "ion_vnewk"
    if x2_col not in ALLOWED_STATS_COLUMNS:
        x2_col = "shat"
    if y2_col not in ALLOWED_STATS_COLUMNS:
        y2_col = "ion_vnewk"
    if x3_col not in ALLOWED_STATS_COLUMNS:
        x3_col = "akappa"
    if y3_col not in ALLOWED_STATS_COLUMNS:
        y3_col = "ion_vnewk"
    if x4_col not in ALLOWED_STATS_COLUMNS:
        x4_col = "betaprim"
    if y4_col not in ALLOWED_STATS_COLUMNS:
        y4_col = "ion_vnewk"
    results_columns: List[Dict[str, str]] = []
    tables: List[str] = []
    columns: List[str] = []
    rows: List[sqlite3.Row] = []
    batch_columns: List[str] = []
    batch_rows: List[sqlite3.Row] = []
    gk_model_columns: List[str] = []
    gk_model_rows: List[sqlite3.Row] = []
    batch_dir = BATCH_NEW_DIR if batch_view == "new" else BATCH_SENT_DIR
    batch_dbs = list_batch_databases(batch_dir)
    batch_error: Optional[str] = None
    edit_gk_input_content = ""
    stats_points: List[Tuple[float, float]] = []
    stats_points_2: List[Tuple[float, float]] = []
    stats_points_3: List[Tuple[float, float]] = []
    stats_points_4: List[Tuple[float, float]] = []
    results_points: List[Tuple[float, float]] = []
    results_points_2: List[Tuple[float, float]] = []
    results_points_3: List[Tuple[float, float]] = []
    results_points_4: List[Tuple[float, float]] = []
    results_warn: bool = False
    results_warn_2: bool = False
    results_warn_3: bool = False
    results_warn_4: bool = False
    results_report: Optional[str] = None
    results_plot_options: List[Dict[str, object]] = []
    results_plot_selected: Optional[Dict[str, Optional[str]]] = None
    results_highlight: Optional[Dict[str, float]] = None
    results_highlight_2: Optional[Dict[str, float]] = None
    results_highlight_3: Optional[Dict[str, float]] = None
    results_highlight_4: Optional[Dict[str, float]] = None
    data_origins: List[Tuple[int, str, Optional[str]]] = []
    sampling_report: Optional[Dict[str, object]] = None
    sampling_coverage: Optional[Dict[str, object]] = None
    sampling_regimes: Optional[Dict[str, object]] = None
    sampling_cluster: Optional[Dict[str, object]] = None
    sampling_pca: Optional[Dict[str, object]] = None
    sampling_selection: Optional[Dict[str, object]] = None
    sampling_bin_labels = ["low", "mid", "high"]
    plasma_report: Optional[Dict[str, object]] = None
    plasma_coverage: Optional[Dict[str, object]] = None
    plasma_regimes: Optional[Dict[str, object]] = None
    plasma_cluster: Optional[Dict[str, object]] = None
    plasma_pca: Optional[Dict[str, object]] = None
    plasma_selection: Optional[Dict[str, object]] = None
    plasma_bin_labels = ["low", "mid", "high"]
    plasma_columns: List[str] = []
    plasma_missing_columns: List[str] = []
    eqp_report: Optional[Dict[str, object]] = None
    eqp_coverage: Optional[Dict[str, object]] = None
    eqp_selection: Optional[Dict[str, object]] = None
    eqp_status_counts: Optional[Dict[str, int]] = None
    sampling_reports = list_sampling_reports(ANALYSIS_DIR)
    sampling_batch_report: Optional[Dict[str, object]] = None
    sampling_batch_error: Optional[str] = None
    sampling_batch_results: List[Dict[str, object]] = []
    sampling_batch_detail: Optional[Dict[str, object]] = None
    sampling_batch_detail_error: Optional[str] = None
    sampling_batch_columns = MHD_COLUMNS
    monitor_report = load_monitor_report(MONITOR_REPORT_PATH)
    hpc_config = load_hpc_config()
    hpc_test_result = load_hpc_test_result()
    action_state = get_action_state()
    surrogate_models: List[Dict[str, object]] = []
    surrogate_model_selected = (request.args.get("surrogate_model") or "").strip()
    surrogate_id_raw = (request.args.get("surrogate_id") or "").strip()
    surrogate_id = int(surrogate_id_raw) if surrogate_id_raw.isdigit() else None
    surrogate_estimate_summary: Optional[Dict[str, object]] = None
    origin_details: List[Dict[str, object]] = []
    selected_origin_details: Optional[Dict[str, object]] = None
    equilibria_summary = {
        "equilibria_total": 0,
        "equilibria_active": 0,
        "gk_input_total": 0,
        "transp_timeseries_total": 0,
    }
    equilibria_preview_columns: List[str] = []
    equilibria_preview_rows: List[sqlite3.Row] = []
    equilibria_preview_total = 0
    equilibria_actions: List[Dict[str, str]] = []
    equilibria_action_notes: List[str] = []
    flux_action_state: Optional[Dict[str, str]] = None
    equilibria_workflow_status: Dict[str, object] = {"stages": [], "notes": []}
    equilibria_ai_advisor: Dict[str, object] = {"available": False}
    equilibria_monitor_report: Optional[Dict[str, object]] = monitor_report
    results_columns = build_results_columns(surrogate_models=surrogate_models)
    table_total_count: Optional[int] = None
    table_filtered_count: Optional[int] = None
    table_schema_rows: List[Dict[str, str]] = []
    table_counts_loaded = False
    ai_suggestions: List[Dict[str, object]] = []
    demo_database_candidates: List[Dict[str, str]] = []
    data_origin_colors: Dict[str, str] = {}
    compute_tables_panel = selected_panel == "tables"
    compute_sampling_panel = selected_panel == "sampling"
    compute_surrogate_panel = selected_panel == "surrogate"
    compute_results_panel = selected_panel == "results"
    compute_workflow_panel = selected_panel == "equilibria"

    def build_index_context(**overrides: object) -> Dict[str, object]:
        context: Dict[str, object] = {
            "db_path": db_path,
            "demo_database_url": DEMO_DATABASE_URL,
            "db_recovery_message": db_recovery_message,
            "db_recovery_error": db_recovery_error,
            "demo_database_candidates": demo_database_candidates,
            "tables": tables,
            "selected_table": selected_table,
            "columns": columns,
            "rows": rows,
            "batch_dbs": batch_dbs,
            "selected_batch_db": selected_batch_db,
            "batch_view": batch_view,
            "batch_columns": batch_columns,
            "batch_rows": batch_rows,
            "batch_error": batch_error,
            "gk_model_columns": gk_model_columns,
            "gk_model_rows": gk_model_rows,
            "edit_gk_input_id": edit_gk_input_id,
            "edit_gk_input_content": edit_gk_input_content,
            "edit_status": edit_status,
            "edit_message": edit_message,
            "edit_error": edit_error,
            "edit_warning": edit_warning,
            "selected_panel": selected_panel,
            "only_active": only_active,
            "table_limit": table_limit,
            "table_total_count": table_total_count,
            "table_filtered_count": table_filtered_count,
            "table_schema_rows": table_schema_rows,
            "table_counts_requested": table_counts_requested,
            "table_counts_loaded": table_counts_loaded,
            "table_origin_id": table_origin_id,
            "table_transpfile_regex": table_transpfile_regex,
            "stats_points": stats_points,
            "stats_columns": ALLOWED_STATS_COLUMNS,
            "stats_x_col": x_col,
            "stats_y_col": y_col,
            "stats_points_2": stats_points_2,
            "stats_x2_col": x2_col,
            "stats_y2_col": y2_col,
            "stats_points_3": stats_points_3,
            "stats_x3_col": x3_col,
            "stats_y3_col": y3_col,
            "stats_points_4": stats_points_4,
            "stats_x4_col": x4_col,
            "stats_y4_col": y4_col,
            "results_points": results_points,
            "results_points_2": results_points_2,
            "results_points_3": results_points_3,
            "results_points_4": results_points_4,
            "results_warn": results_warn,
            "results_warn_2": results_warn_2,
            "results_warn_3": results_warn_3,
            "results_warn_4": results_warn_4,
            "results_filter": results_filter,
            "results_y_col": results_y_col,
            "results_y2_col": results_y2_col,
            "results_y3_col": results_y3_col,
            "results_y4_col": results_y4_col,
            "results_x_col": results_x_col,
            "results_x2_col": results_x2_col,
            "results_x3_col": results_x3_col,
            "results_x4_col": results_x4_col,
            "results_plot_options": results_plot_options,
            "results_plot_selected": results_plot_selected,
            "results_highlight": results_highlight,
            "results_highlight_2": results_highlight_2,
            "results_highlight_3": results_highlight_3,
            "results_highlight_4": results_highlight_4,
            "results_x_min": results_x_min,
            "results_x_max": results_x_max,
            "results_y_min": results_y_min,
            "results_y_max": results_y_max,
            "results_x_scale": results_x_scale,
            "results_y_scale": results_y_scale,
            "results_x2_min": results_x2_min,
            "results_x2_max": results_x2_max,
            "results_y2_min": results_y2_min,
            "results_y2_max": results_y2_max,
            "results_x2_scale": results_x2_scale,
            "results_y2_scale": results_y2_scale,
            "results_x3_min": results_x3_min,
            "results_x3_max": results_x3_max,
            "results_y3_min": results_y3_min,
            "results_y3_max": results_y3_max,
            "results_x3_scale": results_x3_scale,
            "results_y3_scale": results_y3_scale,
            "results_x4_min": results_x4_min,
            "results_x4_max": results_x4_max,
            "results_y4_min": results_y4_min,
            "results_y4_max": results_y4_max,
            "results_x4_scale": results_x4_scale,
            "results_y4_scale": results_y4_scale,
            "results_columns": results_columns,
            "results_report": results_report,
            "data_origins": data_origins,
            "data_origin_colors": data_origin_colors,
            "selected_origin_id": origin_id,
            "sampling_origin_id": sampling_origin_id,
            "sampling_tab": sampling_tab,
            "sampling_report": sampling_report,
            "sampling_coverage": sampling_coverage,
            "sampling_regimes": sampling_regimes,
            "sampling_cluster": sampling_cluster,
            "sampling_pca": sampling_pca,
            "sampling_selection": sampling_selection,
            "sampling_bin_labels": sampling_bin_labels,
            "sampling_columns": MHD_COLUMNS,
            "sampling_k": sampling_k,
            "sampling_target": sampling_target,
            "sampling_max": sampling_max,
            "sampling_reports": sampling_reports,
            "sampling_report_file": sampling_report_file,
            "sampling_batch_origin_id": sampling_batch_origin_id,
            "sampling_batch_report": sampling_batch_report,
            "sampling_batch_results": sampling_batch_results,
            "sampling_batch_detail": sampling_batch_detail,
            "sampling_batch_detail_error": sampling_batch_detail_error,
            "sampling_batch_columns": sampling_batch_columns,
            "sampling_batch_error": sampling_batch_error,
            "monitor_report": monitor_report,
            "equilibria_monitor_report": equilibria_monitor_report,
            "hpc_config": hpc_config,
            "hpc_test_result": hpc_test_result,
            "surrogate_models": surrogate_models,
            "surrogate_model_selected": surrogate_model_selected,
            "surrogate_tab": surrogate_tab,
            "plasma_origin_id": plasma_origin_id,
            "plasma_report": plasma_report,
            "plasma_coverage": plasma_coverage,
            "plasma_regimes": plasma_regimes,
            "plasma_cluster": plasma_cluster,
            "plasma_pca": plasma_pca,
            "plasma_selection": plasma_selection,
            "plasma_bin_labels": plasma_bin_labels,
            "plasma_columns": plasma_columns,
            "plasma_missing_columns": plasma_missing_columns,
            "plasma_k": plasma_k,
            "plasma_target": plasma_target,
            "plasma_max": plasma_max,
            "eqp_method": eqp_method,
            "eqp_analyze": eqp_analyze,
            "eqp_target": eqp_target,
            "eqp_max": eqp_max,
            "eqp_coverage_enabled": eqp_coverage_enabled,
            "eqp_ion_tprim_min": eqp_ion_tprim_min,
            "eqp_columns": EQUIL_PLASMA_COLUMNS,
            "eqp_report": eqp_report,
            "eqp_coverage": eqp_coverage,
            "eqp_selection": eqp_selection,
            "eqp_status_counts": eqp_status_counts,
            "actions": ACTIONS,
            "action_status": action_state,
            "ai_suggestions": ai_suggestions,
            "have_numpy": HAVE_NUMPY,
            "error": None,
            "surrogate_id": surrogate_id,
            "surrogate_estimate_summary": surrogate_estimate_summary,
            "origin_details": origin_details,
            "selected_origin_details": selected_origin_details,
            "equilibria_summary": equilibria_summary,
            "equilibria_preview_columns": equilibria_preview_columns,
            "equilibria_preview_rows": equilibria_preview_rows,
            "equilibria_preview_total": equilibria_preview_total,
            "equilibria_valid_only": equilibria_valid_only,
            "equilibria_actions": equilibria_actions,
            "equilibria_action_notes": equilibria_action_notes,
            "flux_action_state": flux_action_state,
            "equilibria_workflow_status": equilibria_workflow_status,
            "equilibria_ai_advisor": equilibria_ai_advisor,
        }
        context.update(overrides)
        return context

    def render_missing_database(error_message: str):
        missing_db_candidates = find_demo_database_candidates()
        return render_template(
            "index.html",
            **build_index_context(
                selected_panel="tables",
                demo_database_candidates=missing_db_candidates,
                tables=[],
                selected_table=None,
                columns=[],
                rows=[],
                sampling_batch_error="Database not found.",
                plasma_columns=PLASMA_COLUMNS,
                plasma_missing_columns=PLASMA_COLUMNS,
                error=error_message,
            ),
        )

    if not os.path.isfile(db_path):
        return render_missing_database(f"Database not found: {db_path}")

    try:
        conn = get_connection(db_path)
    except sqlite3.OperationalError:
        return render_missing_database(f"Database not found or unreadable: {db_path}")
    try:
        tables = list_tables(conn)
        try:
            ensure_gk_surrogate_table(conn)
            tables = list_tables(conn)
        except sqlite3.Error:
            pass
        ai_feedback = load_ai_feedback()
        ai_suggestions = get_ai_suggestions(conn, ai_feedback)
        table_total_count = None
        table_filtered_count = None
        data_origin_colors: Dict[str, str] = {}
        if "data_origin" in tables:
            data_origins = get_data_origins(conn)
            data_origin_colors = {
                origin_name: data_origin_color(origin_name, origin_color)
                for _, origin_name, origin_color in data_origins
            }
            if origin_id is None and data_origins:
                origin_id = data_origins[0][0]
            if sampling_origin_id is None:
                sampling_origin_id = origin_id
            if plasma_origin_id is None:
                plasma_origin_id = origin_id
        if compute_surrogate_panel or compute_results_panel:
            surrogate_models = list_surrogate_models_db(conn) if "gk_surrogate" in tables else []
        if compute_surrogate_panel and surrogate_models:
            model_ids: List[int] = []
            for model in surrogate_models:
                try:
                    model_ids.append(int(model.get("id")))
                except (TypeError, ValueError):
                    continue
            if surrogate_id is None or (model_ids and surrogate_id not in model_ids):
                surrogate_id = model_ids[0] if model_ids else None
        if compute_surrogate_panel and surrogate_id is not None and "sg_estimate" in tables:
            surrogate_estimate_summary = get_sg_estimate_summary(conn, surrogate_id)
        if compute_results_panel and "gk_run" in tables:
            gk_run_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
            }
            results_columns = build_results_columns(gk_run_columns, surrogate_models)
            if not results_columns:
                results_columns = build_results_columns(surrogate_models=surrogate_models)
        elif compute_results_panel:
            results_columns = build_results_columns(surrogate_models=surrogate_models)
        if selected_table not in tables:
            selected_table = tables[0] if tables else None
        if compute_tables_panel and selected_table:
            table_schema_rows = get_table_schema_rows(conn, selected_table)
            if table_transpfile_regex:
                def _regexp(expr, item):
                    try:
                        return 1 if re.search(expr, item or "") else 0
                    except re.error:
                        return 0

                conn.create_function("REGEXP", 2, _regexp)
            include_table_counts = _should_compute_table_counts(
                selected_table,
                table_counts_requested,
            )
            columns, rows, table_total_count, table_filtered_count = get_table_rows(
                conn,
                selected_table,
                only_active,
                table_limit,
                table_origin_id,
                table_transpfile_regex if table_transpfile_regex else None,
                include_counts=include_table_counts,
            )
            table_counts_loaded = table_total_count is not None
        if compute_sampling_panel and "gk_input" in tables:
            gk_input_columns = get_table_columns(conn, "gk_input")
            stats_points = get_gk_input_points(conn, x_col, y_col, origin_id)
            stats_points_2 = get_gk_input_points(conn, x2_col, y2_col, origin_id)
            stats_points_3 = get_gk_input_points(conn, x3_col, y3_col, origin_id)
            stats_points_4 = get_gk_input_points(conn, x4_col, y4_col, origin_id)
            dataset, total_rows = get_sampling_dataset(
                conn, sampling_origin_id, MHD_COLUMNS
            )
            sampling_report = build_sampling_report(dataset, total_rows, MHD_COLUMNS)
            sampling_coverage = build_sampling_coverage(
                dataset, MHD_COLUMNS, sampling_max
            )
            sampling_regimes = build_sampling_regimes(
                dataset, MHD_COLUMNS, params=MHD_REGIME_DEFAULTS
            )
            sampling_cluster = build_sampling_clustering(
                dataset, MHD_COLUMNS, sampling_k, sampling_max
            )
            sampling_pca = build_sampling_pca(dataset, MHD_COLUMNS, max_points=sampling_max)
            sampling_selection = build_sampling_selection(
                dataset, MHD_COLUMNS, sampling_target, sampling_max, id_column=MHD_ID_COLUMN
            )
            plasma_columns = [col for col in PLASMA_COLUMNS if col in gk_input_columns]
            plasma_missing_columns = [
                col for col in PLASMA_COLUMNS if col not in gk_input_columns
            ]
            if plasma_columns:
                dataset, total_rows = get_sampling_dataset(
                    conn, plasma_origin_id, plasma_columns
                )
                plasma_report = build_sampling_report(
                    dataset, total_rows, plasma_columns
                )
                plasma_coverage = build_sampling_coverage(
                    dataset, plasma_columns, plasma_max
                )
                plasma_regimes = build_sampling_regimes(
                    dataset, plasma_columns, params=PLASMA_REGIME_DEFAULTS
                )
                plasma_cluster = build_sampling_clustering(
                    dataset, plasma_columns, plasma_k, plasma_max
                )
                plasma_pca = build_sampling_pca(
                    dataset, plasma_columns, max_points=plasma_max
                )
                plasma_selection = build_sampling_selection(
                    dataset,
                    plasma_columns,
                    plasma_target,
                    plasma_max,
                    id_column=MHD_ID_COLUMN,
                )
        if compute_workflow_panel:
            workflow_panel_context = build_workflow_panel_context(
                conn=conn,
                tables=tables,
                selected_panel=selected_panel,
                origin_id=origin_id,
                sampling_origin_id=sampling_origin_id,
                plasma_origin_id=plasma_origin_id,
                equilibria_valid_only=equilibria_valid_only,
                monitor_report=monitor_report,
                eqp_analyze=eqp_analyze,
                eqp_ion_tprim_min=eqp_ion_tprim_min,
                eqp_max=eqp_max,
                eqp_coverage_enabled=eqp_coverage_enabled,
                eqp_target=eqp_target,
                eqp_method=eqp_method,
                data_origin_color_fn=data_origin_color,
                get_data_origins_fn=get_data_origins,
                get_data_origin_details_fn=get_data_origin_details,
                get_equilibria_origin_summary_fn=get_equilibria_origin_summary,
                get_equilibria_preview_fn=get_equilibria_preview,
                get_latest_flux_action_state_fn=get_latest_flux_action_state,
                get_equilibria_origin_workflow_status_fn=get_equilibria_origin_workflow_status,
                get_equilibria_origin_actions_fn=get_equilibria_origin_actions,
                filter_monitor_report_for_origin_fn=filter_monitor_report_for_origin,
                get_equil_plasma_dataset_fn=get_equil_plasma_dataset,
                get_equil_plasma_status_counts_fn=get_equil_plasma_status_counts,
                build_sampling_report_fn=build_sampling_report,
                build_sampling_coverage_fn=build_sampling_coverage,
                build_sampling_selection_fn=build_sampling_selection,
                build_kmeans_selection_fn=build_kmeans_selection,
                equil_plasma_columns=EQUIL_PLASMA_COLUMNS,
                mhd_id_column=MHD_ID_COLUMN,
            )
            origin_id = workflow_panel_context["origin_id"]
            sampling_origin_id = workflow_panel_context["sampling_origin_id"]
            plasma_origin_id = workflow_panel_context["plasma_origin_id"]
            data_origins = workflow_panel_context["data_origins"]
            origin_details = workflow_panel_context["origin_details"]
            selected_origin_details = workflow_panel_context["selected_origin_details"]
            equilibria_summary = workflow_panel_context["equilibria_summary"]
            equilibria_preview_columns = workflow_panel_context["equilibria_preview_columns"]
            equilibria_preview_rows = workflow_panel_context["equilibria_preview_rows"]
            equilibria_preview_total = workflow_panel_context["equilibria_preview_total"]
            equilibria_actions = workflow_panel_context["equilibria_actions"]
            equilibria_action_notes = workflow_panel_context["equilibria_action_notes"]
            flux_action_state = workflow_panel_context["flux_action_state"]
            equilibria_workflow_status = workflow_panel_context["equilibria_workflow_status"]
            equilibria_ai_advisor = workflow_panel_context["equilibria_ai_advisor"]
            equilibria_monitor_report = workflow_panel_context["equilibria_monitor_report"]
            data_origin_colors = workflow_panel_context["data_origin_colors"]
            eqp_report = workflow_panel_context["eqp_report"]
            eqp_coverage = workflow_panel_context["eqp_coverage"]
            eqp_selection = workflow_panel_context["eqp_selection"]
            eqp_status_counts = workflow_panel_context["eqp_status_counts"]
        if compute_sampling_panel and sampling_tab == "batch":
            if sampling_reports:
                if sampling_report_file not in sampling_reports:
                    sampling_report_file = sampling_reports[-1]
                report_path = os.path.join(ANALYSIS_DIR, sampling_report_file)
                sampling_batch_report = load_sampling_report(report_path)
                if sampling_batch_report is None:
                    sampling_batch_error = f"Failed to read {sampling_report_file}"
                else:
                    report_columns = sampling_batch_report.get("mhd_columns")
                    if isinstance(report_columns, list) and report_columns:
                        sampling_batch_columns = [str(col) for col in report_columns]
                    results = sampling_batch_report.get("results", [])
                    if isinstance(results, list):
                        sampling_batch_results = results
                        origin_ids: List[int] = []
                        for item in sampling_batch_results:
                            if not isinstance(item, dict):
                                continue
                            origin_id_val = item.get("origin_id")
                            if isinstance(origin_id_val, int):
                                origin_ids.append(origin_id_val)
                            elif isinstance(origin_id_val, str) and origin_id_val.isdigit():
                                origin_ids.append(int(origin_id_val))
                        if origin_ids:
                            if sampling_batch_origin_id not in origin_ids:
                                sampling_batch_origin_id = origin_ids[0]
                            for item in sampling_batch_results:
                                if not isinstance(item, dict):
                                    continue
                                origin_id_val = item.get("origin_id")
                                try:
                                    origin_id_val = int(origin_id_val)
                                except (TypeError, ValueError):
                                    continue
                                if origin_id_val == sampling_batch_origin_id:
                                    sampling_batch_detail = item
                                    break
                        if sampling_batch_origin_id is not None and sampling_batch_detail is None:
                            sampling_batch_detail_error = "Selected origin not found in report."
            else:
                sampling_batch_error = "No sampling_results_*.json files found."
        if compute_results_panel and "gk_input" in tables:
            results_values = {opt["value"] for opt in results_columns}
            if results_x_col is None:
                results_x_col = f"gk_input.{x_col}"
            if results_x2_col is None:
                results_x2_col = f"gk_input.{x2_col}"
            if results_x3_col is None:
                results_x3_col = f"gk_input.{x3_col}"
            if results_x4_col is None:
                results_x4_col = f"gk_input.{x4_col}"
            if results_y_col not in results_values and results_columns:
                results_y_col = results_columns[0]["value"]
            if results_y2_col not in results_values and results_columns:
                results_y2_col = results_columns[0]["value"]
            if results_y3_col not in results_values and results_columns:
                results_y3_col = results_columns[0]["value"]
            if results_y4_col not in results_values and results_columns:
                results_y4_col = results_columns[0]["value"]
            if results_x_col not in results_values and results_columns:
                results_x_col = results_columns[0]["value"]
            if results_x2_col not in results_values and results_columns:
                results_x2_col = results_columns[0]["value"]
            if results_x3_col not in results_values and results_columns:
                results_x3_col = results_columns[0]["value"]
            if results_x4_col not in results_values and results_columns:
                results_x4_col = results_columns[0]["value"]
            results_points, results_warn = get_gk_run_results_points(
                conn, results_x_col, results_y_col, results_filter, origin_id
            )
            results_points_2, results_warn_2 = get_gk_run_results_points(
                conn, results_x2_col, results_y2_col, results_filter, origin_id
            )
            results_points_3, results_warn_3 = get_gk_run_results_points(
                conn, results_x3_col, results_y3_col, results_filter, origin_id
            )
            results_points_4, results_warn_4 = get_gk_run_results_points(
                conn, results_x4_col, results_y4_col, results_filter, origin_id
            )
            results_report = get_gamma_max_status_report(conn, origin_id)
            def _fetch_run_point(
                run_id: int, x_column: str, y_column: str
            ) -> Optional[Dict[str, float]]:
                x_spec = _normalize_axis_spec(x_column)
                if x_spec.startswith("gk_input."):
                    x_field = x_spec.split(".", 1)[1]
                    x_expr = f"gi.{x_field}"
                elif x_spec.startswith("gk_run."):
                    x_field = x_spec.split(".", 1)[1]
                    x_expr = f"gr.{x_field}"
                else:
                    return None
                if y_column.startswith("gk_run."):
                    y_field = y_column.split(".", 1)[1]
                    run_columns = {
                        row["name"]
                        for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
                    }
                    if y_field not in run_columns:
                        return None
                    y_expr = f"gr.{y_field}"
                elif y_column.startswith("gk_input."):
                    y_field = y_column.split(".", 1)[1]
                    if y_field not in ALLOWED_STATS_COLUMNS:
                        return None
                    y_expr = f"gi.{y_field}"
                else:
                    return None
                query = (
                    f"SELECT {x_expr}, {y_expr} "
                    "FROM gk_run gr JOIN gk_input gi ON gi.id = gr.gk_input_id "
                    "WHERE gr.id = ?"
                )
                row = conn.execute(query, (run_id,)).fetchone()
                if row is None:
                    return None
                x_val, y_val = row
                try:
                    x_float = float(x_val)
                    y_float = float(y_val)
                except (TypeError, ValueError):
                    return None
                if not math.isfinite(x_float) or not math.isfinite(y_float):
                    return None
                return {"x": x_float, "y": y_float}
            plot_params: List[object] = []
            origin_filter_sql = ""
            gk_input_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(gk_input)")
            }
            join_study = "gk_study_id" in gk_input_columns and "gk_study" in tables
            join_equil = join_study and "data_equil" in tables
            if origin_id is not None:
                if join_equil:
                    origin_filter_sql = "AND de.data_origin_id = ?"
                    plot_params.append(origin_id)
            results_filter_sql = ""
            if results_filter == "finished":
                results_filter_sql = "AND r.status IN ('SUCCESS', 'CONVERGED')"
            elif results_filter == "growth":
                results_filter_sql = "AND r.gamma_max IS NOT NULL AND r.gamma_max != 0"
            if "gk_run" in tables:
                gk_run_columns = {
                    row["name"] for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
                }
                can_load_results_plots = {
                    "id",
                    "gk_input_id",
                    "input_name",
                }.issubset(gk_run_columns)
                if can_load_results_plots:
                    join_batch = "gk_batch" in tables and "gk_batch_id" in gk_run_columns
                    plot_rows = conn.execute(
                        f"""
                        SELECT r.id AS run_id,
                               r.gk_input_id AS gk_input_id,
                               r.input_name AS input_name,
                               {"b.batch_database_name AS batch_db" if join_batch else "'' AS batch_db"}
                        FROM gk_run r
                        {"LEFT JOIN gk_batch b ON b.id = r.gk_batch_id" if join_batch else ""}
                        LEFT JOIN gk_input gi ON gi.id = r.gk_input_id
                        {"LEFT JOIN gk_study gs ON gs.id = gi.gk_study_id" if join_study else ""}
                        {"LEFT JOIN data_equil de ON de.id = gs.data_equil_id" if join_equil else ""}
                        WHERE r.input_name IS NOT NULL
                        {origin_filter_sql}
                        {results_filter_sql}
                        ORDER BY r.id DESC
                        LIMIT 500
                        """,
                        plot_params,
                    ).fetchall()
                    results_plot_options = [
                        {
                            "run_id": int(row["run_id"]),
                            "gk_input_id": int(row["gk_input_id"] or 0),
                            "input_name": row["input_name"],
                            "batch_db": row["batch_db"],
                        }
                        for row in plot_rows
                    ]
                    selected_run_id = results_plot_run_id
                    if results_plot_options and selected_run_id is None:
                        selected_run_id = int(results_plot_options[0]["run_id"])
                    if selected_run_id is not None:
                        selected_row = next(
                            (
                                row
                                for row in results_plot_options
                                if int(row["run_id"]) == selected_run_id
                            ),
                            None,
                        )
                        if selected_row:
                            input_name = selected_row.get("input_name") or ""
                            batch_db = selected_row.get("batch_db") or ""
                            base = input_name[:-3] if input_name.endswith(".in") else input_name
                            subdir = batch_db.replace(".db", "")
                            growth_name = f"{base}_growth_rate.png"
                            gamma_name = f"{base}_gamma_vs_ky.png"
                            growth_path = os.path.join(BATCH_BASE_DIR, "plots", subdir, growth_name)
                            gamma_path = os.path.join(BATCH_BASE_DIR, "plots", subdir, gamma_name)
                            results_plot_selected = {
                                "run_id": selected_run_id,
                                "gk_input_id": selected_row.get("gk_input_id"),
                                "growth_url": url_for("plots_file", filename=f"{subdir}/{growth_name}")
                                if os.path.exists(growth_path)
                                else None,
                                "gamma_url": url_for("plots_file", filename=f"{subdir}/{gamma_name}")
                                if os.path.exists(gamma_path)
                                else None,
                            }
                            results_highlight = _fetch_run_point(
                                selected_run_id, results_x_col, results_y_col
                            )
                            results_highlight_2 = _fetch_run_point(
                                selected_run_id, results_x2_col, results_y2_col
                            )
                            results_highlight_3 = _fetch_run_point(
                                selected_run_id, results_x3_col, results_y3_col
                            )
                            results_highlight_4 = _fetch_run_point(
                                selected_run_id, results_x4_col, results_y4_col
                            )
        if compute_tables_panel and "gk_input" in tables:
            if edit_gk_input_id and edit_gk_input_id.isdigit():
                row = conn.execute(
                    "SELECT content, status FROM gk_input WHERE id = ?",
                    (int(edit_gk_input_id),),
                ).fetchone()
                if row is None:
                    edit_error = "No gk_input row found for that id."
                else:
                    edit_gk_input_content = str(row["content"])
                    edit_status = str(row["status"])
        if compute_tables_panel and "gk_model" in tables:
            gk_model_columns, gk_model_rows, _, _ = get_table_rows(
                conn, "gk_model", False
            )
    finally:
        conn.close()

    if compute_tables_panel and batch_dbs:
        if selected_batch_db not in batch_dbs:
            selected_batch_db = batch_dbs[0]
        batch_db_path = os.path.join(batch_dir, selected_batch_db)
        if not os.path.exists(batch_db_path):
            batch_error = f"Batch database not found: {batch_db_path}"
        else:
            batch_conn = get_connection(batch_db_path)
            try:
                batch_tables = list_tables(batch_conn)
                if "gk_run" in batch_tables:
                    batch_columns, batch_rows, _, _ = get_table_rows(
                        batch_conn, "gk_run", False
                    )
                else:
                    batch_error = f"Table gk_run not found in {selected_batch_db}"
            finally:
                batch_conn.close()

    return render_template("index.html", **build_index_context())


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, request_handler=DatamakRequestHandler)
