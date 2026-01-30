#!/usr/bin/env python3
import os
import re
import sqlite3
import subprocess
import sys
import threading
import json
import glob
import math
import random
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, send_from_directory, url_for
try:
    import numpy as np
    HAVE_NUMPY = True
except Exception:
    np = None
    HAVE_NUMPY = False

APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(APP_DIR))
DEFAULT_DB = os.path.join(PROJECT_DIR, "gyrokinetic_simulations.db")
DB_UPDATE_DIR = os.path.join(PROJECT_DIR, "db_update")
DOCS_DIR = os.path.join(PROJECT_DIR, "docs")
BATCH_BASE_DIR = os.path.join(PROJECT_DIR, "batch")
BATCH_NEW_DIR = os.path.join(BATCH_BASE_DIR, "new")
BATCH_SENT_DIR = os.path.join(BATCH_BASE_DIR, "sent")
ANALYSIS_DIR = os.path.join(PROJECT_DIR, "db_analysis")
AI_FEEDBACK_PATH = os.path.join(APP_DIR, "ai_feedback.json")
AI_FEEDBACK_LOCK = threading.Lock()
MONITOR_REPORT_PATH = os.path.join(ANALYSIS_DIR, "remote_monitor_report.json")
USAGE_LOG_PATH = os.path.join(ANALYSIS_DIR, "monitor_feedback.json")
USAGE_LOG_LOCK = threading.Lock()

ACTIONS = {
    "populate_mate": {
        "label": "Populate",
        "script": os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Mate_KinEFIT.py"),
    },
    "populate_alexei": {
        "label": "Populate",
        "script": os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_09.py"),
    },
    "populate_alexei_fullauto": {
        "label": "Populate",
        "script": os.path.join(
            DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_09_fullauto.py"
        ),
    },
    "populate_alexei_fullauto_10": {
        "label": "Populate",
        "script": os.path.join(
            DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_10_fullauto.py"
        ),
    },
    "create_inputs_mate": {
        "label": "Create GK Inputs",
        "script": os.path.join(
            DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_pfile_and_gfile.py"
        ),
    },
    "create_inputs_transp": {
        "label": "Create GK Inputs",
        "script": os.path.join(
            DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_transpfile.py"
        ),
    },
    "create_inputs_transp_fullauto": {
        "label": "Create GK Inputs",
        "script": os.path.join(
            DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_transpfile_fullauto.py"
        ),
        "use_db": True,
        "db_arg": "--db",
    },
    "create_batch_db": {
        "label": "Create Batch DB",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "create_batch_database.py"),
        "args": ["--copy-torun"],
        "use_db": True,
        "db_arg": "--source-db",
    },
    "deploy_batch_db": {
        "label": "Deploy Batch DB",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "deploy_batch.py"),
    },
    "deploy_batch_db_large": {
        "label": "Deploy Batch DB (Large)",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "deploy_batch_large.py"),
    },
    "check_launched_batches": {
        "label": "Check Launched Batches",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "check_launched_batches.py"),
        "args": ["--remote-check"],
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
    "monitor_remote_runs": {
        "label": "Run Remote Monitor",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "monitor_remote_runs.py"),
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
    "mark_remote_running_interrupted": {
        "label": "Mark RUNNING as INTERRUPTED",
        "script": os.path.join(
            os.path.dirname(APP_DIR), "batch", "mark_remote_running_interrupted.py"
        ),
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
    "mark_remote_run_restart": {
        "label": "Mark run as RESTART",
        "script": os.path.join(
            os.path.dirname(APP_DIR), "batch", "mark_remote_run_restart.py"
        ),
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
    "launch_remote_slurm_job": {
        "label": "Launch SLURM job",
        "script": os.path.join(
            os.path.dirname(APP_DIR), "batch", "launch_remote_slurm_job.py"
        ),
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
    "mark_empty_gk_input_error": {
        "label": "Mark empty gk_input as ERROR",
        "script": os.path.join(DB_UPDATE_DIR, "mark_empty_gk_input_error.py"),
        "use_db": True,
        "db_arg": "--db",
        "capture_output": True,
    },
}

ACTION_LOCK = threading.Lock()
ACTION_STATE: Dict[str, Optional[str]] = {
    "running": False,
    "name": None,
    "message": None,
    "key": None,
}

app = Flask(__name__, static_folder="logo")


@app.route("/docs/<path:filename>")
def docs_file(filename: str):
    return send_from_directory(DOCS_DIR, filename)


@app.route("/plots/<path:filename>")
def plots_file(filename: str):
    return send_from_directory(os.path.join(BATCH_BASE_DIR, "plots"), filename)


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [row["name"] for row in rows]


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


def data_origin_color(origin_name: str, color: Optional[str] = None) -> str:
    if color:
        return str(color)
    name = str(origin_name)
    if name == "Alexei Transp 09 (full-auto)":
        return "#2ca02c"
    if name.startswith("Alexei Transp 09"):
        return "#d62728"
    return "#1f77b4"


def get_table_rows(
    conn: sqlite3.Connection,
    table: str,
    only_active: bool,
    limit: int = 100,
    origin_filter: Optional[int] = None,
    transpfile_regex: Optional[str] = None,
) -> Tuple[List[str], List[sqlite3.Row], int, int]:
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
    total_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if where_sql:
        filtered_count = conn.execute(
            f"SELECT COUNT(*) FROM {table}{where_sql}",
            params,
        ).fetchone()[0]
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
                        "Review and mark TORUN to queue runs."
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
                    "text": (
                        f"{total_active} batch DB(s) are LAUNCHED/SYNCED. "
                        "Consider checking status with check_launched_batches.py."
                    ),
                    "action": "check_launched_batches",
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
    rows = conn.execute(base_query, params).fetchall()
    points = []
    for x_val, y_val, origin_name, origin_color in rows:
        points.append(
            {
                "x": float(x_val),
                "y": float(y_val),
                "color": data_origin_color(origin_name, origin_color),
            }
        )
    return points


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
    rows = conn.execute(base_query, params).fetchall()
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
    rows = conn.execute(base_query, params).fetchall()
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


def build_results_columns(run_columns: Optional[set] = None) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for col in ALLOWED_STATS_COLUMNS:
        items.append({"value": f"gk_input.{col}", "label": f"gk_input.{col}"})
    for col in ALLOWED_RESULTS_COLUMNS:
        if run_columns is not None and col not in run_columns:
            continue
        items.append({"value": f"gk_run.{col}", "label": f"gk_run.{col}"})
    return items


def get_gk_run_results_points(
    conn: sqlite3.Connection,
    x_col: str,
    y_col: str,
    results_filter: str,
    origin_id: Optional[int],
) -> Tuple[List[dict], bool]:
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


def get_action_state() -> Dict[str, Optional[str]]:
    with ACTION_LOCK:
        return dict(ACTION_STATE)


@app.route("/action_status")
def action_status():
    return jsonify(get_action_state())


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
    action_name: str,
    script_path: str,
    db_path: Optional[str],
    extra_args: Optional[List[str]] = None,
) -> None:
    try:
        script_args: List[str] = []
        use_db = False
        db_arg = None
        capture_output = False
        for action in ACTIONS.values():
            if action.get("script") == script_path:
                script_args = action.get("args", [])
                use_db = action.get("use_db", False)
                db_arg = action.get("db_arg")
                capture_output = action.get("capture_output", False)
                break
        if extra_args:
            script_args = [*script_args, *extra_args]
        if use_db and db_path:
            if db_arg:
                script_args = [*script_args, db_arg, db_path]
            else:
                script_args = [*script_args, db_path]
        if capture_output:
            result = subprocess.run(
                [sys.executable, script_path, *script_args],
                check=True,
                text=True,
                capture_output=True,
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
                message = combined
            else:
                message = None
        else:
            subprocess.run([sys.executable, script_path, *script_args], check=True)
            message = None
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "").strip() if capture_output else ""
        stderr = (exc.stderr or "").strip() if capture_output else ""
        combined = "\n".join([chunk for chunk in (stdout, stderr) if chunk])
        if combined:
            print(combined)
            if len(combined) > 2000:
                combined = combined[:2000].rstrip() + "\n... (truncated)"
            message = f"Action '{action_name}' failed:\n{combined}"
        else:
            message = f"Action '{action_name}' failed: {exc}"
    except Exception as exc:
        message = f"Action '{action_name}' failed: {exc}"
    with ACTION_LOCK:
        ACTION_STATE["running"] = False
        ACTION_STATE["name"] = None
        ACTION_STATE["message"] = message


def _start_action(
    action_name: str,
    db_path: str,
    extra_args: Optional[List[str]] = None,
    panel: str = "action",
):
    action = ACTIONS.get(action_name)
    if action is None:
        with ACTION_LOCK:
            ACTION_STATE["message"] = f"Unknown action '{action_name}'."
        return redirect(url_for("index", panel=panel, db=db_path))
    with ACTION_LOCK:
        if ACTION_STATE["running"]:
            current = ACTION_STATE["name"] or "another action"
            ACTION_STATE["message"] = f"Action '{current}' is already running."
            return redirect(url_for("index", panel=panel, db=db_path))
        ACTION_STATE["running"] = True
        ACTION_STATE["name"] = action["label"]
        ACTION_STATE["message"] = f"Action '{action['label']}' is running."
        ACTION_STATE["key"] = action_name
    thread = threading.Thread(
        target=_run_action,
        args=(action["label"], action["script"], db_path, extra_args),
        daemon=True,
    )
    thread.start()
    return redirect(url_for("index", panel=panel, db=db_path))


@app.route("/action/<action_name>", methods=["POST"])
def run_action(action_name: str):
    db_path = request.form.get("db", DEFAULT_DB)
    panel = request.form.get("panel", "action")
    log_usage("action_click", {"action": action_name, "panel": panel, "db": db_path})
    extra_args: List[str] = []
    if action_name == "check_launched_batches":
        plots_limit = (request.form.get("plots_limit") or "").strip()
        if plots_limit.isdigit():
            extra_args.extend(["--plots-limit", plots_limit])
            if int(plots_limit) > 0:
                extra_args.append("--sync-plots")
                plots_dir = os.path.join(BATCH_BASE_DIR, "plots")
                extra_args.extend(["--plots-dir", plots_dir])
    if action_name == "monitor_remote_runs":
        extra_args.extend(["--user", "jdominsk"])
        if (request.form.get("run_analyze") or "").strip():
            extra_args.append("--run-analyze")
    if action_name == "mark_remote_running_interrupted":
        batch_name = (request.form.get("batch_name") or "").strip()
        if batch_name:
            extra_args.extend(["--batch", batch_name])
        extra_args.extend(["--follow-monitor", "--monitor-user", "jdominsk"])
    if action_name == "mark_remote_run_restart":
        batch_name = (request.form.get("batch_name") or "").strip()
        run_ids = request.form.getlist("run_id")
        if batch_name:
            extra_args.extend(["--batch", batch_name])
        for run_id in run_ids:
            if run_id.isdigit():
                extra_args.extend(["--run-id", run_id])
        extra_args.extend(["--follow-monitor", "--monitor-user", "jdominsk"])
    if action_name == "launch_remote_slurm_job":
        batch_name = (request.form.get("batch_name") or "").strip()
        if batch_name:
            extra_args.extend(["--batch", batch_name])
        extra_args.extend(["--user", "jdominsk"])
        extra_args.extend(["--follow-monitor", "--monitor-user", "jdominsk"])
    return _start_action(action_name, db_path, extra_args or None, panel=panel)


@app.route("/suggestion_action", methods=["POST"])
def suggestion_action():
    db_path = request.form.get("db", DEFAULT_DB)
    action_name = request.form.get("action_name", "")
    suggestion_id = request.form.get("suggestion_id", "")
    panel = request.form.get("panel", "action")
    if suggestion_id and action_name:
        record_ai_feedback(suggestion_id, action_name)
        log_usage(
            "suggestion_click",
            {"action": action_name, "panel": panel, "suggestion_id": suggestion_id},
        )
    return _start_action(action_name, db_path, panel=panel)


@app.route("/update_status", methods=["GET", "POST"])
def update_status():
    if request.method != "POST":
        return redirect(url_for("index"))
    db_path = request.form.get("db", DEFAULT_DB)
    table = request.form.get("table")
    row_id = request.form.get("row_id")
    panel = request.form.get("panel", "tables")
    if table != "gk_input" or not row_id or not row_id.isdigit():
        return redirect(url_for("index", panel=panel, db=db_path, table=table))
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
    return redirect(url_for("index", panel=panel, db=db_path, table=table))


@app.route("/update_status_bulk", methods=["POST"])
def update_status_bulk():
    db_path = request.form.get("db", DEFAULT_DB)
    panel = request.form.get("panel", "tables")
    row_ids_raw = request.form.get("row_ids", "")
    row_ids: List[int] = []
    for part in row_ids_raw.split(","):
        part = part.strip()
        if part.isdigit():
            row_ids.append(int(part))
    if not row_ids:
        return redirect(url_for("index", panel=panel, db=db_path))
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
    return redirect(url_for("index", panel=panel, db=db_path))


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
    log_usage(
        "edit_gk_input",
        {"action": action, "gk_input_id": gk_input_id, "db": db_path},
    )
    if not gk_input_id.isdigit():
        return redirect(
            url_for(
                "index",
                panel="edit",
                db=db_path,
                gk_input_id=gk_input_id,
                edit_error="Enter a numeric gk_input id.",
            )
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
                return redirect(
                    url_for(
                        "index",
                        panel="edit",
                        db=db_path,
                        gk_input_id=gk_input_id,
                        edit_error="No gk_input row found for that id.",
                    )
                )
            if str(row["status"]) != "WAIT":
                return redirect(
                    url_for(
                        "index",
                        panel="edit",
                        db=db_path,
                        gk_input_id=gk_input_id,
                        edit_error="Edits allowed only when status is WAIT.",
                        edit_status=str(row["status"]),
                    )
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
        return redirect(
            url_for(
                "index",
                panel="edit",
                db=db_path,
                gk_input_id=gk_input_id,
                edit_message="Saved.",
                edit_warning=warning,
            )
        )
    return redirect(
        url_for(
            "index",
            panel="edit",
            db=db_path,
            gk_input_id=gk_input_id,
        )
    )


@app.route("/", methods=["GET"])
def index():
    db_path = request.args.get("db", DEFAULT_DB)
    selected_table = request.args.get("table")
    selected_panel = request.args.get("panel", "statistics")
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
    sampling_batch_origin_raw = request.args.get("sampling_batch_origin_id")
    sampling_batch_origin_id = (
        int(sampling_batch_origin_raw)
        if sampling_batch_origin_raw and sampling_batch_origin_raw.isdigit()
        else None
    )
    sampling_k_raw = request.args.get("sampling_k", "6")
    sampling_k = int(sampling_k_raw) if sampling_k_raw.isdigit() else 6
    plasma_k_raw = request.args.get("plasma_k", "6")
    plasma_k = int(plasma_k_raw) if plasma_k_raw.isdigit() else 6
    eqp_method = request.args.get("eqp_method", "farthest").strip().lower()
    if eqp_method not in {"farthest", "kmeans"}:
        eqp_method = "farthest"
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

    if not os.path.exists(db_path):
        return render_template(
            "index.html",
            db_path=db_path,
            tables=[],
            selected_table=None,
            columns=[],
            rows=[],
            selected_panel=selected_panel,
            only_active=only_active,
            table_limit=table_limit,
            table_origin_id=table_origin_id,
            table_transpfile_regex=table_transpfile_regex,
            stats_points=[],
            stats_columns=ALLOWED_STATS_COLUMNS,
            stats_x_col=x_col,
            stats_y_col=y_col,
            stats_points_2=[],
            stats_x2_col=x2_col,
            stats_y2_col=y2_col,
            stats_points_3=[],
            stats_x3_col=x3_col,
            stats_y3_col=y3_col,
            stats_points_4=[],
            stats_x4_col=x4_col,
            stats_y4_col=y4_col,
            results_points=[],
            results_points_2=[],
            results_points_3=[],
            results_points_4=[],
            results_warn=False,
            results_warn_2=False,
            results_warn_3=False,
            results_warn_4=False,
            results_filter=results_filter,
            results_y_col=results_y_col,
            results_y2_col=results_y2_col,
            results_y3_col=results_y3_col,
            results_y4_col=results_y4_col,
            results_x_min=results_x_min,
            results_x_max=results_x_max,
            results_y_min=results_y_min,
            results_y_max=results_y_max,
            results_x_scale=results_x_scale,
            results_y_scale=results_y_scale,
            results_x2_min=results_x2_min,
            results_x2_max=results_x2_max,
            results_y2_min=results_y2_min,
            results_y2_max=results_y2_max,
            results_x2_scale=results_x2_scale,
            results_y2_scale=results_y2_scale,
            results_x3_min=results_x3_min,
            results_x3_max=results_x3_max,
            results_y3_min=results_y3_min,
            results_y3_max=results_y3_max,
            results_x3_scale=results_x3_scale,
            results_y3_scale=results_y3_scale,
            results_x4_min=results_x4_min,
            results_x4_max=results_x4_max,
            results_y4_min=results_y4_min,
            results_y4_max=results_y4_max,
            results_x4_scale=results_x4_scale,
            results_y4_scale=results_y4_scale,
            results_columns=build_results_columns(),
            results_report=None,
            results_plot_options=[],
            results_plot_selected=None,
            results_highlight=None,
            results_highlight_2=None,
            results_highlight_3=None,
            results_highlight_4=None,
            data_origins=[],
            data_origin_colors={},
            selected_origin_id=origin_id,
            sampling_origin_id=sampling_origin_id,
            sampling_report=None,
            sampling_coverage=None,
            sampling_regimes=None,
            sampling_cluster=None,
            sampling_pca=None,
            sampling_selection=None,
            sampling_bin_labels=sampling_bin_labels,
            sampling_columns=MHD_COLUMNS,
            sampling_k=sampling_k,
            sampling_target=sampling_target,
            sampling_max=sampling_max,
            sampling_reports=sampling_reports,
            sampling_report_file=sampling_report_file,
            sampling_batch_origin_id=sampling_batch_origin_id,
            sampling_batch_report=None,
            sampling_batch_results=[],
            sampling_batch_detail=None,
            sampling_batch_detail_error=None,
            sampling_batch_columns=MHD_COLUMNS,
            sampling_batch_error="Database not found.",
            monitor_report=monitor_report,
            plasma_origin_id=plasma_origin_id,
            plasma_report=None,
            plasma_coverage=None,
            plasma_regimes=None,
            plasma_cluster=None,
            plasma_pca=None,
            plasma_selection=None,
            plasma_bin_labels=plasma_bin_labels,
            plasma_columns=PLASMA_COLUMNS,
            plasma_missing_columns=PLASMA_COLUMNS,
            plasma_k=plasma_k,
            plasma_target=plasma_target,
            plasma_max=plasma_max,
            eqp_method=eqp_method,
            eqp_target=eqp_target,
            eqp_max=eqp_max,
            eqp_coverage_enabled=eqp_coverage_enabled,
            eqp_ion_tprim_min=eqp_ion_tprim_min,
            eqp_columns=EQUIL_PLASMA_COLUMNS,
            eqp_report=None,
            eqp_coverage=None,
            eqp_selection=None,
            eqp_status_counts=None,
            batch_dbs=batch_dbs,
            selected_batch_db=selected_batch_db,
            batch_view=batch_view,
            batch_columns=batch_columns,
            batch_rows=batch_rows,
            batch_error=None,
            gk_model_columns=gk_model_columns,
            gk_model_rows=gk_model_rows,
            edit_gk_input_id=edit_gk_input_id,
            edit_gk_input_content=edit_gk_input_content,
            edit_status=edit_status,
            edit_message=edit_message,
            edit_error=edit_error,
            edit_warning=edit_warning,
            table_total_count=0,
            table_filtered_count=0,
            ai_suggestions=[],
            actions=ACTIONS,
            action_status=get_action_state(),
            have_numpy=HAVE_NUMPY,
            error=f"Database not found: {db_path}",
        )

    conn = get_connection(db_path)
    try:
        tables = list_tables(conn)
        ai_feedback = load_ai_feedback()
        ai_suggestions = get_ai_suggestions(conn, ai_feedback)
        table_total_count = 0
        table_filtered_count = 0
        data_origin_colors: Dict[str, str] = {}
        if "gk_run" in tables:
            gk_run_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
            }
            results_columns = build_results_columns(gk_run_columns)
            if not results_columns:
                results_columns = build_results_columns()
        if selected_table not in tables:
            selected_table = tables[0] if tables else None
        if selected_table:
            if table_transpfile_regex:
                def _regexp(expr, item):
                    try:
                        return 1 if re.search(expr, item or "") else 0
                    except re.error:
                        return 0

                conn.create_function("REGEXP", 2, _regexp)
            columns, rows, table_total_count, table_filtered_count = get_table_rows(
                conn,
                selected_table,
                only_active,
                table_limit,
                table_origin_id,
                table_transpfile_regex if table_transpfile_regex else None,
            )
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
        if "gk_input" in tables:
            gk_input_columns = get_table_columns(conn, "gk_input")
            stats_points = get_gk_input_points(conn, x_col, y_col, origin_id)
            stats_points_2 = get_gk_input_points(conn, x2_col, y2_col, origin_id)
            stats_points_3 = get_gk_input_points(conn, x3_col, y3_col, origin_id)
            stats_points_4 = get_gk_input_points(conn, x4_col, y4_col, origin_id)
            if selected_panel in ("sampling", "plasma-sampling"):
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
            if selected_panel in ("plasma-sampling", "sampling"):
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
            if selected_panel == "equil-plasma-sampling":
                dataset, total_rows = get_equil_plasma_dataset(
                    conn, origin_id, ion_tprim_min=eqp_ion_tprim_min
                )
                wait_dataset, wait_total_rows = get_equil_plasma_dataset(
                    conn,
                    origin_id,
                    status_filter="WAIT",
                    ion_tprim_min=eqp_ion_tprim_min,
                )
                eqp_status_counts = get_equil_plasma_status_counts(
                    conn, origin_id, ion_tprim_min=eqp_ion_tprim_min
                )
                eqp_max_effective = eqp_max if eqp_coverage_enabled else wait_total_rows
                start = time.perf_counter()
                eqp_report = build_sampling_report(
                    dataset, total_rows, EQUIL_PLASMA_COLUMNS
                )
                eqp_report["duration_sec"] = time.perf_counter() - start
                if eqp_coverage_enabled:
                    start = time.perf_counter()
                    eqp_coverage = build_sampling_coverage(
                        wait_dataset, EQUIL_PLASMA_COLUMNS, eqp_max_effective
                    )
                    eqp_coverage["duration_sec"] = time.perf_counter() - start
                if eqp_method == "kmeans":
                    start = time.perf_counter()
                    eqp_selection = build_kmeans_selection(
                        wait_dataset,
                        EQUIL_PLASMA_COLUMNS,
                        eqp_target,
                        eqp_max_effective,
                        id_column=MHD_ID_COLUMN,
                    )
                    eqp_selection["duration_sec"] = time.perf_counter() - start
                else:
                    start = time.perf_counter()
                    eqp_selection = build_sampling_selection(
                        wait_dataset,
                        EQUIL_PLASMA_COLUMNS,
                        eqp_target,
                        eqp_max_effective,
                        id_column=MHD_ID_COLUMN,
                    )
                    eqp_selection["duration_sec"] = time.perf_counter() - start
        if selected_panel == "sampling-batch":
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
        if "gk_run" in tables and "gk_input" in tables:
            results_values = {opt["value"] for opt in results_columns}
            if results_y_col not in results_values and results_columns:
                results_y_col = results_columns[0]["value"]
            if results_y2_col not in results_values and results_columns:
                results_y2_col = results_columns[0]["value"]
            if results_y3_col not in results_values and results_columns:
                results_y3_col = results_columns[0]["value"]
            if results_y4_col not in results_values and results_columns:
                results_y4_col = results_columns[0]["value"]
            results_points, results_warn = get_gk_run_results_points(
                conn, x_col, results_y_col, results_filter, origin_id
            )
            results_points_2, results_warn_2 = get_gk_run_results_points(
                conn, x2_col, results_y2_col, results_filter, origin_id
            )
            results_points_3, results_warn_3 = get_gk_run_results_points(
                conn, x3_col, results_y3_col, results_filter, origin_id
            )
            results_points_4, results_warn_4 = get_gk_run_results_points(
                conn, x4_col, results_y4_col, results_filter, origin_id
            )
            results_report = get_gamma_max_status_report(conn, origin_id)
            def _fetch_run_point(
                run_id: int, x_column: str, y_column: str
            ) -> Optional[Dict[str, float]]:
                if x_column not in ALLOWED_STATS_COLUMNS:
                    return None
                if y_column.startswith("gk_run."):
                    y_field = y_column.split(".", 1)[1]
                    run_columns = {
                        row["name"]
                        for row in conn.execute("PRAGMA table_info(gk_run)").fetchall()
                    }
                    if y_field not in run_columns:
                        return None
                    query = (
                        f"SELECT gi.{x_column}, gr.{y_field} "
                        "FROM gk_run gr JOIN gk_input gi ON gi.id = gr.gk_input_id "
                        "WHERE gr.id = ?"
                    )
                elif y_column.startswith("gk_input."):
                    y_field = y_column.split(".", 1)[1]
                    if y_field not in ALLOWED_STATS_COLUMNS:
                        return None
                    query = (
                        f"SELECT gi.{x_column}, gi.{y_field} "
                        "FROM gk_run gr JOIN gk_input gi ON gi.id = gr.gk_input_id "
                        "WHERE gr.id = ?"
                    )
                else:
                    return None
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
            plot_rows = conn.execute(
                f"""
                SELECT r.id AS run_id,
                       r.gk_input_id AS gk_input_id,
                       r.input_name AS input_name,
                       b.batch_database_name AS batch_db
                FROM gk_run r
                LEFT JOIN gk_batch b ON b.id = r.gk_batch_id
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
                        selected_run_id, x_col, results_y_col
                    )
                    results_highlight_2 = _fetch_run_point(
                        selected_run_id, x2_col, results_y2_col
                    )
                    results_highlight_3 = _fetch_run_point(
                        selected_run_id, x3_col, results_y3_col
                    )
                    results_highlight_4 = _fetch_run_point(
                        selected_run_id, x4_col, results_y4_col
                    )
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
        if "gk_model" in tables:
            gk_model_columns, gk_model_rows, _, _ = get_table_rows(
                conn, "gk_model", False
            )
    finally:
        conn.close()

    if batch_dbs:
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

    return render_template(
        "index.html",
        db_path=db_path,
        tables=tables,
        selected_table=selected_table,
        columns=columns,
        rows=rows,
        batch_dbs=batch_dbs,
        selected_batch_db=selected_batch_db,
        batch_view=batch_view,
        batch_columns=batch_columns,
        batch_rows=batch_rows,
        batch_error=batch_error,
        gk_model_columns=gk_model_columns,
        gk_model_rows=gk_model_rows,
        edit_gk_input_id=edit_gk_input_id,
        edit_gk_input_content=edit_gk_input_content,
        edit_status=edit_status,
        edit_message=edit_message,
        edit_error=edit_error,
        edit_warning=edit_warning,
        selected_panel=selected_panel,
        only_active=only_active,
        table_limit=table_limit,
        table_total_count=table_total_count,
        table_filtered_count=table_filtered_count,
        table_origin_id=table_origin_id,
        table_transpfile_regex=table_transpfile_regex,
        stats_points=stats_points,
        stats_columns=ALLOWED_STATS_COLUMNS,
        stats_x_col=x_col,
        stats_y_col=y_col,
        stats_points_2=stats_points_2,
        stats_x2_col=x2_col,
        stats_y2_col=y2_col,
        stats_points_3=stats_points_3,
        stats_x3_col=x3_col,
        stats_y3_col=y3_col,
        stats_points_4=stats_points_4,
        stats_x4_col=x4_col,
        stats_y4_col=y4_col,
        results_points=results_points,
        results_points_2=results_points_2,
        results_points_3=results_points_3,
        results_points_4=results_points_4,
        results_warn=results_warn,
        results_warn_2=results_warn_2,
        results_warn_3=results_warn_3,
        results_warn_4=results_warn_4,
        results_filter=results_filter,
        results_y_col=results_y_col,
        results_y2_col=results_y2_col,
        results_y3_col=results_y3_col,
        results_y4_col=results_y4_col,
        results_plot_options=results_plot_options,
        results_plot_selected=results_plot_selected,
        results_highlight=results_highlight,
        results_highlight_2=results_highlight_2,
        results_highlight_3=results_highlight_3,
        results_highlight_4=results_highlight_4,
        results_x_min=results_x_min,
        results_x_max=results_x_max,
        results_y_min=results_y_min,
        results_y_max=results_y_max,
        results_x_scale=results_x_scale,
        results_y_scale=results_y_scale,
        results_x2_min=results_x2_min,
        results_x2_max=results_x2_max,
        results_y2_min=results_y2_min,
        results_y2_max=results_y2_max,
        results_x2_scale=results_x2_scale,
        results_y2_scale=results_y2_scale,
        results_x3_min=results_x3_min,
        results_x3_max=results_x3_max,
        results_y3_min=results_y3_min,
        results_y3_max=results_y3_max,
        results_x3_scale=results_x3_scale,
        results_y3_scale=results_y3_scale,
        results_x4_min=results_x4_min,
        results_x4_max=results_x4_max,
        results_y4_min=results_y4_min,
        results_y4_max=results_y4_max,
        results_x4_scale=results_x4_scale,
        results_y4_scale=results_y4_scale,
        results_columns=results_columns,
        results_report=results_report,
        data_origins=data_origins,
        data_origin_colors=data_origin_colors,
        selected_origin_id=origin_id,
        sampling_origin_id=sampling_origin_id,
        sampling_report=sampling_report,
        sampling_coverage=sampling_coverage,
        sampling_regimes=sampling_regimes,
        sampling_cluster=sampling_cluster,
        sampling_pca=sampling_pca,
        sampling_selection=sampling_selection,
        sampling_bin_labels=sampling_bin_labels,
        sampling_columns=MHD_COLUMNS,
        sampling_k=sampling_k,
        sampling_target=sampling_target,
        sampling_max=sampling_max,
        sampling_reports=sampling_reports,
        sampling_report_file=sampling_report_file,
        sampling_batch_origin_id=sampling_batch_origin_id,
        sampling_batch_report=sampling_batch_report,
        sampling_batch_results=sampling_batch_results,
        sampling_batch_detail=sampling_batch_detail,
        sampling_batch_detail_error=sampling_batch_detail_error,
        sampling_batch_columns=sampling_batch_columns,
        sampling_batch_error=sampling_batch_error,
        monitor_report=monitor_report,
        plasma_origin_id=plasma_origin_id,
        plasma_report=plasma_report,
        plasma_coverage=plasma_coverage,
        plasma_regimes=plasma_regimes,
        plasma_cluster=plasma_cluster,
        plasma_pca=plasma_pca,
        plasma_selection=plasma_selection,
        plasma_bin_labels=plasma_bin_labels,
        plasma_columns=plasma_columns,
        plasma_missing_columns=plasma_missing_columns,
        plasma_k=plasma_k,
        plasma_target=plasma_target,
        plasma_max=plasma_max,
        eqp_method=eqp_method,
        eqp_target=eqp_target,
        eqp_max=eqp_max,
        eqp_coverage_enabled=eqp_coverage_enabled,
        eqp_ion_tprim_min=eqp_ion_tprim_min,
        eqp_columns=EQUIL_PLASMA_COLUMNS,
        eqp_report=eqp_report,
        eqp_coverage=eqp_coverage,
        eqp_selection=eqp_selection,
        eqp_status_counts=eqp_status_counts,
        actions=ACTIONS,
        action_status=get_action_state(),
        ai_suggestions=ai_suggestions,
        have_numpy=HAVE_NUMPY,
        error=None,
    )


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
