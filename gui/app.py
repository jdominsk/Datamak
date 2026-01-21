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
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from flask import Flask, redirect, render_template, request, send_from_directory, url_for

APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(APP_DIR))
DEFAULT_DB = os.path.join(PROJECT_DIR, "gyrokinetic_simulations.db")
DB_UPDATE_DIR = os.path.join(PROJECT_DIR, "db_update")
DOCS_DIR = os.path.join(PROJECT_DIR, "docs")
BATCH_BASE_DIR = os.path.join(PROJECT_DIR, "batch")
BATCH_NEW_DIR = os.path.join(BATCH_BASE_DIR, "new")
BATCH_SENT_DIR = os.path.join(BATCH_BASE_DIR, "sent")
AI_FEEDBACK_PATH = os.path.join(APP_DIR, "ai_feedback.json")
AI_FEEDBACK_LOCK = threading.Lock()

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
}

ACTION_LOCK = threading.Lock()
ACTION_STATE: Dict[str, Optional[str]] = {
    "running": False,
    "name": None,
    "message": None,
}

app = Flask(__name__, static_folder="logo")


@app.route("/docs/<path:filename>")
def docs_file(filename: str):
    return send_from_directory(DOCS_DIR, filename)


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


def get_data_origins(conn: sqlite3.Connection) -> List[Tuple[int, str]]:
    rows = conn.execute(
        "SELECT id, name FROM data_origin ORDER BY id"
    ).fetchall()
    return [(int(row["id"]), str(row["name"])) for row in rows]


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
        if sent > 0:
            suggestions.append(
                {
                    "id": "batch_sent",
                    "text": (
                        f"{sent} batch DB(s) are SENT. "
                        "Consider preparing newbatch/run scripts remotely."
                    ),
                }
            )
        if launched > 0:
            suggestions.append(
                {
                    "id": "batch_launched",
                    "text": (
                        f"{launched} batch DB(s) are LAUNCHED. "
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
    "electron_temp_ev",
    "electron_tprim",
    "electron_fprim",
    "electron_vnewk",
    "ion_z",
    "ion_mass",
    "ion_dens",
    "ion_temp",
    "ion_temp_ev",
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


def get_gk_input_points(
    conn: sqlite3.Connection, x_col: str, y_col: str, origin_id: Optional[int]
) -> List[dict]:
    base_query = f"""
        SELECT {x_col}, {y_col}, do.name
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
    for x_val, y_val, origin_name in rows:
        color = "#1f77b4"
        if str(origin_name).startswith("Alexei Transp 09"):
            color = "#d62728"
        points.append({"x": float(x_val), "y": float(y_val), "color": color})
    return points


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
    only_finished: bool,
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
            SELECT gk_input.{x_col}, {y_expr}, do.name
            FROM gk_run
            JOIN gk_input ON gk_input.id = gk_run.gk_input_id
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            JOIN data_origin AS do ON do.id = data_equil.data_origin_id
            WHERE gk_input.{x_col} IS NOT NULL AND {y_expr} IS NOT NULL
        """
    else:
        base_query = f"""
            SELECT gk_input.{x_col}, {y_expr}, do.name
            FROM gk_input
            JOIN gk_study ON gk_study.id = gk_input.gk_study_id
            JOIN data_equil ON data_equil.id = gk_study.data_equil_id
            JOIN data_origin AS do ON do.id = data_equil.data_origin_id
            WHERE gk_input.{x_col} IS NOT NULL AND {y_expr} IS NOT NULL
        """
    params = []
    if use_gk_run and only_finished:
        base_query += " AND gk_run.status IN ('SUCCESS', 'CONVERGED')"
    if origin_id is not None:
        base_query += " AND data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    points = []
    has_non_finite = False
    for x_val, y_val, origin_name in rows:
        if not math.isfinite(float(x_val)) or not math.isfinite(float(y_val)):
            has_non_finite = True
            continue
        color = "#1f77b4"
        if str(origin_name) == "Alexei Transp 09 (full-auto)":
            color = "#2ca02c"
        elif str(origin_name).startswith("Alexei Transp 09"):
            color = "#d62728"
        points.append({"x": float(x_val), "y": float(y_val), "color": color})
    return points, has_non_finite


def get_action_state() -> Dict[str, Optional[str]]:
    with ACTION_LOCK:
        return dict(ACTION_STATE)


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


def _run_action(action_name: str, script_path: str, db_path: Optional[str]) -> None:
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


def _start_action(action_name: str, db_path: str):
    action = ACTIONS.get(action_name)
    if action is None:
        with ACTION_LOCK:
            ACTION_STATE["message"] = f"Unknown action '{action_name}'."
        return redirect(url_for("index", panel="action", db=db_path))
    with ACTION_LOCK:
        if ACTION_STATE["running"]:
            current = ACTION_STATE["name"] or "another action"
            ACTION_STATE["message"] = f"Action '{current}' is already running."
            return redirect(url_for("index", panel="action", db=db_path))
        ACTION_STATE["running"] = True
        ACTION_STATE["name"] = action["label"]
        ACTION_STATE["message"] = f"Action '{action['label']}' is running."
    thread = threading.Thread(
        target=_run_action, args=(action["label"], action["script"], db_path), daemon=True
    )
    thread.start()
    return redirect(url_for("index", panel="action", db=db_path))


@app.route("/action/<action_name>", methods=["POST"])
def run_action(action_name: str):
    db_path = request.form.get("db", DEFAULT_DB)
    return _start_action(action_name, db_path)


@app.route("/suggestion_action", methods=["POST"])
def suggestion_action():
    db_path = request.form.get("db", DEFAULT_DB)
    action_name = request.form.get("action_name", "")
    suggestion_id = request.form.get("suggestion_id", "")
    if suggestion_id and action_name:
        record_ai_feedback(suggestion_id, action_name)
    return _start_action(action_name, db_path)


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
    return redirect(url_for("index", panel=panel, db=db_path, table=table))


@app.route("/edit_gk_input", methods=["POST"])
def edit_gk_input():
    db_path = request.form.get("db", DEFAULT_DB)
    gk_input_id = request.form.get("gk_input_id", "").strip()
    action = request.form.get("action", "load")
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
    finished_flags = request.args.getlist("results_only_finished")
    if finished_flags:
        results_only_finished = "1" in finished_flags
    else:
        results_only_finished = False
    selected_batch_db = request.args.get("batch_db")
    batch_view = request.args.get("batch_view", "new")
    edit_gk_input_id = request.args.get("gk_input_id")
    edit_message = request.args.get("edit_message")
    edit_error = request.args.get("edit_error")
    edit_warning = request.args.get("edit_warning")
    edit_status = request.args.get("edit_status")
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
    results_x2_min = _parse_limit(request.args.get("results_x2_min"))
    results_x2_max = _parse_limit(request.args.get("results_x2_max"))
    results_y2_min = _parse_limit(request.args.get("results_y2_min"))
    results_y2_max = _parse_limit(request.args.get("results_y2_max"))
    results_x3_min = _parse_limit(request.args.get("results_x3_min"))
    results_x3_max = _parse_limit(request.args.get("results_x3_max"))
    results_y3_min = _parse_limit(request.args.get("results_y3_min"))
    results_y3_max = _parse_limit(request.args.get("results_y3_max"))
    results_x4_min = _parse_limit(request.args.get("results_x4_min"))
    results_x4_max = _parse_limit(request.args.get("results_x4_max"))
    results_y4_min = _parse_limit(request.args.get("results_y4_min"))
    results_y4_max = _parse_limit(request.args.get("results_y4_max"))
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
    data_origins: List[Tuple[int, str]] = []

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
            results_only_finished=results_only_finished,
            results_y_col=results_y_col,
            results_y2_col=results_y2_col,
            results_y3_col=results_y3_col,
            results_y4_col=results_y4_col,
            results_x_min=results_x_min,
            results_x_max=results_x_max,
            results_y_min=results_y_min,
            results_y_max=results_y_max,
            results_x2_min=results_x2_min,
            results_x2_max=results_x2_max,
            results_y2_min=results_y2_min,
            results_y2_max=results_y2_max,
            results_x3_min=results_x3_min,
            results_x3_max=results_x3_max,
            results_y3_min=results_y3_min,
            results_y3_max=results_y3_max,
            results_x4_min=results_x4_min,
            results_x4_max=results_x4_max,
            results_y4_min=results_y4_min,
            results_y4_max=results_y4_max,
            results_columns=build_results_columns(),
            results_report=None,
            data_origins=[],
            selected_origin_id=origin_id,
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
            error=f"Database not found: {db_path}",
        )

    conn = get_connection(db_path)
    try:
        tables = list_tables(conn)
        ai_feedback = load_ai_feedback()
        ai_suggestions = get_ai_suggestions(conn, ai_feedback)
        table_total_count = 0
        table_filtered_count = 0
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
        if "gk_input" in tables:
            stats_points = get_gk_input_points(conn, x_col, y_col, origin_id)
            stats_points_2 = get_gk_input_points(conn, x2_col, y2_col, origin_id)
            stats_points_3 = get_gk_input_points(conn, x3_col, y3_col, origin_id)
            stats_points_4 = get_gk_input_points(conn, x4_col, y4_col, origin_id)
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
                conn, x_col, results_y_col, results_only_finished, origin_id
            )
            results_points_2, results_warn_2 = get_gk_run_results_points(
                conn, x2_col, results_y2_col, results_only_finished, origin_id
            )
            results_points_3, results_warn_3 = get_gk_run_results_points(
                conn, x3_col, results_y3_col, results_only_finished, origin_id
            )
            results_points_4, results_warn_4 = get_gk_run_results_points(
                conn, x4_col, results_y4_col, results_only_finished, origin_id
            )
            results_report = get_gamma_max_status_report(conn, origin_id)
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
        results_only_finished=results_only_finished,
        results_y_col=results_y_col,
        results_y2_col=results_y2_col,
        results_y3_col=results_y3_col,
        results_y4_col=results_y4_col,
        results_x_min=results_x_min,
        results_x_max=results_x_max,
        results_y_min=results_y_min,
        results_y_max=results_y_max,
        results_x2_min=results_x2_min,
        results_x2_max=results_x2_max,
        results_y2_min=results_y2_min,
        results_y2_max=results_y2_max,
        results_x3_min=results_x3_min,
        results_x3_max=results_x3_max,
        results_y3_min=results_y3_min,
        results_y3_max=results_y3_max,
        results_x4_min=results_x4_min,
        results_x4_max=results_x4_max,
        results_y4_min=results_y4_min,
        results_y4_max=results_y4_max,
        results_columns=results_columns,
        results_report=results_report,
        data_origins=data_origins,
        selected_origin_id=origin_id,
        actions=ACTIONS,
        action_status=get_action_state(),
        ai_suggestions=ai_suggestions,
        error=None,
    )


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
