#!/usr/bin/env python3
import os
import re
import sqlite3
import subprocess
import sys
import threading
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

ACTIONS = {
    "populate_mate": {
        "label": "Populate",
        "script": os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Mate_KinEFIT.py"),
    },
    "populate_alexei": {
        "label": "Populate",
        "script": os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_09.py"),
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
    "create_batch_db": {
        "label": "Create Batch DB",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "create_batch_database.py"),
        "args": ["--copy-torun"],
        "use_db": True,
    },
    "deploy_batch_db": {
        "label": "Deploy Batch DB",
        "script": os.path.join(os.path.dirname(APP_DIR), "batch", "deploy_batch.py"),
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
    conn: sqlite3.Connection, table: str, only_active: bool
) -> Tuple[List[str], List[sqlite3.Row]]:
    columns = [
        row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    ]
    if only_active and "active" in columns:
        rows = conn.execute(f"SELECT * FROM {table} WHERE active = 1").fetchall()
    else:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return columns, rows


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
        if origin_name == "Alexei Transp 09":
            color = "#d62728"
        points.append({"x": float(x_val), "y": float(y_val), "color": color})
    return points


def get_action_state() -> Dict[str, Optional[str]]:
    with ACTION_LOCK:
        return dict(ACTION_STATE)


def _run_action(action_name: str, script_path: str, db_path: Optional[str]) -> None:
    try:
        script_args: List[str] = []
        use_db = False
        for action in ACTIONS.values():
            if action.get("script") == script_path:
                script_args = action.get("args", [])
                use_db = action.get("use_db", False)
                break
        if use_db and db_path:
            script_args = [*script_args, "--source-db", db_path]
        subprocess.run([sys.executable, script_path, *script_args], check=True)
    except Exception as exc:
        message = f"Action '{action_name}' failed: {exc}"
    else:
        message = None
    with ACTION_LOCK:
        ACTION_STATE["running"] = False
        ACTION_STATE["name"] = None
        ACTION_STATE["message"] = message


@app.route("/action/<action_name>", methods=["POST"])
def run_action(action_name: str):
    action = ACTIONS.get(action_name)
    db_path = request.form.get("db", DEFAULT_DB)
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
    selected_batch_db = request.args.get("batch_db")
    batch_view = request.args.get("batch_view", "new")
    edit_gk_input_id = request.args.get("gk_input_id")
    edit_message = request.args.get("edit_message")
    edit_error = request.args.get("edit_error")
    edit_warning = request.args.get("edit_warning")
    edit_status = request.args.get("edit_status")
    origin_id_raw = request.args.get("origin_id")
    origin_id = int(origin_id_raw) if origin_id_raw and origin_id_raw.isdigit() else None
    x_col = request.args.get("x_col", "qinp")
    y_col = request.args.get("y_col", "ion_vnewk")
    x2_col = request.args.get("x2_col", "shat")
    y2_col = request.args.get("y2_col", "ion_vnewk")
    x3_col = request.args.get("x3_col", "akappa")
    y3_col = request.args.get("y3_col", "ion_vnewk")
    x4_col = request.args.get("x4_col", "betaprim")
    y4_col = request.args.get("y4_col", "ion_vnewk")
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
            actions=ACTIONS,
            action_status=get_action_state(),
            error=f"Database not found: {db_path}",
        )

    conn = get_connection(db_path)
    try:
        tables = list_tables(conn)
        if selected_table not in tables:
            selected_table = tables[0] if tables else None
        if selected_table:
            columns, rows = get_table_rows(conn, selected_table, only_active)
        if "data_origin" in tables:
            data_origins = get_data_origins(conn)
        if "gk_input" in tables:
            stats_points = get_gk_input_points(conn, x_col, y_col, origin_id)
            stats_points_2 = get_gk_input_points(conn, x2_col, y2_col, origin_id)
            stats_points_3 = get_gk_input_points(conn, x3_col, y3_col, origin_id)
            stats_points_4 = get_gk_input_points(conn, x4_col, y4_col, origin_id)
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
            gk_model_columns, gk_model_rows = get_table_rows(conn, "gk_model", False)
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
                    batch_columns, batch_rows = get_table_rows(
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
        data_origins=data_origins,
        selected_origin_id=origin_id,
        actions=ACTIONS,
        action_status=get_action_state(),
        error=None,
    )


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
