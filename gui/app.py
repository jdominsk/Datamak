#!/usr/bin/env python3
import os
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


@app.route("/", methods=["GET"])
def index():
    db_path = request.args.get("db", DEFAULT_DB)
    selected_table = request.args.get("table")
    selected_panel = request.args.get("panel", "statistics")
    only_active = request.args.get("only_active") == "1"
    selected_batch_db = request.args.get("batch_db")
    batch_view = request.args.get("batch_view", "new")
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
    batch_dir = BATCH_NEW_DIR if batch_view == "new" else BATCH_SENT_DIR
    batch_dbs = list_batch_databases(batch_dir)
    batch_error: Optional[str] = None
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
