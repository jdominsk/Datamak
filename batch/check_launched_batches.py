#!/usr/bin/env python3
import argparse
import sqlite3
import subprocess
from pathlib import Path
import os
import json


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))


def parse_remote(remote_folder: str, remote_host: str) -> tuple[str, str]:
    remote_folder = remote_folder or ""
    remote_host = remote_host or ""
    if ":" in remote_folder:
        host, path = remote_folder.split(":", 1)
        return host, path
    if remote_host:
        return remote_host, remote_folder
    raise ValueError(f"Invalid remote_folder: {remote_folder}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check LAUNCHED gk_batch runs for finished gk_run rows."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--status",
        default="SUCCESS,CONVERGED",
        help="Comma-separated statuses to treat as finished (default: SUCCESS,CONVERGED).",
    )
    parser.add_argument(
        "--batch",
        action="append",
        default=[],
        help="Limit to specific batch database name(s). Can be used multiple times.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SSH timeout in seconds per host.",
    )
    parser.add_argument(
        "--remote-check",
        action="store_true",
        help="Query remote batch databases (defaults to local listing only).",
    )
    parser.add_argument(
        "--dump-gk-run",
        action="store_true",
        help="Dump full gk_run content from remote batch databases.",
    )
    parser.add_argument(
        "--sync-plots",
        action="store_true",
        help="Rsync growth rate plots from remote batch folders.",
    )
    parser.add_argument(
        "--plots-dir",
        default=str(ROOT_DIR / "batch" / "plots"),
        help="Local folder to store synced plots.",
    )
    parser.add_argument(
        "--plots-limit",
        type=int,
        default=50,
        help="Maximum number of plot files to sync per batch database.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    batch_filter = {name.strip() for name in args.batch if name.strip()}
    rows = conn.execute(
        """
        SELECT batch_database_name, remote_folder, remote_host
        FROM gk_batch
        WHERE status IN ('LAUNCHED', 'SYNCED')
        """
    ).fetchall()
    conn.close()
    if batch_filter:
        rows = [row for row in rows if row["batch_database_name"] in batch_filter]

    if not rows:
        if batch_filter:
            print("No matching LAUNCHED gk_batch rows found for batch filter.")
        else:
            print("No LAUNCHED gk_batch rows found.")
        return

    if not args.remote_check:
        for row in rows:
            remote_folder = row["remote_folder"] or ""
            try:
                remote_host = row["remote_host"] or ""
            except Exception:
                remote_host = ""
            if not remote_host and remote_folder and ":" in remote_folder:
                remote_host = remote_folder.split(":", 1)[0]
            warning = ""
            if not remote_folder or (not remote_host and ":" not in remote_folder):
                warning = " WARNING: missing remote info"
            print(
                f"{row['batch_database_name']}: "
                f"status=LAUNCHED, "
                f"remote_folder={remote_folder}, "
                f"remote_host={remote_host}"
                f"{warning}"
            )
        return

    by_host: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        db_name = row["batch_database_name"]
        remote_folder = row["remote_folder"] or ""
        remote_host = ""
        try:
            remote_host = row["remote_host"] or ""
        except Exception:
            remote_host = ""
        try:
            host, remote_path = parse_remote(remote_folder, remote_host)
        except ValueError as exc:
            print(f"{db_name}: {exc}")
            continue
        by_host.setdefault(host, []).append((db_name, remote_path))

    run_columns = [
        "remote_id",
        "gk_input_id",
        "gk_batch_id",
        "input_folder",
        "job_folder",
        "archive_folder",
        "input_name",
        "nb_nodes",
        "job_id",
        "status",
        "input_content",
        "t_max",
        "nb_restart",
        "ky_abs_mean",
        "gamma_max",
        "diffusion",
    ]

    def ensure_local_gk_run(conn: sqlite3.Connection) -> None:
        columns_info = conn.execute("PRAGMA table_info(gk_run)").fetchall()
        columns = {row[1] for row in columns_info}
        if columns and next((row for row in columns_info if row[1] == "id"), None):
            id_pk = next(row[5] for row in columns_info if row[1] == "id")
            if id_pk == 0:
                row_count = conn.execute("SELECT COUNT(*) FROM gk_run").fetchone()[0]
                if row_count == 0:
                    conn.execute("DROP TABLE gk_run")
                    columns = set()
                else:
                    raise SystemExit(
                        "Local gk_run has non-PK id with data; migration required."
                    )

        if not columns:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gk_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    remote_id INTEGER NOT NULL,
                    gk_input_id INTEGER,
                    gk_batch_id INTEGER,
                    input_folder TEXT,
                    job_folder TEXT,
                    archive_folder TEXT,
                    input_name TEXT,
                    nb_nodes INTEGER,
                    job_id TEXT,
                    status TEXT,
                    input_content TEXT,
                    t_max REAL,
                    nb_restart INTEGER,
                    ky_abs_mean REAL,
                    gamma_max REAL,
                    diffusion REAL,
                    remote_host TEXT,
                    remote_folder TEXT,
                    creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
        if "remote_id" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN remote_id INTEGER NOT NULL DEFAULT 0")
        if "gk_batch_id" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN gk_batch_id INTEGER")
        if "t_max" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN t_max REAL")
        if "nb_restart" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN nb_restart INTEGER")
        if "ky_abs_mean" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN ky_abs_mean REAL")
        if "gamma_max" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN gamma_max REAL")
        if "diffusion" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN diffusion REAL")
        if "remote_host" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN remote_host TEXT")
        if "remote_folder" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN remote_folder TEXT")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_gk_run_remote_id_batch
            ON gk_run (remote_id, gk_batch_id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_gk_run_id_batch
            ON gk_run (id, gk_batch_id)
            """
        )

    local_conn = sqlite3.connect(args.db)
    try:
        ensure_local_gk_run(local_conn)
        for host, items in by_host.items():
            db_list = []
            for db_name, remote_path in items:
                remote_db = f"{remote_path.rstrip('/')}/{db_name}"
                db_list.append((db_name, remote_db))
            db_literal = repr(db_list)
            payload = f"""
set -euo pipefail
python3 - <<'PY'
import json
import os
import sqlite3

dbs = {db_literal}

for name, path in dbs:
    if not os.path.exists(path):
        print(f"MISSING\\t{{name}}\\t{{path}}")
        continue
    print(f"USING\\t{{name}}\\t{{path}}")
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        columns = {{row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}}
        if "synced" not in columns:
            conn.execute("ALTER TABLE gk_run ADD COLUMN synced INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        for row in conn.execute("SELECT * FROM gk_run WHERE synced = 0"):
            data = dict(row)
            print("ROW\\t" + name + "\\t" + json.dumps(data))
PY
"""
            print(f"Checking {len(items)} batch DB(s) on {host}...")
            result = subprocess.run(
                ["ssh", host, "bash", "-c", payload],
                text=True,
                capture_output=True,
                timeout=args.timeout,
            )
            if result.returncode != 0:
                if result.stderr:
                    print(result.stderr.strip())
                raise SystemExit(result.returncode)

            batch_rows: dict[str, list[dict]] = {}
            missing: list[tuple[str, str]] = []
            using: list[tuple[str, str]] = []
            for line in result.stdout.splitlines():
                if line.startswith("MISSING\t"):
                    _, db_name, path = line.split("\t", 2)
                    missing.append((db_name, path))
                    continue
                if line.startswith("USING\t"):
                    _, db_name, path = line.split("\t", 2)
                    using.append((db_name, path))
                    continue
                if not line.startswith("ROW\t"):
                    continue
                _, db_name, payload_json = line.split("\t", 2)
                batch_rows.setdefault(db_name, []).append(json.loads(payload_json))

            for db_name, path in using:
                print(f"using: {db_name} -> {path}")
            for db_name, path in missing:
                print(f"=== {db_name} ===")
                print(f"missing: {path}")

            missing_names = {name for name, _ in missing}
            for db_name, rows_data in batch_rows.items():
                remote_folder = next(
                    (p for n, p in items if n == db_name), ""
                )
                remote_path = remote_folder
                if remote_path.endswith(db_name):
                    remote_path = remote_path[: -len(db_name)].rstrip("/")
                gk_batch_id = local_conn.execute(
                    "SELECT id FROM gk_batch WHERE batch_database_name = ?",
                    (db_name,),
                ).fetchone()
                gk_batch_id_val = gk_batch_id[0] if gk_batch_id else None
                for row in rows_data:
                    local_conn.execute(
                        """
                        INSERT OR REPLACE INTO gk_run (
                            remote_id, gk_input_id, gk_batch_id, input_folder, job_folder, archive_folder,
                            input_name, nb_nodes, job_id, status, input_content,
                            t_max, nb_restart, ky_abs_mean, gamma_max, diffusion,
                            remote_host, remote_folder
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row.get("id"),
                            row.get("gk_input_id"),
                            gk_batch_id_val,
                            row.get("input_folder"),
                            row.get("job_folder"),
                            row.get("archive_folder"),
                            row.get("input_name"),
                            row.get("nb_nodes"),
                            row.get("job_id"),
                            row.get("status"),
                            row.get("input_content"),
                            row.get("t_max"),
                            row.get("nb_restart"),
                            row.get("ky_abs_mean"),
                            row.get("gamma_max"),
                            row.get("diffusion"),
                            host,
                            remote_path,
                        ),
                    )
                local_conn.commit()

                if args.dump_gk_run:
                    print(f"=== {db_name} ===")
                    cols = [c for c in run_columns if c != "input_content"]
                    widths = [len(c) for c in cols]
                    for row in rows_data:
                        for idx, col in enumerate(cols):
                            val = "" if row.get(col) is None else str(row.get(col))
                            widths[idx] = max(widths[idx], len(val))
                    header = "  ".join(col.ljust(widths[idx]) for idx, col in enumerate(cols))
                    print(header)
                    for row in rows_data:
                        line = "  ".join(
                            ("" if row.get(col) is None else str(row.get(col))).ljust(widths[idx])
                            for idx, col in enumerate(cols)
                        )
                        print(line.rstrip())
                else:
                    status_set = {s.strip() for s in args.status.split(",") if s.strip()}
                    count = sum(1 for r in rows_data if r.get("status") in status_set)
                    total = len(rows_data)
                    status_counts: dict[str, int] = {}
                    for row in rows_data:
                        status = str(row.get("status") or "").strip() or "UNKNOWN"
                        status_counts[status] = status_counts.get(status, 0) + 1
                    status_summary = ", ".join(
                        f"{status}={status_counts[status]}"
                        for status in sorted(status_counts)
                    )
                    print(
                        f"{db_name}: fetched_unsynced={total}, "
                        f"finished({args.status})={count}, "
                        f"status_counts: {status_summary}"
                    )

            if args.sync_plots:
                plots_root = Path(args.plots_dir)
                plots_root.mkdir(parents=True, exist_ok=True)
                for db_name, remote_path in items:
                    target_dir = plots_root / db_name.replace(".db", "")
                    target_dir.mkdir(parents=True, exist_ok=True)
                    rows_data = batch_rows.get(db_name, [])
                    basenames = set()
                    for row in rows_data:
                        input_name = (row.get("input_name") or "").strip()
                        if not input_name:
                            continue
                        stem = input_name
                        if stem.endswith(".in"):
                            stem = stem[: -len(".in")]
                        basenames.add(f"{stem}_growth_rate.png")
                        basenames.add(f"{stem}_gamma_vs_ky.png")
                    if basenames:
                        basenames = sorted(basenames)
                        if args.plots_limit > 0:
                            basenames = basenames[: args.plots_limit]
                        print(
                            f"{db_name}: syncing pngs for {len(basenames)} updated runs "
                            f"from {host}:{remote_path} -> {target_dir}"
                        )
                        list_payload = f"""
python3 - <<'PY'
import json
import os

remote_path = {json.dumps(remote_path)}
basenames = {json.dumps(basenames)}
existing = []
for name in basenames:
    if os.path.exists(os.path.join(remote_path, name)):
        existing.append(name)
for name in existing:
    print(name)
PY
"""
                    else:
                        print(
                            f"{db_name}: syncing up to {args.plots_limit} png files "
                            f"from {host}:{remote_path} -> {target_dir}"
                        )
                        list_payload = f"""
python3 - <<'PY'
import glob
import os

remote_path = {json.dumps(remote_path)}
limit = {int(args.plots_limit)}
patterns = ("*_growth_rate.png", "*_gamma_vs_ky.png")
files = []
for pattern in patterns:
    files.extend(glob.glob(os.path.join(remote_path, pattern)))
files = sorted(set(files))
if limit > 0:
    files = files[:limit]
for path in files:
    print(os.path.basename(path))
PY
"""
                    list_result = subprocess.run(
                        ["ssh", host, "bash", "-c", list_payload],
                        text=True,
                        capture_output=True,
                        timeout=args.timeout,
                    )
                    if list_result.returncode != 0:
                        if list_result.stderr:
                            print(list_result.stderr.strip())
                        print(f"{db_name}: plot list failed.")
                        continue
                    basenames = [
                        line.strip()
                        for line in list_result.stdout.splitlines()
                        if line.strip()
                    ]
                    if not basenames:
                        print(f"{db_name}: no plot files found.")
                        continue
                    rsync_cmd = [
                        "rsync",
                        "-av",
                        "--update",
                        "--no-perms",
                        "--no-owner",
                        "--no-group",
                        "--files-from=-",
                        f"{host}:{remote_path.rstrip('/')}/",
                        str(target_dir) + "/",
                    ]
                    rsync_input = "\n".join(basenames) + "\n"
                    rsync_result = subprocess.run(
                        rsync_cmd,
                        input=rsync_input,
                        text=True,
                        capture_output=True,
                        timeout=args.timeout,
                    )
                    if rsync_result.returncode != 0:
                        if rsync_result.stderr:
                            print(rsync_result.stderr.strip())
                        print(f"{db_name}: rsync failed.")
                        continue
                    print(f"{db_name}: rsynced {len(basenames)} png files.")

            if batch_rows:
                ids_by_db = {
                    name: [row["id"] for row in rows]
                    for name, rows in batch_rows.items()
                }
                db_paths = {
                    db_name: f"{remote_path.rstrip('/')}/{db_name}"
                    for db_name, remote_path in items
                }
                update_payload = f"""
set -euo pipefail
python3 - <<'PY'
import json
import sqlite3

db_paths = {json.dumps(db_paths)}
ids_by_db = {json.dumps(ids_by_db)}

for name, ids in ids_by_db.items():
    if not ids:
        continue
    path = db_paths.get(name)
    if not path:
        continue
    conn = sqlite3.connect(path)
    try:
        conn.executemany("UPDATE gk_run SET synced = 1 WHERE id = ?", [(i,) for i in ids])
        conn.commit()
    finally:
        conn.close()
PY
"""
                result = subprocess.run(
                    ["ssh", host, "bash", "-c", update_payload],
                    text=True,
                    capture_output=True,
                    timeout=args.timeout,
                )
                if result.returncode != 0:
                    if result.stderr:
                        print(result.stderr.strip())
                    raise SystemExit(result.returncode)

            for db_name, _ in items:
                rows_data = batch_rows.get(db_name, [])
                if rows_data:
                    print(f"{db_name}: synchronizing {len(rows_data)} runs")
                else:
                    print(f"{db_name}: already synced")
                if db_name not in missing_names:
                    local_conn.execute(
                        "UPDATE gk_batch SET status = 'SYNCED' WHERE batch_database_name = ?",
                        (db_name,),
                    )
                    local_conn.commit()
    finally:
        local_conn.close()

if __name__ == "__main__":
    main()
