#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import subprocess
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from ssh_utils import build_ssh_base_args, get_ssh_connect_timeout, get_ssh_identity_file


ROOT_DIR = Path(os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[1]))
SSH_CONNECT_TIMEOUT = 10
SSH_PREFLIGHT_TIMEOUT = 12


def check_ssh_ready(host: str, connect_timeout: int) -> Optional[str]:
    ssh_args = build_ssh_base_args(host, connect_timeout)
    try:
        result = subprocess.run(
            [*ssh_args, "true"],
            text=True,
            capture_output=True,
            timeout=SSH_PREFLIGHT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return f"ssh preflight timed out after {SSH_PREFLIGHT_TIMEOUT}s (check sshproxy)"
    except Exception as exc:
        return f"ssh preflight error: {exc}"
    if result.returncode != 0:
        err = (result.stderr or "").strip()
        return err or "ssh preflight failed (check sshproxy)"
    return None


def emit_error_summary(errors: List[str]) -> None:
    if not errors:
        return
    print("Monitor errors:")
    for err in errors:
        print(f"- {err}")


def parse_remote(remote_folder: str, remote_host: str) -> tuple[str, str]:
    remote_folder = remote_folder or ""
    remote_host = remote_host or ""
    if ":" in remote_folder:
        host, path = remote_folder.split(":", 1)
        return host, path
    if remote_host:
        return remote_host, remote_folder
    raise ValueError(f"Invalid remote_folder: {remote_folder}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a remote monitor report for batch runs."
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Local gyrokinetic database path.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="SSH timeout in seconds per host.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "db_analysis" / "remote_monitor_report.json"),
        help="Path to write report JSON.",
    )
    parser.add_argument(
        "--run-analyze",
        action="store_true",
        help="Run gx_analyze.py on SUCCESS runs missing convergence.",
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("DTWIN_REMOTE_USER", os.environ.get("USER", "")),
        help="Username to query with squeue on the remote host.",
    )
    args = parser.parse_args()
    identity = get_ssh_identity_file()
    if identity:
        print(f"Using ssh identity: {identity}")
    else:
        print("No ssh identity file found (DTWIN_SSH_IDENTITY or ~/.ssh/nersc).")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, batch_database_name, remote_folder, remote_host
        FROM gk_batch
        WHERE status IN ('LAUNCHED', 'SYNCED')
        """
    ).fetchall()
    origin_map: dict[int, list[str]] = {}
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {"gk_run", "gk_input", "gk_study", "data_equil", "data_origin"}
        if required.issubset(tables):
            origin_rows = conn.execute(
                """
                SELECT gk_batch.id AS batch_id, data_origin.name AS origin_name
                FROM gk_batch
                JOIN gk_run ON gk_run.gk_batch_id = gk_batch.id
                JOIN gk_input ON gk_input.id = gk_run.gk_input_id
                JOIN gk_study ON gk_study.id = gk_input.gk_study_id
                JOIN data_equil ON data_equil.id = gk_study.data_equil_id
                JOIN data_origin ON data_origin.id = data_equil.data_origin_id
                WHERE gk_batch.status IN ('LAUNCHED', 'SYNCED')
                GROUP BY gk_batch.id, data_origin.name
                """
            ).fetchall()
            for row in origin_rows:
                batch_id = int(row["batch_id"])
                name = str(row["origin_name"] or "").strip()
                if not name:
                    continue
                origin_map.setdefault(batch_id, []).append(name)
            for batch_id, names in origin_map.items():
                origin_map[batch_id] = sorted(set(names))
    except Exception:
        origin_map = {}
    finally:
        conn.close()

    try:
        local_db_epoch = os.path.getmtime(args.db)
    except Exception:
        local_db_epoch = None

    report = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": args.db,
        "batches": [],
        "errors": [],
        "jobs": [],
        "jobs_debug": [],
    }

    if not rows:
        report["errors"].append("No LAUNCHED/SYNCED gk_batch rows found.")
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        return 0

    by_host: dict[str, list[tuple[str, str]]] = {}
    batch_id_map: dict[str, int] = {}
    for row in rows:
        batch_id = int(row["id"])
        db_name = row["batch_database_name"]
        batch_id_map[db_name] = batch_id
        remote_folder = row["remote_folder"] or ""
        remote_host = ""
        try:
            remote_host = row["remote_host"] or ""
        except Exception:
            remote_host = ""
        try:
            host, remote_path = parse_remote(remote_folder, remote_host)
        except ValueError as exc:
            report["errors"].append(f"{db_name}: {exc}")
            continue
        by_host.setdefault(host, []).append((db_name, remote_path))

    run_analyze_flag = "1" if args.run_analyze else "0"
    squeue_user = args.user or ""
    for host, items in by_host.items():
        db_list = []
        for db_name, remote_path in items:
            remote_db = f"{remote_path.rstrip('/')}/{db_name}"
            db_list.append((db_name, remote_db, remote_path))
        db_literal = repr(db_list)
        payload = """
set -euo pipefail
python3 - <<'PY'
import json
import os
import re
import sqlite3
import subprocess
import time
import datetime as _dt

dbs = <<<DBS>>>
run_analyze = int(<<<RUN_ANALYZE>>>) == 1
squeue_user = <<<SQUEUE_USER>>>

def tail_lines(path, limit=12):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.read().splitlines()
        tail = [line for line in lines if line.strip()][-limit:]
        return tail
    except Exception:
        return []

def latest_run_timestamp(base_dir):
    import glob
    patterns = ["*.out.nc", "*.log", "*.err"]
    candidates = []
    for pattern in patterns:
        candidates.extend((pattern, p) for p in glob.glob(os.path.join(base_dir, pattern)))
    if not candidates:
        return "", "", ""
    newest = None
    newest_ts = None
    newest_pattern = ""
    for pattern, path in candidates:
        try:
            mtime = os.path.getmtime(path)
        except Exception:
            continue
        if newest_ts is None or mtime > newest_ts:
            newest_ts = mtime
            newest = path
            newest_pattern = pattern
    if not newest:
        return "", "", ""
    try:
        import datetime as _dt
        ts = _dt.datetime.utcfromtimestamp(newest_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        ts = ""
    return ts, newest_pattern, newest


def db_last_modified(db_path):
    try:
        mtime = os.path.getmtime(db_path)
    except Exception:
        return "", None
    try:
        import datetime as _dt
        return _dt.datetime.utcfromtimestamp(mtime).strftime(
            "%B %d, %Y at %H:%M UTC"
        ), float(mtime)
    except Exception:
        return "", float(mtime) if "mtime" in locals() else None
def classify_error(lines):
    text = "\\n".join(lines).lower()
    if "toml::syntax_error" in text or "parse_key_value_pair" in text:
        return "toml_syntax_error"
    if "time limit" in text or "cancelled at" in text:
        return "time_limit"
    if "segmentation fault" in text:
        return "segfault"
    if "floating point exception" in text:
        return "floating_point"
    if "killed" in text or "oom" in text:
        return "killed_or_oom"
    return ""

def run_gx_analyze(db_path, run_id, nc_path, base_dir):
    script_path = os.path.join(base_dir, "gx_analyze.py")
    if not os.path.exists(script_path):
        return False, "gx_analyze.py not found"
    cmd = ["python3", script_path, db_path, str(run_id), nc_path, "--save-plot"]
    try:
        subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
        return True, ""
    except subprocess.CalledProcessError as exc:
        return False, (exc.stderr or exc.stdout or "").strip()

def get_jobs():
    jobs = []
    debug = {"cmd": "", "stdout": "", "stderr": "", "error": ""}
    try:
        user = squeue_user.strip().strip('"')
        cmd = ["squeue", "-h", "-o", "%i|%T|%j|%Z"]
        if user:
            cmd = ["squeue", "-u", user, "-h", "-o", "%i|%T|%j|%Z"]
        debug["cmd"] = " ".join(cmd)
        debug["timestamp"] = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        start = time.time()
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
        debug["duration_sec"] = round(time.time() - start, 3)
        debug["stdout"] = (result.stdout or "").strip()
        debug["stderr"] = (result.stderr or "").strip()
        lines = [j.strip() for j in result.stdout.splitlines() if j.strip()]
    except Exception as exc:
        debug["error"] = str(exc)
        return jobs, debug
    for line in lines:
        parts = line.split("|", 3)
        if len(parts) < 4:
            continue
        job_id, state, name, workdir = parts
        jobs.append({
            "job_id": job_id.strip(),
            "state": state.strip(),
            "name": name.strip(),
            "workdir": workdir.strip(),
        })
    return jobs, debug

report_batches = []
all_jobs, jobs_debug = get_jobs()

def job_matches_batch(job, batch_name, base_dir):
    workdir = job.get("workdir") or ""
    if not workdir:
        return False
    if workdir.startswith(base_dir):
        return True
    try:
        return os.path.exists(os.path.join(workdir, batch_name))
    except Exception:
        return False

def format_job_state_message(jobs):
    counts = {}
    for job in jobs:
        state = (job.get("state") or "").strip() or "UNKNOWN"
        counts[state] = counts.get(state, 0) + 1
    if not counts:
        return ""
    if len(counts) == 1:
        state, count = next(iter(counts.items()))
        if count == 1:
            return f"Wait, there is a {state} SLURM job for this batch."
        return f"Wait, there are {count} {state} SLURM jobs for this batch."
    parts = []
    for state in sorted(counts):
        parts.append(f"{state}={counts[state]}")
    return "Wait, SLURM jobs for this batch are in states: " + ", ".join(parts) + "."

for name, db_path, base_dir in dbs:
    if not os.path.exists(db_path):
        report_batches.append({
            "batch": name,
            "base_dir": base_dir,
            "error": f"missing db {db_path}",
        })
        continue
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        have_conv = "gk_convergence_timeseries" in tables

        rows = conn.execute(
            "SELECT id, status, input_name, ky_abs_mean, gamma_max, diffusion FROM gk_run"
        ).fetchall()
        unsynced_count = 0
        try:
            unsynced_count = int(
                conn.execute("SELECT COUNT(*) FROM gk_run WHERE synced = 0").fetchone()[0]
            )
        except Exception:
            unsynced_count = 0

        if run_analyze:
            for row in rows:
                status = str(row["status"] or "")
                if status != "SUCCESS":
                    continue
                input_name = row["input_name"] or ""
                if not input_name:
                    continue
                nc_path = os.path.join(base_dir, input_name.replace(".in", ".out.nc"))
                if os.path.exists(nc_path):
                    run_gx_analyze(db_path, int(row["id"]), nc_path, base_dir)

            rows = conn.execute(
                "SELECT id, status, input_name, ky_abs_mean, gamma_max, diffusion FROM gk_run"
            ).fetchall()

        status_counts = {}
        failures = []
        running_logs = []
        running_log_missing = 0
        restarts = []
        pending_analysis = []
        gamma_values = []
        failure_ids = []
        for row in rows:
            status = str(row["status"] or "")
            status_counts[status] = status_counts.get(status, 0) + 1
            if row["gamma_max"] is not None:
                try:
                    gamma_values.append(float(row["gamma_max"]))
                except Exception:
                    pass
            run_id = int(row["id"])
            input_name = row["input_name"] or ""
            if status in ("CRASHED", "ERROR", "INTERRUPTED"):
                err_path = os.path.join(base_dir, input_name.replace(".in", ".err")) if input_name else ""
                tail = tail_lines(err_path)
                failures.append({
                    "run_id": run_id,
                    "status": status,
                    "err_path": err_path,
                    "error_type": classify_error(tail),
                    "tail": tail,
                })
                failure_ids.append(run_id)
            if status == "RUNNING":
                log_path = os.path.join(base_dir, input_name.replace(".in", ".log")) if input_name else ""
                tail = tail_lines(log_path)
                if tail:
                    running_logs.append({
                        "run_id": run_id,
                        "status": status,
                        "log_path": log_path,
                        "tail": tail,
                    })
                else:
                    running_log_missing += 1
            if status == "RESTART":
                restarts.append(run_id)
            if status == "SUCCESS" and row["gamma_max"] is None:
                pending_analysis.append(run_id)

        gamma_summary = {}
        if gamma_values:
            gamma_values = sorted(gamma_values)
            gamma_summary = {
                "count": len(gamma_values),
                "min": gamma_values[0],
                "median": gamma_values[len(gamma_values)//2],
                "max": gamma_values[-1],
            }

        gamma_mean_summary = {}
        if have_conv:
            try:
                vals = [
                    float(r[0]) for r in conn.execute(
                        "SELECT gamma_mean FROM gk_convergence_timeseries "
                        "WHERE gamma_mean IS NOT NULL"
                    ).fetchall()
                ]
            except Exception:
                vals = []
            if vals:
                vals = sorted(vals)
                gamma_mean_summary = {
                    "count": len(vals),
                    "min": vals[0],
                    "median": vals[len(vals)//2],
                    "max": vals[-1],
                }

        last_ts, last_src, last_file = latest_run_timestamp(base_dir)
        db_ts, db_epoch = db_last_modified(db_path)
        suggestions = []
        torun_count = status_counts.get("TORUN", 0)
        restart_count = len(restarts)
        failure_count = len(failures)
        pending_count = len(pending_analysis)
        matched_jobs = [j for j in all_jobs if job_matches_batch(j, name, base_dir)]
        has_active_job = bool(matched_jobs)
        running_count = status_counts.get("RUNNING", 0)
        running_without_job = running_count > 0 and not has_active_job
        if has_active_job:
            message = format_job_state_message(matched_jobs)
            if message:
                suggestions.append(message)
            else:
                suggestions.append("Wait, SLURM job(s) are present for this batch.")
        if running_without_job:
            suggestions.append(
                "No active SLURM job found but RUNNING rows exist. Suggest marking them as INTERRUPTED after checking logs."
            )
        if unsynced_count > 0:
            suggestions.append("Sync remote batch DB to local.")
        if pending_count > 0:
            suggestions.append(
                f"Run gx_analyze on {pending_count} SUCCESS runs missing gamma_max."
            )
        can_launch_job = not has_active_job and (torun_count > 0 or restart_count > 0)
        if can_launch_job:
            parts = []
            if torun_count > 0:
                parts.append(f"{torun_count} TORUN")
            if restart_count > 0:
                parts.append(f"{restart_count} RESTART")
            suggestions.append(f"Launch SLURM job for {', '.join(parts)} runs.")
        if failure_count > 0:
            suggestions.append("Inspect failures")

        report_batches.append({
            "batch": name,
            "base_dir": base_dir,
            "db_last_modified": db_ts,
            "db_last_modified_epoch": db_epoch,
            "last_run_time": last_ts,
            "last_run_source": last_src,
            "last_run_file": last_file,
            "status_counts": status_counts,
            "running_without_job": running_without_job,
            "unsynced_count": unsynced_count,
            "can_launch_job": can_launch_job,
            "failures": failures,
            "failure_ids": failure_ids,
            "running_logs": running_logs,
            "running_log_missing": running_log_missing,
            "restart_needed": restarts,
            "pending_analysis": pending_analysis,
            "gamma_max_summary": gamma_summary,
            "gamma_mean_summary": gamma_mean_summary,
            "suggestions": suggestions,
            "jobs": matched_jobs,
        })

print(json.dumps({"batches": report_batches, "jobs": all_jobs, "jobs_debug": jobs_debug}))
PY
"""
        payload = payload.replace("<<<DBS>>>", db_literal).replace(
            "<<<RUN_ANALYZE>>>", run_analyze_flag
        ).replace("<<<SQUEUE_USER>>>", json.dumps(squeue_user))
        connect_timeout = get_ssh_connect_timeout(
            min(SSH_CONNECT_TIMEOUT, max(1, args.timeout))
        )
        preflight_error = check_ssh_ready(host, connect_timeout)
        if preflight_error:
            report["errors"].append(f"{host}: {preflight_error}")
            continue
        ssh_cmd = [*build_ssh_base_args(host, connect_timeout), "bash", "-s"]
        try:
            result = subprocess.run(
                ssh_cmd,
                input=payload,
                text=True,
                capture_output=True,
                timeout=args.timeout,
            )
        except subprocess.TimeoutExpired:
            report["errors"].append(
                f"{host}: ssh timed out after {args.timeout}s (check sshproxy)"
            )
            continue
        except Exception as exc:
            report["errors"].append(f"{host}: ssh error: {exc}")
            continue
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            report["errors"].append(
                f"{host}: {err or 'remote command failed (check sshproxy)'}"
            )
            continue
        raw_lines = [line for line in result.stdout.splitlines() if line.strip()]
        json_line = ""
        for line in reversed(raw_lines):
            if line.lstrip().startswith("{") or line.lstrip().startswith("["):
                json_line = line
                break
        if not json_line:
            report["errors"].append(f"{host}: no JSON payload from remote")
            continue
        try:
            payload_data = json.loads(json_line)
        except json.JSONDecodeError:
            report["errors"].append(f"{host}: invalid JSON from remote")
            continue
        if isinstance(payload_data, dict):
            batches = payload_data.get("batches", [])
            jobs_debug = payload_data.get("jobs_debug", {})
            report["jobs_debug"].append({"host": host, "detail": jobs_debug})
            cmd = jobs_debug.get("cmd")
            timestamp = jobs_debug.get("timestamp")
            duration = jobs_debug.get("duration_sec")
            if cmd:
                when = f" at {timestamp}" if timestamp else ""
                took = f" ({duration}s)" if duration is not None else ""
                print(f"{host}: squeue{when}{took} -> {cmd}")
            for job in payload_data.get("jobs", []):
                job["remote_host"] = host
                report.setdefault("jobs", []).append(job)
        else:
            batches = payload_data
        for item in batches:
            item["remote_host"] = host
            item["batch_id"] = batch_id_map.get(item.get("batch"), 0)
            origins = origin_map.get(item["batch_id"], [])
            item["origin_names"] = origins
            if isinstance(item.get("suggestions"), list):
                item["suggestions"] = [
                    s for s in item["suggestions"] if s != "Sync remote batch DB to local."
                ]
            report["batches"].append(item)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    report["batches"] = sorted(
        report["batches"],
        key=lambda item: item.get("db_last_modified_epoch") or 0,
        reverse=True,
    )
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    emit_error_summary(report.get("errors", []))
    print(f"Wrote report to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
