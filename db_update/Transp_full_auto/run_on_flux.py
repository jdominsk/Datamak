#!/usr/bin/env python3
import argparse
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional


ROOT_DIR = Path(
    os.environ.get("DTWIN_ROOT", Path(__file__).resolve().parents[2])
).resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import resolve_flux_profile  # noqa: E402
from database.create_gyrokinetic_db import ensure_flux_action_log_schema  # noqa: E402


SCRIPT_DIR = Path(__file__).resolve().parent
STEP1_SCRIPT = SCRIPT_DIR / "MainSteps_1_launch_on_laptop.sh"
LOCAL_STAGE_DIR = ROOT_DIR / "transp_full_auto"
PYRO_TEMPLATE_DIR = ROOT_DIR / "pyrokinetics"
REMOTE_STEP2_WRAPPER = "flux/run_mainsteps2_slurm.sh"
SSH_WITH_DUO = ROOT_DIR / "tools" / "ssh_with_duo.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare or reuse a staged Flux DB for a full-auto TRANSP origin "
            "and submit MainSteps_2_launch_on_flux.sh through Slurm."
        )
    )
    parser.add_argument(
        "--db",
        default=str(ROOT_DIR / "gyrokinetic_simulations.db"),
        help="Main Datamak database.",
    )
    parser.add_argument("--origin-id", type=int, help="data_origin.id to process.")
    parser.add_argument("--origin-name", default="", help="Fallback data_origin.name.")
    parser.add_argument("--slurm-partition", default="all", help="Slurm partition.")
    parser.add_argument("--slurm-time", default="04:00:00", help="Slurm walltime.")
    parser.add_argument("--slurm-mem", default="8G", help="Slurm memory request.")
    return parser.parse_args()


def _ssh_control_options() -> list[str]:
    return [
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPersist=10m",
        "-o",
        f"ControlPath={Path.home() / '.ssh' / 'cm-%r@%h:%p'}",
    ]


def _ssh_command(flux_profile: Dict[str, str], *args: str) -> list[str]:
    duo_option = str(flux_profile.get("duo_option") or "").strip()
    if duo_option:
        return [
            sys.executable,
            str(SSH_WITH_DUO),
            "--duo-option",
            duo_option,
            "--",
            "ssh",
            *args,
        ]
    return ["ssh", *args]


def _prime_ssh(remote_host: str, flux_profile: Dict[str, str]) -> None:
    subprocess.run(
        _ssh_command(flux_profile, *_ssh_control_options(), "-t", remote_host, "true"),
        check=True,
    )


def _rsync_ssh_command() -> str:
    return shlex.join(["ssh", *_ssh_control_options()])


def load_origin(conn: sqlite3.Connection, origin_id: Optional[int], origin_name: str) -> Dict[str, str]:
    if origin_id is not None:
        row = conn.execute(
            """
            SELECT id, name, origin, file_type
            FROM data_origin
            WHERE id = ?
            """,
            (origin_id,),
        ).fetchone()
        if row:
            return {
                "id": str(int(row[0])),
                "name": str(row[1] or ""),
                "origin": str(row[2] or ""),
                "file_type": str(row[3] or ""),
            }
    if origin_name.strip():
        row = conn.execute(
            """
            SELECT id, name, origin, file_type
            FROM data_origin
            WHERE name = ?
            """,
            (origin_name.strip(),),
        ).fetchone()
        if row:
            return {
                "id": str(int(row[0])),
                "name": str(row[1] or ""),
                "origin": str(row[2] or ""),
                "file_type": str(row[3] or ""),
            }
    raise SystemExit("Selected data_origin was not found in the main DB.")


def latest_flux_action(
    conn: sqlite3.Connection, origin_id: int, origin_name: str
) -> Optional[Dict[str, str]]:
    try:
        row = conn.execute(
            """
            SELECT id, flux_db_name, remote_host, remote_dir, status, slurm_job_id,
                   submitted_at, synced_at, created_at
            FROM flux_action_log
            WHERE data_origin_id = ?
               OR data_origin_name = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (origin_id, origin_name),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return {
        "id": str(row[0] or ""),
        "flux_db_name": str(row[1] or ""),
        "remote_host": str(row[2] or ""),
        "remote_dir": str(row[3] or ""),
        "status": str(row[4] or ""),
        "slurm_job_id": str(row[5] or ""),
        "submitted_at": str(row[6] or ""),
        "synced_at": str(row[7] or ""),
        "created_at": str(row[8] or ""),
    }


def effective_remote_host(log_row: Optional[Dict[str, str]], flux_profile: Dict[str, str]) -> str:
    configured_remote = str(flux_profile.get("remote") or "").strip()
    if configured_remote:
        return configured_remote
    return str((log_row or {}).get("remote_host") or "").strip()


def refresh_flux_action_endpoint(
    main_db: str,
    log_id: int,
    remote_host: str,
    remote_dir: str,
) -> None:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        conn.execute(
            """
            UPDATE flux_action_log
            SET remote_host = ?, remote_dir = ?
            WHERE id = ?
            """,
            (remote_host, remote_dir, log_id),
        )
        conn.commit()


def write_runtime_env(path: Path, flux_profile: Dict[str, str]) -> None:
    lines = [
        f"export DTWIN_FLUX_USER={shlex.quote(str(flux_profile.get('user') or ''))}",
        f"export DTWIN_FLUX_HOST={shlex.quote(str(flux_profile.get('host') or ''))}",
        f"export DTWIN_FLUX_REMOTE={shlex.quote(str(flux_profile.get('remote') or ''))}",
        f"export DTWIN_FLUX_BASE_DIR={shlex.quote(str(flux_profile.get('base_dir') or ''))}",
        f"export DTWIN_FLUX_PYTHON={shlex.quote(str(flux_profile.get('python_bin') or ''))}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sync_runtime_support(remote_host: str, remote_dir: str, flux_profile: Dict[str, str]) -> None:
    LOCAL_STAGE_DIR.mkdir(parents=True, exist_ok=True)
    templates_dir = LOCAL_STAGE_DIR / "templates"
    templates_dir.mkdir(parents=True, exist_ok=True)
    for template in PYRO_TEMPLATE_DIR.glob("*.in"):
        shutil.copy2(template, templates_dir / template.name)

    shutil.copy2(SCRIPT_DIR / "MainSteps_2_launch_on_flux.sh", LOCAL_STAGE_DIR / "MainSteps_2_launch_on_flux.sh")
    shutil.copy2(SCRIPT_DIR / "build_flux_equil_inputs.py", LOCAL_STAGE_DIR / "build_flux_equil_inputs.py")
    shutil.copytree(SCRIPT_DIR / "flux", LOCAL_STAGE_DIR / "flux", dirs_exist_ok=True)
    write_runtime_env(LOCAL_STAGE_DIR / "datamak_runtime.env", flux_profile)

    _prime_ssh(remote_host, flux_profile)
    rsync_ssh = _rsync_ssh_command()
    subprocess.run(
        [
            "rsync",
            "-av",
            "-e",
            rsync_ssh,
            str(LOCAL_STAGE_DIR / "MainSteps_2_launch_on_flux.sh"),
            str(LOCAL_STAGE_DIR / "build_flux_equil_inputs.py"),
            str(LOCAL_STAGE_DIR / "datamak_runtime.env"),
            f"{remote_host}:{remote_dir}/",
        ],
        check=True,
    )
    subprocess.run(
        [
            "rsync",
            "-av",
            "-e",
            rsync_ssh,
            str(LOCAL_STAGE_DIR / "flux"),
            f"{remote_host}:{remote_dir}/",
        ],
        check=True,
    )
    subprocess.run(
        [
            "rsync",
            "-av",
            "-e",
            rsync_ssh,
            str(LOCAL_STAGE_DIR / "templates"),
            f"{remote_host}:{remote_dir}/",
        ],
        check=True,
    )


def run_stage_step(origin_id: int, origin_name: str) -> None:
    env = os.environ.copy()
    env["ORIGIN_ID"] = str(origin_id)
    env["ORIGIN_NAME"] = origin_name
    subprocess.run(["bash", str(STEP1_SCRIPT)], check=True, env=env)


def submit_remote_slurm(
    remote_host: str,
    remote_dir: str,
    remote_db_path: str,
    origin_id: int,
    origin_name: str,
    remote_path: str,
    partition: str,
    walltime: str,
    memory: str,
    flux_profile: Dict[str, str],
) -> str:
    command_lines = [
        "set -euo pipefail",
        "if [[ -f /etc/profile ]]; then source /etc/profile; fi",
        "if [[ -f ~/.bash_profile ]]; then source ~/.bash_profile; fi",
        "if [[ -f ~/.bashrc ]]; then source ~/.bashrc; fi",
        "if command -v module >/dev/null 2>&1; then module load slurm >/dev/null 2>&1 || true; fi",
        "if ! command -v sbatch >/dev/null 2>&1; then",
        "  for candidate in /usr/bin/sbatch /usr/local/bin/sbatch /opt/slurm/bin/sbatch /cm/shared/apps/slurm/current/bin/sbatch; do",
        "    if [[ -x \"$candidate\" ]]; then export PATH=\"$(dirname \"$candidate\"):$PATH\"; break; fi",
        "  done",
        "fi",
        "command -v sbatch >/dev/null 2>&1 || { echo 'sbatch not found on Flux login environment.' >&2; exit 127; }",
        f"cd {shlex.quote(remote_dir)}",
        f"test -f {shlex.quote(remote_db_path)}",
        f"test -x {shlex.quote(REMOTE_STEP2_WRAPPER)}",
        "mkdir -p logs",
        f"export ORIGIN_ID={shlex.quote(str(origin_id))}",
        f"export ORIGIN_NAME={shlex.quote(origin_name)}",
    ]
    if remote_path.strip():
        command_lines.append(f"export REMOTE_PATH={shlex.quote(remote_path)}")
    sbatch_parts = [
        "sbatch",
        "--parsable",
        "--export=ALL",
        "--partition",
        partition,
        "--time",
        walltime,
        "--mem",
        memory,
        REMOTE_STEP2_WRAPPER,
        remote_db_path,
    ]
    command_lines.append(f"job_id=$({shlex.join(sbatch_parts)})")
    command_lines.append(
        f"printf 'Submitted Flux step 2 job %s for {origin_name} using {remote_db_path}\\n' \"$job_id\""
    )
    command = [
        *_ssh_command(
            flux_profile,
            *_ssh_control_options(),
            "-t",
            remote_host,
            "bash",
            "-lc",
            "\n".join(command_lines),
        ),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, end="")
        if exc.stderr:
            print(exc.stderr, end="", file=sys.stderr)
        raise
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    match = re.search(r"Submitted Flux step 2 job (\S+)", completed.stdout or "")
    if not match:
        raise SystemExit("Flux job submission succeeded but no Slurm job id was reported.")
    return match.group(1)


def mark_flux_action_submitted(main_db: str, log_id: int, slurm_job_id: str) -> None:
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        conn.execute(
            """
            UPDATE flux_action_log
            SET status = 'SUBMITTED',
                slurm_job_id = ?,
                status_detail = 'PENDING',
                submitted_at = datetime('now'),
                status_checked_at = datetime('now'),
                synced_at = NULL
            WHERE id = ?
            """,
            (slurm_job_id, log_id),
        )
        conn.commit()


def run_for_origin(
    main_db: str,
    origin_id: Optional[int],
    origin_name: str,
    partition: str,
    walltime: str,
    memory: str,
) -> Dict[str, str]:
    flux_profile = resolve_flux_profile()
    with sqlite3.connect(main_db) as conn:
        ensure_flux_action_log_schema(conn)
        origin = load_origin(conn, origin_id, origin_name)
        origin_db_id = int(origin["id"])
        log_row = latest_flux_action(conn, origin_db_id, origin["name"])
    if log_row is not None and str(log_row.get("status") or "").upper() in {"SUBMITTED", "RUNNING"}:
        job_id = (log_row.get("slurm_job_id") or "").strip()
        job_fragment = f" (job {job_id})" if job_id else ""
        raise SystemExit(
            "A Flux run is already active for "
            f"{origin['name']}{job_fragment}. Use Sync Back From Flux before submitting again."
        )

    if log_row is None or not log_row.get("flux_db_name"):
        print(f"No staged Flux DB recorded for {origin['name']}; running step 1.")
        run_stage_step(origin_db_id, origin["name"])
        with sqlite3.connect(main_db) as conn:
            ensure_flux_action_log_schema(conn)
            log_row = latest_flux_action(conn, origin_db_id, origin["name"])
        if log_row is None or not log_row.get("flux_db_name"):
            raise SystemExit(
                "Step 1 completed but no flux_action_log entry was recorded for this origin."
            )
    else:
        print(
            f"Reusing staged Flux DB {log_row['flux_db_name']} "
            f"recorded on {log_row.get('created_at') or 'unknown date'}."
        )
        remote_host = effective_remote_host(log_row, flux_profile)
        remote_dir = log_row.get("remote_dir") or str(flux_profile.get("base_dir") or "")
        if not remote_host or not remote_dir:
            raise SystemExit("Flux runtime settings are incomplete for support sync.")
        log_id = int(log_row.get("id") or 0)
        if log_id > 0:
            refresh_flux_action_endpoint(main_db, log_id, remote_host, remote_dir)
            log_row["remote_host"] = remote_host
            log_row["remote_dir"] = remote_dir
        sync_runtime_support(remote_host, remote_dir, flux_profile)

    remote_host = effective_remote_host(log_row, flux_profile)
    remote_dir = log_row.get("remote_dir") or str(flux_profile.get("base_dir") or "")
    flux_db_name = log_row.get("flux_db_name") or ""
    if not remote_host or not remote_dir or not flux_db_name:
        raise SystemExit("Missing remote_host, remote_dir, or flux_db_name for Flux submission.")

    remote_db_path = str(Path(remote_dir) / flux_db_name)
    slurm_job_id = submit_remote_slurm(
        remote_host=remote_host,
        remote_dir=remote_dir,
        remote_db_path=remote_db_path,
        origin_id=origin_db_id,
        origin_name=origin["name"],
        remote_path=origin["origin"],
        partition=partition,
        walltime=walltime,
        memory=memory,
        flux_profile=flux_profile,
    )
    log_id = int(log_row.get("id") or 0)
    if log_id <= 0:
        raise SystemExit("Flux action log row is missing a valid id for submission tracking.")
    mark_flux_action_submitted(main_db, log_id, slurm_job_id)
    return {
        "origin_name": origin["name"],
        "remote_host": remote_host,
        "remote_dir": remote_dir,
        "flux_db_name": flux_db_name,
        "remote_db_path": remote_db_path,
        "slurm_job_id": slurm_job_id,
    }


def main() -> None:
    args = parse_args()
    result = run_for_origin(
        main_db=args.db,
        origin_id=args.origin_id,
        origin_name=args.origin_name,
        partition=args.slurm_partition,
        walltime=args.slurm_time,
        memory=args.slurm_mem,
    )
    print(
        f"Flux run submitted for {result['origin_name']} "
        f"using {result['remote_host']}:{result['remote_db_path']} "
        f"(job {result['slurm_job_id']})"
    )


if __name__ == "__main__":
    main()
