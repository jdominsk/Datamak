#!/usr/bin/env python3
import argparse
import gc
import os
import random
import re
import sqlite3
import sys
import tempfile
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyrokinetics as pk

ROOT_DIR = os.environ.get(
    "DTWIN_ROOT",
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dtwin_config import resolve_flux_profile  # noqa: E402


def _default_tmp_dir(base_dir: str) -> str:
    if not base_dir:
        return ""
    parent = Path(base_dir).expanduser().parent
    return str(parent / "tmp_inputs")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate GX inputs on Flux and store into gk_input.content."
    )
    parser.add_argument("--db", default="", help="Flux temp DB path.")
    parser.add_argument(
        "--template-dir",
        default="",
        help="Template directory (defaults to <db_dir>/templates).",
    )
    parser.add_argument(
        "--tmp-dir",
        default="",
        help="Temporary directory for generated GX inputs.",
    )
    parser.add_argument("--status", default="WAIT")
    parser.add_argument("--max-mem-gb", type=float, default=1.0)
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--randomize", action="store_true")
    parser.add_argument("--no-randomize", action="store_false", dest="randomize")
    parser.add_argument(
        "--enforce-local-quasineutrality",
        type=int,
        default=1,
        choices=[0, 1],
    )
    parser.add_argument(
        "--qn-modify-species",
        default="electron",
    )
    parser.set_defaults(randomize=True)
    return parser.parse_args()


def get_rss_gb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        kb = float(parts[1])
                        return kb / (1024 * 1024)
    except OSError:
        return 0.0
    return 0.0


def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def drop_gx_nkx_nky(content: str) -> Tuple[str, bool]:
    lines = content.splitlines()
    header_idx = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "[dimensions]":
            header_idx = idx
            break
    if header_idx is None:
        return content, False
    changed = False
    out = []
    for idx, line in enumerate(lines):
        if idx <= header_idx:
            out.append(line)
            continue
        if line.strip().startswith("[") and line.strip().endswith("]"):
            out.extend(lines[idx:])
            break
        if re.match(r"^\s*nkx\s*=", line) or re.match(r"^\s*nky\s*=", line):
            changed = True
            continue
        out.append(line)
    if not changed:
        return content, False
    return "\n".join(out) + ("\n" if content.endswith("\n") else ""), True


def adjust_gx_input_for_adiabatic(content: str) -> Tuple[str, bool]:
    lines = content.splitlines()
    nspecies_idx = None
    nspecies_value = None
    nspecies_re = re.compile(r"^\s*nspecies\s*=\s*(\d+)\s*$")
    beta_re = re.compile(r"^\s*beta\s*=\s*")
    fapar_re = re.compile(r"^\s*fapar\s*=\s*")
    fbpar_re = re.compile(r"^\s*fbpar\s*=\s*")
    for idx, line in enumerate(lines):
        match = nspecies_re.match(line)
        if match:
            nspecies_idx = idx
            nspecies_value = int(match.group(1))
            break
    if nspecies_idx is None or nspecies_value is None:
        return content, False
    new_nspecies = max(1, nspecies_value - 1)
    changed = False
    if new_nspecies != nspecies_value:
        lines[nspecies_idx] = f"nspecies = {new_nspecies}"
        changed = True
    for idx, line in enumerate(lines):
        if beta_re.match(line):
            lines[idx] = "beta = 0.0"
            changed = True
            continue
        if fapar_re.match(line):
            lines[idx] = "fapar = 1.0"
            changed = True
            continue
        if fbpar_re.match(line):
            lines[idx] = "fbpar = 1.0"
            changed = True
            continue
    if not changed:
        return content, False
    return ("\n".join(lines) + ("\n" if content.endswith("\n") else "")), True


def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: Tuple[object, ...] = (),
    retries: int = 10,
    delay: float = 0.2,
    backoff: float = 1.5,
) -> None:
    sleep_time = delay
    for attempt in range(retries):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if conn.in_transaction:
                conn.rollback()
            if attempt >= retries - 1:
                raise
            time.sleep(sleep_time)
            sleep_time *= backoff


def commit_with_retry(
    conn: sqlite3.Connection,
    retries: int = 10,
    delay: float = 0.2,
    backoff: float = 1.5,
) -> None:
    sleep_time = delay
    for attempt in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if conn.in_transaction:
                conn.rollback()
            if attempt >= retries - 1:
                raise
            time.sleep(sleep_time)
            sleep_time *= backoff


def claim_equilibrium_rows(
    conn: sqlite3.Connection, randomize: bool
) -> Tuple[Optional[Dict[str, object]], List[Dict[str, object]]]:
    order = "RANDOM()" if randomize else "de.id"
    if conn.in_transaction:
        commit_with_retry(conn)
    sleep_time = 0.2
    for attempt in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            equil = conn.execute(
                f"""
                SELECT de.id AS data_equil_id,
                       de.shot_number,
                       de.shot_variant,
                       de.shot_time
                FROM gk_input AS gi
                JOIN gk_study AS gs ON gs.id = gi.gk_study_id
                JOIN data_equil AS de ON de.id = gs.data_equil_id
                WHERE gi.status = 'NEW'
                  AND (gi.content IS NULL OR gi.content = '')
                GROUP BY de.id
                ORDER BY {order}
                LIMIT 1
                """
            ).fetchone()
            if not equil:
                commit_with_retry(conn)
                return None, []
            data_equil_id = int(equil["data_equil_id"])
            ids = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT gi.id
                    FROM gk_input AS gi
                    JOIN gk_study AS gs ON gs.id = gi.gk_study_id
                    WHERE gs.data_equil_id = ?
                      AND gi.status = 'NEW'
                      AND (gi.content IS NULL OR gi.content = '')
                    ORDER BY gi.id
                    """,
                    (data_equil_id,),
                ).fetchall()
            ]
            if not ids:
                commit_with_retry(conn)
                return None, []
            conn.executemany(
                "UPDATE gk_input SET status = 'WAIT' WHERE id = ?",
                [(int(i),) for i in ids],
            )
            commit_with_retry(conn)
            break
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if conn.in_transaction:
                conn.rollback()
            if attempt >= 9:
                raise
            time.sleep(sleep_time)
            sleep_time *= 1.5
    else:
        return None, []
    placeholders = ", ".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT gi.id, gi.gk_study_id, gi.gk_model_id, gi.psin,
               gm.input_template,
               de.id AS data_equil_id,
               de.shot_number,
               de.shot_variant,
               de.shot_time,
               de.folder_path, de.transpfile,
               gc.name AS gk_code
        FROM gk_input AS gi
        JOIN gk_model AS gm ON gm.id = gi.gk_model_id
        JOIN gk_study AS gs ON gs.id = gi.gk_study_id
        JOIN data_equil AS de ON de.id = gs.data_equil_id
        JOIN gk_code AS gc ON gc.id = gs.gk_code_id
        WHERE gi.id IN ({placeholders})
        ORDER BY gi.id
        """,
        ids,
    ).fetchall()
    return dict(equil), [dict(row) for row in rows]


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def process_rows(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    rows: List[Dict[str, object]],
    processed_rows: int,
    total_rows: int,
    max_rows: int,
    start_time: float,
) -> Tuple[int, bool]:
    stop_early = False
    db_dir = os.path.dirname(os.path.abspath(args.db))
    template_dir = args.template_dir or os.path.join(db_dir, "templates")
    os.makedirs(args.tmp_dir, exist_ok=True)
    if not rows:
        return processed_rows, stop_early

    first = rows[0]
    transpfile = str(first["transpfile"])
    folder_path = str(first["folder_path"])
    transp_time = first["shot_time"]
    template_name = str(first["input_template"])

    if transp_time is None:
        for row in rows:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                ("Missing shot_time for data_equil.", int(row["id"])),
            )
        return processed_rows, stop_early

    transpfile_path = os.path.join(folder_path, transpfile)
    if not os.path.isfile(transpfile_path):
        for row in rows:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"Missing CDF: {transpfile_path}", int(row["id"])),
            )
        return processed_rows, stop_early

    template_path = os.path.join(template_dir, template_name)
    if not os.path.isfile(template_path):
        for row in rows:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"Template missing: {template_path}", int(row["id"])),
            )
        return processed_rows, stop_early

    log(
        "Initializing equilibrium: "
        f"transpfile={transpfile} shot_time={transp_time} template={template_name}"
    )
    init_start = time.monotonic()
    try:
        with warnings.catch_warnings(record=True) as caught_init:
            warnings.simplefilter("always")
            pyro_transp = pk.Pyro(
                eq_file=transpfile_path,
                eq_type="TRANSP",
                eq_kwargs={"time": float(transp_time), "neighbors": 256},
                kinetics_file=transpfile_path,
                kinetics_type="TRANSP",
                kinetics_kwargs={"time": float(transp_time)},
                gk_file=template_path,
            )
        init_elapsed = time.monotonic() - init_start
        log(f"Equilibrium init finished in {init_elapsed:.1f}s")
        if caught_init:
            warning_msgs = [str(w.message).strip() for w in caught_init if str(w.message).strip()]
            if warning_msgs:
                comment_init = "warnings: " + "; ".join(warning_msgs)
            else:
                comment_init = ""
        else:
            comment_init = ""
    except Exception as exc:
        for row in rows:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"WARNING: error: {exc}", int(row["id"])),
            )
        return processed_rows, stop_early

    for row in rows:
        if stop_early:
            break
        row_id = int(row["id"])
        psin = float(row["psin"])
        comment_parts: List[str] = []
        if comment_init:
            comment_parts.append(comment_init)

        tmp_name = f"gk_input_{row_id}.in"
        tmp_path = os.path.join(args.tmp_dir, tmp_name)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                pyro_transp.load_local(psi_n=psin, local_geometry="Miller", show_fit=False)
            if not pyro_transp.local_species.check_quasineutrality():
                if args.enforce_local_quasineutrality == 1:
                    modify_species = args.qn_modify_species
                    if modify_species not in pyro_transp.local_species.names:
                        if "electron" in pyro_transp.local_species.names:
                            modify_species = "electron"
                        else:
                            modify_species = pyro_transp.local_species.names[0]
                    pyro_transp.local_species.enforce_quasineutrality(modify_species)
                    comment_parts.append(
                        f"enforced local quasineutrality on {modify_species}"
                    )
                else:
                    raise SystemExit("LocalSpecies not quasineutral.")
            with warnings.catch_warnings(record=True) as caught_write:
                warnings.simplefilter("always")
                pyro_transp.write_gk_file(file_name=tmp_path, gk_code=str(row["gk_code"]))
            caught.extend(caught_write)
            if caught:
                warning_msgs = [str(w.message).strip() for w in caught if str(w.message).strip()]
                if warning_msgs:
                    comment_parts.append("warnings: " + "; ".join(warning_msgs))
            content = read_file(tmp_path)
            content, dropped = drop_gx_nkx_nky(content)
            if dropped:
                with open(tmp_path, "w", encoding="utf-8") as handle:
                    handle.write(content)
                comment_parts.append("removed nkx/nky from Dimensions")
            if "adiabe" in os.path.basename(template_path).lower():
                content, adjusted = adjust_gx_input_for_adiabatic(content)
                if adjusted:
                    with open(tmp_path, "w", encoding="utf-8") as handle:
                        handle.write(content)
                    comment_parts.append("adiabatic adjustments: nspecies, beta, fapar, fbpar")
            status = args.status
        except Exception as exc:
            status = "CRASHED"
            content = ""
            comment_parts.append(f"error: {exc}")

        comment = ""
        if comment_parts:
            comment = "WARNING: " + "; ".join(comment_parts)
        execute_with_retry(
            conn,
            """
            UPDATE gk_input
            SET status = ?,
                file_name = ?,
                file_path = ?,
                content = ?,
                comment = ?
            WHERE id = ?
            """,
            (status, tmp_name, tmp_path, content, comment, row_id),
        )
        processed_rows += 1
        elapsed = time.monotonic() - start_time
        avg = elapsed / processed_rows if processed_rows else 0.0
        remaining = avg * (total_rows - processed_rows)
        rss_gb = get_rss_gb()
        print(
            f"progress {processed_rows}/{max_rows} ({total_rows}): "
            f"row_id={row_id} psin={psin:.3f} elapsed={elapsed:.1f}s "
            f"eta={remaining:.1f}s rss={rss_gb:.2f}GB",
            flush=True,
        )
        if processed_rows % 100 == 0:
            commit_with_retry(conn)
            gc.collect()
        if rss_gb and rss_gb >= args.max_mem_gb:
            print(
                f"Stopping early: RSS {rss_gb:.2f}GB exceeds limit "
                f"{args.max_mem_gb:.2f}GB."
            )
            stop_early = True
        if processed_rows >= max_rows:
            stop_early = True
        gc.collect()
    del pyro_transp
    return processed_rows, stop_early


def main() -> None:
    args = parse_args()
    flux = resolve_flux_profile()
    flux_base_dir = str(flux.get("base_dir") or "").strip()
    args.db = (args.db or "").strip() or os.path.join(flux_base_dir, "flux_equil_inputs.db")
    args.tmp_dir = (args.tmp_dir or "").strip() or _default_tmp_dir(flux_base_dir)
    if not args.db:
        raise SystemExit("Flux DB path is empty. Provide --db or configure Flux base dir.")
    if not args.tmp_dir:
        raise SystemExit("Flux tmp dir is empty. Provide --tmp-dir or configure Flux base dir.")
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    os.makedirs(args.tmp_dir, exist_ok=True)
    if args.loop and args.batch_size <= 0:
        raise SystemExit("--loop requires --batch-size")

    conn = sqlite3.connect(args.db, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 60000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        processed_rows = 0
        start_time = time.monotonic()
        if args.batch_size and args.batch_size > 0:
            total_rows = conn.execute(
                """
                SELECT COUNT(*)
                FROM gk_input
                WHERE status = 'NEW'
                  AND (content IS NULL OR content = '')
                """
            ).fetchone()[0]
            if total_rows == 0:
                print("No NEW gk_input rows found.")
                return
            max_rows = (
                args.max_rows if args.max_rows and args.max_rows > 0 else total_rows
            )
            while True:
                if processed_rows >= max_rows:
                    break
                batch_target = args.batch_size
                batch_done = 0
                while batch_done < batch_target and processed_rows < max_rows:
                    equil, rows = claim_equilibrium_rows(conn, args.randomize)
                    if not rows:
                        print("No NEW gk_input rows found.")
                        return
                    if equil:
                        print(
                            "Selected equil: "
                            f"shot={equil['shot_number']} "
                            f"variant={equil['shot_variant']} "
                            f"time={equil['shot_time']}"
                        )
                    processed_rows, stop_early = process_rows(
                        conn,
                        args,
                        rows,
                        processed_rows,
                        total_rows,
                        max_rows,
                        start_time,
                    )
                    batch_done += len(rows)
                    if stop_early:
                        break
                if stop_early:
                    break
                if not args.loop:
                    break
        else:
            total_rows = conn.execute(
                """
                SELECT COUNT(*)
                FROM gk_input
                WHERE status = 'NEW'
                  AND (content IS NULL OR content = '')
                """
            ).fetchone()[0]
            if total_rows == 0:
                print("No NEW gk_input rows found.")
                return
            max_rows = (
                args.max_rows if args.max_rows and args.max_rows > 0 else total_rows
            )
            while processed_rows < max_rows:
                equil, rows = claim_equilibrium_rows(conn, args.randomize)
                if not rows:
                    print("No NEW gk_input rows found.")
                    return
                if equil:
                    print(
                        "Selected equil: "
                        f"shot={equil['shot_number']} "
                        f"variant={equil['shot_variant']} "
                        f"time={equil['shot_time']}"
                    )
                processed_rows, stop_early = process_rows(
                    conn,
                    args,
                    rows,
                    processed_rows,
                    total_rows,
                    max_rows,
                    start_time,
                )
                if stop_early:
                    break
        commit_with_retry(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
