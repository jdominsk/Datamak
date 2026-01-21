#!/usr/bin/env python3
import argparse
import gc
import os
import random
import re
import sqlite3
import tempfile
import time
import warnings
from typing import Dict, List, Tuple

import pyrokinetics as pk


DEFAULT_DB = "/u/jdominsk/DTwin/transp_full_auto/flux_equil_inputs.db"
DEFAULT_TMP_DIR = "/u/jdominsk/DTwin/tmp_inputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate GX inputs on Flux and store into gk_input.content."
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Flux temp DB path.")
    parser.add_argument(
        "--template-dir",
        default="",
        help="Template directory (defaults to <db_dir>/templates).",
    )
    parser.add_argument(
        "--tmp-dir",
        default=DEFAULT_TMP_DIR,
        help="Temporary directory for generated GX inputs.",
    )
    parser.add_argument("--status", default="WAIT")
    parser.add_argument("--max-mem-gb", type=float, default=1.0)
    parser.add_argument("--max-rows", type=int, default=100)
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


def claim_rows(
    conn: sqlite3.Connection, batch_size: int, randomize: bool
) -> List[Dict[str, object]]:
    order = "RANDOM()" if randomize else "id"
    if conn.in_transaction:
        commit_with_retry(conn)
    sleep_time = 0.2
    for attempt in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            ids = [
                row[0]
                for row in conn.execute(
                    f"SELECT id FROM gk_input "
                    f"WHERE status = 'NEW' "
                    f"  AND (content IS NULL OR content = '') "
                    f"ORDER BY {order} LIMIT ?",
                    (batch_size,),
                ).fetchall()
            ]
            if not ids:
                commit_with_retry(conn)
                return []
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
        return []
    placeholders = ", ".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT gi.id, gi.gk_study_id, gi.gk_model_id, gi.psin,
               gm.input_template,
               de.folder_path, de.transpfile, de.shot_time,
               gc.name AS gk_code
        FROM gk_input AS gi
        JOIN gk_model AS gm ON gm.id = gi.gk_model_id
        JOIN gk_study AS gs ON gs.id = gi.gk_study_id
        JOIN data_equil AS de ON de.id = gs.data_equil_id
        JOIN gk_code AS gc ON gc.id = gs.gk_code_id
        WHERE gi.id IN ({placeholders})
        """,
        ids,
    ).fetchall()
    return [dict(row) for row in rows]


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
    for row in rows:
        if stop_early:
            break
        row_id = int(row["id"])
        psin = float(row["psin"])
        transpfile = str(row["transpfile"])
        folder_path = str(row["folder_path"])
        transp_time = row["shot_time"]
        if transp_time is None:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                ("Missing shot_time for data_equil.", row_id),
            )
            continue
        transpfile_path = os.path.join(folder_path, transpfile)
        if not os.path.isfile(transpfile_path):
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"Missing CDF: {transpfile_path}", row_id),
            )
            continue
        template_name = str(row["input_template"])
        template_path = os.path.join(template_dir, template_name)
        if not os.path.isfile(template_path):
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"Template missing: {template_path}", row_id),
            )
            continue

        comment_parts: List[str] = []
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
            if caught_init:
                warning_msgs = [str(w.message).strip() for w in caught_init if str(w.message).strip()]
                if warning_msgs:
                    comment_parts.append("warnings: " + "; ".join(warning_msgs))
        except Exception as exc:
            execute_with_retry(
                conn,
                "UPDATE gk_input SET status = 'CRASHED', comment = ? WHERE id = ?",
                (f"WARNING: error: {exc}", row_id),
            )
            continue

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
            f"eta={remaining:.1f}s rss={rss_gb:.2f}GB"
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
        del pyro_transp
        gc.collect()
    return processed_rows, stop_early


def main() -> None:
    args = parse_args()
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
                "SELECT COUNT(*) FROM gk_input WHERE status = 'NEW'"
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
                batch_size = args.batch_size
                remaining = max_rows - processed_rows
                if remaining < batch_size:
                    batch_size = remaining
                rows = claim_rows(conn, batch_size, args.randomize)
                if not rows:
                    print("No NEW gk_input rows found.")
                    break
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
                if not args.loop:
                    break
        else:
            rows = conn.execute(
                """
                SELECT gi.id, gi.gk_study_id, gi.gk_model_id, gi.psin,
                       gm.input_template,
                       de.folder_path, de.transpfile, de.shot_time,
                       gc.name AS gk_code
                FROM gk_input AS gi
                JOIN gk_model AS gm ON gm.id = gi.gk_model_id
                JOIN gk_study AS gs ON gs.id = gi.gk_study_id
                JOIN data_equil AS de ON de.id = gs.data_equil_id
                JOIN gk_code AS gc ON gc.id = gs.gk_code_id
                WHERE gi.status = 'NEW'
                ORDER BY gi.id
                """
            ).fetchall()
            if not rows:
                print("No NEW gk_input rows found.")
                return
            total_rows = len(rows)
            max_rows = (
                args.max_rows if args.max_rows and args.max_rows > 0 else total_rows
            )
            process_rows(
                conn,
                args,
                [dict(row) for row in rows],
                processed_rows,
                total_rows,
                max_rows,
                start_time,
            )
        commit_with_retry(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
