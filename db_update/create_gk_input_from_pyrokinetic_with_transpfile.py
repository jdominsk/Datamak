#!/usr/bin/env python3
import argparse
import os
import re
import sqlite3
import subprocess
from typing import Dict, List, Tuple

import pyrokinetics as pk


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".",
    "gyrokinetic_simulations.db",
)
DEFAULT_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "pyrokinetics",
)
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gk_inputs")
DEFAULT_REMOTE = "jdominsk@flux"
DEFAULT_ORIGIN_NAME = "Alexei Transp 09"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create gk_input entries for TRANSP CDF files using pyrokinetics.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to the SQLite database file.")
    parser.add_argument("--template-dir", default=DEFAULT_TEMPLATE_DIR, help="GK input templates.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output folder for GK inputs.")
    parser.add_argument("--file-prefix", default="gk_input_", help="Prefix for generated filenames.")
    parser.add_argument("--remote", default=DEFAULT_REMOTE, help="SSH host for remote copy.")
    parser.add_argument(
        "--origin-name",
        default=DEFAULT_ORIGIN_NAME,
        help="data_origin.name value to filter on.",
    )
    parser.add_argument("--psin-start", type=float, default=0.1)
    parser.add_argument("--psin-end", type=float, default=0.9)
    parser.add_argument("--psin-step", type=float, default=0.1)
    parser.add_argument("--time", type=float, default=0.5, help="TRANSP time to load.")
    parser.add_argument("--neighbors", type=int, default=256, help="TRANSP neighbors value.")
    parser.add_argument(
        "--status",
        default="WAIT",
        choices=["WAIT", "TORUN", "BATCH", "CRASHED", "SUCCESS"],
        help="Status for inserted gk_input rows.",
    )
    parser.add_argument(
        "--enforce-local-quasineutrality",
        type=int,
        default=1,
        choices=[0, 1],
        help="Call Pyrokinetics local_species.enforce_quasineutrality when needed.",
    )
    parser.add_argument(
        "--qn-modify-species",
        default="electron",
        help="Species name to modify when enforcing local quasineutrality.",
    )
    return parser.parse_args()


def psin_values(start: float, end: float, step: float) -> List[float]:
    values: List[float] = []
    current = start
    while current <= end + 1e-9:
        values.append(round(current, 10))
        current += step
    return values


def resolve_template(template_dir: str, is_linear: int, is_adiabatic_electron: int) -> str:
    linear_token = "linear" if is_linear == 1 else "nonlinear"
    electron_token = "adiabe" if is_adiabatic_electron == 1 else "kine"
    matches = []
    for name in os.listdir(template_dir):
        lower_name = name.lower()
        if "copy" in lower_name:
            continue
        if electron_token not in lower_name:
            continue
        if linear_token == "linear":
            if "nonlinear" in lower_name:
                continue
            if "linear" not in lower_name:
                continue
        else:
            if "nonlinear" not in lower_name:
                continue
        matches.append(os.path.join(template_dir, name))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise SystemExit(
            f"No template found for {linear_token}/{electron_token} in {template_dir}."
        )
    raise SystemExit(
        f"Multiple templates found for {linear_token}/{electron_token}: {matches}"
    )


def drop_gx_nkx_nky(content: str) -> tuple:
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
    else:
        out = out
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


def parse_geometry_fields(content: str) -> dict:
    lines = content.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "[geometry]":
            start = idx + 1
            break
    if start is None:
        return {}
    end = len(lines)
    for idx in range(start, len(lines)):
        if lines[idx].strip().startswith("[") and idx != start:
            end = idx
            break
    values = {}
    for line in lines[start:end]:
        if "=" not in line:
            continue
        key, raw_val = line.split("=", 1)
        key = key.strip()
        raw_val = raw_val.strip().strip('"').strip("'")
        if not key:
            continue
        if key == "geo_option":
            values["geo_option"] = raw_val
            continue
        try:
            values[key] = float(raw_val)
        except ValueError:
            continue
    return values


def parse_physics_fields(content: str) -> dict:
    lines = content.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == "[physics]":
            start = idx + 1
            break
    if start is None:
        return {}
    end = len(lines)
    for idx in range(start, len(lines)):
        if lines[idx].strip().startswith("[") and idx != start:
            end = idx
            break
    values = {}
    for line in lines[start:end]:
        if "=" not in line:
            continue
        key, raw_val = line.split("=", 1)
        key = key.strip()
        raw_val = raw_val.strip().strip('"').strip("'")
        if not key:
            continue
        try:
            values[key] = float(raw_val)
        except ValueError:
            continue
    return values


def parse_list_values(raw: str) -> list:
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return []
    items = []
    for item in raw[start + 1 : end].split(","):
        item = item.strip().strip('"').strip("'")
        if not item:
            continue
        try:
            items.append(float(item))
        except ValueError:
            items.append(item)
    return items


def parse_species_fields(content: str) -> dict:
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
    fields = {}
    for line in lines[start:end]:
        if "=" not in line:
            continue
        key, raw_val = line.split("=", 1)
        key = key.strip().lower()
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
    result = {}
    for label, idx in (("electron", electron_idx), ("ion", main_ion_idx)):
        if idx is None:
            continue
        for key in ("z", "mass", "dens", "temp", "tprim", "fprim", "vnewk"):
            values = fields.get(key, [])
            if idx < len(values):
                result[f"{label}_{key}"] = values[idx]
    return result


def temp_to_ev(value) -> float:
    try:
        return float(value.to("eV").m)
    except Exception:
        try:
            return float(value)
        except Exception:
            return None


def extract_local_temps_ev(pyro_obj) -> dict:
    electron_temp_ev = None
    ion_temp_ev = None
    ion_density_max = None
    for name in pyro_obj.local_species.names:
        species = pyro_obj.local_species[name]
        temp = species["temp"]
        dens = species["dens"]
        if name == "electron":
            electron_temp_ev = temp_to_ev(temp)
            continue
        try:
            dens_val = float(dens.m)
        except Exception:
            try:
                dens_val = float(dens)
            except Exception:
                dens_val = None
        if dens_val is None:
            continue
        if ion_density_max is None or dens_val > ion_density_max:
            ion_density_max = dens_val
            ion_temp_ev = temp_to_ev(temp)
    return {"electron_temp_ev": electron_temp_ev, "ion_temp_ev": ion_temp_ev}


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def existing_inputs(conn: sqlite3.Connection, gk_study_id: int) -> dict:
    rows = conn.execute(
        "SELECT id, psin, status, gk_model_id FROM gk_input WHERE gk_study_id = ?",
        (gk_study_id,),
    ).fetchall()
    inputs = {}
    for row_id, psin, status, gk_model_id in rows:
        key = (float(psin), gk_model_id)
        inputs[key] = (int(row_id), str(status))
    return inputs


def fetch_active_models(
    conn: sqlite3.Connection, template_dir: str
) -> List[Dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, is_linear, is_adiabatic, is_electrostatic, input_template
        FROM gk_model
        WHERE active = 1
        ORDER BY id
        """
    ).fetchall()
    if not rows:
        raise SystemExit("No active gk_model rows found.")
    seen = set()
    models: List[Dict[str, object]] = []
    for row in rows:
        model_id, is_linear, is_adiabatic, is_electrostatic, template_name = row
        is_adiabatic_electron = 1 if int(is_adiabatic) in (1, 2) else 0
        key = (int(is_linear), is_adiabatic_electron)
        if key in seen:
            print(
                "Warning: multiple active gk_model rows for "
                f"is_linear={key[0]} is_adiabatic_electron={key[1]}; "
                f"keeping model_id={model_id} and skipping duplicate."
            )
            continue
        template_path = os.path.join(template_dir, str(template_name))
        if not os.path.isfile(template_path):
            raise SystemExit(f"Template file not found: {template_path}")
        seen.add(key)
        models.append(
            {
                "id": int(model_id),
                "is_linear": int(is_linear),
                "is_adiabatic": int(is_adiabatic),
                "is_adiabatic_electron": int(is_adiabatic_electron),
                "is_electrostatic": int(is_electrostatic),
                "template_name": str(template_name),
                "template_path": template_path,
            }
        )
    return models


def fetch_active_transp_studies(
    conn: sqlite3.Connection, origin_name: str
) -> List[Tuple[int, int, str, str, str, str, float]]:
    rows = conn.execute(
        """
        SELECT gs.id, gs.gk_code_id, de.transpfile, do.origin, do.copy, gc.name, de.time
        FROM gk_study AS gs
        JOIN data_equil AS de ON de.id = gs.data_equil_id
        JOIN data_origin AS do ON do.id = de.data_origin_id
        JOIN gk_code AS gc ON gc.id = gs.gk_code_id
        WHERE de.active = 1
          AND do.name = ?
          AND de.transpfile IS NOT NULL
          AND de.transpfile != ''
        """,
        (origin_name,),
    ).fetchall()
    return [
        (int(r[0]), int(r[1]), str(r[2]), str(r[3]), str(r[4]), str(r[5]), r[6])
        for r in rows
    ]


def scp_if_missing(remote: str, remote_path: str, copy_path: str, filename: str) -> str:
    if copy_path.lower() == "n/a":
        raise SystemExit("data_origin.copy is 'n/a'; set a valid destination folder.")
    os.makedirs(copy_path, exist_ok=True)
    dest = os.path.join(copy_path, filename)
    if os.path.exists(dest):
        return dest
    source = f"{remote}:{remote_path}/{filename}"
    subprocess.run(["scp", source, dest], check=True)
    return dest


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"Database not found: {args.db}")
    if not os.path.isdir(args.template_dir):
        raise SystemExit(f"Template directory not found: {args.template_dir}")
    os.makedirs(args.output_dir, exist_ok=True)
    psins = psin_values(args.psin_start, args.psin_end, args.psin_step)
    conn = sqlite3.connect(args.db)
    inserted = 0
    try:
        models = fetch_active_models(conn, args.template_dir)
        conn.execute("PRAGMA foreign_keys = ON")
        studies = fetch_active_transp_studies(conn, args.origin_name)
        if not studies:
            raise SystemExit("No active TRANSP studies found.")
        for (
            study_id,
            gk_code_id,
            transpfile,
            origin_path,
            copy_path,
            gk_code,
            transp_time,
        ) in studies:
            filepath = scp_if_missing(args.remote, origin_path, copy_path, transpfile)
            time_val = args.time if transp_time is None else float(transp_time)
            existing = existing_inputs(conn, study_id)
            for model in models:
                is_linear = int(model["is_linear"])
                is_adiabatic_electron = int(model["is_adiabatic_electron"])
                gk_model_id = int(model["id"])
                template_path = str(model["template_path"])
                try:
                    pyro_transp = pk.Pyro(
                        eq_file=filepath,
                        eq_type="TRANSP",
                        eq_kwargs={"time": time_val, "neighbors": args.neighbors},
                        kinetics_file=filepath,
                        kinetics_type="TRANSP",
                        kinetics_kwargs={"time": time_val},
                        gk_file=template_path,
                    )
                except Exception as exc:
                    print(f"Warning: failed to initialize Pyro for study {study_id}: {exc}")
                    for psin in psins:
                        key = (psin, gk_model_id)
                        if key in existing and existing[key][1] != "CRASHED":
                            continue
                        comment = f"WARNING: error: {exc}; file not written"
                        if key in existing:
                            row_id = existing[key][0]
                        else:
                            stub = conn.execute(
                                """
                                INSERT INTO gk_input
                                    (gk_study_id, file_name, file_path, content, psin,
                                     gk_model_id, status, comment)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    study_id,
                                    "pending",
                                    "",
                                    "",
                                    psin,
                                    gk_model_id,
                                    "CRASHED",
                                    "",
                                ),
                            )
                            row_id = int(stub.lastrowid)
                        linear_tag = "linear" if is_linear == 1 else "nonlinear"
                        adiabatic_tag = "adiabe" if is_adiabatic_electron == 1 else "kine"
                        filename = (
                            f"{args.file_prefix}gk_input_{row_id}_study_{study_id}"
                            f"_psin_{psin:.2f}_{linear_tag}_{adiabatic_tag}.in"
                        )
                        outpath = os.path.join(args.output_dir, filename)
                        conn.execute(
                            """
                            UPDATE gk_input
                            SET file_name = ?, file_path = ?, content = ?, status = ?, comment = ?
                            WHERE id = ?
                            """,
                            (filename, outpath, "", "CRASHED", comment, row_id),
                        )
                        inserted += 1
                    continue

                for psin in psins:
                    key = (psin, gk_model_id)
                    if key in existing and existing[key][1] != "CRASHED":
                        continue
                    if key in existing:
                        row_id = existing[key][0]
                    else:
                        stub = conn.execute(
                            """
                            INSERT INTO gk_input
                                (gk_study_id, file_name, file_path, content, psin,
                                 gk_model_id, status, comment)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                study_id,
                                "pending",
                                "",
                                "",
                                psin,
                                gk_model_id,
                                "WAIT",
                                "",
                            ),
                        )
                        row_id = int(stub.lastrowid)
                    linear_tag = "linear" if is_linear == 1 else "nonlinear"
                    adiabatic_tag = "adiabe" if is_adiabatic_electron == 1 else "kine"
                    filename = (
                        f"{args.file_prefix}gk_input_{row_id}_study_{study_id}"
                        f"_psin_{psin:.2f}_{linear_tag}_{adiabatic_tag}.in"
                    )
                    outpath = os.path.join(args.output_dir, filename)
                    comment_parts = []
                    local_evs = {"electron_temp_ev": None, "ion_temp_ev": None}
                    try:
                        pyro_transp.load_local(
                            psi_n=psin,
                            local_geometry="Miller",
                            show_fit=False,
                        )
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
                                raise SystemExit(
                                    "LocalSpecies is not quasineutral. "
                                    "Use --enforce-local-quasineutrality 1 to override."
                                )
                        local_evs = extract_local_temps_ev(pyro_transp)
                        pyro_transp.write_gk_file(file_name=outpath, gk_code=gk_code)
                        content = read_file(outpath)
                        content, dropped = drop_gx_nkx_nky(content)
                        if dropped:
                            with open(outpath, "w", encoding="utf-8") as handle:
                                handle.write(content)
                            comment_parts.append("removed nkx/nky from Dimensions")
                        if is_adiabatic_electron == 1:
                            content, adjusted = adjust_gx_input_for_adiabatic(content)
                            if adjusted:
                                with open(outpath, "w", encoding="utf-8") as handle:
                                    handle.write(content)
                                comment_parts.append(
                                    "adiabatic adjustments: nspecies, beta, fapar, fbpar"
                                )
                        else:
                            lines = content.splitlines()
                            fapar_re = re.compile(r"^\s*fapar\s*=\s*")
                            fbpar_re = re.compile(r"^\s*fbpar\s*=\s*")
                            changed = False
                            for idx, line in enumerate(lines):
                                if fapar_re.match(line):
                                    lines[idx] = "fapar = 0.0"
                                    changed = True
                                    continue
                                if fbpar_re.match(line):
                                    lines[idx] = "fbpar = 0.0"
                                    changed = True
                                    continue
                            if changed:
                                content = "\n".join(lines) + ("\n" if content.endswith("\n") else "")
                                with open(outpath, "w", encoding="utf-8") as handle:
                                    handle.write(content)
                                comment_parts.append("kinetic adjustments: fapar, fbpar")
                        geometry = parse_geometry_fields(content)
                        physics = parse_physics_fields(content)
                        species = parse_species_fields(content)
                        status = args.status
                    except Exception as exc:
                        print(
                            "Warning: failed to create gk_input for study "
                            f"{study_id} psin={psin}: {exc}"
                        )
                        comment_parts.append(f"error: {exc}")
                        comment_parts.append("file not written")
                        content = ""
                        geometry = {}
                        physics = {}
                        species = {}
                        status = "CRASHED"
                    comment = ""
                    if comment_parts:
                        comment = "WARNING: " + "; ".join(comment_parts)
                    conn.execute(
                        """
                        UPDATE gk_input
                        SET file_name = ?,
                            file_path = ?,
                            content = ?,
                            status = ?,
                            comment = ?,
                            geo_option = ?,
                            rhoc = ?,
                            Rmaj = ?,
                            R_geo = ?,
                            qinp = ?,
                            shat = ?,
                            shift = ?,
                            akappa = ?,
                            akappri = ?,
                            tri = ?,
                            tripri = ?,
                            betaprim = ?,
                            beta = ?,
                            electron_z = ?,
                            electron_mass = ?,
                            electron_dens = ?,
                            electron_temp = ?,
                            electron_temp_ev = ?,
                            electron_tprim = ?,
                            electron_fprim = ?,
                            electron_vnewk = ?,
                            ion_z = ?,
                            ion_mass = ?,
                            ion_dens = ?,
                            ion_temp = ?,
                            ion_temp_ev = ?,
                            ion_tprim = ?,
                            ion_fprim = ?,
                            ion_vnewk = ?
                        WHERE id = ?
                        """,
                        (
                            filename,
                            outpath,
                            content,
                            status,
                            comment,
                            geometry.get("geo_option"),
                            geometry.get("rhoc"),
                            geometry.get("Rmaj"),
                            geometry.get("R_geo"),
                            geometry.get("qinp"),
                            geometry.get("shat"),
                            geometry.get("shift"),
                            geometry.get("akappa"),
                            geometry.get("akappri"),
                            geometry.get("tri"),
                            geometry.get("tripri"),
                            geometry.get("betaprim"),
                            physics.get("beta"),
                            species.get("electron_z"),
                            species.get("electron_mass"),
                            species.get("electron_dens"),
                            species.get("electron_temp"),
                            local_evs.get("electron_temp_ev"),
                            species.get("electron_tprim"),
                            species.get("electron_fprim"),
                            species.get("electron_vnewk"),
                            species.get("ion_z"),
                            species.get("ion_mass"),
                            species.get("ion_dens"),
                            species.get("ion_temp"),
                            local_evs.get("ion_temp_ev"),
                            species.get("ion_tprim"),
                            species.get("ion_fprim"),
                            species.get("ion_vnewk"),
                            row_id,
                        ),
                    )
                    inserted += 1
        conn.commit()
    finally:
        conn.close()
    print(f"Inserted {inserted} gk_input rows.")


if __name__ == "__main__":
    main()
