#!/usr/bin/env python3
import argparse
import os
import re
import sqlite3
import tempfile
from typing import Iterable, List, Set, Tuple


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".",
    "gyrokinetic_simulations.db",
)
DEFAULT_PSIN_START = 0.1
DEFAULT_PSIN_END = 0.9
DEFAULT_PSIN_STEP = 0.1
DEFAULT_GK_TEMPLATE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "pyrokinetics",
    "gx_template_miller_nonlinear_kine.in",
)
DEFAULT_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "pyrokinetics",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create gk_input entries for each gk_study using pyrokinetic.",
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, help="Path to the SQLite database file."
    )
    parser.add_argument(
        "--template-dir",
        default=DEFAULT_TEMPLATE_DIR,
        help="Directory containing GK input templates.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "gk_inputs"),
        help="Directory to write generated GK input files.",
    )
    parser.add_argument(
        "--file-prefix",
        default="gk_input_",
        help="Prefix for generated GK input filenames.",
    )
    parser.add_argument(
        "--eq-type",
        default="GEQDSK",
        help="Equilibrium file type for pyrokinetic (eq_type).",
    )
    parser.add_argument(
        "--kinetics-type",
        default="pFile",
        help="Kinetics type for pyrokinetic (kinetics_type).",
    )
    parser.add_argument(
        "--local-geometry",
        default="Miller",
        help="Local geometry model for pyrokinetic.",
    )
    parser.add_argument(
        "--psin-start",
        type=float,
        default=DEFAULT_PSIN_START,
        help="Starting psin value (inclusive).",
    )
    parser.add_argument(
        "--psin-end",
        type=float,
        default=DEFAULT_PSIN_END,
        help="Ending psin value (inclusive).",
    )
    parser.add_argument(
        "--psin-step",
        type=float,
        default=DEFAULT_PSIN_STEP,
        help="Step size for psin values.",
    )
    parser.add_argument(
        "--is-linear",
        type=int,
        default=0,
        choices=[0, 1],
        help="Deprecated: ignored, script generates both linear and nonlinear inputs.",
    )
    parser.add_argument(
        "--is-adiabatic-electron",
        type=int,
        default=1,
        choices=[0, 1],
        help="Deprecated: ignored, script generates both adiabatic and kinetic inputs.",
    )
    parser.add_argument(
        "--status",
        default="WAIT",
        choices=["WAIT", "TORUN", "BATCH", "CRASHED", "SUCCESS"],
        help="Status for inserted gk_input rows.",
    )
    parser.add_argument(
        "--quasineutrality-tol",
        type=float,
        default=1e-3,
        help="Relative tolerance for ni=ne check.",
    )
    parser.add_argument(
        "--enforce-quasineutrality",
        type=int,
        default=1,
        choices=[0, 1],
        help="Overwrite ni profile with ne profile in temporary pfile when mismatched.",
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


def fetch_studies(conn: sqlite3.Connection) -> List[Tuple[int, str, str, str, str, str]]:
    rows = conn.execute(
        """
        SELECT gs.id,
               de.folder_path,
               de.pfile,
               de.gfile,
               de.pfile_content,
               de.gfile_content,
               gc.name
        FROM gk_study AS gs
        JOIN data_equil AS de ON de.id = gs.data_equil_id
        JOIN gk_code AS gc ON gc.id = gs.gk_code_id
        JOIN data_origin AS do ON do.id = de.data_origin_id
        WHERE de.active = 1
        AND do.name = 'Mate Kinetic EFIT';
        """
    ).fetchall()
    return [
        (
            int(r[0]),
            str(r[1]),
            str(r[2]),
            str(r[3]),
            str(r[4]),
            str(r[5]),
            str(r[6]),
        )
        for r in rows
    ]


def existing_inputs(conn: sqlite3.Connection, gk_study_id: int) -> dict:
    rows = conn.execute(
        "SELECT id, psin, status, is_linear, is_adiabatic_electron FROM gk_input WHERE gk_study_id = ?",
        (gk_study_id,),
    ).fetchall()
    inputs = {}
    for row_id, psin, status, is_linear, is_adiabatic_electron in rows:
        key = (float(psin), int(is_linear), int(is_adiabatic_electron))
        inputs[key] = (int(row_id), str(status))
    return inputs


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def has_hydrogenic_species_block(content: str) -> bool:
    match = re.search(r"^\s*(\d+)\s+N\s+Z\s+A\s+of\s+ION\s+SPECIES\s*$", content, re.MULTILINE)
    if not match:
        return False
    try:
        count = int(match.group(1))
    except ValueError:
        return False
    lines = content[match.end() :].splitlines()
    species_lines = [line.strip() for line in lines if line.strip()]
    if len(species_lines) < count:
        return False
    for line in species_lines[:count]:
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            z = float(parts[1])
            a = float(parts[2])
        except ValueError:
            continue
        if abs(z - 1.0) < 1e-6 and a >= 1.0:
            return True
    return False


def has_ni_ti_profiles(content: str) -> bool:
    has_ni = re.search(r"^\s*\d+\s+psinorm\s+ni\(", content, re.MULTILINE) is not None
    has_ti = re.search(r"^\s*\d+\s+psinorm\s+ti\(", content, re.MULTILINE) is not None
    return has_ni and has_ti


def validate_ion_block(content: str) -> tuple:
    match = re.search(
        r"^\s*(\d+)\s+N\s+Z\s+A\s+of\s+ION\s+SPECIES\s*$",
        content,
        re.MULTILINE,
    )
    if not match:
        return False, "missing 'N Z A of ION SPECIES' block"
    try:
        count = int(match.group(1))
    except ValueError:
        return False, "invalid ion species count"
    if count < 1:
        return False, "ion species count must be >= 1"
    lines = content[match.end() :].splitlines()
    species_lines = [line.strip() for line in lines if line.strip()]
    if len(species_lines) < count:
        return False, "ion species block has fewer lines than declared count"
    return True, ""


def strip_no_data_blocks(content: str) -> str:
    lines = content.splitlines()
    header_re = re.compile(r"^\s*(\d+)\s+psinorm\s+\S+\(NO DATA\)\s+\S+")
    out = []
    idx = 0
    changed = False
    while idx < len(lines):
        match = header_re.match(lines[idx])
        if match:
            count = int(match.group(1))
            idx += 1 + count
            changed = True
            continue
        out.append(lines[idx])
        idx += 1
    if changed:
        return "\n".join(out) + ("\n" if content.endswith("\n") else "")
    return content


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


def adjust_gx_input_for_adiabatic(content: str) -> tuple:
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


def parse_profile_block(lines: List[str], label: str) -> tuple:
    header_re = re.compile(rf"^\s*(\d+)\s+psinorm\s+{label}\(")
    for idx, line in enumerate(lines):
        match = header_re.match(line)
        if not match:
            continue
        count = int(match.group(1))
        start = idx + 1
        end = start + count
        if end > len(lines):
            return None
        data = []
        for row in lines[start:end]:
            parts = row.split()
            if len(parts) < 3:
                return None
            try:
                psi = float(parts[0])
                val = float(parts[1])
                grad = float(parts[2])
            except ValueError:
                return None
            data.append((psi, val, grad))
        return idx, start, end, data
    return None


def check_quasineutrality(content: str, tol: float) -> tuple:
    lines = content.splitlines()
    ne_block = parse_profile_block(lines, "ne")
    ni_block = parse_profile_block(lines, "ni")
    if not ne_block or not ni_block:
        return None
    _, _, _, ne_data = ne_block
    _, _, _, ni_data = ni_block
    if len(ne_data) != len(ni_data):
        return False, "ne/ni profile lengths differ"
    max_rel = 0.0
    for (psi_ne, ne_val, _), (psi_ni, ni_val, _) in zip(ne_data, ni_data):
        if abs(psi_ne - psi_ni) > tol:
            return False, "ne/ni psinorm grids differ"
        if ne_val != 0:
            rel = abs((ni_val - ne_val) / ne_val)
            max_rel = max(max_rel, rel)
    return max_rel <= tol, f"max relative |ni-ne|/ne = {max_rel:.3e}"


def enforce_ni_equals_ne(content: str, tol: float) -> str:
    lines = content.splitlines()
    ne_block = parse_profile_block(lines, "ne")
    ni_block = parse_profile_block(lines, "ni")
    if not ne_block or not ni_block:
        return content
    _, _, _, ne_data = ne_block
    _, ni_start, ni_end, ni_data = ni_block
    if len(ne_data) != len(ni_data):
        return content
    for (psi_ne, _, _), (psi_ni, _, _) in zip(ne_data, ni_data):
        if abs(psi_ne - psi_ni) > tol:
            return content
    new_lines = list(lines)
    for i, (psi, val, grad) in enumerate(ne_data):
        new_lines[ni_start + i] = f"{psi:10.6f}   {val:10.6f}   {grad:10.6f}"
    return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"Database not found: {args.db}")
    if not os.path.isdir(args.template_dir):
        raise SystemExit(f"Template directory not found: {args.template_dir}")
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        from pyrokinetics import Pyro
    except ModuleNotFoundError as exc:
        raise SystemExit("pyrokinetics is not installed in this environment.") from exc

    conn = sqlite3.connect(args.db)
    inserted = 0
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        studies = fetch_studies(conn)
        if not studies:
            raise SystemExit("No gk_study entries found.")
        psins = psin_values(args.psin_start, args.psin_end, args.psin_step)
        template_map = {}
        for is_linear in (1, 0):
            for is_adiabatic_electron in (1, 0):
                template_map[(is_linear, is_adiabatic_electron)] = resolve_template(
                    args.template_dir, is_linear, is_adiabatic_electron
                )
        with tempfile.TemporaryDirectory(prefix="pyro_inputs_") as source_dir:
            for (
                study_id,
                folder_path,
                pfile,
                gfile,
                pfile_content,
                gfile_content,
                gk_code,
            ) in studies:
                print(
                    "Processing study "
                    f"{study_id}: folder={folder_path}, pfile={pfile}, gfile={gfile}"
                )
                pfile_path = os.path.join(source_dir, f"study_{study_id}_{pfile}")
                gfile_path = os.path.join(source_dir, f"study_{study_id}_{gfile}")
                base_comment_parts = []
                cleaned_content = strip_no_data_blocks(pfile_content)
                if cleaned_content != pfile_content:
                    pfile_content = cleaned_content
                    print("Warning: removed NO DATA blocks from pfile content.")
                    base_comment_parts.append("removed NO DATA blocks")
                quasi = check_quasineutrality(pfile_content, args.quasineutrality_tol)
                if quasi is None:
                    print("Warning: could not validate ne/ni profiles for quasineutrality.")
                else:
                    ok, detail = quasi
                    if not ok and args.enforce_quasineutrality == 1:
                        print(f"Warning: {detail}. Overwriting ni with ne.")
                        pfile_content = enforce_ni_equals_ne(
                            pfile_content, args.quasineutrality_tol
                        )
                        base_comment_parts.append("enforced quasineutrality")
                    elif not ok:
                        raise SystemExit(
                            f"ni != ne beyond tolerance ({detail}). "
                            "Use --enforce-quasineutrality 1 to override."
                        )
                with open(pfile_path, "w", encoding="utf-8") as handle:
                    handle.write(pfile_content)
                with open(gfile_path, "w", encoding="utf-8") as handle:
                    handle.write(gfile_content)
                existing = existing_inputs(conn, study_id)
                ion_ok, ion_reason = validate_ion_block(pfile_content)
                if not ion_ok:
                    print(
                        f"Warning: invalid ion species block ({ion_reason}). "
                        f"Marking study {study_id} inputs as CRASHED."
                    )
                    for is_linear in (1, 0):
                        for is_adiabatic_electron in (1, 0):
                            for psin in psins:
                                key = (psin, is_linear, is_adiabatic_electron)
                                if key in existing and existing[key][1] != "CRASHED":
                                    continue
                                comment_parts = list(base_comment_parts)
                                comment_parts.append(f"error: {ion_reason}")
                                comment_parts.append("file not written")
                                if key in existing:
                                    row_id = existing[key][0]
                                else:
                                    stub = conn.execute(
                                        """
                                        INSERT INTO gk_input
                                            (gk_study_id, file_name, file_path, content, psin,
                                             is_linear, is_adiabatic_electron, status, comment)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            study_id,
                                            "pending",
                                            "",
                                            "",
                                            psin,
                                            is_linear,
                                            is_adiabatic_electron,
                                            "CRASHED",
                                            "",
                                        ),
                                    )
                                    row_id = int(stub.lastrowid)
                                linear_tag = "linear" if is_linear == 1 else "nonlinear"
                                adiabatic_tag = (
                                    "adiabe" if is_adiabatic_electron == 1 else "kine"
                                )
                                filename = (
                                    f"{args.file_prefix}gk_input_{row_id}_study_{study_id}"
                                    f"_psin_{psin:.2f}_{linear_tag}_{adiabatic_tag}.in"
                                )
                                filepath = os.path.join(args.output_dir, filename)
                                comment = "WARNING: " + "; ".join(comment_parts)
                                conn.execute(
                                    """
                                    UPDATE gk_input
                                    SET file_name = ?, file_path = ?, content = ?, status = ?, comment = ?
                                    WHERE id = ?
                                    """,
                                    (filename, filepath, "", "CRASHED", comment, row_id),
                                )
                                inserted += 1
                    continue
                for is_linear in (1, 0):
                    for is_adiabatic_electron in (1, 0):
                        template_path = template_map[(is_linear, is_adiabatic_electron)]
                        try:
                            pyro = Pyro(
                                eq_file=gfile_path,
                                eq_type=args.eq_type,
                                kinetics_type=args.kinetics_type,
                                kinetics_file=pfile_path,
                                gk_file=template_path,
                                eq_kwargs={"psi_n_lcfs": 0.99},
                            )
                        except Exception as exc:
                            print(
                                "Warning: failed to initialize Pyro for study "
                                f"{study_id} template {os.path.basename(template_path)}: {exc}"
                            )
                            for psin in psins:
                                key = (psin, is_linear, is_adiabatic_electron)
                                if key in existing and existing[key][1] != "CRASHED":
                                    continue
                                comment_parts = list(base_comment_parts)
                                comment_parts.append(f"error: {exc}")
                                comment_parts.append("file not written")
                                if key in existing:
                                    row_id = existing[key][0]
                                else:
                                    stub = conn.execute(
                                        """
                                        INSERT INTO gk_input
                                            (gk_study_id, file_name, file_path, content, psin,
                                             is_linear, is_adiabatic_electron, status, comment)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            study_id,
                                            "pending",
                                            "",
                                            "",
                                            psin,
                                            is_linear,
                                            is_adiabatic_electron,
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
                                filepath = os.path.join(args.output_dir, filename)
                                comment = ""
                                if comment_parts:
                                    comment = "WARNING: " + "; ".join(comment_parts)
                                conn.execute(
                                    """
                                    UPDATE gk_input
                                    SET file_name = ?, file_path = ?, content = ?, status = ?, comment = ?
                                    WHERE id = ?
                                    """,
                                    (filename, filepath, "", "CRASHED", comment, row_id),
                                )
                                inserted += 1
                            continue

                        for psin in psins:
                            key = (psin, is_linear, is_adiabatic_electron)
                            if key in existing and existing[key][1] != "CRASHED":
                                continue
                            comment_parts = list(base_comment_parts)
                            if key in existing:
                                row_id = existing[key][0]
                            else:
                                stub = conn.execute(
                                    """
                                    INSERT INTO gk_input
                                        (gk_study_id, file_name, file_path, content, psin,
                                         is_linear, is_adiabatic_electron, status, comment)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        study_id,
                                        "pending",
                                        "",
                                        "",
                                        psin,
                                        is_linear,
                                        is_adiabatic_electron,
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
                            filepath = os.path.join(args.output_dir, filename)
                            local_evs = {"electron_temp_ev": None, "ion_temp_ev": None}
                            try:
                                pyro.load_local(
                                    psi_n=psin,
                                    local_geometry=args.local_geometry,
                                    show_fit=False,
                                )
                                if not pyro.local_species.check_quasineutrality(
                                    tol=args.quasineutrality_tol
                                ):
                                    if args.enforce_local_quasineutrality == 1:
                                        modify_species = args.qn_modify_species
                                        if modify_species not in pyro.local_species.names:
                                            if "electron" in pyro.local_species.names:
                                                modify_species = "electron"
                                            else:
                                                modify_species = pyro.local_species.names[0]
                                        pyro.local_species.enforce_quasineutrality(
                                            modify_species
                                        )
                                        comment_parts.append(
                                            f"enforced local quasineutrality on {modify_species}"
                                        )
                                    else:
                                        raise SystemExit(
                                            "LocalSpecies is not quasineutral. "
                                            "Use --enforce-local-quasineutrality 1 to override."
                                        )
                                local_evs = extract_local_temps_ev(pyro)
                                pyro.write_gk_file(file_name=filepath, gk_code=gk_code)
                                content = read_file(filepath)
                                if is_adiabatic_electron == 1:
                                    content, adjusted = adjust_gx_input_for_adiabatic(content)
                                    if adjusted:
                                        with open(filepath, "w", encoding="utf-8") as handle:
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
                                        content = "\n".join(lines) + (
                                            "\n" if content.endswith("\n") else ""
                                        )
                                        with open(filepath, "w", encoding="utf-8") as handle:
                                            handle.write(content)
                                        comment_parts.append(
                                            "kinetic adjustments: fapar, fbpar"
                                        )
                                geometry = parse_geometry_fields(content)
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
                                    filepath,
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
