#!/usr/bin/env python3
import argparse
import os
import sqlite3
from typing import Dict, List


DEFAULT_DB = os.path.join(
    os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
    "gyrokinetic_simulations.db",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill gk_input physics columns from stored content.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to main DB.")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def parse_geometry_fields(content: str) -> Dict[str, float]:
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
    values: Dict[str, float] = {}
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


def parse_physics_fields(content: str) -> Dict[str, float]:
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
    values: Dict[str, float] = {}
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


def parse_list_values(raw: str) -> List[object]:
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return []
    items: List[object] = []
    for item in raw[start + 1 : end].split(","):
        item = item.strip().strip('"').strip("'")
        if not item:
            continue
        try:
            items.append(float(item))
        except ValueError:
            items.append(item)
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
    fields: Dict[str, List[object]] = {}
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
    result: Dict[str, float] = {}
    for label, idx in (("electron", electron_idx), ("ion", main_ion_idx)):
        if idx is None:
            continue
        for key in ("z", "mass", "dens", "temp", "tprim", "fprim", "vnewk"):
            values = fields.get(key, [])
            if idx < len(values):
                result[f"{label}_{key}"] = values[idx]
    return result


def get_gk_input_columns(conn: sqlite3.Connection) -> set:
    return {row[1] for row in conn.execute("PRAGMA table_info(gk_input)").fetchall()}


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    conn = sqlite3.connect(args.db)
    try:
        conn.row_factory = sqlite3.Row
        gk_input_cols = get_gk_input_columns(conn)
        target_cols = {
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
            "geo_option",
        }
        target_cols = {col for col in target_cols if col in gk_input_cols}
        if not target_cols:
            raise SystemExit("No matching physics columns found in gk_input.")
        where_clause = " OR ".join(f"{col} IS NULL" for col in target_cols)
        limit_sql = f" LIMIT {args.limit}" if args.limit and args.limit > 0 else ""
        rows = conn.execute(
            f"SELECT id, content FROM gk_input WHERE {where_clause}{limit_sql}"
        ).fetchall()
        updated = 0
        for row in rows:
            row_id = int(row["id"])
            content = str(row["content"] or "")
            if not content:
                continue
            data: Dict[str, object] = {}
            data.update(parse_geometry_fields(content))
            data.update(parse_physics_fields(content))
            data.update(parse_species_fields(content))
            data = {key: value for key, value in data.items() if key in target_cols}
            if not data:
                continue
            set_clause = ", ".join(f"{col} = ?" for col in data.keys())
            conn.execute(
                f"UPDATE gk_input SET {set_clause} WHERE id = ?",
                [*data.values(), row_id],
            )
            updated += 1
        conn.commit()
    finally:
        conn.close()
    print(f"Updated {updated} gk_input rows.")


if __name__ == "__main__":
    main()
