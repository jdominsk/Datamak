#!/usr/bin/env python3
import argparse
import json
import math
import os
import pickle
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple


DIRECT_COLUMNS = [
    "electron_tprim",
    "electron_fprim",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
    "electron_vnewk",
    "Rmaj",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
]

RATIO_INPUT_COLUMNS = [
    "ion_temp",
    "electron_temp",
    "ion_dens",
    "electron_dens",
    "ion_mass",
    "electron_mass",
]

FEATURES = [
    "temp_ratio",
    "dens_ratio",
    "electron_tprim",
    "electron_fprim",
    "ion_tprim",
    "ion_fprim",
    "ion_vnewk",
    "electron_vnewk",
    "mass_ratio",
    "Rmaj",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
]


def _as_finite_float(value: object) -> Optional[float]:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def _safe_ratio(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    if num is None or denom is None:
        return None
    try:
        denom_val = float(denom)
    except (TypeError, ValueError):
        return None
    if denom_val == 0.0:
        return None
    try:
        return float(num) / denom_val
    except (TypeError, ValueError):
        return None


def ensure_sg_estimate_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sg_estimate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_surrogate_id INTEGER NOT NULL,
            gk_input_id INTEGER NOT NULL,
            sg_estimate REAL,
            sg_quality REAL,
            UNIQUE (gk_surrogate_id, gk_input_id)
        )
        """
    )


def load_surrogate_model(conn: sqlite3.Connection, surrogate_id: int) -> Tuple[str, dict]:
    row = conn.execute(
        """
        SELECT model_path, meta_path
        FROM gk_surrogate
        WHERE id = ?
        """,
        (surrogate_id,),
    ).fetchone()
    if row is None:
        raise SystemExit(f"gk_surrogate id {surrogate_id} not found.")
    model_path = row["model_path"] or ""
    meta_path = row["meta_path"] or ""
    if not model_path and meta_path:
        try:
            with open(meta_path, "r", encoding="utf-8") as handle:
                meta = json.load(handle)
            model_path = meta.get("model_path") or ""
        except (OSError, json.JSONDecodeError):
            model_path = ""
    if not model_path or not os.path.exists(model_path):
        raise SystemExit(f"Model path not found: {model_path}")
    with open(model_path, "rb") as handle:
        payload = pickle.load(handle)
    return model_path, payload


def fetch_gk_inputs(conn: sqlite3.Connection) -> Tuple[List[int], List[List[float]]]:
    cols = DIRECT_COLUMNS + RATIO_INPUT_COLUMNS
    rows = conn.execute(
        "SELECT id, " + ", ".join(cols) + " FROM gk_input"
    ).fetchall()
    ids: List[int] = []
    vectors: List[List[float]] = []
    for row in rows:
        gk_input_id = int(row["id"])
        features: List[float] = []
        missing = False
        for col in FEATURES:
            if col == "temp_ratio":
                val = _safe_ratio(row["ion_temp"], row["electron_temp"])
            elif col == "dens_ratio":
                val = _safe_ratio(row["ion_dens"], row["electron_dens"])
            elif col == "mass_ratio":
                denom = None
                if row["electron_mass"] is not None:
                    denom = float(row["electron_mass"]) * 1836.0
                val = _safe_ratio(row["ion_mass"], denom)
            else:
                val = row[col]
            val = _as_finite_float(val)
            if val is None:
                missing = True
                break
            features.append(val)
        if missing:
            continue
        ids.append(gk_input_id)
        vectors.append(features)
    return ids, vectors


def predict_with_uncertainty(model, x_rows: List[List[float]], target_transform: str):
    import numpy as np

    preds = np.asarray([tree.predict(x_rows) for tree in model.estimators_])
    mean = preds.mean(axis=0)
    std = preds.std(axis=0)
    if target_transform == "log1p":
        mean_out = np.expm1(mean)
        lower = np.expm1(mean - std)
        upper = np.expm1(mean + std)
        quality = 0.5 * (upper - lower)
    else:
        mean_out = mean
        quality = std
    return mean_out, quality


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Estimate gamma_max for all gk_input rows using a trained surrogate."
    )
    parser.add_argument(
        "--db",
        default=os.path.join(
            os.environ.get("DTWIN_ROOT", os.path.dirname(os.path.dirname(__file__))),
            "gyrokinetic_simulations.db",
        ),
        help="Path to gyrokinetic_simulations.db",
    )
    parser.add_argument(
        "--surrogate-id",
        type=int,
        required=True,
        help="gk_surrogate.id to use for predictions.",
    )
    args = parser.parse_args()

    try:
        import numpy as np  # noqa: F401
    except Exception as exc:
        raise SystemExit("numpy is required to run surrogate estimates.") from exc

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        ensure_sg_estimate_table(conn)
        _, payload = load_surrogate_model(conn, args.surrogate_id)
        model = payload["model"]
        target_transform = payload.get("target_transform", "none")
        ids, vectors = fetch_gk_inputs(conn)
        if not ids:
            raise SystemExit("No complete gk_input rows found for prediction.")
        mean_out, quality = predict_with_uncertainty(model, vectors, target_transform)
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        data = [
            (args.surrogate_id, gk_input_id, float(mean_out[idx]), float(quality[idx]))
            for idx, gk_input_id in enumerate(ids)
        ]
        conn.executemany(
            """
            INSERT INTO sg_estimate (gk_surrogate_id, gk_input_id, sg_estimate, sg_quality)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(gk_surrogate_id, gk_input_id)
            DO UPDATE SET
                sg_estimate = excluded.sg_estimate,
                sg_quality = excluded.sg_quality
            """,
            data,
        )
        conn.commit()
    finally:
        conn.close()
    print(f"Updated {len(data)} surrogate estimates for gk_surrogate_id={args.surrogate_id}.")


if __name__ == "__main__":
    main()
