#!/usr/bin/env python3
import argparse
import json
import math
import os
import pickle
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple


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

DEFAULT_STATUS_FILTER = ("SUCCESS", "CONVERGED")

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


def _as_finite_float(value: object) -> Optional[float]:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def _require_columns(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    missing = [col for col in columns if col not in existing]
    if missing:
        raise SystemExit(f"Missing columns in {table}: {', '.join(missing)}")


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


def _load_rows(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    origin_name: Optional[str],
    statuses: Optional[List[str]],
    mapsto: str,
) -> Tuple[List[List[float]], List[float], Dict[str, int], Dict[str, float], Dict[str, float]]:
    if not mapsto.startswith("gk_run."):
        raise SystemExit("Only gk_run.* targets are supported for now.")
    target_col = mapsto.split(".", 1)[1]
    if not target_col:
        raise SystemExit("Invalid mapsto target.")
    _require_columns(conn, "gk_run", ["gk_input_id", "status", target_col])
    _require_columns(conn, "gk_input", DIRECT_COLUMNS + RATIO_INPUT_COLUMNS)

    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    need_origin = origin_id is not None or origin_name is not None
    have_origin = {"gk_study", "data_equil", "data_origin"}.issubset(tables)
    if need_origin and not have_origin:
        raise SystemExit("Origin filtering requested but origin tables are missing.")

    select_cols = DIRECT_COLUMNS + RATIO_INPUT_COLUMNS
    base_query = (
        "SELECT "
        + ", ".join(f"gi.{col} AS {col}" for col in select_cols)
        + f", gr.{target_col} AS target_val "
        "FROM gk_run gr "
        "JOIN gk_input gi ON gi.id = gr.gk_input_id "
    )
    if have_origin:
        base_query += (
            "LEFT JOIN gk_study gs ON gs.id = gi.gk_study_id "
            "LEFT JOIN data_equil de ON de.id = gs.data_equil_id "
            "LEFT JOIN data_origin do ON do.id = de.data_origin_id "
        )
    params: List[object] = []
    if statuses:
        placeholders = ", ".join(["?"] * len(statuses))
        base_query += f"WHERE gr.status IN ({placeholders}) AND gr.{target_col} IS NOT NULL"
        params.extend(statuses)
    else:
        base_query += f"WHERE gr.{target_col} IS NOT NULL"
    if origin_id is not None and have_origin:
        base_query += " AND do.id = ?"
        params.append(origin_id)
    if origin_name is not None and have_origin:
        base_query += " AND LOWER(do.name) = LOWER(?)"
        params.append(origin_name)

    rows = conn.execute(base_query, params).fetchall()
    counts = {
        "total_rows": 0,
        "missing_feature": 0,
        "missing_target": 0,
        "non_finite": 0,
        "negative_target": 0,
    }

    data_x: List[List[float]] = []
    data_y: List[float] = []
    feature_min: Dict[str, float] = {}
    feature_max: Dict[str, float] = {}

    for row in rows:
        counts["total_rows"] += 1
        feature_vals: List[float] = []
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
            feature_vals.append(val)
        if missing:
            counts["missing_feature"] += 1
            continue
        target_val = _as_finite_float(row["target_val"])
        if target_val is None:
            counts["missing_target"] += 1
            continue
        if not math.isfinite(target_val):
            counts["non_finite"] += 1
            continue
        data_x.append(feature_vals)
        data_y.append(target_val)
        for col, val in zip(FEATURES, feature_vals):
            if col not in feature_min or val < feature_min[col]:
                feature_min[col] = val
            if col not in feature_max or val > feature_max[col]:
                feature_max[col] = val

    return data_x, data_y, counts, feature_min, feature_max


def _train_test_split(
    x: List[List[float]],
    y: List[float],
    test_size: float,
    seed: int,
) -> Tuple[List[List[float]], List[List[float]], List[float], List[float]]:
    if test_size <= 0:
        return x, [], y, []
    if test_size >= 1:
        raise SystemExit("--test-size must be < 1.0")
    try:
        import numpy as np
    except Exception as exc:
        raise SystemExit("numpy is required for train/test split.") from exc
    n = len(x)
    idx = np.arange(n)
    rng = np.random.default_rng(seed)
    rng.shuffle(idx)
    test_n = int(round(n * test_size))
    if test_n <= 0 or test_n >= n:
        raise SystemExit("test split resulted in empty train or test set.")
    test_idx = idx[:test_n]
    train_idx = idx[test_n:]
    x_train = [x[i] for i in train_idx]
    y_train = [y[i] for i in train_idx]
    x_test = [x[i] for i in test_idx]
    y_test = [y[i] for i in test_idx]
    return x_train, x_test, y_train, y_test


def _metrics(y_true: List[float], y_pred: List[float]) -> Dict[str, float]:
    if not y_true:
        return {}
    import numpy as np

    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    mae = float(np.mean(np.abs(yt - yp)))
    rmse = float(math.sqrt(np.mean((yt - yp) ** 2)))
    denom = float(np.sum((yt - float(np.mean(yt))) ** 2))
    r2 = float(1.0 - np.sum((yt - yp) ** 2) / denom) if denom > 0 else float("nan")
    return {"mae": mae, "rmse": rmse, "r2": r2}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train a surrogate model for gamma_max from gk_input features."
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
        "--origin-id",
        type=int,
        default=None,
        help="Optional data_origin id to filter.",
    )
    parser.add_argument(
        "--origin",
        default=None,
        help="Optional data_origin name to filter (case-insensitive).",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.0,
        help="Fraction of rows to hold out for testing (default: 0.0).",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Name for the surrogate model (used for output filenames).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for train/test split.",
    )
    parser.add_argument(
        "--statuses",
        default=",".join(DEFAULT_STATUS_FILTER),
        help=(
            "Comma-separated gk_run statuses to include "
            "(default: SUCCESS,CONVERGED). Use 'ALL' to disable status filtering."
        ),
    )
    parser.add_argument(
        "--mapsto",
        default="gk_run.gamma_max",
        help="Target column to model (default: gk_run.gamma_max).",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=300,
        help="Number of trees for RandomForestRegressor.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Max depth for trees (default: None).",
    )
    parser.add_argument(
        "--min-samples-leaf",
        type=int,
        default=2,
        help="Min samples per leaf (default: 2).",
    )
    parser.add_argument(
        "--log1p-target",
        action="store_true",
        help="Train on log1p(gamma_max) instead of gamma_max.",
    )
    parser.add_argument(
        "--model-out",
        default=None,
        help="Output path for trained model (pickle).",
    )
    parser.add_argument(
        "--meta-out",
        default=None,
        help="Optional JSON metadata output path.",
    )
    args = parser.parse_args()

    try:
        import numpy as np
    except Exception as exc:
        raise SystemExit("numpy is required to train the surrogate model.") from exc
    try:
        from sklearn.ensemble import RandomForestRegressor
    except Exception as exc:
        raise SystemExit("scikit-learn is required to train the surrogate model.") from exc

    db_path = args.db
    status_arg = (args.statuses or "").strip()
    statuses: Optional[List[str]]
    if not status_arg or status_arg.upper() == "ALL":
        statuses = None
    else:
        statuses = [s.strip() for s in status_arg.split(",") if s.strip()]
        if not statuses:
            raise SystemExit("No valid statuses provided.")

    mapsto = (args.mapsto or "").strip()
    if not mapsto:
        raise SystemExit("mapsto is required.")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        data_x, data_y, counts, feature_min, feature_max = _load_rows(
            conn, args.origin_id, args.origin, statuses, mapsto
        )
    finally:
        conn.close()

    if not data_x:
        raise SystemExit("No rows available after filtering.")

    if args.log1p_target:
        filtered_x = []
        filtered_y = []
        for x_row, y_val in zip(data_x, data_y):
            if y_val < 0:
                counts["negative_target"] += 1
                continue
            filtered_x.append(x_row)
            filtered_y.append(math.log1p(y_val))
        data_x, data_y = filtered_x, filtered_y

    x_train, x_test, y_train, y_test = _train_test_split(
        data_x, data_y, args.test_size, args.seed
    )

    model = RandomForestRegressor(
        n_estimators=args.n_estimators,
        random_state=args.seed,
        n_jobs=-1,
        max_depth=args.max_depth,
        min_samples_leaf=args.min_samples_leaf,
    )
    model.fit(x_train, y_train)

    metrics = {}
    if x_test:
        preds = model.predict(x_test)
        metrics = _metrics(y_test, preds.tolist())

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    model_name = (args.name or "").strip()
    if model_name:
        if any(sep in model_name for sep in ("/", "\\", os.path.sep)):
            raise SystemExit("Model name cannot contain path separators.")
        safe_name = "".join(
            ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in model_name
        ).strip("_")
        if not safe_name:
            raise SystemExit("Model name is empty after sanitization.")
        default_model = os.path.join(
            os.path.dirname(__file__), "models", f"{safe_name}.pkl"
        )
    else:
        default_model = os.path.join(
            os.path.dirname(__file__), "models", f"gamma_surrogate_{timestamp}.pkl"
        )
    model_out = args.model_out or default_model
    model_dir = os.path.dirname(model_out)
    if model_dir:
        os.makedirs(model_dir, exist_ok=True)

    target_label = mapsto.split(".", 1)[1] if "." in mapsto else mapsto
    payload = {
        "model": model,
        "features": FEATURES,
        "target": target_label,
        "target_transform": "log1p" if args.log1p_target else "none",
        "status_filter": statuses or "ALL",
        "name": model_name or None,
        "model_path": model_out,
        "mapsto": mapsto,
        "origin_id": args.origin_id,
        "origin_name": args.origin,
        "db_path": db_path,
        "train_rows": len(x_train),
        "test_rows": len(x_test),
        "feature_min": feature_min,
        "feature_max": feature_max,
        "metrics": metrics,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    with open(model_out, "wb") as handle:
        pickle.dump(payload, handle)

    meta_out = args.meta_out or f"{model_out}.json"
    payload["meta_path"] = meta_out
    with open(meta_out, "w", encoding="utf-8") as handle:
        json.dump(
            {
                k: v
                for k, v in payload.items()
                if k
                not in {
                    "model",
                }
            },
            handle,
            indent=2,
            sort_keys=True,
        )

    print("Loaded rows:", counts["total_rows"])
    print("Dropped (missing feature):", counts["missing_feature"])
    print("Dropped (missing target):", counts["missing_target"])
    print("Dropped (non-finite):", counts["non_finite"])
    if counts["negative_target"]:
        print("Dropped (negative target):", counts["negative_target"])
    print("Train rows:", len(x_train))
    print("Test rows:", len(x_test))
    if metrics:
        print("Metrics:", json.dumps(metrics))
    print("Model saved:", model_out)
    print("Metadata saved:", meta_out)


if __name__ == "__main__":
    main()
