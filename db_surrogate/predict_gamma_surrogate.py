#!/usr/bin/env python3
import argparse
import csv
import math
import os
import pickle
from typing import Dict, List, Tuple


def _as_finite_float(value: object) -> float:
    try:
        val = float(value)
    except (TypeError, ValueError):
        raise ValueError("not a number")
    if not math.isfinite(val):
        raise ValueError("not finite")
    return val


def _predict_with_uncertainty(model, x_rows: List[List[float]]):
    import numpy as np

    preds = np.asarray([tree.predict(x_rows) for tree in model.estimators_])
    mean = preds.mean(axis=0)
    std = preds.std(axis=0)
    return mean, std


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Predict gamma_max with uncertainty using a trained surrogate model."
    )
    parser.add_argument("--model", required=True, help="Path to trained model pickle.")
    parser.add_argument("--input-csv", required=True, help="CSV with input features.")
    parser.add_argument(
        "--output-csv", default=None, help="Optional CSV output path (default: stdout)."
    )
    args = parser.parse_args()

    try:
        import numpy as np
    except Exception as exc:
        raise SystemExit("numpy is required to run predictions.") from exc

    with open(args.model, "rb") as handle:
        payload = pickle.load(handle)
    model = payload["model"]
    features = payload["features"]
    transform = payload.get("target_transform", "none")

    rows: List[Dict[str, str]] = []
    with open(args.input_csv, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SystemExit("Input CSV has no header.")
        missing = [col for col in features if col not in reader.fieldnames]
        if missing:
            raise SystemExit(f"Missing required columns: {', '.join(missing)}")
        for row in reader:
            rows.append(row)

    x_rows: List[List[float]] = []
    keep_rows: List[Dict[str, str]] = []
    dropped = 0
    for row in rows:
        try:
            x_row = [_as_finite_float(row.get(col)) for col in features]
        except Exception:
            dropped += 1
            continue
        x_rows.append(x_row)
        keep_rows.append(row)

    if not x_rows:
        raise SystemExit("No valid rows to predict.")

    mean, std = _predict_with_uncertainty(model, x_rows)

    output_rows: List[Dict[str, str]] = []
    for row, pred_mean, pred_std in zip(keep_rows, mean, std):
        out = dict(row)
        if transform == "log1p":
            pred = math.expm1(float(pred_mean))
            low = math.expm1(float(pred_mean - pred_std))
            high = math.expm1(float(pred_mean + pred_std))
            out["pred_gamma_max"] = f"{pred:.6g}"
            out["pred_gamma_max_low"] = f"{low:.6g}"
            out["pred_gamma_max_high"] = f"{high:.6g}"
            out["pred_gamma_max_log"] = f"{float(pred_mean):.6g}"
            out["pred_gamma_max_log_std"] = f"{float(pred_std):.6g}"
        else:
            out["pred_gamma_max"] = f"{float(pred_mean):.6g}"
            out["pred_gamma_max_std"] = f"{float(pred_std):.6g}"
        output_rows.append(out)

    output = open(args.output_csv, "w", newline="", encoding="utf-8") if args.output_csv else None
    try:
        fieldnames = list(output_rows[0].keys())
        writer = csv.DictWriter(output or os.sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
    finally:
        if output:
            output.close()

    print(f"Dropped rows (invalid/missing features): {dropped}", file=os.sys.stderr)
    print(f"Wrote predictions: {len(output_rows)}", file=os.sys.stderr)


if __name__ == "__main__":
    main()
