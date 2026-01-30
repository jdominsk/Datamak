#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys

import numpy as np
from netCDF4 import Dataset

def _find_script_dir(start_dir: str) -> str:
    current = os.path.abspath(start_dir)
    for _ in range(6):
        if os.path.isfile(os.path.join(current, "linear_convergence.py")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return ""


# Allow running from batch/hpc or a copied script in a run directory.
SCRIPT_DIR = _find_script_dir(os.path.dirname(__file__))
if SCRIPT_DIR and SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

try:
    from linear_convergence import compute_gamma_convergence_from_total_phi2
except ModuleNotFoundError:
    local_path = os.path.join(os.path.dirname(__file__), "linear_convergence.py")
    if not os.path.isfile(local_path):
        raise
    import importlib.util

    spec = importlib.util.spec_from_file_location("linear_convergence", local_path)
    if spec is None or spec.loader is None:
        raise
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    compute_gamma_convergence_from_total_phi2 = (
        module.compute_gamma_convergence_from_total_phi2
    )


def ensure_gk_convergence_timeseries_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_convergence_timeseries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_run_id INTEGER NOT NULL,
            gk_input_id INTEGER NOT NULL,
            phi2_tot_f32 BLOB NOT NULL,
            n_points INTEGER NOT NULL,
            window_t_min REAL,
            window_t_max REAL,
            gamma_mean REAL,
            relstd REAL,
            slope_norm REAL,
            method TEXT NOT NULL DEFAULT 'A',
            r2 REAL,
            ky_abs_mean REAL,
            gamma_max REAL,
            diffusion REAL,
            is_converged INTEGER NOT NULL DEFAULT 0 CHECK (is_converged IN (0, 1)),
            creation_date TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_convergence_timeseries)")}
    missing = []
    for col, decl in (
        ("n_points", "INTEGER"),
        ("window_t_min", "REAL"),
        ("window_t_max", "REAL"),
        ("gamma_mean", "REAL"),
        ("relstd", "REAL"),
        ("slope_norm", "REAL"),
        ("method", "TEXT NOT NULL DEFAULT 'A'"),
        ("r2", "REAL"),
        ("ky_abs_mean", "REAL"),
        ("gamma_max", "REAL"),
        ("diffusion", "REAL"),
        ("is_converged", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if col not in columns:
            missing.append((col, decl))
    for col, decl in missing:
        conn.execute(f"ALTER TABLE gk_convergence_timeseries ADD COLUMN {col} {decl}")


def fetch_time_array(nc_path: str) -> np.ndarray:
    with Dataset(nc_path, "r") as nc:
        return np.array(nc["Grids/time"][:], dtype=float).reshape(-1)


def load_ky_phi2_kxkyt(nc_path: str):
    with Dataset(nc_path, "r") as nc:
        ky = np.array(nc["Grids/ky"][:], dtype=float).reshape(-1)
        phi_var = nc["Diagnostics/Phi2_kxkyt"]
        phi = np.array(phi_var[:], dtype=float)
        dims = getattr(phi_var, "dimensions", ())
    if "time" in dims:
        t_axis = dims.index("time")
        if t_axis != 0:
            phi = np.moveaxis(phi, t_axis, 0)
    if "ky" in dims:
        ky_axis = dims.index("ky")
        if ky_axis != 1:
            phi = np.moveaxis(phi, ky_axis, 1)
    return ky, phi


def fit_gamma_linear(time, phi2, window_frac=0.3, min_pts=20, eps_floor=1e-300):
    Nt = time.size
    nwin = max(min_pts, int(round(window_frac * Nt)))
    i0 = max(0, Nt - nwin)
    t = time[i0:]
    y = phi2[i0:]
    y = np.where(np.isfinite(y), y, np.nan)
    y = np.where(y > eps_floor, y, eps_floor)
    logy = np.log(y)
    mask = np.isfinite(t) & np.isfinite(logy)
    if np.count_nonzero(mask) < 6:
        return np.nan
    p = np.polyfit(t[mask], logy[mask], deg=1)
    return 0.5 * p[0]


def metric_gamma_over_abs_kymean2(ky, gamma):
    ky = np.asarray(ky, dtype=float)
    gamma = np.asarray(gamma, dtype=float)
    mask = gamma > 0
    if not np.any(mask):
        return np.nan, np.nan, np.nan
    w = gamma[mask]
    ky_mask = np.isfinite(ky)
    x = np.abs(ky[mask])
    ky_abs_mean = np.sum(w * x) / np.sum(w)
    gamma_max = np.max(w)
    if not np.isfinite(ky_abs_mean) or ky_abs_mean <= 0:
        ky_valid = np.abs(ky[ky_mask])
        ky_valid = ky_valid[np.isfinite(ky_valid) & (ky_valid > 0)]
        if ky_valid.size > 0:
            ky_abs_mean = float(np.mean(ky_valid))
    metric = (
        np.inf
        if not np.isfinite(ky_abs_mean) or ky_abs_mean == 0
        else gamma_max / ky_abs_mean**2
    )
    return metric, ky_abs_mean, gamma_max


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze GX output and store convergence time series in batch DB."
    )
    parser.add_argument("db", help="Path to batch SQLite database.")
    parser.add_argument("run_id", type=int, help="gk_run.id to update.")
    parser.add_argument("nc_path", help="Path to GX .out.nc file.")
    parser.add_argument("--window-frac", type=float, default=0.3)
    parser.add_argument("--min-pts", type=int, default=20)
    parser.add_argument("--tol-relstd", type=float, default=0.02)
    parser.add_argument("--tol-slope-norm", type=float, default=0.01)
    parser.add_argument("--smooth-frac", type=float, default=0.05)
    parser.add_argument("--method", type=str, default="A")
    parser.add_argument("--r2-min", type=float, default=0.999)
    parser.add_argument("--b-fracs", type=str, default="0.3,0.4,0.5")
    parser.add_argument("--b-tol", type=float, default=0.02)
    parser.add_argument("--b-r2-min", type=float, default=0.995)
    parser.add_argument("--save-plot", action="store_true")
    parser.add_argument("--no-db", action="store_true", help="Skip DB writes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    fracs_B = tuple(
        float(item.strip()) for item in args.b_fracs.split(",") if item.strip()
    )
    plot_path = None
    if args.save_plot:
        base = os.path.basename(args.nc_path)
        stem = base
        for suffix in (".out.nc", ".nc"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        plot_path = os.path.join(
            os.path.dirname(args.nc_path),
            f"{stem}_growth_rate.png",
        )
    gamma_conv, is_converged, t_window, diagnostics = compute_gamma_convergence_from_total_phi2(
        args.nc_path,
        window_frac=args.window_frac,
        min_pts=args.min_pts,
        tol_relstd=args.tol_relstd,
        tol_slope_norm=args.tol_slope_norm,
        smooth_frac=args.smooth_frac,
        method=args.method,
        r2_min=args.r2_min,
        fracs_B=fracs_B,
        tol_B=args.b_tol,
        r2_min_B=args.b_r2_min,
        make_plots=args.save_plot,
        save_plot_path=plot_path,
    )

    time = fetch_time_array(args.nc_path)
    phi2_tot = np.array(diagnostics["Phi2_tot"], dtype=float).reshape(-1)
    if time.size != phi2_tot.size:
        raise ValueError(
            f"time length {time.size} does not match phi2_tot length {phi2_tot.size}"
        )

    n_target = 100
    tmin = float(t_window[0])
    tmax = float(t_window[1])
    if tmax <= tmin:
        raise ValueError(f"Invalid time window [{tmin}, {tmax}]")
    time_sample = np.linspace(tmin, tmax, n_target)
    phi2_sample = np.interp(time_sample, time, phi2_tot)
    time = time_sample
    phi2_tot = phi2_sample

    phi2_f32 = np.asarray(phi2_tot, dtype=np.float32)

    ky_abs_mean = None
    gamma_max = None
    gamma_metric = None
    try:
        ky, phi = load_ky_phi2_kxkyt(args.nc_path)
        if phi.ndim == 3 and phi.shape[0] == fetch_time_array(args.nc_path).size:
            phi2_kyt = np.nansum(phi, axis=2)
            gamma_each = np.array(
                [
                    fit_gamma_linear(
                        fetch_time_array(args.nc_path),
                        phi2_kyt[:, j],
                        window_frac=args.window_frac,
                        min_pts=args.min_pts,
                    )
                    for j in range(phi2_kyt.shape[1])
                ],
                dtype=float,
            )
            gamma_metric, ky_abs_mean, gamma_max = metric_gamma_over_abs_kymean2(
                ky, gamma_each
            )
    except Exception:
        pass

    if (
        ky_abs_mean is not None
        and gamma_max is not None
        and gamma_metric is not None
        and np.isfinite(ky_abs_mean)
        and np.isfinite(gamma_max)
        and np.isfinite(gamma_metric)
    ):
        print(
            f"<|ky|> = {ky_abs_mean:.6g}, gamma_max = {gamma_max:.6g}, "
            f"D = {gamma_metric:.6g}"
        )

    status = "CONVERGED" if is_converged else "RESTART"

    if args.no_db:
        return 0

    with sqlite3.connect(args.db) as conn:
        ensure_gk_convergence_timeseries_table(conn)
        conn.row_factory = sqlite3.Row
        columns_run = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
        if "ky_abs_mean" not in columns_run:
            conn.execute("ALTER TABLE gk_run ADD COLUMN ky_abs_mean REAL")
        if "gamma_max" not in columns_run:
            conn.execute("ALTER TABLE gk_run ADD COLUMN gamma_max REAL")
        if "diffusion" not in columns_run:
            conn.execute("ALTER TABLE gk_run ADD COLUMN diffusion REAL")
        row = conn.execute(
            "SELECT gk_input_id FROM gk_run WHERE id = ?",
            (args.run_id,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"gk_run id {args.run_id} not found in {args.db}")
        gk_input_id = int(row["gk_input_id"]) if row["gk_input_id"] is not None else 0

        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_convergence_timeseries)")}
        method = diagnostics.get("method", "A")
        method = str(method).strip().upper() or "A"
        r2_val = None
        if method == "A":
            r2_val = diagnostics.get("R2_A")
        elif method == "B":
            r2_val = diagnostics.get("R2_B_mean")
        elif method == "C":
            r2_val = None

        if "time_f32" in columns:
            insert_sql = (
                "INSERT INTO gk_convergence_timeseries ("
                "gk_run_id, gk_input_id, time_f32, phi2_tot_f32, n_points, "
                "window_t_min, window_t_max, gamma_mean, relstd, slope_norm, "
                "method, r2, ky_abs_mean, gamma_max, diffusion, is_converged"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            insert_params = (
                args.run_id,
                gk_input_id,
                sqlite3.Binary(b""),
                sqlite3.Binary(phi2_f32.tobytes()),
                int(time.size),
                float(t_window[0]),
                float(t_window[1]),
                float(gamma_conv),
                float(diagnostics["relstd"]),
                float(diagnostics["slope_norm"]),
                method,
                None if r2_val is None else float(r2_val),
                None if ky_abs_mean is None else float(ky_abs_mean),
                None if gamma_max is None else float(gamma_max),
                None if gamma_metric is None else float(gamma_metric),
                int(bool(is_converged)),
            )
        else:
            insert_sql = (
                "INSERT INTO gk_convergence_timeseries ("
                "gk_run_id, gk_input_id, phi2_tot_f32, n_points, "
                "window_t_min, window_t_max, gamma_mean, relstd, slope_norm, "
                "method, r2, ky_abs_mean, gamma_max, diffusion, is_converged"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            insert_params = (
                args.run_id,
                gk_input_id,
                sqlite3.Binary(phi2_f32.tobytes()),
                int(time.size),
                float(t_window[0]),
                float(t_window[1]),
                float(gamma_conv),
                float(diagnostics["relstd"]),
                float(diagnostics["slope_norm"]),
                method,
                None if r2_val is None else float(r2_val),
                None if ky_abs_mean is None else float(ky_abs_mean),
                None if gamma_max is None else float(gamma_max),
                None if gamma_metric is None else float(gamma_metric),
                int(bool(is_converged)),
            )
        conn.execute(insert_sql, insert_params)
        conn.execute(
            "UPDATE gk_run SET ky_abs_mean = ?, gamma_max = ?, diffusion = ?, synced = 0 "
            "WHERE id = ?",
            (
                None if ky_abs_mean is None else float(ky_abs_mean),
                None if gamma_max is None else float(gamma_max),
                None if gamma_metric is None else float(gamma_metric),
                args.run_id,
            ),
        )
        conn.execute(
            "UPDATE gk_run SET status = ?, synced = 0 WHERE id = ?",
            (status, args.run_id),
        )
        conn.commit()

    print(
        f"Saved convergence series for run_id={args.run_id} (status={status})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
