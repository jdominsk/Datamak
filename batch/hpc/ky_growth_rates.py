#!/usr/bin/env python3
import argparse
import os

import numpy as np
from netCDF4 import Dataset
import matplotlib.pyplot as plt


def _load_phi2_kxkyt(nc_path: str):
    with Dataset(nc_path, "r") as nc:
        time = np.array(nc["Grids/time"][:], dtype=float).reshape(-1)
        ky = np.array(nc["Grids/ky"][:], dtype=float).reshape(-1)
        phi_var = nc["Diagnostics/Phi2_kxkyt"]
        phi = np.array(phi_var[:], dtype=float)
        dims = getattr(phi_var, "dimensions", ())
    # Move axes to (time, ky, kx)
    if "time" in dims:
        t_axis = dims.index("time")
        if t_axis != 0:
            phi = np.moveaxis(phi, t_axis, 0)
    if "ky" in dims:
        ky_axis = dims.index("ky")
        if ky_axis != 1:
            phi = np.moveaxis(phi, ky_axis, 1)
    return time, ky, phi


def _fit_gamma(time, phi2, window_frac=0.3, min_pts=20, eps_floor=1e-300):
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
        return np.nan, np.nan
    p = np.polyfit(t[mask], logy[mask], deg=1)
    gamma = 0.5 * p[0]
    yfit = np.polyval(p, t[mask])
    ssres = np.sum((logy[mask] - yfit) ** 2)
    sstot = np.sum((logy[mask] - np.nanmean(logy[mask])) ** 2)
    r2 = 1 - ssres / max(sstot, np.finfo(float).eps)
    return gamma, r2, np.nan


def _gamma_std(time, phi2, window_frac=0.3, min_pts=20, eps_floor=1e-300):
    Nt = time.size
    nwin = max(min_pts, int(round(window_frac * Nt)))
    i0 = max(0, Nt - nwin)
    t = time[i0:]
    y = phi2[i0:]

    y = np.where(np.isfinite(y), y, np.nan)
    y = np.where(y > eps_floor, y, eps_floor)
    logy = np.log(y)

    dlog_dt = np.empty_like(logy)
    dlog_dt[1:-1] = (logy[2:] - logy[:-2]) / (t[2:] - t[:-2])
    dlog_dt[0] = (logy[1] - logy[0]) / (t[1] - t[0])
    dlog_dt[-1] = (logy[-1] - logy[-2]) / (t[-1] - t[-2])
    gamma_inst = 0.5 * dlog_dt
    return float(np.nanstd(gamma_inst))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute per-ky growth rates from GX Diagnostics/Phi2_kxkyt."
    )
    parser.add_argument("nc_path", help="Path to GX .out.nc file")
    parser.add_argument("--window-frac", type=float, default=0.3)
    parser.add_argument("--min-pts", type=int, default=20)
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--save", action="store_true", help="Save plot as *_gamma_vs_ky.png")
    return parser.parse_args()


def main():
    args = parse_args()
    time, ky, phi = _load_phi2_kxkyt(args.nc_path)

    if phi.ndim != 3:
        raise ValueError(f"Expected Phi2_kxkyt 3D, got shape {phi.shape}")
    if phi.shape[0] != time.size:
        raise ValueError("time axis mismatch in Phi2_kxkyt")

    # Sum over kx -> Phi2_ky(t)
    phi2_kyt = np.nansum(phi, axis=2)

    gamma = np.full(ky.size, np.nan, dtype=float)
    r2 = np.full(ky.size, np.nan, dtype=float)
    gamma_std = np.full(ky.size, np.nan, dtype=float)
    for j in range(ky.size):
        gamma[j], r2[j], _ = _fit_gamma(
            time,
            phi2_kyt[:, j],
            window_frac=args.window_frac,
            min_pts=args.min_pts,
        )
        gamma_std[j] = _gamma_std(
            time,
            phi2_kyt[:, j],
            window_frac=args.window_frac,
            min_pts=args.min_pts,
        )

    print("ky gamma R2 gamma_std")
    for k, g, r, gs in zip(ky, gamma, r2, gamma_std):
        print(f"{k:.6g} {g:.6g} {r:.6g} {gs:.6g}")

    if not args.no_plot:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.errorbar(ky, gamma, yerr=gamma_std, marker="o", linewidth=1.5, capsize=3)
        ax.set_xlabel("ky")
        ax.set_ylabel("gamma")
        ax.set_title("Growth rate vs ky")
        ax.grid(True)
        fig.tight_layout()
        if args.save:
            base = os.path.basename(args.nc_path)
            stem = base
            for suffix in (".out.nc", ".nc"):
                if stem.endswith(suffix):
                    stem = stem[: -len(suffix)]
                    break
            out_path = os.path.join(
                os.path.dirname(args.nc_path),
                f"{stem}_gamma_vs_ky.png",
            )
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
        else:
            plt.show()


if __name__ == "__main__":
    main()
