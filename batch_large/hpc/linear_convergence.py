import numpy as np
from netCDF4 import Dataset
import matplotlib.pyplot as plt


def moving_median(x, k):
    """Simple moving median with odd window k."""
    k = int(k)
    if k < 3:
        return x.copy()
    if k % 2 == 0:
        k += 1
    n = len(x)
    y = np.empty_like(x)
    half = k // 2
    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        y[i] = np.nanmedian(x[lo:hi])
    return y


def compute_gamma_convergence_from_total_phi2(
    filename,
    window_frac=0.3,      # check last 30% of samples
    min_pts=20,           # minimum points in window
    tol_relstd=0.02,      # std/|mean| < 2%
    tol_slope_norm=0.01,  # |slope|*Tspan/|mean| < 0.01
    smooth_frac=0.05,     # median smoothing window ~5% of samples
    method="A",           # "C" (old plateau), "A", or "B"
    r2_min=0.999,         # criterion A linearity threshold
    fracs_B=(0.3, 0.4, 0.5),
    tol_B=0.02,           # criterion B spread/mean threshold
    r2_min_B=0.995,       # criterion B per-window linearity
    eps_floor=1e-300,
    make_plots=True,
    save_plot_path=None,
):
    """
    Returns:
      gamma_conv (float)
      is_converged (bool)
      t_window (tuple)
      diagnostics (dict)
    """

    # --- Read from netCDF ---
    with Dataset(filename, "r") as nc:
        time = np.array(nc["Grids/time"][:], dtype=float).reshape(-1)
        # Phi2_kxkyt is typically (Nkx, Nky, Nt) but may be (Nt, Nky, Nkx)
        phi_var = nc["Diagnostics/Phi2_kxkyt"]
        Phi2_kxkyt = np.array(phi_var[:], dtype=float)
        phi_dims = getattr(phi_var, "dimensions", ())

    Nt = time.size
    if Phi2_kxkyt.ndim != 3:
        raise ValueError(f"Expected Phi2_kxkyt to be 3D, got shape {Phi2_kxkyt.shape}")
    if "time" in phi_dims:
        t_axis = phi_dims.index("time")
        if t_axis != 2:
            Phi2_kxkyt = np.moveaxis(Phi2_kxkyt, t_axis, 2)
    if Phi2_kxkyt.shape[2] != Nt:
        raise ValueError(
            f"time length {Nt} must match Phi2_kxkyt time dim {Phi2_kxkyt.shape[2]}"
        )

    # --- Total field energy trace Phi2_tot(t) ---
    # sum over kx, ky -> shape (Nt,)
    Phi2_tot = np.nansum(Phi2_kxkyt, axis=(0, 1)).reshape(-1)

    # floor for log safety
    Phi2_tot = np.where(np.isfinite(Phi2_tot), Phi2_tot, np.nan)
    Phi2_tot = np.where(Phi2_tot > eps_floor, Phi2_tot, eps_floor)

    logP = np.log(Phi2_tot)

    # --- d/dt log(Phi2_tot) robust to nonuniform time spacing ---
    dlogP_dt = np.empty_like(logP)
    # interior: central difference
    dt_c = time[2:] - time[:-2]
    dlogP_dt[1:-1] = (logP[2:] - logP[:-2]) / dt_c
    # endpoints: one-sided
    dlogP_dt[0] = (logP[1] - logP[0]) / (time[1] - time[0])
    dlogP_dt[-1] = (logP[-1] - logP[-2]) / (time[-1] - time[-2])

    gamma_inst = 0.5 * dlogP_dt

    # --- Optional smoothing (median is robust) ---
    smooth_pts = max(5, int(round(smooth_frac * Nt)))
    if smooth_pts % 2 == 0:
        smooth_pts += 1
    gamma_s = moving_median(gamma_inst, smooth_pts)

    # --- Choose trailing window for convergence check ---
    nwin = max(min_pts, int(round(window_frac * Nt)))
    i0 = max(0, Nt - nwin)
    idx = np.arange(i0, Nt)

    twin = time[idx]
    gwin = gamma_s[idx]

    # ======================================================================
    # CRITERION OLD: plateau test on gamma_s(t)
    # ======================================================================
    gmean_old = np.nanmean(gwin)
    gstd_old = np.nanstd(gwin)
    relstd_old = gstd_old / max(abs(gmean_old), np.finfo(float).eps)

    mask = np.isfinite(twin) & np.isfinite(gwin)
    if np.count_nonzero(mask) < 6:
        slope_old = np.nan
        slope_norm_old = np.nan
        is_converged_old = False
    else:
        p_old = np.polyfit(twin[mask], gwin[mask], deg=1)
        slope_old = p_old[0]
        Tspan = twin[mask].max() - twin[mask].min()
        slope_norm_old = abs(slope_old) * Tspan / max(abs(gmean_old), np.finfo(float).eps)
        is_converged_old = (
            (gmean_old > 0)
            and (relstd_old < tol_relstd)
            and (slope_norm_old < tol_slope_norm)
        )

    # ======================================================================
    # CRITERION A: linear fit of log(Phi2_tot) over late window
    # ======================================================================
    yA = logP[idx]
    maskA = np.isfinite(twin) & np.isfinite(yA)
    if np.count_nonzero(maskA) >= 6:
        pA = np.polyfit(twin[maskA], yA[maskA], deg=1)
        gamma_A = 0.5 * pA[0]
        yfit = np.polyval(pA, twin[maskA])
        SSres = np.sum((yA[maskA] - yfit) ** 2)
        SStot = np.sum((yA[maskA] - np.nanmean(yA[maskA])) ** 2)
        R2_A = 1 - SSres / max(SStot, np.finfo(float).eps)
        is_converged_A = (gamma_A > 0) and (R2_A > r2_min)
    else:
        gamma_A = np.nan
        R2_A = np.nan
        is_converged_A = False

    # ======================================================================
    # CRITERION B: stability across multiple late windows
    # ======================================================================
    gamma_B_each = np.full(len(fracs_B), np.nan, dtype=float)
    R2_B_each = np.full(len(fracs_B), np.nan, dtype=float)

    for j, frac in enumerate(fracs_B):
        nwin_B = max(min_pts, int(round(frac * Nt)))
        iB = max(0, Nt - nwin_B)
        idB = np.arange(iB, Nt)
        tB = time[idB]
        yB = logP[idB]
        maskB = np.isfinite(tB) & np.isfinite(yB)
        if np.count_nonzero(maskB) >= 6:
            pB = np.polyfit(tB[maskB], yB[maskB], deg=1)
            gamma_B_each[j] = 0.5 * pB[0]
            yfitB = np.polyval(pB, tB[maskB])
            SSresB = np.sum((yB[maskB] - yfitB) ** 2)
            SStotB = np.sum((yB[maskB] - np.nanmean(yB[maskB])) ** 2)
            R2_B_each[j] = 1 - SSresB / max(SStotB, np.finfo(float).eps)

    gamma_B_mean = np.nanmean(gamma_B_each)
    R2_B_mean = np.nanmean(R2_B_each)
    spread_over_mean_B = np.nanstd(gamma_B_each) / max(abs(gamma_B_mean), np.finfo(float).eps)
    finite_R2_B = R2_B_each[np.isfinite(R2_B_each)]
    is_converged_B = (
        (gamma_B_mean > 0)
        and (spread_over_mean_B < tol_B)
        and (finite_R2_B.size > 0)
        and np.all(finite_R2_B > r2_min_B)
    )

    method_key = str(method).strip().upper()
    if method_key == "OLD":
        method_key = "C"
    if method_key == "C":
        gamma_conv = gmean_old
        is_converged = is_converged_old
    elif method_key == "B":
        gamma_conv = gamma_B_mean
        is_converged = is_converged_B
    else:
        method_key = "A"
        gamma_conv = gamma_A
        is_converged = is_converged_A

    t_window = (twin[0], twin[-1])

    diagnostics = dict(
        Phi2_tot=Phi2_tot,
        logPhi2_tot=logP,
        gamma_inst=gamma_inst,
        gamma_smooth=gamma_s,
        relstd=relstd_old,
        slope=slope_old,
        slope_norm=slope_norm_old,
        i0=i0,
        smooth_pts=smooth_pts,
        gamma_old=gmean_old,
        is_converged_old=is_converged_old,
        gamma_A=gamma_A,
        R2_A=R2_A,
        is_converged_A=is_converged_A,
        gamma_B_each=gamma_B_each,
        R2_B_each=R2_B_each,
        R2_B_mean=R2_B_mean,
        gamma_B_mean=gamma_B_mean,
        spread_over_mean_B=spread_over_mean_B,
        is_converged_B=is_converged_B,
        method=method_key,
    )

    print(
        f"=== Late window: t in [{t_window[0]:.6g}, {t_window[1]:.6g}] "
        f"(window_frac={window_frac:g}) ==="
    )
    print("")
    print("[A] linear fit of log(Phi2_tot):")
    print(f"  gamma_fit = {gamma_A:.6g}, R2 = {R2_A:.6f} (min {r2_min:.6f})")
    print(f"  CONVERGED? {int(is_converged_A)}")
    print("")
    fracs_str = ", ".join(f"{f:g}" for f in fracs_B)
    gamma_each_str = ", ".join(f"{g:.6g}" for g in gamma_B_each)
    r2_each_str = ", ".join(f"{r2:.6g}" for r2 in R2_B_each)
    print(f"[B] stability across late windows (fractions = [{fracs_str}]):")
    print(f"  gamma_each = [{gamma_each_str}]")
    print(f"  R2_each    = [{r2_each_str}]")
    print(
        f"  gamma_mean = {gamma_B_mean:.6g}, spread/mean = {spread_over_mean_B:.3g} "
        f"(tol {tol_B:g})"
    )
    print(f"  CONVERGED? {int(is_converged_B)}")
    print("")
    print("[C] plateau on gamma_s(t):")
    print(
        f"  gamma_mean = {gmean_old:.6g}, relstd = {relstd_old:.3g} "
        f"(tol {tol_relstd:g}), slope_norm = {slope_norm_old:.3g} "
        f"(tol {tol_slope_norm:g})"
    )
    print(f"  CONVERGED? {int(is_converged_old)}")

    if make_plots:
        fig, axes = plt.subplots(2, 1, sharex=True, figsize=(7.5, 6.5))

        axes[0].plot(time, logP, linewidth=1.5)
        axes[0].axvline(t_window[0], linestyle="--")
        axes[0].axvline(t_window[1], linestyle="--")
        axes[0].set_ylabel("log(Phi2_tot)")
        axes[0].set_title("(a)")
        axes[0].grid(True)

        axes[1].plot(time, gamma_inst, linewidth=1.0, label="gamma_inst")
        axes[1].plot(time, gamma_s, linewidth=1.5, label="gamma_smooth")
        axes[1].axvline(t_window[0], linestyle="--")
        axes[1].axvline(t_window[1], linestyle="--")
        axes[1].set_xlabel("time")
        axes[1].set_ylabel("gamma(t)")
        axes[1].set_title("(b)")
        axes[1].grid(True)
        axes[1].legend()

        fig.tight_layout()
        if save_plot_path:
            fig.savefig(save_plot_path, dpi=150, bbox_inches="tight")
        else:
            plt.show()

    return gamma_conv, is_converged, t_window, diagnostics


def _build_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="Check GX linear convergence from Diagnostics/Phi2_kxkyt."
    )
    parser.add_argument("filename", help="Path to gx out .nc file")
    parser.add_argument("--no-plots", action="store_true", help="Disable plots")
    parser.add_argument("--method", type=str, default="A", help="C, A, or B")
    parser.add_argument("--window-frac", type=float, default=0.3)
    parser.add_argument("--min-pts", type=int, default=20)
    parser.add_argument("--tol-relstd", type=float, default=0.02)
    parser.add_argument("--tol-slope-norm", type=float, default=0.01)
    parser.add_argument("--smooth-frac", type=float, default=0.05)
    parser.add_argument("--r2-min", type=float, default=0.999)
    parser.add_argument("--b-fracs", type=str, default="0.3,0.4,0.5")
    parser.add_argument("--b-tol", type=float, default=0.02)
    parser.add_argument("--b-r2-min", type=float, default=0.995)
    return parser


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()
    fracs_B = tuple(
        float(item.strip()) for item in args.b_fracs.split(",") if item.strip()
    )

    compute_gamma_convergence_from_total_phi2(
        args.filename,
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
        make_plots=not args.no_plots,
    )


if __name__ == "__main__":
    main()
