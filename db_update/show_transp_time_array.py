#!/usr/bin/env python3
import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print TIME3 array from a TRANSP CDF file.",
    )
    parser.add_argument(
        "path",
        nargs="?",
        default="/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin/tmp_copy_transp/NSTX/09/133964I85.CDF",
        help="Path to the TRANSP .CDF file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        import netCDF4 as nc
    except ModuleNotFoundError as exc:
        raise SystemExit("netCDF4 is required. Run in your pyrokinetics env.") from exc
    with nc.Dataset(args.path) as ds:
        if "TIME3" not in ds.variables:
            raise SystemExit("TIME3 variable not found in this file.")
        t = ds["TIME3"][:]
        print("TIME3 length:", len(t))
        print("TIME3 first/last:", float(t[0]), float(t[-1]))
        print("TIME3 values:")
        print(t)


if __name__ == "__main__":
    main()
