#!/usr/bin/env python3
import argparse
import io
import json
import shutil
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DIR = PROJECT_ROOT / "tests"
DEFAULT_REPORT_DIR = DEFAULT_TEST_DIR / "reports"


def _result_list(items: List[tuple[object, str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for test_case, traceback_str in items:
        test_id = ""
        if hasattr(test_case, "id"):
            try:
                test_id = str(test_case.id())
            except Exception:
                test_id = str(test_case)
        rows.append({"test": test_id, "traceback": traceback_str})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all unit tests and emit centralized reports."
    )
    parser.add_argument(
        "--start-dir",
        default=str(DEFAULT_TEST_DIR),
        help=f"Test discovery start directory (default: {DEFAULT_TEST_DIR}).",
    )
    parser.add_argument(
        "--pattern",
        default="test_*.py",
        help="Test file pattern (default: test_*.py).",
    )
    parser.add_argument(
        "--report-dir",
        default=str(DEFAULT_REPORT_DIR),
        help=f"Directory where reports are written (default: {DEFAULT_REPORT_DIR}).",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Do not write report files; only print to stdout.",
    )
    parser.add_argument(
        "--verbosity",
        type=int,
        default=2,
        help="unittest verbosity (default: 2).",
    )
    args = parser.parse_args()

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    start_dir = Path(args.start_dir).resolve()
    report_dir = Path(args.report_dir).resolve()

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(start_dir), pattern=args.pattern, top_level_dir=str(PROJECT_ROOT))

    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=args.verbosity)
    t0 = time.perf_counter()
    result = runner.run(suite)
    duration = time.perf_counter() - t0
    text_output = stream.getvalue()

    print(text_output, end="")

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    summary = {
        "generated_at_utc": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "duration_sec": round(duration, 3),
        "tests_run": result.testsRun,
        "successful": result.wasSuccessful(),
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "expected_failures": len(result.expectedFailures),
        "unexpected_successes": len(result.unexpectedSuccesses),
        "failure_details": _result_list(result.failures),
        "error_details": _result_list(result.errors),
    }

    if not args.no_report:
        report_dir.mkdir(parents=True, exist_ok=True)
        text_report = report_dir / f"unit_test_report_{stamp}.txt"
        json_report = report_dir / f"unit_test_report_{stamp}.json"
        latest_text = report_dir / "unit_test_report_latest.txt"
        latest_json = report_dir / "unit_test_report_latest.json"

        text_report.write_text(text_output, encoding="utf-8")
        json_report.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        shutil.copyfile(text_report, latest_text)
        shutil.copyfile(json_report, latest_json)
        print(f"Text report: {text_report}")
        print(f"JSON report: {json_report}")
        print(f"Latest report pointers: {latest_text}, {latest_json}")

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
