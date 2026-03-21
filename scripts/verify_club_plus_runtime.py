#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ui.live_verification import build_club_plus_verification_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the local Club-Plus runtime state of a running ts-connect connector.")
    parser.add_argument("--data-dir", default=str(REPO_ROOT / "ui" / "data"), help="Path to the ts-connect ui/data directory")
    parser.add_argument("--json", action="store_true", help="Print the full report as JSON")
    args = parser.parse_args()

    report = build_club_plus_verification_report(Path(args.data_dir))
    if args.json:
        print(json.dumps(report, ensure_ascii=True, indent=2))
    else:
        summary = report.get("summary", {})
        print("Club-Plus Live Verification")
        print(f"Data dir: {report.get('data_dir')}")
        print(f"Result: {'OK' if summary.get('ok') else 'BLOCKED'}")
        print(f"Blocking failures: {summary.get('blocking_failures', 0)}")
        print(f"Warnings: {summary.get('warnings', 0)}")
        print("")
        for check in report.get("checks", []):
            marker = "OK" if check.get("ok") else "FAIL"
            print(f"[{marker}] {check.get('id')}: {check.get('detail')}")
        if report.get("warnings"):
            print("")
            print("Warnings:")
            for warning in report["warnings"]:
                print(f"- {warning}")
        if report.get("blocking_issues"):
            print("")
            print("Blocking issues:")
            for issue in report["blocking_issues"]:
                print(f"- {issue}")
    return 0 if report.get("summary", {}).get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
