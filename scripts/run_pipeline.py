from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from common import load_dotenv, previous_business_day_str

DEFAULT_START_DATE = "2025-12-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full local-to-Supabase pipeline.")
    parser.add_argument(
        "--mode",
        choices=["daily", "all"],
        default="daily",
        help="`daily` runs the previous business day only. `all` rebuilds the full date range.",
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Start date for `all` mode, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="End date override, YYYY-MM-DD.")
    parser.add_argument(
        "--timezone",
        default="Asia/Tokyo",
        help="Timezone used to resolve the previous business day for `daily` mode.",
    )
    parser.add_argument("--skip-calc", action="store_true", help="Skip the calculation step and reuse existing outputs.")
    parser.add_argument("--skip-sync", action="store_true", help="Skip the Supabase upload step.")
    parser.add_argument("--dry-run-sync", action="store_true", help="Show upload counts without calling Supabase.")
    return parser.parse_args()


def run_step(script_name: str, extra_args: list[str]) -> None:
    script_path = Path(__file__).resolve().with_name(script_name)
    cmd = [sys.executable, str(script_path), *extra_args]
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    load_dotenv()

    if args.mode == "daily":
        target_date = args.end_date or previous_business_day_str(args.timezone)
        calc_start_date = target_date
        calc_end_date = target_date
    else:
        calc_start_date = args.start_date
        calc_end_date = args.end_date or previous_business_day_str(args.timezone)

    if not args.skip_calc:
        calc_args = ["--start-date", calc_start_date, "--end-date", calc_end_date]
        run_step("calculate_var.py", calc_args)

    run_step("combine_outputs.py", [])

    if not args.skip_sync:
        sync_args: list[str] = []
        if args.dry_run_sync:
            sync_args.append("--dry-run")
        run_step("sync_supabase.py", sync_args)


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parents[1])
    main()
