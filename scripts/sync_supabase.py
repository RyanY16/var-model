from __future__ import annotations

import argparse
import json
import os
import ssl
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import certifi
import pandas as pd
from common import load_dotenv

BATCH_SIZE = 5_000

TABLE_CONFIG = {
    "risk_metrics": {
        "csv": "risk_all_dates.csv",
        "conflict": ["date", "scenario", "sim_days"],
        "rename": {
            "Portfolio Value": "portfolio_value",
            "VaR_95": "var_95",
            "VaR_99": "var_99",
            "ES_95": "es_95",
            "ES_99": "es_99",
        },
    },
    "marginal_risk": {
        "csv": "marginal_risk_all_dates.csv",
        "conflict": ["date", "sim_days", "excluded_ticker"],
        "rename": {
            "VaR_95": "var_95",
            "VaR_99": "var_99",
            "ES_95": "es_95",
            "ES_99": "es_99",
        },
    },
    "pnls": {
        "csv": "pnls_all_dates.csv",
        "conflict": ["date", "sim_days", "sim_index"],
        "rename": {},
    },
    "portfolio_breakdown": {
        "csv": "portfolio_breakdown_all_dates.csv",
        "conflict": ["date", "ticker"],
        "rename": {},
    },
    "stock_prices": {
        "csv": "stock_prices_all_dates.csv",
        "conflict": ["date", "ticker"],
        "rename": {},
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload combined CSVs to Supabase.")
    parser.add_argument("--combined-dir", default="outputs/combined", help="Directory containing combined CSV files.")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Rows per upload request.")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=sorted(TABLE_CONFIG.keys()),
        default=sorted(TABLE_CONFIG.keys()),
        help="Subset of tables to upload.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be uploaded without sending data.")
    return parser.parse_args()


def normalize_frame(frame: pd.DataFrame, rename_map: dict[str, str]) -> pd.DataFrame:
    frame = frame.rename(columns=rename_map).copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    frame = frame.where(pd.notnull(frame), None)
    return frame


def chunked_records(frame: pd.DataFrame, batch_size: int) -> list[list[dict[str, Any]]]:
    if frame.empty:
        return []
    records = frame.to_dict(orient="records")
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]


def postgrest_upsert(
    supabase_url: str,
    api_key: str,
    table: str,
    rows: list[dict[str, Any]],
    conflict_columns: list[str],
) -> None:
    query = parse.urlencode({"on_conflict": ",".join(conflict_columns)})
    url = f"{supabase_url.rstrip('/')}/rest/v1/{table}?{query}"
    payload = json.dumps(rows).encode("utf-8")
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    req = request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with request.urlopen(req, context=ssl_context) as response:
            if response.status >= 300:
                body = response.read().decode("utf-8")
                raise RuntimeError(f"Supabase upsert failed for {table}: {response.status} {body}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase upsert failed for {table}: {exc.code} {body}") from exc


def main() -> None:
    args = parse_args()
    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not args.dry_run and (not supabase_url or not supabase_service_role_key):
        raise SystemExit(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. "
            "Create a local .env file or export them in your shell."
        )

    combined_dir = Path(args.combined_dir)
    for table_name in args.tables:
        config = TABLE_CONFIG[table_name]
        csv_path = combined_dir / config["csv"]
        if not csv_path.exists():
            print(f"Skipping {table_name}: {csv_path} not found")
            continue

        frame = pd.read_csv(csv_path)
        frame = normalize_frame(frame, config["rename"])
        batches = chunked_records(frame, args.batch_size)
        total_rows = len(frame)
        total_batches = len(batches)

        if args.dry_run:
            print(f"[dry-run] {table_name}: {total_rows} rows across {total_batches} batch(es)")
            continue

        for batch_number, rows in enumerate(batches, start=1):
            postgrest_upsert(
                supabase_url=supabase_url,
                api_key=supabase_service_role_key,
                table=table_name,
                rows=rows,
                conflict_columns=config["conflict"],
            )
            print(f"Uploaded {table_name}: batch {batch_number}/{total_batches} ({len(rows)} rows)")


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parents[1])
    main()
