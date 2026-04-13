from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import yfinance as yf

TOPIX_TICKERS = ["EWJ"]
TOPIX_OUTPUT_TICKER = "JAPAN_ETF"
TOPIX_NAME = "Japan Equity ETF (EWJ proxy)"
FILE_NAMES = [
    "pnls.csv",
    "risk.csv",
    "marginal_risk.csv",
    "portfolio_breakdown.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combine dated CSV outputs into all-date CSVs.")
    parser.add_argument("--output-dir", default="outputs", help="Directory containing dated result folders.")
    return parser.parse_args()


def normalize_downloaded_prices(downloaded: pd.DataFrame, tickers: list[str], field: str) -> pd.DataFrame:
    if downloaded.empty:
        return pd.DataFrame(columns=tickers)

    if isinstance(downloaded.columns, pd.MultiIndex):
        return downloaded.xs(field, level=1, axis=1).reindex(columns=tickers)

    return downloaded[[field]].rename(columns={field: tickers[0]}).reindex(columns=tickers)


def combine_outputs(base_dir: Path) -> None:
    combined_dir = base_dir / "combined"
    combined_dir.mkdir(exist_ok=True)
    date_dirs = sorted(path for path in base_dir.iterdir() if path.is_dir() and path.name.isdigit())

    combined_data: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict[str, str | int]] = []
    ticker_frames: list[pd.DataFrame] = []

    for date_dir in date_dirs:
        portfolio_path = date_dir / "portfolio_breakdown.csv"
        if portfolio_path.exists():
            ticker_frame = pd.read_csv(portfolio_path)[["ticker", "company_name"]].drop_duplicates()
            ticker_frames.append(ticker_frame)

    for file_name in FILE_NAMES:
        frames: list[pd.DataFrame] = []
        for date_dir in date_dirs:
            csv_path = date_dir / file_name
            if not csv_path.exists():
                continue
            frame = pd.read_csv(csv_path)
            frame.insert(0, "date", date_dir.name)
            frames.append(frame)
            summary_rows.append({"date": date_dir.name, "file": file_name, "rows": len(frame)})

        combined_frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        output_name = file_name.replace(".csv", "_all_dates.csv")
        combined_frame.to_csv(combined_dir / output_name, index=False)
        combined_data[file_name] = combined_frame

    ticker_lookup = (
        pd.concat(ticker_frames, ignore_index=True)
        .drop_duplicates(subset=["ticker"])
        .sort_values("ticker")
        if ticker_frames
        else pd.DataFrame(columns=["ticker", "company_name"])
    )
    ticker_lookup = (
        pd.concat(
            [
                ticker_lookup,
                pd.DataFrame([{"ticker": ticker, "company_name": TOPIX_NAME} for ticker in TOPIX_TICKERS]),
            ],
            ignore_index=True,
        )
        .drop_duplicates(subset=["ticker"], keep="first")
        .sort_values("ticker")
        .reset_index(drop=True)
    )

    if date_dirs:
        start_date = pd.to_datetime(date_dirs[0].name, format="%Y%m%d")
        end_date = pd.to_datetime(date_dirs[-1].name, format="%Y%m%d") + pd.Timedelta(days=1)
    else:
        start_date = None
        end_date = None

    stock_tickers = ticker_lookup["ticker"].tolist()
    if stock_tickers and start_date is not None and end_date is not None:
        downloaded_prices = yf.download(
            tickers=stock_tickers,
            start=start_date,
            end=end_date,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
        )
        if isinstance(downloaded_prices.columns, pd.MultiIndex):
            second_level = downloaded_prices.columns.get_level_values(1)
            price_field = "Adj Close" if "Adj Close" in second_level else "Close"
        else:
            price_field = "Adj Close" if "Adj Close" in downloaded_prices.columns else "Close"

        close_prices = normalize_downloaded_prices(downloaded_prices, stock_tickers, price_field)
        stock_prices_all_dates = close_prices.reset_index()
        stock_prices_all_dates = stock_prices_all_dates.rename(columns={stock_prices_all_dates.columns[0]: "date"})
        stock_prices_all_dates = stock_prices_all_dates.melt(id_vars="date", var_name="ticker", value_name="price")
        stock_prices_all_dates = stock_prices_all_dates.dropna(subset=["price"])
        stock_prices_all_dates = stock_prices_all_dates.merge(ticker_lookup, on="ticker", how="left")
        stock_prices_all_dates["ticker"] = stock_prices_all_dates["ticker"].replace(
            {ticker: TOPIX_OUTPUT_TICKER for ticker in TOPIX_TICKERS}
        )
        stock_prices_all_dates["company_name"] = stock_prices_all_dates["company_name"].fillna(
            stock_prices_all_dates["ticker"]
        )
        stock_prices_all_dates["date"] = pd.to_datetime(stock_prices_all_dates["date"]).dt.strftime("%Y%m%d")
        stock_prices_all_dates = stock_prices_all_dates[["date", "ticker", "company_name", "price"]]
        stock_prices_all_dates = (
            stock_prices_all_dates.sort_values(["date", "ticker"])
            .drop_duplicates(subset=["date", "ticker"], keep="first")
            .reset_index(drop=True)
        )
    else:
        stock_prices_all_dates = pd.DataFrame(columns=["date", "ticker", "company_name", "price"])

    stock_prices_all_dates.to_csv(combined_dir / "stock_prices_all_dates.csv", index=False)

    summary = pd.DataFrame(summary_rows).sort_values(["file", "date"]).reset_index(drop=True)
    print(f"Loaded {len(date_dirs)} date folders")
    print(f"Combined files written to: {combined_dir}")
    if not summary.empty:
        print(summary.tail(10).to_string(index=False))


def main() -> None:
    args = parse_args()
    combine_outputs(Path(args.output_dir))


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parents[1])
    main()
