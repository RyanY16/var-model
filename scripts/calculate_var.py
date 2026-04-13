from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from arch import arch_model

TRADING_DAYS = 245
DEFAULT_START_DATE = "2025-12-01"
STRESS_DATE_START = pd.Timestamp("2008-07-01")
STRESS_DATE_END = pd.Timestamp("2009-03-31")
SIM_DAYS_LIST = [1, 10]
N_SIMS = 1_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily VaR outputs.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Start calculation date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="End calculation date, YYYY-MM-DD. Defaults to today.")
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where dated output folders are written.",
    )
    return parser.parse_args()


def normalize_close_frame(downloaded: pd.DataFrame, tickers: list[str], field: str) -> pd.DataFrame:
    if downloaded.empty:
        raise ValueError("No price data returned from Yahoo Finance.")

    if isinstance(downloaded.columns, pd.MultiIndex):
        frame = downloaded.xs(field, level=1, axis=1)
    else:
        frame = downloaded[[field]].rename(columns={field: tickers[0]})

    return frame.reindex(columns=tickers)


def simulate_returns(log_returns: pd.DataFrame, sim_days: int) -> np.ndarray:
    garch_params: dict[str, dict[str, float]] = {}
    tickers = log_returns.columns.tolist()

    for ticker in tickers:
        r_pct = log_returns[ticker].dropna() * 100
        if r_pct.empty:
            raise ValueError(f"No return history available for {ticker}.")

        model = arch_model(r_pct, vol="Garch", p=1, q=1, mean="Constant", dist="normal")
        result = model.fit(disp="off")
        params = result.params
        garch_params[ticker] = {
            "mu": params["mu"] / 100,
            "omega": params["omega"],
            "alpha": params["alpha[1]"],
            "beta": params["beta[1]"],
            "sigma_pct": result.conditional_volatility.iloc[-1],
        }

    n_assets = len(tickers)
    corr = log_returns[tickers].corr().values
    cholesky = np.linalg.cholesky(corr)
    sim_returns = np.zeros((N_SIMS, sim_days, n_assets))
    mu_vec = np.array([garch_params[ticker]["mu"] for ticker in tickers])

    for sim in range(N_SIMS):
        sigma2_pct = np.array([garch_params[ticker]["sigma_pct"] ** 2 for ticker in tickers])
        for step in range(sim_days):
            z = np.random.normal(size=n_assets)
            z_corr = cholesky @ z
            sigma_pct = np.sqrt(sigma2_pct)
            epsilon_pct = sigma_pct * z_corr
            sim_returns[sim, step, :] = mu_vec + (epsilon_pct / 100)

            for idx, ticker in enumerate(tickers):
                params = garch_params[ticker]
                sigma2_pct[idx] = (
                    params["omega"]
                    + params["alpha"] * (epsilon_pct[idx] ** 2)
                    + params["beta"] * sigma2_pct[idx]
                )

    return sim_returns


def get_pnls(positions: pd.Series, close_prices: pd.DataFrame, sim_returns: np.ndarray) -> dict[str, np.ndarray | float]:
    n_assets = sim_returns.shape[2]
    s0 = close_prices.iloc[-1].values.reshape(1, -1)
    cum_log_returns = sim_returns.sum(axis=1)
    final_prices = s0 * np.exp(cum_log_returns)
    final_prices.shape = (N_SIMS, n_assets)
    positions_vec = positions.reindex(close_prices.columns).fillna(0).values
    asset_pnl = (final_prices - s0) * positions_vec
    pnl = asset_pnl.sum(axis=1)

    var_95 = np.percentile(pnl, 5)
    var_99 = np.percentile(pnl, 1)
    es_95 = pnl[pnl <= var_95].mean()
    es_99 = pnl[pnl <= var_99].mean()

    return {
        "pnls": pnl,
        "VaR_95": var_95,
        "VaR_99": var_99,
        "ES_95": es_95,
        "ES_99": es_99,
    }


def generate_for_date(date: pd.Timestamp, output_root: Path, portfolio_df: pd.DataFrame) -> None:
    price_start_date = date - pd.tseries.offsets.BDay(TRADING_DAYS)
    price_end_date = date + pd.Timedelta(days=1)
    date_str = date.strftime("%Y%m%d")
    output_dir = output_root / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    positions_df = portfolio_df.copy()
    positions_df["purchase_date"] = pd.to_datetime(positions_df["purchase_date"])
    positions_df = positions_df[positions_df["purchase_date"] <= date]
    positions_df["signed_qty"] = positions_df["quantity"]
    positions_df.loc[positions_df["buy_sell"] == "sell", "signed_qty"] *= -1

    positions = positions_df.groupby("ticker")["signed_qty"].sum()
    positions = positions[positions != 0]
    if positions.empty:
        raise ValueError(f"No open positions found for {date_str}.")

    tickers = positions.index.tolist()
    company_lookup = portfolio_df[["ticker", "company_name"]].drop_duplicates(subset=["ticker"])

    price_data = yf.download(
        tickers=tickers,
        start=price_start_date,
        end=price_end_date,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    close_prices = normalize_close_frame(price_data, tickers, "Close")
    log_returns = np.log(close_prices / close_prices.shift(1)).dropna()

    stress_data = yf.download(
        tickers=tickers,
        start=STRESS_DATE_START,
        end=STRESS_DATE_END,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    stress_close_prices = normalize_close_frame(stress_data, tickers, "Close")
    stress_log_returns = np.log(stress_close_prices / stress_close_prices.shift(1)).dropna()

    latest_prices = close_prices.iloc[-1].reindex(positions.index)
    holdings_df = positions.rename("quantity").reset_index()
    holdings_df = holdings_df.merge(company_lookup, on="ticker", how="left")
    holdings_df["price"] = holdings_df["ticker"].map(latest_prices)
    holdings_df["market_value"] = holdings_df["quantity"] * holdings_df["price"]
    holdings_df["weight"] = holdings_df["market_value"] / holdings_df["market_value"].sum()
    holdings_df = holdings_df[["ticker", "company_name", "quantity", "price", "market_value", "weight"]]
    holdings_df.to_csv(output_dir / "portfolio_breakdown.csv", index=False)

    stock_prices_df = holdings_df[["ticker", "company_name", "price"]].copy()
    stock_prices_df.to_csv(output_dir / "stock_prices.csv", index=False)

    portfolio_value = holdings_df["market_value"].sum()
    pnl_rows: list[dict[str, float | int]] = []
    risk_rows: list[dict[str, float | int | str]] = []
    marginal_risk_rows: list[dict[str, float | int | str]] = []

    for sim_days in SIM_DAYS_LIST:
        sim_returns = simulate_returns(log_returns, sim_days)
        normal_results = get_pnls(positions, close_prices, sim_returns)

        pnl_rows.extend(
            {"sim_days": sim_days, "sim_index": sim_index, "pnl": pnl}
            for sim_index, pnl in enumerate(normal_results["pnls"], start=1)
        )
        risk_rows.append(
            {
                "scenario": "normal",
                "sim_days": sim_days,
                "Portfolio Value": portfolio_value,
                "VaR_95": normal_results["VaR_95"],
                "VaR_99": normal_results["VaR_99"],
                "ES_95": normal_results["ES_95"],
                "ES_99": normal_results["ES_99"],
            }
        )

        for ticker in positions.index:
            positions_excluded = positions.copy()
            positions_excluded.loc[ticker] = 0
            excluded_results = get_pnls(positions_excluded, close_prices, sim_returns)
            marginal_risk_rows.append(
                {
                    "sim_days": sim_days,
                    "excluded_ticker": ticker,
                    "VaR_95": excluded_results["VaR_95"],
                    "VaR_99": excluded_results["VaR_99"],
                    "ES_95": excluded_results["ES_95"],
                    "ES_99": excluded_results["ES_99"],
                }
            )

        stress_returns = simulate_returns(stress_log_returns, sim_days)
        stress_results = get_pnls(positions, close_prices, stress_returns)
        risk_rows.append(
            {
                "scenario": "stress",
                "sim_days": sim_days,
                "Portfolio Value": portfolio_value,
                "VaR_95": stress_results["VaR_95"],
                "VaR_99": stress_results["VaR_99"],
                "ES_95": stress_results["ES_95"],
                "ES_99": stress_results["ES_99"],
            }
        )

    pd.DataFrame(pnl_rows).to_csv(output_dir / "pnls.csv", index=False)
    pd.DataFrame(risk_rows).to_csv(output_dir / "risk.csv", index=False)
    pd.DataFrame(marginal_risk_rows).to_csv(output_dir / "marginal_risk.csv", index=False)
    print(f"Completed calculations for date: {date_str}")


def main() -> None:
    args = parse_args()
    end_date = pd.Timestamp.today().normalize() if args.end_date is None else pd.Timestamp(args.end_date)
    calc_dates = pd.bdate_range(start=args.start_date, end=end_date)

    portfolio_df = pd.read_csv("portfolio.csv")
    portfolio_df["ticker"] = portfolio_df["ticker"].astype(str)

    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    for date in calc_dates:
        generate_for_date(date, output_root, portfolio_df)


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parents[1])
    main()
