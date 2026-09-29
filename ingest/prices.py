"""Fetches daily OHLCV price data from yfinance and writes it to data/raw/*.parquet."""

import logging

import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COMPANIES_CSV = "data/reference/companies.csv"
OUTPUT_PARQUET = "data/raw/prices.parquet"

COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]


def fetch_prices(ticker: str) -> pd.DataFrame | None:
    try:
        history = yf.Ticker(ticker).history(period="1y")
    except Exception as exc:
        logger.warning("Failed to download %s: %s", ticker, exc)
        return None

    if history.empty:
        logger.warning("No price data returned for %s", ticker)
        return None

    history = history.reset_index()
    history["ticker"] = ticker
    history = history.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    history = history[COLUMNS]

    null_mask = history[["open", "high", "low", "close"]].isnull().any(axis=1)
    if null_mask.any():
        dropped_dates = history.loc[null_mask, "date"].dt.date.tolist()
        logger.warning(
            "%s: dropping %d rows with null OHLC values, dates: %s",
            ticker,
            null_mask.sum(),
            dropped_dates,
        )
        history = history[~null_mask]

    history["date"] = history["date"].dt.tz_localize(None).dt.normalize().dt.date

    return history


def main() -> None:
    companies = pd.read_csv(COMPANIES_CSV)

    frames = []
    for ticker in companies["ticker"]:
        prices = fetch_prices(ticker)
        if prices is not None:
            frames.append(prices)

    if not frames:
        logger.error("No price data fetched for any ticker; not writing %s", OUTPUT_PARQUET)
        return

    combined = pd.concat(frames, ignore_index=True)
    combined.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info("Wrote %d rows for %d tickers to %s", len(combined), len(frames), OUTPUT_PARQUET)


if __name__ == "__main__":
    main()
