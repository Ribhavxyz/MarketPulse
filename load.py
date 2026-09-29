"""Loads Parquet/CSV data into Supabase via idempotent upserts and computes daily_return."""

import logging
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COMPANIES_CSV = "data/reference/companies.csv"
PRICES_PARQUET = "data/raw/prices.parquet"
NEWS_PARQUET = "data/raw/news.parquet"
PAGE_SIZE = 1000


# Opens a connection to Supabase using DATABASE_URL from .env.
def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# Writes one summary row to pipeline_runs for the given step.
def log_pipeline_run(conn, step, rows_loaded):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (step, rows_loaded) VALUES (%s, %s)",
            (step, rows_loaded),
        )
    conn.commit()


# Upserts companies.csv into dim_company.
def load_companies(conn):
    companies = pd.read_csv(COMPANIES_CSV)
    rows = list(companies[["ticker", "name", "sector"]].itertuples(index=False, name=None))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_company (ticker, name, sector)
            VALUES %s
            ON CONFLICT (ticker) DO UPDATE SET name = EXCLUDED.name, sector = EXCLUDED.sector
            """,
            rows,
            page_size=PAGE_SIZE,
        )
    conn.commit()
    logger.info("dim_company: processed %d rows", len(rows))
    log_pipeline_run(conn, "load_companies", len(rows))


# Derives distinct dates from prices and upserts them into dim_date.
def load_dim_dates(conn, prices):
    dates = sorted(prices["date"].unique())
    rows = [(d, d.year, d.month, (d.month - 1) // 3 + 1, d.isoweekday()) for d in dates]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_date (date_key, year, month, quarter, weekday)
            VALUES %s
            ON CONFLICT (date_key) DO NOTHING
            """,
            rows,
            page_size=PAGE_SIZE,
        )
    conn.commit()
    logger.info("dim_date: processed %d rows", len(rows))
    log_pipeline_run(conn, "load_dim_dates", len(rows))


# Upserts OHLCV rows into fact_prices (daily_return is computed separately in SQL).
def load_fact_prices(conn, prices):
    rows = [
        (
            row.ticker,
            row.date,
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            int(row.volume),
        )
        for row in prices.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fact_prices (ticker, date_key, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker, date_key) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """,
            rows,
            page_size=PAGE_SIZE,
        )
    conn.commit()
    logger.info("fact_prices: processed %d rows", len(rows))
    log_pipeline_run(conn, "load_fact_prices", len(rows))


# Recomputes daily_return for every row as a fraction, using LAG(close) per ticker.
def compute_daily_return(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE fact_prices f
            SET daily_return = sub.daily_return
            FROM (
                SELECT
                    ticker,
                    date_key,
                    (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date_key))
                        / NULLIF(LAG(close) OVER (PARTITION BY ticker ORDER BY date_key), 0)
                        AS daily_return
                FROM fact_prices
            ) sub
            WHERE f.ticker = sub.ticker AND f.date_key = sub.date_key
            """
        )
        rows_updated = cur.rowcount
    conn.commit()
    logger.info("fact_prices: updated daily_return for %d rows", rows_updated)
    log_pipeline_run(conn, "compute_daily_return", rows_updated)


# Upserts news articles into news_articles (link is the dedup key).
def load_news_articles(conn):
    news = pd.read_parquet(NEWS_PARQUET)
    rows = [
        (
            None if pd.isna(row.ticker) else row.ticker,
            None if pd.isna(row.title) else row.title,
            None if pd.isna(row.summary) else row.summary,
            None if pd.isna(row.link) else row.link,
            None if pd.isna(row.published) else row.published.to_pydatetime(),
            None if pd.isna(row.source) else row.source,
        )
        for row in news.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO news_articles (ticker, title, summary, link, published, source)
            VALUES %s
            ON CONFLICT (link) DO NOTHING
            """,
            rows,
            page_size=PAGE_SIZE,
        )
    conn.commit()
    logger.info("news_articles: processed %d rows", len(rows))
    log_pipeline_run(conn, "load_news_articles", len(rows))


def main():
    prices = pd.read_parquet(PRICES_PARQUET)

    conn = get_connection()
    try:
        load_companies(conn)
        load_dim_dates(conn, prices)
        load_fact_prices(conn, prices)
        compute_daily_return(conn)
        load_news_articles(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
