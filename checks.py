"""Runs data quality and reconciliation checks and logs results to pipeline_runs."""

import logging
import os
import sys

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PRICES_PARQUET = "data/raw/prices.parquet"
NEWS_PARQUET = "data/raw/news.parquet"
DAILY_RETURN_WARNING_THRESHOLD = 0.25


# Opens a connection to Supabase using DATABASE_URL from .env, autocommit so a
# failed check's aborted statement doesn't block the checks that follow it.
def get_connection():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    return conn


# Writes one summary row to pipeline_runs for the whole checks run.
def log_pipeline_run(conn, checks_passed, checks_failed, notes):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (step, checks_passed, checks_failed, notes) VALUES (%s, %s, %s, %s)",
            ("checks", checks_passed, checks_failed, notes),
        )


# Fails if any OHLCV column in fact_prices is null.
def check_nulls(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_prices
            WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} fact_prices rows with null OHLCV"


# Fails if any (ticker, date_key) pair appears more than once in fact_prices.
def check_duplicate_prices(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT ticker, date_key FROM fact_prices
                GROUP BY ticker, date_key HAVING COUNT(*) > 1
            ) d
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} duplicate (ticker, date_key) pairs in fact_prices"


# Fails if any link appears more than once in news_articles.
def check_duplicate_news_links(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT link FROM news_articles GROUP BY link HAVING COUNT(*) > 1
            ) d
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} duplicate links in news_articles"


# Fails if low <= open, low <= close, close <= high, open <= high, or all-positive don't hold.
def check_price_bounds(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_prices
            WHERE NOT (
                low <= open AND low <= close AND close <= high AND open <= high
                AND open > 0 AND high > 0 AND low > 0 AND close > 0
            )
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} fact_prices rows violating OHLC bounds"


# Fails if any news_articles.ticker isn't a row in dim_company.
def check_news_ticker_fk(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM news_articles n
            WHERE n.ticker IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM dim_company c WHERE c.ticker = n.ticker)
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} news_articles rows with ticker not in dim_company"


# Fails if any fact_prices.date_key isn't a row in dim_date.
def check_fact_prices_date_fk(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_prices f
            WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = f.date_key)
            """
        )
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} fact_prices rows with date_key not in dim_date"


# Fails if per-ticker row count or summed volume differs between the raw parquet and fact_prices.
def check_price_reconciliation(conn):
    raw = pd.read_parquet(PRICES_PARQUET)
    raw_counts = raw.groupby("ticker").size().to_dict()
    raw_volume = raw.groupby("ticker")["volume"].sum().to_dict()

    with conn.cursor() as cur:
        cur.execute("SELECT ticker, COUNT(*), SUM(volume) FROM fact_prices GROUP BY ticker")
        loaded = {ticker: (count, volume) for ticker, count, volume in cur.fetchall()}

    mismatches = []
    for ticker, raw_count in raw_counts.items():
        loaded_count, loaded_volume = loaded.get(ticker, (0, 0))
        if loaded_count != raw_count:
            mismatches.append(f"{ticker}: row count raw={raw_count} loaded={loaded_count}")
        elif int(loaded_volume or 0) != int(raw_volume[ticker]):
            mismatches.append(
                f"{ticker}: volume raw={int(raw_volume[ticker])} loaded={int(loaded_volume or 0)}"
            )

    detail = "; ".join(mismatches) if mismatches else "prices reconciliation OK"
    return len(mismatches) == 0, detail


# Fails if per-ticker row count differs between the raw parquet and news_articles.
def check_news_reconciliation(conn):
    raw = pd.read_parquet(NEWS_PARQUET)
    raw_counts = raw.groupby("ticker").size().to_dict()

    with conn.cursor() as cur:
        cur.execute("SELECT ticker, COUNT(*) FROM news_articles GROUP BY ticker")
        loaded_counts = dict(cur.fetchall())

    mismatches = []
    for ticker, raw_count in raw_counts.items():
        loaded_count = loaded_counts.get(ticker, 0)
        if loaded_count != raw_count:
            mismatches.append(f"{ticker}: raw={raw_count} loaded={loaded_count}")

    detail = "; ".join(mismatches) if mismatches else "news reconciliation OK"
    return len(mismatches) == 0, detail


# Logs (not fails) any fact_prices row whose |daily_return| exceeds the warning threshold.
def warn_daily_return_outliers(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, date_key, daily_return FROM fact_prices
            WHERE daily_return IS NOT NULL AND ABS(daily_return) > %s
            ORDER BY ticker, date_key
            """,
            (DAILY_RETURN_WARNING_THRESHOLD,),
        )
        rows = cur.fetchall()

    for ticker, date_key, daily_return in rows:
        logger.warning(
            "daily_return outlier: %s %s daily_return=%.4f (threshold %.2f)",
            ticker,
            date_key,
            daily_return,
            DAILY_RETURN_WARNING_THRESHOLD,
        )
    return len(rows)


CHECKS = [
    ("nulls", check_nulls),
    ("duplicate_prices", check_duplicate_prices),
    ("duplicate_news_links", check_duplicate_news_links),
    ("price_bounds", check_price_bounds),
    ("news_ticker_fk", check_news_ticker_fk),
    ("fact_prices_date_fk", check_fact_prices_date_fk),
    ("price_reconciliation", check_price_reconciliation),
    ("news_reconciliation", check_news_reconciliation),
]


def main():
    conn = get_connection()
    try:
        passed = 0
        failed = 0
        notes = []

        for name, check_fn in CHECKS:
            try:
                ok, detail = check_fn(conn)
            except Exception as exc:
                ok = False
                detail = f"error running check: {exc}"

            if ok:
                passed += 1
                logger.info("[PASS] %s: %s", name, detail)
            else:
                failed += 1
                logger.error("[FAIL] %s: %s", name, detail)
            notes.append(f"{name}: {'PASS' if ok else 'FAIL'} - {detail}")

        outlier_count = warn_daily_return_outliers(conn)
        if outlier_count:
            notes.append(
                f"daily_return_outliers: {outlier_count} rows exceed "
                f"|{DAILY_RETURN_WARNING_THRESHOLD}| (warning only, not counted as a failure)"
            )

        log_pipeline_run(conn, passed, failed, "; ".join(notes))
        logger.info("checks complete: %d passed, %d failed", passed, failed)
    finally:
        conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
