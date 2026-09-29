"""Fetches company news from Google News RSS and writes it to data/raw/*.parquet."""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import quote

import feedparser
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COMPANIES_CSV = "data/reference/companies.csv"
OUTPUT_PARQUET = "data/raw/news.parquet"

RSS_URL_TEMPLATE = "https://news.google.com/rss/search?q={query}+stock&hl=en-IN&gl=IN&ceid=IN:en"
MAX_ITEMS_PER_COMPANY = 15

COLUMNS = ["ticker", "title", "summary", "link", "published", "source"]

HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    return HTML_TAG_RE.sub("", text or "").strip()


def fetch_news(ticker: str, name: str) -> list[dict]:
    url = RSS_URL_TEMPLATE.format(query=quote(name))

    try:
        feed = feedparser.parse(url)
    except Exception as exc:
        logger.warning("Failed to fetch news for %s: %s", ticker, exc)
        return []

    if not feed.entries:
        logger.warning("No news entries returned for %s", ticker)
        return []

    rows = []
    for entry in feed.entries[:MAX_ITEMS_PER_COMPANY]:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

        rows.append(
            {
                "ticker": ticker,
                "title": entry.get("title", ""),
                "summary": strip_html(entry.get("summary", "")),
                "link": entry.get("link", ""),
                "published": published,
                "source": entry.get("source", {}).get("title", ""),
            }
        )

    logger.info("%s: %d items", ticker, len(rows))
    return rows


def main() -> None:
    companies = pd.read_csv(COMPANIES_CSV)

    all_rows = []
    for _, company in companies.iterrows():
        all_rows.extend(fetch_news(company["ticker"], company["name"]))

    if not all_rows:
        logger.error("No news fetched for any company; not writing %s", OUTPUT_PARQUET)
        return

    combined = pd.DataFrame(all_rows, columns=COLUMNS)
    combined = combined.drop_duplicates(subset="link")
    combined.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info("Wrote %d rows to %s", len(combined), OUTPUT_PARQUET)


if __name__ == "__main__":
    main()
