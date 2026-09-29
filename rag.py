"""Chunks and embeds news, stores vectors in news_chunks, and answers questions via RAG."""

import logging
import os
import re
import sys
import time

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

os.environ.setdefault("HF_HUB_OFFLINE", "1")

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
REQUEST_TIMEOUT = 60
RETRY_SLEEP_SECONDS = 3
MAX_RETRIES = 2
FETCH_K = 15
KEEP_K = 5
MAX_TOKENS = 1500

SYSTEM_PROMPT = (
    "You answer questions about NSE-listed stocks. Answer ONLY from the news articles and "
    "price metrics provided in the user message. Cite articles only by their number, like [1] "
    "or [2]; never write URLs. If the provided context is not enough to answer, say so "
    "clearly. Never invent causes or reasons for a price move that the articles do not state. "
    "When the question is about the stock's performance or asks for the \"latest news\", "
    "mention the latest close and the 5- and 30-day returns from the price metrics."
)

USAGE = 'Usage:\n  python rag.py index\n  python rag.py ask "<question>"'

_model = None


# Opens a connection to Supabase using DATABASE_URL from .env.
def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# Loads the embedding model once (imported here so `python rag.py` with no args starts instantly).
def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


# Embeds texts into unit-length vectors, so cosine distance (<=>) ranks by similarity.
def embed(texts):
    return get_model().encode(texts, normalize_embeddings=True, batch_size=32, show_progress_bar=False)


# Formats a vector as the text '[0.1,0.2,...]' that Postgres casts with ::vector.
def to_vector_string(vector):
    return "[" + ",".join(f"{x:.6f}" for x in vector) + "]"


# Embeds every article that has no row in news_chunks yet (one chunk = title + ". " + summary).
# Re-running only picks up new articles, so it never creates duplicates.
def index_articles(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.id, a.title, a.summary FROM news_articles a
            WHERE NOT EXISTS (SELECT 1 FROM news_chunks c WHERE c.article_id = a.id)
            ORDER BY a.id
            """
        )
        articles = cur.fetchall()

    if not articles:
        logger.info("index: no new articles to embed")
        return 0

    texts = [f"{title}. {summary}" if summary else title for _, title, summary in articles]
    logger.info("index: embedding %d articles", len(texts))
    vectors = embed(texts)

    rows = [
        (article_id, text, to_vector_string(vector))
        for (article_id, _, _), text, vector in zip(articles, texts, vectors)
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO news_chunks (article_id, chunk_text, embedding) VALUES %s",
            rows,
            template="(%s, %s, %s::vector)",
            page_size=500,
        )
        cur.execute(
            "INSERT INTO pipeline_runs (step, rows_loaded) VALUES (%s, %s)",
            ("rag_index", len(rows)),
        )
    conn.commit()
    logger.info("index: stored %d chunks", len(rows))
    return len(rows)


# Returns the ticker of the first company whose name or bare ticker (no .NS) appears in the question.
def detect_ticker(conn, question):
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, name FROM dim_company ORDER BY ticker")
        companies = cur.fetchall()

    text = question.lower()
    for ticker, name in companies:
        bare = ticker.removesuffix(".NS").lower()
        if name.lower() in text or re.search(rf"\b{re.escape(bare)}\b", text):
            return ticker
    return None


# Retrieves articles for a question: the FETCH_K closest chunks by cosine distance (optionally for
# one ticker), then the KEEP_K newest of those. Returns (title, source, published, summary, link).
def retrieve(conn, question_vector, ticker):
    sql = """
        SELECT title, source, published, summary, link FROM (
            SELECT a.title, a.source, a.published, a.summary, a.link,
                   c.embedding <=> %s::vector AS distance
            FROM news_chunks c
            JOIN news_articles a ON a.id = c.article_id
    """
    params = [to_vector_string(question_vector)]
    if ticker:
        sql += " WHERE a.ticker = %s"
        params.append(ticker)
    sql += """
            ORDER BY distance LIMIT %s
        ) closest
        ORDER BY published DESC NULLS LAST
        LIMIT %s
    """
    params += [FETCH_K, KEEP_K]

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# Latest close and its 5- and 30-trading-day returns (in %), all computed in SQL.
def get_price_metrics(conn, ticker):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT date_key, close,
                       ROW_NUMBER() OVER (ORDER BY date_key DESC) AS rn
                FROM fact_prices WHERE ticker = %s
            )
            SELECT
                MAX(date_key) FILTER (WHERE rn = 1),
                MAX(close) FILTER (WHERE rn = 1),
                (MAX(close) FILTER (WHERE rn = 1) / NULLIF(MAX(close) FILTER (WHERE rn = 6), 0) - 1) * 100,
                (MAX(close) FILTER (WHERE rn = 1) / NULLIF(MAX(close) FILTER (WHERE rn = 31), 0) - 1) * 100
            FROM ranked
            """,
            (ticker,),
        )
        latest_date, latest_close, return_5d, return_30d = cur.fetchone()

    if latest_close is None:
        return None
    return {
        "latest_date": latest_date,
        "latest_close": float(latest_close),
        "return_5d": None if return_5d is None else float(return_5d),
        "return_30d": None if return_30d is None else float(return_30d),
    }


# Formats a percentage like +1.23%, or n/a when there isn't enough price history.
def format_percent(value):
    return "n/a" if value is None else f"{value:+.2f}%"


# One-line summary of the price metrics, used both in the log and in the prompt.
def format_metrics(ticker, metrics):
    return (
        f"{ticker}: latest close {metrics['latest_close']:.2f} on {metrics['latest_date']}, "
        f"5-trading-day return {format_percent(metrics['return_5d'])}, "
        f"30-trading-day return {format_percent(metrics['return_30d'])}"
    )


# Builds the user message: the question, price metrics for the detected ticker, and numbered
# articles as "[n] title (source, date): summary". URLs are never sent to the model.
def build_prompt(question, ticker, metrics, articles):
    parts = [f"Question: {question}", ""]

    if metrics:
        parts.append(f"Price metrics (from the database) - {format_metrics(ticker, metrics)}")
    else:
        parts.append("Price metrics: none (no company detected in the question).")

    parts.append("")
    parts.append("News articles:")
    if not articles:
        parts.append("(no articles found)")
    for i, (title, source, published, summary, _) in enumerate(articles, start=1):
        date = published.date() if published else "date unknown"
        line = f"[{i}] {title} ({source or 'unknown source'}, {date})"
        parts.append(f"{line}: {summary}" if summary else line)

    return "\n".join(parts)


# Formats the numbered source list from the code's own retrieved articles (not from the model).
def format_sources(articles):
    lines = []
    for i, (title, _, published, _, link) in enumerate(articles, start=1):
        date = published.date() if published else "date unknown"
        lines.append(f"[{i}] {title} ({date}) {link}")
    return "\n".join(lines)


# Reads the OpenRouter key and model from .env; fails early with a clear message if either is missing.
def check_llm_config():
    api_key = os.environ.get("OPENROUTER_API_KEY")
    model = os.environ.get("LLM_MODEL")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is missing. Add it to .env.")
    if not model:
        raise RuntimeError("LLM_MODEL is missing. Add it to .env (an OpenRouter model id).")
    return api_key, model


# Raised when one model can't give an answer (retries used up, or model unavailable), so
# call_llm may try the fallback model. Other errors (bad key, no credits) are plain RuntimeErrors.
class ModelFailure(RuntimeError):
    pass


# Logs why a response had no usable content: HTTP status, first 300 chars of the body, and the
# "error" message if the body has one. The Authorization header / API key is never logged.
def log_unusable_response(response):
    logger.warning("OpenRouter response has no usable content: HTTP %d, body: %s", response.status_code, response.text[:300])
    try:
        error = response.json().get("error")
    except (ValueError, AttributeError):
        return
    if error:
        message = error.get("message") if isinstance(error, dict) else error
        logger.warning("OpenRouter error message: %s", message)


# Sends the prompt to one OpenRouter model (OpenAI-compatible chat API); returns (reply text,
# was_truncated). HTTP 429/5xx, network errors and responses with no usable
# choices[0].message.content are all retried up to MAX_RETRIES more times with a short sleep.
def request_model(api_key, model, prompt):
    body = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            reason = f"request failed ({type(exc).__name__})"
        else:
            status = response.status_code
            if status == 200:
                try:
                    choice = response.json()["choices"][0]
                    content = choice["message"]["content"]
                except (ValueError, KeyError, IndexError, TypeError):
                    content = None
                if isinstance(content, str) and content.strip():
                    return content, choice.get("finish_reason") == "length"
                log_unusable_response(response)
                reason = "no usable content in the response"
            elif status == 429 or status >= 500:
                reason = f"HTTP {status}"
            else:
                try:
                    detail = response.json()["error"]["message"]
                except (ValueError, KeyError, TypeError):
                    detail = response.text[:200]
                if status in (400, 404):
                    raise ModelFailure(f"Model '{model}' is unavailable or invalid on OpenRouter: {detail}")
                if status in (401, 403):
                    raise RuntimeError(f"OpenRouter rejected the API key (HTTP {status}): {detail}")
                if status == 402:
                    raise RuntimeError(f"OpenRouter account has insufficient credits: {detail}")
                raise RuntimeError(f"OpenRouter request failed with HTTP {status}: {detail}")

        if attempt <= MAX_RETRIES:
            logger.warning("Model '%s': %s, retrying (%d/%d)", model, reason, attempt, MAX_RETRIES)
            time.sleep(RETRY_SLEEP_SECONDS)

    raise ModelFailure(f"Model '{model}' failed after {MAX_RETRIES + 1} attempts: {reason}")


# Asks LLM_MODEL; if that model fails, tries LLM_FALLBACK_MODEL (if set) before giving up.
def call_llm(prompt):
    api_key, model = check_llm_config()
    try:
        return request_model(api_key, model, prompt)
    except ModelFailure as primary_error:
        fallback = os.environ.get("LLM_FALLBACK_MODEL")
        if not fallback or fallback == model:
            raise
        logger.warning("%s Falling back to '%s'.", primary_error, fallback)
        try:
            return request_model(api_key, fallback, prompt)
        except ModelFailure as fallback_error:
            raise RuntimeError(f"{primary_error} Fallback failed too: {fallback_error}") from fallback_error


# Answers a question: retrieve articles + price metrics, then ask the LLM to answer from them only.
# Returns (answer_text, sources_text, was_truncated).
def answer(question):
    check_llm_config()
    conn = get_connection()
    try:
        ticker = detect_ticker(conn, question)
        logger.info("detected ticker: %s", ticker or "none (searching all news)")
        articles = retrieve(conn, embed([question])[0], ticker)
        metrics = get_price_metrics(conn, ticker) if ticker else None
    finally:
        conn.close()

    if metrics:
        logger.info("price metrics: %s", format_metrics(ticker, metrics))
    if not articles and not metrics:
        return "No articles or price data found. Run `python rag.py index` first?", "", False

    reply, truncated = call_llm(build_prompt(question, ticker, metrics, articles))
    return reply, format_sources(articles), truncated


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "index":
        conn = get_connection()
        try:
            index_articles(conn)
        finally:
            conn.close()
    elif len(sys.argv) == 3 and sys.argv[1] == "ask":
        try:
            text, sources, truncated = answer(sys.argv[2])
        except RuntimeError as exc:
            logger.error("%s", exc)
            sys.exit(1)
        if truncated:
            logger.warning("The answer was truncated (hit the %d token limit); it is cut off.", MAX_TOKENS)
        logger.info("Answer:\n%s\n", text)
        if sources:
            logger.info("Sources:\n%s\n", sources)
    else:
        logger.info(USAGE)


if __name__ == "__main__":
    main()
