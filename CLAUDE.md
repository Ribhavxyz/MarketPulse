# MarketPulse

Daily pipeline for 20 NSE stocks + RAG Q&A. Portfolio project for the Illumina IDS Data Engineer 1 application.
The owner (Ribhav) must be able to explain every line in interviews: keep code simple and explain your choices.

## Flow
yfinance + companies.csv + Google News RSS -> data/raw/*.parquet -> Supabase PostgreSQL (star schema + pgvector)
-> quality/reconciliation checks -> RAG (embed question -> vector search + SQL price metrics -> Claude answer with sources)

## Stack
Python 3.11+, pandas, pyarrow, yfinance, feedparser, psycopg2-binary, python-dotenv,
sentence-transformers (all-MiniLM-L6-v2, 384 dims), anthropic SDK.
DB: Supabase PostgreSQL + pgvector via the Session pooler URI in DATABASE_URL. Windows, venv, no Docker.

## Layout
ingest/prices.py, ingest/news.py -> Parquet in data/raw/
load.py      -> Parquet/CSV -> Supabase, idempotent upserts, compute daily_return
checks.py    -> nulls, duplicates, low <= close <= high, raw rows == loaded rows; log to pipeline_runs
rag.py       -> chunk, embed, store in news_chunks, answer(question)
run_pipeline.py -> runs everything in order
sql/schema.sql  -> dim_company, dim_date, fact_prices, news_articles, news_chunks, pipeline_runs

## Rules
- One step at a time, in the order above. After each step, stop and tell me how to test it.
- Explain the approach before writing code. Write only the file for the current step.
- Loads must be idempotent (ON CONFLICT). Re-running must never create duplicates.
- Secrets only in .env (gitignored). Never print or commit keys.
- Use the logging module, not print. One failed ticker must not crash the run.
- Never claim a feature in the README or resume unless it runs.
- If my idea is bad or my code has a bug, say so directly.

## Status
- [ ] Scaffold + schema.sql + companies.csv
- [ ] Supabase project created, schema run, connection tested
- [ ] ingest/prices.py
- [ ] ingest/news.py
- [ ] load.py
- [x] checks.py
- [ ] rag.py
- [ ] run_pipeline.py + README with sample output