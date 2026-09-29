# MarketPulse: Project Memory

Working context for anyone (or any Claude session) picking this project up. Static rules and stack live in `CLAUDE.md`; this file tracks where things stand.

## What this is
Daily pipeline for 20 NSE stocks plus RAG Q&A over news. Portfolio project for the Illumina IDS Data Engineer 1 application. Ribhav must be able to explain every line in interviews, so code stays simple.

## Progress (as of 2026-09-30)
Trust the git log over the CLAUDE.md checklist, which is not ticked yet.

| Step | State |
|---|---|
| Scaffold, `sql/schema.sql`, `companies.csv` | Done |
| Supabase connection (`test_connection.py`) | Done |
| `ingest/prices.py` (yfinance -> parquet) | Done (drops null-OHLC rows with a warning, normalizes dates) |
| `ingest/news.py` (Google News RSS -> parquet) | Done |
| `load.py` (star schema upserts, SQL daily_return, news articles) | Done |
| `checks.py` (nulls, dups, low<=close<=high, row reconciliation, `pipeline_runs`) | Done. Tested: 8/8 pass; a deliberate `low = high + 1` UPDATE on one INFY.NS row failed `price_bounds` (7 passed, 1 failed, exit 1), then repaired by rerunning `load.py` |
| `rag.py` | Done (OpenRouter, retries + fallback model) |
| `run_pipeline.py` | Done (stops on first failed step, logs step=pipeline) |
| README | Done, with real sample output and screenshots in docs/ |

Next: all planned steps done. Ideas: dbt, scheduled GitHub Actions run, full-article text.

Testing rule: never INSERT or DELETE rows in `fact_prices` to test checks, because that breaks reconciliation. Use an UPDATE on an existing row, then rerun `load.py` to repair.

## Working agreements
- One step at a time, in pipeline order. After each step, stop and say how to test it.
- Explain the approach before writing code. Write only the current step's file.
- Loads are idempotent (`ON CONFLICT`). Re-runs never create duplicates.
- Use `logging`, not `print`. One failed ticker must not crash the run.
- Secrets only in `.env` (gitignored). Never print or commit keys.
- Never claim a feature in the README or resume unless it runs.
- If an idea or code is bad, say so directly.

## Environment notes
- Windows, venv, no Docker.
- Supabase PostgreSQL + pgvector via the Session pooler URI in `DATABASE_URL`.
- Embeddings: sentence-transformers all-MiniLM-L6-v2 (384 dims).
- LLM: OpenRouter (no Anthropic credits). `OPENROUTER_API_KEY` and `LLM_MODEL` come from `.env`; `rag.py` calls it with `requests`.
