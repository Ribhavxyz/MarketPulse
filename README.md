# MarketPulse

Daily pipeline for 20 NSE stocks plus a RAG Q&A layer over related news.

## Architecture

yfinance + companies.csv + Google News RSS -> data/raw/*.parquet -> Supabase PostgreSQL (star schema + pgvector) -> quality/reconciliation checks -> RAG (embed question -> vector search + SQL price metrics -> Claude answer with sources)

## Setup

1. Create and activate a virtual environment
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in `DATABASE_URL` and `ANTHROPIC_API_KEY`
4. Run `sql/schema.sql` against your Supabase database
5. `python run_pipeline.py`

## Sample output

_TBD_
