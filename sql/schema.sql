CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS dim_company (
    ticker TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sector TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    weekday INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_prices (
    ticker TEXT NOT NULL REFERENCES dim_company (ticker),
    date_key DATE NOT NULL REFERENCES dim_date (date_key),
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    daily_return NUMERIC,
    PRIMARY KEY (ticker, date_key)
);

CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    ticker TEXT REFERENCES dim_company (ticker),
    title TEXT NOT NULL,
    summary TEXT,
    link TEXT UNIQUE NOT NULL,
    published TIMESTAMPTZ,
    source TEXT
);

CREATE TABLE IF NOT EXISTS news_chunks (
    id SERIAL PRIMARY KEY,
    article_id INT REFERENCES news_articles (id) ON DELETE CASCADE,
    chunk_text TEXT NOT NULL,
    embedding vector(384)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMPTZ DEFAULT now(),
    step TEXT NOT NULL,
    rows_loaded INT,
    checks_passed INT,
    checks_failed INT,
    notes TEXT
);
