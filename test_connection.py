"""One-off script: verifies DATABASE_URL connects and the schema/pgvector are set up."""

import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

database_url = os.environ["DATABASE_URL"]

with psycopg2.connect(database_url) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dim_company;")
        company_count = cur.fetchone()[0]

        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector');")
        vector_installed = cur.fetchone()[0]

print(f"dim_company row count: {company_count}")
print(f"pgvector extension installed: {vector_installed}")
