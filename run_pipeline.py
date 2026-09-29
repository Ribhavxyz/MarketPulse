"""Runs the full MarketPulse pipeline: ingest, load, checks, and RAG indexing in order."""

import logging
import os
import subprocess
import sys
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Each step runs as `python -m <module> [args]` in its own process, so every step keeps its own
# logging setup and we get a real exit code (checks.py signals failure with sys.exit(1)).
STEPS = [
    ("ingest_prices", ["ingest.prices"]),
    ("ingest_news", ["ingest.news"]),
    ("load", ["load"]),
    ("checks", ["checks"]),
    ("rag_index", ["rag", "index"]),
]


# Runs one step with the same Python interpreter (the venv's), logging start and duration.
# Returns the step's exit code.
def run_step(name, module_args):
    logger.info("step %s: starting", name)
    start = time.monotonic()
    result = subprocess.run([sys.executable, "-m", *module_args])
    duration = time.monotonic() - start
    if result.returncode == 0:
        logger.info("step %s: finished in %.1fs", name, duration)
    else:
        logger.error("step %s: failed with exit code %d after %.1fs", name, result.returncode, duration)
    return result.returncode


# Writes one pipeline_runs row summarising the whole run. A DB error here is logged, not raised,
# so it can't hide the real pipeline status.
def log_pipeline_run(notes):
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO pipeline_runs (step, notes) VALUES (%s, %s)", ("pipeline", notes))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("could not write pipeline_runs row: %s", exc)


def main():
    start = time.monotonic()
    status = "success"

    # Any failing step stops the run: loading after a failed ingest, or indexing after failed
    # checks, would build on bad data.
    for name, module_args in STEPS:
        if run_step(name, module_args) != 0:
            status = f"failed at {name}"
            break

    total = time.monotonic() - start
    notes = f"status={status}; total_duration={total:.1f}s"
    log_pipeline_run(notes)

    if status != "success":
        logger.error("pipeline %s after %.1fs; later steps were skipped", status, total)
        sys.exit(1)
    logger.info("pipeline finished successfully in %.1fs", total)


if __name__ == "__main__":
    main()
