# Reliable Paginated API Ingestion Pipeline

A local-first data engineering project for the YottaFlex.ai Associate AI Forward Deployed Engineer coding challenge. It demonstrates paginated HTTP ingestion, API-key authentication, retry/backoff, rate limiting, incremental page checkpoints, raw response retention, schema/business validation, quarantine, idempotent publishing, event history, operational metrics and automated tests.

## Architecture

```text
Synthetic source generator -> FastAPI mock API -> Requests ingestion client
    -> raw page JSON + SQLite raw_pages -> schema/business validation
    -> customers / orders / order_events OR quarantine
    -> checkpoints + run metrics -> SQL reconciliation report
```

See [data dictionary](docs/data_dictionary.md) and [design trade-offs](docs/design_tradeoffs.md).

## Requirements
- Python 3.10+
- No paid services or external credentials required

## Setup

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate
python -m pip install -e '.[test]'
# .env.example documents the supported environment variables; export them in your shell.
# The app does not automatically load a .env file.
```

Default API key is `local-demo-key`. Do not commit real credentials.

## Run locally
Open two terminals from the repository root.

Terminal 1 — start the mock source API:

```bash
uvicorn pipeline.mock_api:app --app-dir src --host 127.0.0.1 --port 8000
```

Terminal 2 — run ingestion:

```bash
# macOS/Linux example; Windows PowerShell can set equivalent environment variables
export API_BASE_URL=http://127.0.0.1:8000
export API_KEY=local-demo-key
python -m pipeline.ingest
```

To replay all pages as a backfill:

```bash
python -m pipeline.ingest --backfill
```

The default generator provides 100 customers and 505 orders (500 normal plus labelled edge cases). Configure mock volume before starting the API:

```bash
export MOCK_CUSTOMERS=10000
export MOCK_ORDERS=100000
uvicorn pipeline.mock_api:app --app-dir src --host 127.0.0.1 --port 8000
```

Use `PAGE_SIZE=500` to reduce request count for large runs. `MOCK_TRANSIENT_FAILURE=1` injects one transient HTTP 500 on page 2 per entity to exercise retries. The API rate limit may produce HTTP 429 responses; the client respects `Retry-After` and retries transient errors.

## Tests

```bash
pytest
```

The suite covers deterministic generation, empty input, malformed/missing fields, negative amounts, invalid foreign keys, duplicate keys, raw retention, rerun idempotency and unexpected schema shape. GitHub Actions runs the test suite on pushes and pull requests.

## Inspect results

Open SQLite with any SQLite browser or run:

```bash
sqlite3 data/output/pipeline.db "SELECT status, input_count, output_count, rejected_count, pages_fetched, duration_seconds FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;"
sqlite3 data/output/pipeline.db "SELECT entity, reason, COUNT(*) AS rejected_rows FROM quarantine GROUP BY entity, reason ORDER BY rejected_rows DESC;"
sqlite3 data/output/pipeline.db "SELECT COUNT(*) AS customers FROM customers; SELECT COUNT(*) AS orders FROM orders; SELECT COUNT(*) AS event_versions FROM order_events;"
sqlite3 data/output/pipeline.db "SELECT COUNT(*) AS orphan_orders FROM orders o LEFT JOIN customers c USING(customer_id) WHERE c.customer_id IS NULL;"
```

Expected invariant: `orphan_orders = 0`; `order_events` has at most one row per `(order_id, event_version)`; `orders` has at most one row per `order_id`. Rejected records remain traceable in `quarantine`. A run's input count is the number of records fetched on that run; output count is the number of records accepted for processing, including accepted upserts. Therefore input may differ from current table counts, and quarantine counts should be used to explain differences.

## Retry, restart, and backfill
- Transient HTTP failures and network timeouts retry up to `MAX_RETRIES` using exponential backoff; `Retry-After` takes precedence.
- Non-retryable HTTP failures (including 401/404) fail the run and record the failure reason.
- Each page's raw response, record writes, quarantine writes and checkpoint are committed together. Restart resumes at the next unprocessed page.
- `--backfill` clears checkpoints and replays source pages; deterministic keys and upserts keep target tables idempotent.
- The mock API can be reset by calling `POST /admin/reset-faults` with `X-API-Key: local-demo-key`.

## Performance measurement
The `scripts/benchmark.py` script runs repeatable synthetic volumes and reports elapsed time and records/second. In this environment, generating 1,000 customers and 10,005 orders took 0.038 seconds (about 289,670 generated records/second); this measures synthetic data generation, not database ingestion. A real mock-API integration run ingested 605 source records across 7 pages in about 1.50 seconds, accepted 601 and quarantined 4. A sensible first optimization is batched SQLite writes (`executemany`) after profiling. The design intentionally avoids unnecessary cloud infrastructure for a 72-hour take-home.

## Repository contents
- `src/pipeline/mock_api.py` — reproducible local API with pagination, auth, rate limit and transient faults
- `src/pipeline/ingest.py` — retrying, checkpointed ingestion and data quality rules
- `src/pipeline/db.py` — target and operational schema
- `src/pipeline/generator.py` — deterministic synthetic data
- `tests/` — automated tests
- `docs/` — schema dictionary and trade-offs
- `.github/workflows/tests.yml` — CI tests
