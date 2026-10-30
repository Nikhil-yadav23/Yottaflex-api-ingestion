
# Reliable Paginated API Ingestion Pipeline

A local-first data engineering project built for the YottaFlex.ai Associate AI Forward Deployed Engineer coding challenge.

The pipeline demonstrates reliable paginated HTTP ingestion, API-key authentication, retry and backoff, rate-limit handling, incremental checkpoints, raw response retention, schema and business validation, quarantine, idempotent publishing, event history, operational metrics, and automated testing.

## Architecture

```text
Synthetic source generator
        |
        v
FastAPI mock API
        |
        v
Paginated HTTP ingestion client
        |
        v
Raw page JSON + SQLite raw_pages
        |
        v
Schema and business validation
        |
        +----------------------+
        |                      |
        v                      v
Customers / Orders /       Quarantine
Order Events
        |
        v
Checkpoints + Run Metrics
        |
        v
SQL Reconciliation Checks
```

See the [data dictionary](docs/data_dictionary.md) and [design trade-offs](docs/design_tradeoffs.md).

## Requirements

- Python 3.10 or newer
- Windows PowerShell for the commands below
- No paid services or external credentials required for the local demo

## Setup

Run these commands from the repository root.

### 1. Create a virtual environment

```powershell
python -m venv .venv
```

### 2. Activate the environment

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, run the following command in the current terminal session, then activate the environment:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
python -m pip install -e ".[test]"
```

The `.env.example` file documents supported environment variables. The application does not automatically load a `.env` file.

The default local mock API key is `local-demo-key`. Do not commit real credentials or secrets.

## Run Locally

Open two PowerShell terminals from the repository root. Activate the virtual environment in both terminals.

### Terminal 1: Start the mock API

```powershell
$env:PYTHONPATH = "src"
python -m uvicorn pipeline.mock_api:app --host 127.0.0.1 --port 8000
```

Keep this terminal running.

### Terminal 2: Run ingestion

```powershell
$env:PYTHONPATH = "src"
$env:API_BASE_URL = "http://127.0.0.1:8000"
$env:API_KEY = "local-demo-key"
python -m pipeline.ingest
```

The pipeline prints a run summary containing the number of records fetched, accepted and rejected, pages fetched, and execution duration.

### Backfill

To replay all source pages:

```powershell
python -m pipeline.ingest --backfill
```

Run this command in Terminal 2.

## Optional Configuration

To increase the mock dataset, set these variables in Terminal 1 before starting the mock API:

```powershell
$env:MOCK_CUSTOMERS = "10000"
$env:MOCK_ORDERS = "100000"
$env:PYTHONPATH = "src"
python -m uvicorn pipeline.mock_api:app --host 127.0.0.1 --port 8000
```

You can configure `PAGE_SIZE` to reduce the number of requests for larger runs.

To simulate a transient HTTP 500 response on page 2 for each entity, set the following variable before starting the mock API:

```powershell
$env:MOCK_TRANSIENT_FAILURE = "1"
```

The ingestion client retries transient failures and respects `Retry-After` when provided.

## Automated Tests

Run the test suite from the repository root:

```powershell
python -m pytest -v
```

For a concise summary:

```powershell
python -m pytest -q
```

The suite covers the following areas:

- Deterministic synthetic data generation
- Empty input and unexpected schema shapes
- Missing or malformed fields
- Negative amounts and invalid foreign keys
- Duplicate business keys
- Raw response retention
- Idempotent reruns
- Malformed-record quarantine and stale-event handling
- Rate-limit handling and `Retry-After`
- Retry exhaustion
- Authentication failures that must not be retried

The exact passing test count should be taken from the latest test run. GitHub Actions is configured to run the test suite on pushes and pull requests.

## Validation Results

The following figures are from a demonstrated local integration run. Execution time can vary by environment and run.

| Metric | Observed result |
|---|---:|
| Records fetched | 605 |
| Records accepted | 601 |
| Records quarantined | 4 |
| Pages fetched | 7 |
| Execution time | Approximately 1.67 seconds |

A simulated transient HTTP 500 response was retried, and ingestion subsequently completed successfully.

### Database Reconciliation

The following checks returned zero violations in the demonstrated run:

| Check | Observed violations |
|---|---:|
| Orphan orders | 0 |
| Duplicate order IDs | 0 |
| Duplicate event versions | 0 |

Rejected records remain traceable in the quarantine table for investigation.

These figures describe the demonstrated run, not a guarantee for every configuration or dataset.

## Inspect Results

The pipeline uses SQLite for its output and operational state. You can inspect the database with a SQLite browser or the SQLite command-line tool.

View recent pipeline runs:

```sql
SELECT
    status,
    input_count,
    output_count,
    rejected_count,
    pages_fetched,
    duration_seconds
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 5;
```

Summarize quarantined records:

```sql
SELECT
    entity,
    reason,
    COUNT(*) AS rejected_rows
FROM quarantine
GROUP BY entity, reason
ORDER BY rejected_rows DESC;
```

Count the records in the target tables:

```sql
SELECT COUNT(*) AS customers FROM customers;
SELECT COUNT(*) AS orders FROM orders;
SELECT COUNT(*) AS event_versions FROM order_events;
```

Check for orphan orders:

```sql
SELECT COUNT(*) AS orphan_orders
FROM orders o
LEFT JOIN customers c USING (customer_id)
WHERE c.customer_id IS NULL;
```

Expected invariant: `orphan_orders = 0`.

The target tables are designed to avoid duplicate order IDs and duplicate `(order_id, event_version)` pairs. Rejected records remain available in quarantine.

A run's input count represents records fetched during that run. Its output count represents accepted records processed, including accepted upserts. These counts may differ from the current number of rows in the target tables.

## Retry, Restart, and Backfill

- Transient HTTP failures and network timeouts are retried up to `MAX_RETRIES` using exponential backoff.
- `Retry-After` takes precedence when provided by the API.
- Non-retryable HTTP errors, including 401 and 404, fail the run rather than being retried as transient failures.
- Raw page retention, record writes, quarantine writes, and the page checkpoint are committed together in SQLite.
- Restarting ingestion resumes from the next unprocessed page.
- `--backfill` clears checkpoints and replays source pages. Deterministic keys and upserts support idempotent publishing.
- The mock API can be reset by calling `POST /admin/reset-faults` with `X-API-Key: local-demo-key`.

Page-number pagination cannot guarantee a consistent snapshot if the source dataset changes while pages are being fetched. Raw files and SQLite transactions are also separate resources and are not committed atomically together.

## Performance Measurement

Run the synthetic-data benchmark with:

```powershell
python scripts/benchmark.py
```

The previously recorded benchmark generated 1,000 customers and 10,005 orders in approximately 0.038 seconds, or about 289,670 generated records per second.

**This measures synthetic-data generation, not end-to-end database ingestion.**

The demonstrated mock-API integration run processed 605 source records across 7 pages in approximately 1.67 seconds, accepted 601 records, and quarantined 4. These figures are illustrative and may vary by environment.

Performance optimization should be based on profiling. Batched SQLite writes using `executemany` may be worth evaluating if database writes are identified as a bottleneck.

## Repository Contents

- `src/pipeline/mock_api.py` — local mock API with pagination, authentication, rate limiting, and transient fault simulation
- `src/pipeline/ingest.py` — retrying, checkpointed ingestion and data-quality rules
- `src/pipeline/db.py` — target tables and operational database schema
- `src/pipeline/generator.py` — deterministic synthetic data generation
- `tests/` — automated tests
- `docs/` — data dictionary and design trade-offs
- `sql/` — SQL validation and reconciliation queries
- `scripts/benchmark.py` — synthetic-data generation benchmark
- `.github/workflows/tests.yml` — automated test workflow

## Project Goal

Demonstrate a reliable, testable, and locally reproducible API ingestion pipeline with explicit handling of transient failures, data quality issues, restartability, and idempotent publishing.
