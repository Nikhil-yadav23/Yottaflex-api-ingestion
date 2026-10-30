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


## Setup (Windows PowerShell)

Requirements: Python 3.10 or later.

Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
```

If PowerShell blocks virtual-environment activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

The `.env.example` file documents supported environment variables. The application does not automatically load a `.env` file. The default mock API key is `local-demo-key`. Do not commit real credentials.

## Run locally (Windows PowerShell)

Open two PowerShell terminals from the repository root.

### Terminal 1 — Start the mock API

```powershell
$env:PYTHONPATH = "src"
python -m uvicorn pipeline.mock_api:app --host 127.0.0.1 --port 8000
```

Keep this terminal running while you execute the ingestion pipeline.

### Terminal 2 — Run ingestion

```powershell
$env:PYTHONPATH = "src"
$env:API_BASE_URL = "http://127.0.0.1:8000"
$env:API_KEY = "local-demo-key"
python -m pipeline.ingest
```

The pipeline prints a run summary containing input, accepted output, rejected records, pages fetched, and execution duration.

### Backfill

To replay all source pages:

```powershell
python -m pipeline.ingest --backfill
```

### Optional configuration

Set mock data volumes before starting the API in Terminal 1:

```powershell
$env:MOCK_CUSTOMERS = "10000"
$env:MOCK_ORDERS = "100000"
python -m uvicorn pipeline.mock_api:app --host 127.0.0.1 --port 8000
```

You can configure `PAGE_SIZE` to reduce requests for larger runs. Set `MOCK_TRANSIENT_FAILURE=1` before starting the mock API to simulate a transient HTTP 500 response on page 2 for each entity. The ingestion client retries transient errors and respects `Retry-After` when provided.


## Tests
Run the automated test suite from the repository root:
```powershell
python -m pytest -v
```
The test suite initially passed 8 tests. After adding malformed-record and stale-event coverage, the latest GitHub Actions workflow passed successfully with the expanded test suite.
The tests cover:
- Deterministic synthetic data generation
- Empty input and unexpected schema shapes
- Missing or malformed fields
- Negative amounts and invalid foreign keys
- Duplicate business keys
- Raw response retention
- Idempotent reruns
- Malformed-record quarantine and stale-event handling

GitHub Actions automatically runs the test suite on pushes and pull requests. 
The initial workflow completed successfully.

## Validation results
The local integration run demonstrated successful ingestion with retry handling and data quality checks.
Sample ingestion run
- Records fetched: 605
- Records accepted: 601
- Records quarantined: 4
- Pages fetched: 7
- Execution time: approximately 1.67 seconds
A simulated transient HTTP 500 response was retried, and the ingestion run subsequently completed successfully.

Database reconciliation
The following checks returned zero violations:
- Orphan orders: 0
- Duplicate order IDs: 0
- Duplicate event versions: 0
Rejected records remain traceable in the quarantine table for investigation. These results represent the demonstrated local run; results may differ with other data volumes or configurations.

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
The `scripts/benchmark.py` script runs repeatable synthetic volumes and reports elapsed time and records/second. In this environment, generating 1,000 customers and 10,005 orders took 0.038 seconds (about 289,670 generated records/second); this measures synthetic data generation, not database ingestion. A real mock-API integration run ingested 605 source records across 7 pages in approximately 1.67 seconds, accepted 601 and quarantined 4. Actual execution time may vary between runs. A sensible first optimization is batched SQLite writes (`executemany`) after profiling. The design intentionally avoids unnecessary cloud infrastructure for a 72-hour take-home.

## Repository contents
- `src/pipeline/mock_api.py` — reproducible local API with pagination, auth, rate limit and transient faults
- `src/pipeline/ingest.py` — retrying, checkpointed ingestion and data quality rules
- `src/pipeline/db.py` — target and operational schema
- `src/pipeline/generator.py` — deterministic synthetic data
- `tests/` — automated tests
- `docs/` — schema dictionary and trade-offs
- `.github/workflows/tests.yml` — CI tests
