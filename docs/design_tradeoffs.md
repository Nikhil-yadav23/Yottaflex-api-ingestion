# Design and trade-off note

## Decisions
- **Python + Requests + FastAPI:** familiar, easy to run locally, and enough to demonstrate a real HTTP boundary without cloud credentials.
- **SQLite:** zero-service local persistence, SQL inspection, constraints, and transactional page commits. A production deployment would likely use a warehouse/lakehouse plus a durable orchestration platform.
- **Page-level checkpoints:** checkpoint updates commit in the same SQLite transaction as page processing. A failed run can resume after the last committed page. A `--backfill` run clears checkpoints and replays all pages.
- **Retry policy:** retry only transient HTTP status codes (429, 500, 502, 503, 504) and connection/timeouts, using Retry-After when supplied or exponential backoff otherwise. Authentication and other 4xx errors fail fast.
- **Raw retention:** each successful page response is persisted before transformation, both as a JSON file and in `raw_pages`; this supports replay and debugging. In a real environment, apply retention and access controls.
- **Quarantine over silent drops:** every rejected record keeps its source payload, page, key, run and reason.
- **Current state + history:** `orders` stores the latest event version while `order_events` preserves event versions. The mock data includes a stale/out-of-order example.
- **Idempotency:** deterministic source keys, upserts, unique event keys, and checkpoints prevent unintended duplicates on rerun. Backfill can add a new quarantine occurrence per run for rejected records; this is intentional audit history.

## Limitations
- Checkpoints are page-based; this demo does not model cursor tokens or source-side change-data-capture watermarks. A production incremental API should use a stable cursor/updated-since watermark and define deletion semantics.
- The mock API's transient failure is deterministic and limited to page 2; it exists for repeatable retry demonstrations. The local rate limit is intentionally small.
- SQLite is suitable for a local assessment, not concurrent multi-worker production ingestion.
- Raw JSON files and the SQLite raw page table store parsed JSON rather than byte-for-byte HTTP wire data.

## Optimization
For larger volumes, use `executemany`/batched writes and bulk validation rather than row-at-a-time statements. The current page-level transaction already avoids committing each row. Measure with the included benchmark script before optimizing.
