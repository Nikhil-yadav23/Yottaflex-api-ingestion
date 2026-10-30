
"""Benchmark end-to-end ingestion through the local mock API.

Measures HTTP pagination, validation, SQLite writes, and pipeline overhead.
This is a local benchmark, not a production-load test.
"""
import argparse
import json
import os
import tempfile
import time
from pathlib import Path

from pipeline.ingest import IngestionPipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark end-to-end ingestion against a running mock API."
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--requests-per-second", type=float, default=100)
    parser.add_argument("--repeats", type=int, default=1)
    args = parser.parse_args()

    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    if args.requests_per_second <= 0:
        parser.error("--requests-per-second must be positive")
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")

    base_url = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
    api_key = os.getenv("API_KEY", "local-demo-key")
    results = []

    with tempfile.TemporaryDirectory(prefix="ingestion-benchmark-") as temp:
        root = Path(temp)

        for iteration in range(1, args.repeats + 1):
            db_path = root / f"run_{iteration}.db"
            raw_dir = root / f"raw_{iteration}"

            pipeline = IngestionPipeline(
                base_url=base_url,
                api_key=api_key,
                db_path=str(db_path),
                raw_dir=str(raw_dir),
                page_size=args.page_size,
                requests_per_second=args.requests_per_second,
            )

            start = time.perf_counter()
            try:
                result = pipeline.run(backfill=True)
            finally:
                pipeline.session.close()

            elapsed = time.perf_counter() - start
            total = result["input"]
            results.append(
                {
                    "iteration": iteration,
                    "status": result["status"],
                    "input_records": total,
                    "accepted_records": result["output"],
                    "rejected_records": result["rejected"],
                    "pages_fetched": result["pages"],
                    "elapsed_seconds": round(elapsed, 4),
                    "input_records_per_second": round(total / elapsed, 2)
                    if elapsed > 0
                    else 0,
                }
            )

    print(
        json.dumps(
            {
                "benchmark": "end_to_end_mock_api_ingestion",
                "base_url": base_url,
                "page_size": args.page_size,
                "requests_per_second": args.requests_per_second,
                "results": results,
                "note": (
                    "Measures mock API requests, validation, SQLite writes, and "
                    "pipeline overhead. Uses a fresh temporary database per run."
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
