"""Benchmark deterministic synthetic-data generation.

This benchmark does not measure HTTP ingestion or database write performance.
"""

import argparse
import time

from pipeline.generator import generate_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark synthetic-data generation only."
    )
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--orders", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.customers < 1:
        parser.error("--customers must be at least 1")
    if args.orders < 1:
        parser.error("--orders must be at least 1")

    start = time.perf_counter()
    data = generate_dataset(
        args.customers,
        args.orders,
        seed=args.seed,
        include_edge_cases=True,
    )
    elapsed = time.perf_counter() - start

    customer_count = len(data["customers"])
    order_count = len(data["orders"])
    total_records = sum(len(records) for records in data.values())
    records_per_second = total_records / elapsed if elapsed > 0 else 0

    print("Benchmark: synthetic-data generation only")
    print(f"customers={customer_count}")
    print(f"orders={order_count}")
    print(f"total_records={total_records}")
    print(f"elapsed_seconds={elapsed:.4f}")
    print(f"generated_records_per_second={records_per_second:.0f}")
    print(f"seed={args.seed}")


if __name__ == "__main__":
    main()
