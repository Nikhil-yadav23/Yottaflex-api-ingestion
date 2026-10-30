"""Small deterministic generator benchmark; no network calls required."""
import argparse, time
from pipeline.generator import generate_dataset

p = argparse.ArgumentParser()
p.add_argument("--customers", type=int, default=10000)
p.add_argument("--orders", type=int, default=100000)
p.add_argument("--seed", type=int, default=42)
a = p.parse_args()
start = time.perf_counter()
data = generate_dataset(a.customers, a.orders, seed=a.seed, include_edge_cases=True)
elapsed = time.perf_counter() - start
n = sum(map(len, data.values()))
print(f"customers={len(data['customers'])} orders={len(data['orders'])} total_records={n} elapsed_seconds={elapsed:.4f} records_per_second={n/elapsed:.0f} seed={a.seed}")
