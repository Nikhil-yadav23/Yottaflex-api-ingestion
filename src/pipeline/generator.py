"""Deterministic synthetic source data generator."""
from __future__ import annotations
import random
from datetime import datetime, timedelta, timezone
from typing import Any


def generate_dataset(n_customers: int = 100, n_orders: int = 500, seed: int = 42,
                     include_edge_cases: bool = True) -> dict[str, list[dict[str, Any]]]:
    """Generate reproducible customer/order records; edge cases are intentionally labelled."""
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customers = []
    for i in range(1, n_customers + 1):
        customers.append({
            "customer_id": f"cus_{i:06d}",
            "email": f"customer{i}@example.test",
            "created_at": (base + timedelta(days=i % 365)).isoformat().replace("+00:00", "Z"),
            "status": "active" if i % 7 else "inactive",
        })
    orders = []
    for i in range(1, n_orders + 1):
        customer_num = rng.randint(1, max(n_customers, 1)) if n_customers else 0
        orders.append({
            "order_id": f"ord_{i:07d}",
            "customer_id": f"cus_{customer_num:06d}",
            "amount": round(rng.uniform(1, 2500), 2),
            "currency": "USD",
            "status": rng.choice(["created", "paid", "shipped", "cancelled"]),
            "updated_at": (base + timedelta(minutes=i * 7)).isoformat().replace("+00:00", "Z"),
            "event_version": 1,
        })
    if include_edge_cases and n_customers >= 2 and n_orders >= 5:
        # Duplicate primary key; missing required field; invalid FK; negative amount;
        # late/out-of-order update. Labels are metadata, not used as input fields.
        orders.append(dict(orders[0]))  # duplicate order_id
        bad_missing = dict(orders[1]); bad_missing["amount"] = None; orders.append(bad_missing)
        bad_fk = dict(orders[2]); bad_fk["order_id"] = "ord_invalid_fk"; bad_fk["customer_id"] = "cus_999999"; orders.append(bad_fk)
        bad_amount = dict(orders[3]); bad_amount["order_id"] = "ord_negative_amount"; bad_amount["amount"] = -5.0; orders.append(bad_amount)
        late = dict(orders[4]); late["order_id"] = "ord_late_event"; late["updated_at"] = "2024-12-01T00:00:00Z"; late["event_version"] = 2; orders.append(late)
    return {"customers": customers, "orders": orders}
