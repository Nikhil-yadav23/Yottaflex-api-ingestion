"""SQLite storage and schema management."""
from __future__ import annotations
import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS pipeline_runs (
 run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT, status TEXT NOT NULL,
 input_count INTEGER NOT NULL DEFAULT 0, output_count INTEGER NOT NULL DEFAULT 0,
 rejected_count INTEGER NOT NULL DEFAULT 0, pages_fetched INTEGER NOT NULL DEFAULT 0,
 error_message TEXT, duration_seconds REAL
);
CREATE TABLE IF NOT EXISTS checkpoints (
 entity TEXT PRIMARY KEY, last_page INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS customers (
 customer_id TEXT PRIMARY KEY, email TEXT NOT NULL, created_at TEXT NOT NULL,
 status TEXT NOT NULL, source_run_id TEXT NOT NULL, source_page INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
 order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, amount REAL NOT NULL CHECK(amount >= 0),
 currency TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL,
 event_version INTEGER NOT NULL, source_run_id TEXT NOT NULL, source_page INTEGER NOT NULL,
 FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);
CREATE TABLE IF NOT EXISTS order_events (
 order_id TEXT NOT NULL, event_version INTEGER NOT NULL, customer_id TEXT, amount REAL,
 currency TEXT, status TEXT, updated_at TEXT, payload_json TEXT NOT NULL,
 source_run_id TEXT NOT NULL, PRIMARY KEY(order_id, event_version)
);
CREATE TABLE IF NOT EXISTS quarantine (
 reject_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, entity TEXT NOT NULL,
 source_page INTEGER NOT NULL, record_key TEXT, reason TEXT NOT NULL, payload_json TEXT NOT NULL,
 rejected_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_pages (
 raw_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, entity TEXT NOT NULL,
 page INTEGER NOT NULL, fetched_at TEXT NOT NULL, http_status INTEGER NOT NULL,
 response_json TEXT NOT NULL, UNIQUE(run_id, entity, page)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_run ON quarantine(run_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
"""

def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn
