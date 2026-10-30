"""Restartable paginated ingestion, validation, transformation and publishing."""
from __future__ import annotations
import argparse, json, logging, os, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests
from pipeline.db import connect

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _validate_customer(record: dict[str, Any]) -> str | None:
    for field in ("customer_id", "email", "created_at", "status"):
        if record.get(field) in (None, ""):
            return f"missing_required_field:{field}"
    if "@" not in str(record["email"]): return "invalid_email"
    if record["status"] not in {"active", "inactive"}: return "invalid_customer_status"
    try: datetime.fromisoformat(str(record["created_at"]).replace("Z", "+00:00"))
    except ValueError: return "invalid_created_at"
    return None

def _validate_order(record: dict[str, Any], customer_ids: set[str]) -> str | None:
    for field in ("order_id", "customer_id", "amount", "currency", "status", "updated_at", "event_version"):
        if record.get(field) is None or record.get(field) == "": return f"missing_required_field:{field}"
    try: amount = float(record["amount"])
    except (TypeError, ValueError): return "amount_not_numeric"
    if amount < 0: return "negative_amount"
    if record["currency"] not in {"USD", "EUR", "GBP", "INR"}: return "unsupported_currency"
    if record["status"] not in {"created", "paid", "shipped", "cancelled"}: return "invalid_order_status"
    try: datetime.fromisoformat(str(record["updated_at"]).replace("Z", "+00:00"))
    except ValueError: return "invalid_updated_at"
    try:
        if int(record["event_version"]) < 1: return "invalid_event_version"
    except (TypeError, ValueError): return "invalid_event_version"
    if record["customer_id"] not in customer_ids: return "invalid_customer_reference"
    return None

class IngestionPipeline:
    def __init__(self, base_url: str = "http://127.0.0.1:8000", api_key: str = "local-demo-key",
                 db_path: str = "data/output/pipeline.db", raw_dir: str = "data/output/raw",
                 page_size: int = 100, requests_per_second: float = 5, max_retries: int = 4,
                 backoff_base_seconds: float = 0.25, timeout: float = 10.0):
        self.base_url, self.api_key = base_url.rstrip("/"), api_key
        self.db_path, self.raw_dir = db_path, Path(raw_dir)
        self.page_size, self.requests_per_second = page_size, max(requests_per_second, 0.1)
        self.max_retries, self.backoff_base_seconds, self.timeout = max_retries, backoff_base_seconds, timeout
        self.session = requests.Session(); self.session.headers.update({"X-API-Key": api_key})
        self._last_request = 0.0

    def _get_page(self, entity: str, page: int) -> tuple[dict[str, Any], int]:
        url = f"{self.base_url}/api/{entity}"
        for attempt in range(self.max_retries + 1):
            delay = 1.0 / self.requests_per_second - (time.monotonic() - self._last_request)
            if delay > 0: time.sleep(delay)
            try:
                response = self.session.get(url, params={"page": page, "page_size": self.page_size}, timeout=self.timeout)
                self._last_request = time.monotonic()
                if response.status_code == 200: return response.json(), response.status_code
                if response.status_code not in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else self.backoff_base_seconds * (2 ** attempt)
                if attempt >= self.max_retries: response.raise_for_status()
                log.warning("Transient HTTP %s for %s page %s; retry %s/%s in %.2fs", response.status_code, entity, page, attempt+1, self.max_retries, wait)
                time.sleep(wait)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt >= self.max_retries: raise
                wait = self.backoff_base_seconds * (2 ** attempt)
                log.warning("Transient request error %s; retrying in %.2fs", exc, wait); time.sleep(wait)
        raise RuntimeError("Retry loop exited unexpectedly")

    def _save_raw(self, conn, run_id: str, entity: str, page: int, status: int, payload: dict[str, Any]) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        path = self.raw_dir / run_id / entity
        path.mkdir(parents=True, exist_ok=True)
        (path / f"page_{page:06d}.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        conn.execute("INSERT OR IGNORE INTO raw_pages(run_id,entity,page,fetched_at,http_status,response_json) VALUES(?,?,?,?,?,?)",
                     (run_id, entity, page, utc_now(), status, json.dumps(payload, sort_keys=True)))

    def run(self, backfill: bool = False) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        started = time.monotonic(); started_at = utc_now()
        counts = {"input": 0, "output": 0, "rejected": 0, "pages": 0}
        conn = connect(self.db_path)
        conn.execute("INSERT INTO pipeline_runs(run_id,started_at,status) VALUES(?,?,?)", (run_id, started_at, "running")); conn.commit()
        try:
            if backfill:
                conn.execute("DELETE FROM checkpoints"); conn.commit()
            seen_customer_ids: set[str] = set()
            seen_order_event_keys: set[str] = set()
            for entity in ("customers", "orders"):
                checkpoint = conn.execute("SELECT last_page FROM checkpoints WHERE entity=?", (entity,)).fetchone()
                page = (checkpoint["last_page"] + 1) if checkpoint else 1
                while True:
                    payload, status = self._get_page(entity, page)
                    self._save_raw(conn, run_id, entity, page, status, payload)
                    rows = payload.get("data")
                    if not isinstance(rows, list): raise ValueError(f"Schema change: {entity} page {page} missing list field 'data'")
                    counts["pages"] += 1; counts["input"] += len(rows)
                    # Duplicate keys within the same response are quarantined deterministically.
                    seen: set[str] = set()
                    customer_ids = {r[0] for r in conn.execute("SELECT customer_id FROM customers").fetchall()}
                    if entity == "customers":
                        for record in rows:
                            if not isinstance(record, dict):
                                self._reject(
                                    conn, run_id, entity, page, None, 
                                    "record_not_object", record
                                )
                                counts["rejected"] += 1
                                continue
                            key = str(record.get("customer_id", ""))
                            reason = _validate_customer(record)
                            if not reason and (key in seen or key in seen_customer_ids): reason = "duplicate_primary_key_in_run"
                            if not reason:
                                prior_this_run = conn.execute("SELECT 1 FROM customers WHERE customer_id=? AND source_run_id=?", (key, run_id)).fetchone()
                                if prior_this_run: reason = "duplicate_primary_key_in_run"
                            if reason:
                                self._reject(conn, run_id, entity, page, key, reason, record); counts["rejected"] += 1; continue
                            seen.add(key); seen_customer_ids.add(key)
                            conn.execute("INSERT INTO customers(customer_id,email,created_at,status,source_run_id,source_page) VALUES(?,?,?,?,?,?) ON CONFLICT(customer_id) DO UPDATE SET email=excluded.email,created_at=excluded.created_at,status=excluded.status,source_run_id=excluded.source_run_id,source_page=excluded.source_page",
                                         (key, record["email"], record["created_at"], record["status"], run_id, page)); counts["output"] += 1
                    else:
                        for record in rows:
                            if not isinstance(record, dict):
                                self._reject(
                                    conn, run_id, entity, page, None,
                                    "record_not_object", record
                                )
                                counts["rejected"] += 1
                                continue
                            key = str(record.get("order_id", ""))
                            event_key = f"{key}:{record.get('event_version', '')}"
                            reason = _validate_order(record, customer_ids)
                            if not reason and (event_key in seen or event_key in seen_order_event_keys): reason = "duplicate_event_in_run"
                            if not reason:
                                prior_this_run = conn.execute("SELECT 1 FROM order_events WHERE order_id=? AND event_version=? AND source_run_id=?", (key, int(record["event_version"]), run_id)).fetchone()
                                if prior_this_run: reason = "duplicate_event_in_run"
                            if not reason:
                                existing = conn.execute("SELECT event_version FROM orders WHERE order_id=?", (key,)).fetchone()
                                if existing and int(record["event_version"]) < existing["event_version"]:
                                    reason = "stale_out_of_order_event"
                            if reason:
                                self._reject(conn, run_id, entity, page, key, reason, record); counts["rejected"] += 1; continue
                            seen.add(event_key); seen_order_event_keys.add(event_key)
                            payload_json = json.dumps(record, sort_keys=True)
                            conn.execute("INSERT OR IGNORE INTO order_events(order_id,event_version,customer_id,amount,currency,status,updated_at,payload_json,source_run_id) VALUES(?,?,?,?,?,?,?,?,?)",
                                         (key, int(record["event_version"]), record["customer_id"], float(record["amount"]), record["currency"], record["status"], record["updated_at"], payload_json, run_id))
                            conn.execute("INSERT INTO orders(order_id,customer_id,amount,currency,status,updated_at,event_version,source_run_id,source_page) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET customer_id=excluded.customer_id,amount=excluded.amount,currency=excluded.currency,status=excluded.status,updated_at=excluded.updated_at,event_version=excluded.event_version,source_run_id=excluded.source_run_id,source_page=excluded.source_page WHERE excluded.event_version >= orders.event_version",
                                         (key, record["customer_id"], float(record["amount"]), record["currency"], record["status"], record["updated_at"], int(record["event_version"]), run_id, page)); counts["output"] += 1
                    conn.execute("INSERT INTO checkpoints(entity,last_page,updated_at) VALUES(?,?,?) ON CONFLICT(entity) DO UPDATE SET last_page=excluded.last_page,updated_at=excluded.updated_at", (entity, page, utc_now()))
                    conn.commit()  # Page-level commit makes restart resume from last successful page.
                    if payload.get("next_page") is None: break
                    page = int(payload["next_page"])
            duration = time.monotonic() - started
            conn.execute("UPDATE pipeline_runs SET ended_at=?,status='success',input_count=?,output_count=?,rejected_count=?,pages_fetched=?,duration_seconds=? WHERE run_id=?",
                         (utc_now(), counts["input"], counts["output"], counts["rejected"], counts["pages"], duration, run_id)); conn.commit()
            report = {"run_id": run_id, "status": "success", **counts, "duration_seconds": round(duration, 4), "db_path": self.db_path}
            log.info("Run complete: %s", json.dumps(report)); return report
        except Exception as exc:
            duration = time.monotonic() - started
            conn.execute("UPDATE pipeline_runs SET ended_at=?,status='failed',input_count=?,output_count=?,rejected_count=?,pages_fetched=?,error_message=?,duration_seconds=? WHERE run_id=?",
                         (utc_now(), counts["input"], counts["output"], counts["rejected"], counts["pages"], str(exc), duration, run_id)); conn.commit()
            log.exception("Pipeline failed run_id=%s", run_id); raise
        finally: conn.close()

    @staticmethod
    def _reject(conn, run_id, entity, page, record_key, reason, record):
        conn.execute("INSERT INTO quarantine(run_id,entity,source_page,record_key,reason,payload_json,rejected_at) VALUES(?,?,?,?,?,?,?)",
                     (run_id, entity, page, record_key or None, reason, json.dumps(record, sort_keys=True), utc_now()))

def main() -> None:
    parser = argparse.ArgumentParser(description="Run the reliable API ingestion pipeline")
    parser.add_argument("--backfill", action="store_true", help="clear page checkpoints and replay all source pages")
    args = parser.parse_args()
    pipeline = IngestionPipeline(
        base_url=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"), api_key=os.getenv("API_KEY", "local-demo-key"),
        db_path=os.getenv("DB_PATH", "data/output/pipeline.db"), raw_dir=os.getenv("RAW_DIR", "data/output/raw"),
        page_size=int(os.getenv("PAGE_SIZE", "100")), requests_per_second=float(os.getenv("REQUESTS_PER_SECOND", "5")),
        max_retries=int(os.getenv("MAX_RETRIES", "4")), backoff_base_seconds=float(os.getenv("BACKOFF_BASE_SECONDS", "0.25")))
    print(json.dumps(pipeline.run(backfill=args.backfill), indent=2))

if __name__ == "__main__": main()
