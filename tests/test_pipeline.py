import json
from pathlib import Path
from pipeline.db import connect
from pipeline.generator import generate_dataset
from pipeline.ingest import IngestionPipeline, _validate_customer, _validate_order


def test_empty_input_is_supported_and_deterministic():
    data = generate_dataset(0, 0, seed=7, include_edge_cases=False)
    assert data == {"customers": [], "orders": []}


def test_generator_is_reproducible():
    assert generate_dataset(5, 10, seed=11) == generate_dataset(5, 10, seed=11)


def test_customer_validation_missing_and_malformed():
    assert _validate_customer({"customer_id": "x"}) == "missing_required_field:email"
    assert _validate_customer({"customer_id": "x", "email": "bad", "created_at": "2025-01-01T00:00:00Z", "status": "active"}) == "invalid_email"


def test_order_domain_edge_cases():
    good = {"order_id": "o1", "customer_id": "c1", "amount": 2, "currency": "USD", "status": "paid", "updated_at": "2025-01-01T00:00:00Z", "event_version": 1}
    assert _validate_order({**good, "amount": -1}, {"c1"}) == "negative_amount"
    assert _validate_order({**good, "customer_id": "missing"}, {"c1"}) == "invalid_customer_reference"
    assert _validate_order({**good, "amount": None}, {"c1"}) == "missing_required_field:amount"


def test_happy_path_quarantine_idempotency_and_raw_retention(tmp_path):

    data = {
        "customers": [
            {
                "customer_id": "c1",
                "email": "c1@example.test",
                "created_at": "2025-01-01T00:00:00Z",
                "status": "active",
            },
            None,
            "malformed customer",
        ],
        "orders": [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "amount": 10.5,
                "currency": "USD",
                "status": "paid",
                "updated_at": "2025-01-01T00:00:00Z",
                "event_version": 1,
            },
            {
                "order_id": "o1",
                "customer_id": "c1",
                "amount": 10.5,
                "currency": "USD",
                "status": "paid",
                "updated_at": "2025-01-01T00:00:00Z",
                "event_version": 1,
            },
            {
                "order_id": "o2",
                "customer_id": "missing",
                "amount": 5,
                "currency": "USD",
                "status": "created",
                "updated_at": "2025-01-01T00:00:00Z",
                "event_version": 1,
            },
            None,
            "malformed order",
        ],
    }

    class FakePipeline(IngestionPipeline):
        def _get_page(self, entity, page):
            records = data[entity]
            if page > 1: return {"data": [], "next_page": None}, 200
            return {"data": records, "next_page": None}, 200
    db = tmp_path / "test.db"
    pipe = FakePipeline(db_path=str(db), raw_dir=str(tmp_path / "raw"), page_size=10, requests_per_second=10000)
    first = pipe.run()
    assert first["status"] == "success"
    assert first["input"] == 8
    assert first["output"] == 2
    assert first["rejected"] == 6
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 6
    assert conn.execute("SELECT COUNT(*) FROM raw_pages").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM quarantine WHERE reason = 'record_not_object'").fetchone()[0] == 4
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'failed'").fetchone()[0] == 0
    conn.close()
    # A normal rerun starts after the saved checkpoints and does not duplicate target rows.
    second = pipe.run()
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0] == 1
    conn.close()
    assert second["input"] == 0
    assert list((tmp_path / "raw").glob("*/*/*.json"))


def test_schema_change_fails_and_records_run(tmp_path):
    class BadPipeline(IngestionPipeline):
        def _get_page(self, entity, page): return {"unexpected": []}, 200
    pipe = BadPipeline(db_path=str(tmp_path / "bad.db"), raw_dir=str(tmp_path / "raw"))
    try:
        pipe.run()
        assert False, "Expected schema validation failure"
    except ValueError as exc:
        assert "Schema change" in str(exc)
    conn = connect(tmp_path / "bad.db")
    assert conn.execute("SELECT status FROM pipeline_runs").fetchone()[0] == "failed"
    conn.close()


def test_empty_pipeline_batch_succeeds(tmp_path):
    class EmptyPipeline(IngestionPipeline):
        def _get_page(self, entity, page): return {"data": [], "next_page": None}, 200
    result = EmptyPipeline(db_path=str(tmp_path / "empty.db"), raw_dir=str(tmp_path / "raw")).run()
    assert result["status"] == "success"
    assert result["input"] == result["output"] == result["rejected"] == 0


def test_transient_http_error_is_retried(monkeypatch, tmp_path):
    import pipeline.ingest as ingest_module
    monkeypatch.setattr(ingest_module.time, "sleep", lambda _: None)
    class Response:
        def __init__(self, status, payload=None): self.status_code, self.payload, self.headers = status, payload or {}, {}
        def json(self): return self.payload
        def raise_for_status(self):
            if self.status_code >= 400:
                import requests
                raise requests.HTTPError(f"HTTP {self.status_code}")
    class FakeSession:
        def __init__(self): self.headers = {}; self.calls = 0
        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1: return Response(500, {"detail":"temporary"})
            return Response(200, {"data": [], "next_page": None})
    pipe = IngestionPipeline(db_path=str(tmp_path / "retry.db"), raw_dir=str(tmp_path / "raw"), max_retries=2)
    fake = FakeSession(); pipe.session = fake
    payload, status = pipe._get_page("customers", 1)
    assert status == 200 and payload["data"] == []
    assert fake.calls == 2



def test_stale_order_event_is_quarantined_without_overwriting_newer_version(tmp_path):
    data = {
        "customers": [
            {
                "customer_id": "c1",
                "email": "c1@example.test",
                "created_at": "2025-01-01T00:00:00Z",
                "status": "active",
            }
        ],
        "orders": [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "amount": 10.5,
                "currency": "USD",
                "status": "paid",
                "updated_at": "2025-01-02T00:00:00Z",
                "event_version": 2,
            },
            {
                "order_id": "o1",
                "customer_id": "c1",
                "amount": 5.0,
                "currency": "USD",
                "status": "created",
                "updated_at": "2025-01-01T00:00:00Z",
                "event_version": 1,
            },
        ],
    }

    class FakePipeline(IngestionPipeline):
        def _get_page(self, entity, page):
            records = data[entity]
            if page > 1:
                return {"data": [], "next_page": None}, 200
            return {"data": records, "next_page": None}, 200

    db = tmp_path / "stale_event.db"
    pipe = FakePipeline(
        db_path=str(db),
        raw_dir=str(tmp_path / "raw"),
        page_size=10,
        requests_per_second=10000,
    )

    result = pipe.run()

    assert result["status"] == "success"
    assert result["input"] == 3
    assert result["output"] == 2
    assert result["rejected"] == 1

    conn = connect(db)

    order = conn.execute(
        "SELECT event_version, amount, status FROM orders WHERE order_id = 'o1'"
    ).fetchone()

    assert order["event_version"] == 2
    assert order["amount"] == 10.5
    assert order["status"] == "paid"

    assert conn.execute(
        "SELECT COUNT(*) FROM order_events"
    ).fetchone()[0] == 1

    assert conn.execute(
        "SELECT COUNT(*) FROM quarantine "
        "WHERE reason = 'stale_out_of_order_event'"
    ).fetchone()[0] == 1

    conn.close()
