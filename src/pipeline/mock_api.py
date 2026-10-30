"""Local mock API with API-key auth, pagination, rate limiting and transient faults."""
from __future__ import annotations
import os
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Any
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pipeline.generator import generate_dataset

app = FastAPI(title="YottaFlex Mock Source API", version="1.0.0")
API_KEY = os.getenv("API_KEY", "local-demo-key")
DATA = generate_dataset(
    n_customers=int(os.getenv("MOCK_CUSTOMERS", "100")),
    n_orders=int(os.getenv("MOCK_ORDERS", "500")), seed=42, include_edge_cases=True,
)
_lock = Lock()
_requests: dict[str, deque[float]] = defaultdict(deque)
_failed_once: set[str] = set()

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.post("/admin/reset-faults")
def reset_faults(x_api_key: str | None = Header(default=None)) -> dict[str, str]:
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    with _lock:
        _failed_once.clear(); _requests.clear()
    return {"status": "fault state reset"}

@app.get("/api/{entity}")
def get_records(entity: str, request: Request, page: int = Query(1, ge=1),
                page_size: int = Query(100, ge=1, le=1000),
                x_api_key: str | None = Header(default=None)) -> JSONResponse:
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid API key")
    if entity not in DATA:
        raise HTTPException(status_code=404, detail="Unknown entity")
    # Rate limit: at most 10 requests per 2 seconds per client.
    client = request.client.host if request.client else "unknown"
    now = time.monotonic()
    with _lock:
        history = _requests[client]
        while history and now - history[0] > 2.0:
            history.popleft()
        if len(history) >= 10:
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"}, headers={"Retry-After": "1"})
        history.append(now)
        # Simulate one transient 500 on page 2 for deterministic retry demonstrations.
        fault_key = f"{entity}:{page}"
        if page == 2 and fault_key not in _failed_once and os.getenv("MOCK_TRANSIENT_FAILURE", "1") == "1":
            _failed_once.add(fault_key)
            return JSONResponse(status_code=500, content={"detail": "Injected transient mock failure"})
    records = DATA[entity]
    start = (page - 1) * page_size
    items = records[start:start + page_size]
    next_page = page + 1 if start + page_size < len(records) else None
    return JSONResponse(content={"entity": entity, "page": page, "page_size": page_size,
        "total": len(records), "next_page": next_page, "data": items})
