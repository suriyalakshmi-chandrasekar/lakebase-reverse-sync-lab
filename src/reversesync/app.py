"""Minimal app backend: reads the unified view, writes through apply_inventory_edit().

    DATABASE_URL=postgresql://user:pass@host:5432/db uvicorn reversesync.app:app

On Lakebase, the password is a short-lived OAuth token; refresh it before it
expires (see the Lakebase docs on OAuth roles).
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from pydantic import BaseModel

from .changes import STORE_INVENTORY

app = FastAPI(title="Lakebase reverse-sync demo app")


@contextmanager
def connect():
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row, autocommit=True) as conn:
        yield conn


class InventoryValues(BaseModel):
    on_hand_qty: int | None = None
    reorder_point: int | None = None
    safety_stock: int | None = None
    supplier_id: str | None = None
    lead_time_days: int | None = None
    status: str | None = None


def _edit(sku: str, store_id: str, kind: str, values: dict[str, Any], user: str) -> dict:
    started = time.perf_counter()
    try:
        with connect() as conn:
            change_id = conn.execute(
                "SELECT app.apply_inventory_edit(%s, %s, %s, %s::jsonb, %s) AS id",
                (sku, store_id, kind, json.dumps(values), user),
            ).fetchone()["id"]
    except psycopg.errors.RaiseException as exc:
        raise HTTPException(409 if "exists" in str(exc) else 404, str(exc).splitlines()[0]) from None
    return {"change_id": change_id, "ms": round((time.perf_counter() - started) * 1000, 1)}


@app.get("/inventory/{sku}/{store_id}")
def get_item(sku: str, store_id: str) -> dict:
    with connect() as conn:
        row = conn.execute("SELECT * FROM app.store_inventory_current WHERE sku = %s AND store_id = %s",
                           (sku, store_id)).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.post("/inventory/{sku}/{store_id}", status_code=201)
def create_item(sku: str, store_id: str, body: InventoryValues, user: str = "demo") -> dict:
    return _edit(sku, store_id, "I", body.model_dump(), user)


@app.patch("/inventory/{sku}/{store_id}")
def update_item(sku: str, store_id: str, body: InventoryValues, user: str = "demo") -> dict:
    changes = body.model_dump(exclude_none=True)
    if not changes:
        raise HTTPException(400, "nothing to update")
    return _edit(sku, store_id, "U", changes, user)


@app.delete("/inventory/{sku}/{store_id}")
def delete_item(sku: str, store_id: str, user: str = "demo") -> dict:
    return _edit(sku, store_id, "D", {}, user)


@app.get("/health")
def health() -> dict:
    with connect() as conn:
        pending = conn.execute("SELECT count(*) AS n FROM app.store_inventory_changes").fetchone()["n"]
    return {"ok": True, "pending_changes": pending, "table": STORE_INVENTORY.name}
