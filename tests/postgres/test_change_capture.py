"""The Postgres side: change table, unified view, write function, retirement. Runs in CI."""

import json
from datetime import datetime, timedelta, timezone

import pytest


def edit(pg, sku, store, kind, values=None, user="tester"):
    return pg.execute("SELECT app.apply_inventory_edit(%s,%s,%s,%s::jsonb,%s) AS id",
                      (sku, store, kind, json.dumps(values or {}), user)).fetchone()["id"]


def current(pg, sku, store):
    return pg.execute("SELECT * FROM app.store_inventory_current WHERE sku=%s AND store_id=%s",
                      (sku, store)).fetchone()


def test_view_shows_synced_rows_without_edits(pg):
    assert pg.execute("SELECT count(*) n FROM app.store_inventory_current").fetchone()["n"] == 300
    assert current(pg, "SKU0000", "ST00")["has_pending_edit"] is False


def test_partial_update_is_visible_immediately_and_keeps_other_columns(pg):
    before = current(pg, "SKU0001", "ST01")
    edit(pg, "SKU0001", "ST01", "U", {"on_hand_qty": 7})
    after = current(pg, "SKU0001", "ST01")
    assert after["on_hand_qty"] == 7 and after["has_pending_edit"]
    assert after["supplier_id"] == before["supplier_id"]
    # the synced table itself is untouched (it belongs to the sync pipeline)
    synced = pg.execute("SELECT on_hand_qty FROM app.store_inventory_synced "
                        "WHERE sku='SKU0001' AND store_id='ST01'").fetchone()
    assert synced["on_hand_qty"] == before["on_hand_qty"]


def test_previous_data_snapshots_what_the_user_saw(pg):
    edit(pg, "SKU0002", "ST02", "U", {"status": "HOLD"})
    cid = edit(pg, "SKU0002", "ST02", "U", {"status": "ACTIVE"})
    prev = pg.execute("SELECT previous_data FROM app.store_inventory_changes WHERE change_id=%s",
                      (cid,)).fetchone()["previous_data"]
    assert prev["status"] == "HOLD"                      # the earlier edit, not the synced value


def test_insert_and_delete(pg):
    edit(pg, "NEW0001", "ST00", "I", {"on_hand_qty": 5, "status": "NEW"})
    assert current(pg, "NEW0001", "ST00")["status"] == "NEW"
    edit(pg, "SKU0003", "ST03", "D")
    assert current(pg, "SKU0003", "ST03") is None


@pytest.mark.parametrize("sku, store, kind", [("SKU0004", "ST04", "I"), ("NOPE", "ST00", "U"), ("NOPE", "ST00", "D")])
def test_invalid_edits_are_rejected(pg, sku, store, kind):
    import psycopg
    with pytest.raises(psycopg.errors.RaiseException):
        edit(pg, sku, store, kind, {"on_hand_qty": 1})


def test_change_table_has_replica_identity_full(pg):
    ident = pg.execute("SELECT relreplident FROM pg_class WHERE oid = 'app.store_inventory_changes'::regclass"
                       ).fetchone()["relreplident"]
    assert ident == "f"


def test_retirement_needs_both_conditions(pg):
    n_before = pg.execute("SELECT count(*) n FROM app.store_inventory_changes").fetchone()["n"]
    max_id = pg.execute("SELECT max(change_id) m FROM app.store_inventory_changes").fetchone()["m"]
    past = datetime.now(timezone.utc) - timedelta(days=1)
    # streams caught up, but the synced table has not refreshed since: keep everything
    assert pg.execute("SELECT app.retire_inventory_changes(%s, %s) n", (max_id, past)).fetchone()["n"] == 0
    # synced refreshed, but streams processed only up to change 1: retire only that one
    future = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert pg.execute("SELECT app.retire_inventory_changes(1, %s) n", (future,)).fetchone()["n"] == 1
    assert pg.execute("SELECT count(*) n FROM app.store_inventory_changes").fetchone()["n"] == n_before - 1


def test_app_endpoints(pg):
    from fastapi.testclient import TestClient

    from reversesync.app import app
    c = TestClient(app)
    assert c.get("/inventory/SKU0005/ST05").status_code == 200
    r = c.patch("/inventory/SKU0005/ST05", json={"reorder_point": 99}, params={"user": "ana"})
    assert r.status_code == 200 and r.json()["change_id"] > 0
    assert c.get("/inventory/SKU0005/ST05").json()["reorder_point"] == 99
    assert c.post("/inventory/SKU0005/ST05", json={"on_hand_qty": 1}).status_code == 409
    assert c.delete("/inventory/NOPE/ST00").status_code == 404
    assert c.patch("/inventory/SKU0005/ST05", json={}).status_code == 400


def test_two_edits_of_different_columns_both_survive(pg):
    edit(pg, "SKU0006", "ST06", "U", {"on_hand_qty": 11})
    edit(pg, "SKU0006", "ST06", "U", {"status": "HOLD"})
    row = current(pg, "SKU0006", "ST06")
    assert row["on_hand_qty"] == 11 and row["status"] == "HOLD"
