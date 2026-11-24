from reversesync.changes import (
    STORE_INVENTORY as SPEC,
)
from reversesync.changes import (
    apply_changes,
    changed_columns,
    latest_per_key,
    retire_point,
)

BASE = {("A1", "S1"): {"sku": "A1", "store_id": "S1", "on_hand_qty": 10, "reorder_point": 5,
                       "safety_stock": 2, "supplier_id": "V1", "lead_time_days": 7, "status": "ACTIVE"}}


def change(cid, kind, sku="A1", store="S1", prev=None, **cols):
    row = {"change_id": cid, "change_type": kind, "sku": sku, "store_id": store, "previous_data": prev}
    row.update({c: cols.get(c) for c in SPEC.columns})
    return row


def test_partial_update_keeps_other_columns():
    out = apply_changes(BASE, [change(1, "U", on_hand_qty=4)], SPEC)
    assert out[("A1", "S1")]["on_hand_qty"] == 4
    assert out[("A1", "S1")]["supplier_id"] == "V1"


def test_changes_apply_in_change_id_order():
    out = apply_changes(BASE, [change(2, "U", on_hand_qty=9), change(1, "U", on_hand_qty=3)], SPEC)
    assert out[("A1", "S1")]["on_hand_qty"] == 9


def test_insert_and_delete():
    out = apply_changes(BASE, [change(1, "I", sku="B2", on_hand_qty=1, status="NEW"), change(2, "D")], SPEC)
    assert ("A1", "S1") not in out
    assert out[("B2", "S1")] == {"sku": "B2", "store_id": "S1", "on_hand_qty": 1, "reorder_point": None,
                                 "safety_stock": None, "supplier_id": None, "lead_time_days": None,
                                 "status": "NEW"}


def test_original_state_is_not_mutated():
    apply_changes(BASE, [change(1, "U", on_hand_qty=0)], SPEC)
    assert BASE[("A1", "S1")]["on_hand_qty"] == 10


def test_latest_per_key():
    latest = latest_per_key([change(1, "U"), change(3, "U"), change(2, "U", sku="B")], SPEC)
    assert latest[("A1", "S1")]["change_id"] == 3 and latest[("B", "S1")]["change_id"] == 2


def test_changed_columns_update_reports_only_real_changes():
    prev = BASE[("A1", "S1")]
    diffs = changed_columns(change(1, "U", prev=prev, on_hand_qty=4, status="ACTIVE"), SPEC)
    assert diffs == [("on_hand_qty", 10, 4)]          # status was set to the same value: not a change


def test_changed_columns_insert_and_delete():
    ins = changed_columns(change(1, "I", on_hand_qty=1, status="NEW"), SPEC)
    assert ("on_hand_qty", None, 1) in ins and len(ins) == 2
    dele = changed_columns(change(2, "D", prev=BASE[("A1", "S1")]), SPEC)
    assert ("supplier_id", "V1", None) in dele and len(dele) == 6


def test_retire_point_waits_for_every_stream():
    assert retire_point({"gold": 40, "changelog": 25}, ["gold", "changelog"]) == 25
    assert retire_point({"gold": 40}, ["gold", "changelog"]) is None
    assert retire_point({}, []) is None
