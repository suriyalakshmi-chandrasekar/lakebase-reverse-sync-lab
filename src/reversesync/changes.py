"""Pure-Python core of the pattern: how edits combine, what changed, when to retire.

The Postgres view and the Spark MERGE both implement these rules; this module
is the reference used by tests to check that all three agree.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableSpec:
    name: str
    keys: tuple[str, ...]
    columns: tuple[str, ...]          # business columns (not keys, not audit columns)


STORE_INVENTORY = TableSpec(
    name="store_inventory",
    keys=("sku", "store_id"),
    columns=("on_hand_qty", "reorder_point", "safety_stock", "supplier_id", "lead_time_days", "status"),
)


def key_of(row: Mapping[str, Any], spec: TableSpec) -> tuple:
    return tuple(row[k] for k in spec.keys)


def latest_per_key(changes: Iterable[Mapping[str, Any]], spec: TableSpec) -> dict[tuple, Mapping[str, Any]]:
    """The newest change (highest change_id) for each key."""
    latest: dict[tuple, Mapping[str, Any]] = {}
    for c in changes:
        k = key_of(c, spec)
        if k not in latest or c["change_id"] > latest[k]["change_id"]:
            latest[k] = c
    return latest


def apply_changes(state: dict[tuple, dict], changes: Iterable[Mapping[str, Any]], spec: TableSpec) -> dict[tuple, dict]:
    """Apply edits to a {key: row} state, in change_id order. Returns a new state.

    I = insert the row, U = overwrite only the non-NULL columns, D = remove.
    """
    out = {k: dict(v) for k, v in state.items()}
    for c in sorted(changes, key=lambda r: r["change_id"]):
        k = key_of(c, spec)
        if c["change_type"] == "D":
            out.pop(k, None)
        elif c["change_type"] == "I" or k not in out:
            out[k] = {**{x: c[x] for x in spec.keys}, **{col: c.get(col) for col in spec.columns}}
        else:
            out[k].update({col: c[col] for col in spec.columns if c.get(col) is not None})
    return out


def changed_columns(change: Mapping[str, Any], spec: TableSpec) -> list[tuple[str, Any, Any]]:
    """(column, old_value, new_value) for every column an edit actually changed.

    old comes from previous_data (the row the user saw); new is the edit's value,
    falling back to previous_data for columns the edit left NULL. Columns whose
    value did not change are not reported.
    """
    prev = change.get("previous_data") or {}
    kind = change["change_type"]
    out = []
    for col in spec.columns:
        old = prev.get(col)
        new = None if kind == "D" else (change.get(col) if change.get(col) is not None else old)
        if kind == "I":
            old = None
        if _norm(old) != _norm(new):
            out.append((col, old, new))
    return out


def _norm(v: Any) -> str | None:
    return None if v is None else str(v)


def retire_point(progress: Mapping[str, int | None], required_streams: Iterable[str]) -> int | None:
    """Highest change_id that every required stream has processed.

    Returns None if any stream has not reported progress yet: nothing is safe
    to delete until all consumers have caught up at least once.
    """
    values = [progress.get(s) for s in required_streams]
    if not values or any(v is None for v in values):
        return None
    return min(values)
