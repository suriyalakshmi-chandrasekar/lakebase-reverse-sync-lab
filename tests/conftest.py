import os
import random
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SQL = REPO / "sql" / "postgres" / "01_change_capture.sql"


def seed_rows(n=300, seed=11):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append({"sku": f"SKU{i // 10:04d}", "store_id": f"ST{i % 10:02d}",
                     "on_hand_qty": rng.randint(0, 500), "reorder_point": rng.randint(10, 60),
                     "safety_stock": rng.randint(0, 20), "supplier_id": f"V{rng.randint(1, 25):03d}",
                     "lead_time_days": rng.choice([3, 5, 7, 14]), "status": "ACTIVE"})
    return rows


@pytest.fixture(scope="module")
def pg():
    """A fresh 'app' schema on the Postgres in DATABASE_URL (CI runs a Postgres service)."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    psycopg = pytest.importorskip("psycopg")
    from psycopg.rows import dict_row

    conn = psycopg.connect(url, autocommit=True, row_factory=dict_row)
    conn.execute("DROP SCHEMA IF EXISTS app CASCADE")
    conn.execute(SQL.read_text())
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO app.store_inventory_synced VALUES (%(sku)s, %(store_id)s, %(on_hand_qty)s, "
            "%(reorder_point)s, %(safety_stock)s, %(supplier_id)s, %(lead_time_days)s, %(status)s)",
            seed_rows())
    yield conn
    conn.close()
