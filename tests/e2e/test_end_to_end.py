"""Full loop: app edits in Postgres -> change feed -> Spark -> Gold + changelog -> retirement.

Lakebase Change Data Feed is simulated by reading the change table with psycopg
and turning the rows into a Spark DataFrame with the same columns. Runs in CI
(Postgres service + local Spark/Delta).
"""

import json
import random

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from conftest import seed_rows  # noqa: E402

from reversesync.changes import STORE_INVENTORY as SPEC  # noqa: E402
from reversesync.changes import apply_changes, retire_point  # noqa: E402

CHANGE_SCHEMA = ("change_id BIGINT, sku STRING, store_id STRING, on_hand_qty INT, reorder_point INT, "
                 "safety_stock INT, supplier_id STRING, lead_time_days INT, status STRING, "
                 "change_type STRING, previous_data STRING, updated_by STRING, updated_at TIMESTAMP")
GOLD_SCHEMA = ("sku STRING, store_id STRING, on_hand_qty INT, reorder_point INT, safety_stock INT, "
               "supplier_id STRING, lead_time_days INT, status STRING")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        b = (SparkSession.builder.master("local[2]").appName("reverse-sync")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false"))
        s = configure_spark_with_delta_pip(b).getOrCreate()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Spark not available: {exc}")
    s.sql("CREATE SCHEMA IF NOT EXISTS gold")
    s.sql("CREATE SCHEMA IF NOT EXISTS silver")
    s.sql("CREATE SCHEMA IF NOT EXISTS meta")
    s.createDataFrame([tuple(r.values()) for r in seed_rows()], GOLD_SCHEMA) \
        .write.format("delta").mode("overwrite").saveAsTable("gold.store_inventory")
    return s


def edit(pg, sku, store, kind, values=None):
    pg.execute("SELECT app.apply_inventory_edit(%s,%s,%s,%s::jsonb,%s)",
               (sku, store, kind, json.dumps(values or {}), "e2e"))


def read_changes(pg, spark, after_id=0):
    """Stand-in for spark.readStream on the Lakebase CDF table."""
    rows = pg.execute("SELECT * FROM app.store_inventory_changes WHERE change_id > %s ORDER BY change_id",
                      (after_id,)).fetchall()
    data = [(r["change_id"], r["sku"], r["store_id"], r["on_hand_qty"], r["reorder_point"], r["safety_stock"],
             r["supplier_id"], r["lead_time_days"], r["status"], r["change_type"],
             None if r["previous_data"] is None else json.dumps(r["previous_data"]), r["updated_by"],
             r["updated_at"].replace(tzinfo=None)) for r in rows]
    return spark.createDataFrame(data, CHANGE_SCHEMA), rows


def view_df(pg, spark):
    rows = pg.execute("SELECT sku, store_id, on_hand_qty, reorder_point, safety_stock, supplier_id, "
                      "lead_time_days, status FROM app.store_inventory_current").fetchall()
    return spark.createDataFrame([tuple(r.values()) for r in rows], GOLD_SCHEMA)


def test_round_trip(pg, spark):
    from reversesync.resolve import Progress, compare, resolve_to_gold, write_changelog

    rng = random.Random(3)
    keys = [(r["sku"], r["store_id"]) for r in seed_rows()]
    for sku, store in rng.sample(keys, 60):                       # partial updates
        edit(pg, sku, store, "U", {"on_hand_qty": rng.randint(0, 50)})
    for sku, store in rng.sample(keys, 10):                       # second edit of some rows
        edit(pg, sku, store, "U", {"status": "HOLD"})
    for i in range(5):                                            # new rows
        edit(pg, f"NEW{i:04d}", "ST00", "I", {"on_hand_qty": i, "status": "NEW"})
    deleted = rng.sample(keys, 4)
    for sku, store in deleted:
        edit(pg, sku, store, "D")

    changes, raw = read_changes(pg, spark)
    progress = Progress(spark, "meta.reverse_sync_progress")
    progress.record("gold", SPEC.name, resolve_to_gold(spark, changes, "gold.store_inventory", SPEC))
    progress.record("changelog", SPEC.name, write_changelog(spark, changes, "silver.ui_changelog", SPEC))

    # Gold now matches what the app shows
    result = compare(view_df(pg, spark), spark.table("gold.store_inventory"), SPEC)
    assert result["missing_in_actual"] == result["extra_in_actual"] == result["value_mismatches"] == 0, result

    # ... and both match the pure-Python reference rules
    state = {(r["sku"], r["store_id"]): r for r in seed_rows()}
    expected = apply_changes(state, raw, SPEC)
    assert spark.table("gold.store_inventory").count() == len(expected) == 300 + 5 - 4

    # changelog: one row per really-changed column; deletes log every non-null column
    log = spark.table("silver.ui_changelog")
    assert log.where("change_type = 'D'").select("row_key").distinct().count() == 4
    assert log.where("change_type = 'I'").count() == 10           # 2 columns x 5 inserts
    assert log.where("old_value <=> new_value").count() == 0

    # replaying the same batch changes nothing (idempotent consumers)
    n_log = log.count()
    resolve_to_gold(spark, changes, "gold.store_inventory", SPEC)
    write_changelog(spark, changes, "silver.ui_changelog", SPEC)
    assert spark.table("silver.ui_changelog").count() == n_log
    assert compare(view_df(pg, spark), spark.table("gold.store_inventory"), SPEC)["value_mismatches"] == 0

    # forward sync: Gold -> synced table (Lakebase does this; simulated here)
    gold_rows = spark.table("gold.store_inventory").collect()
    pg.execute("TRUNCATE app.store_inventory_synced")
    with pg.cursor() as cur:
        cur.executemany("INSERT INTO app.store_inventory_synced VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        [tuple(r) for r in gold_rows])
    synced_as_of = pg.execute("SELECT now() t").fetchone()["t"]

    # retire: both streams caught up and the synced table refreshed after the edits
    point = retire_point(progress.read(SPEC.name), ["gold", "changelog"])
    assert point == max(r["change_id"] for r in raw)
    retired = pg.execute("SELECT app.retire_inventory_changes(%s, %s) n", (point, synced_as_of)).fetchone()["n"]
    assert retired == len(raw)

    # the app still sees exactly the same data, now served from the synced table alone
    after = compare(view_df(pg, spark), spark.table("gold.store_inventory"), SPEC)
    assert after["value_mismatches"] == after["missing_in_actual"] == after["extra_in_actual"] == 0
    assert pg.execute("SELECT count(*) n FROM app.store_inventory_current WHERE has_pending_edit").fetchone()["n"] == 0
