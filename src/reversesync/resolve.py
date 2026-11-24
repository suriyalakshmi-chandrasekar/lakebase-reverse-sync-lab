"""Databricks side: change rows (from Lakebase Change Data Feed) -> Gold + changelog.

Two independent consumers read the same change feed:

* ``resolve_to_gold``   MERGE the latest edit per key into the Gold table
* ``write_changelog``   one audit row per changed column, for the UI history

They run on their own schedules and never read each other's output, so one
being slow or failing never blocks the other. Each records how far it got in a
progress table; the Postgres change rows are retired only when both caught up.

In production the input DataFrame is a stream over the Lakebase CDF table
(``readStream ... foreachBatch``). The functions take a plain DataFrame, so the
same code runs in batch tests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .changes import TableSpec

CHANGELOG_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  change_id BIGINT, table_name STRING, row_key STRING, column_name STRING,
  old_value STRING, new_value STRING, change_type STRING,
  updated_by STRING, updated_at TIMESTAMP, logged_at TIMESTAMP
) USING DELTA
"""

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  stream STRING, table_name STRING, last_change_id BIGINT, updated_at TIMESTAMP
) USING DELTA
"""


def latest_changes(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """Newest change per key inside this batch (a batch may hold several edits of one row)."""
    w = Window.partitionBy(*spec.keys).orderBy(F.col("change_id").desc())
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def resolve_to_gold(spark: SparkSession, changes: DataFrame, gold_table: str, spec: TableSpec) -> int:
    """MERGE edits into Gold. Returns the highest change_id applied (0 if none).

    I -> insert, U -> update only the columns the edit set (NULL = unchanged),
    D -> delete. Re-running the same batch is safe: the result is identical.
    """
    if changes.isEmpty():
        return 0
    from delta.tables import DeltaTable

    batch = latest_changes(changes, spec)
    on = " AND ".join(f"t.{k} = s.{k}" for k in spec.keys)
    update_set = {c: f"coalesce(s.{c}, t.{c})" for c in spec.columns}
    insert_vals = {c: f"s.{c}" for c in (*spec.keys, *spec.columns)}

    (DeltaTable.forName(spark, gold_table).alias("t")
     .merge(batch.alias("s"), on)
     .whenMatchedDelete(condition="s.change_type = 'D'")
     .whenMatchedUpdate(condition="s.change_type IN ('I', 'U')", set=update_set)
     .whenNotMatchedInsert(condition="s.change_type IN ('I', 'U')", values=insert_vals)
     .execute())
    return int(changes.agg(F.max("change_id")).first()[0])


def build_changelog(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """One row per (change, changed column), new_value rebuilt from the change row itself.

    new_value = coalesce(edit value, previous value): no lookup of Gold, so this
    consumer has no timing dependency on ``resolve_to_gold``.
    """
    prev_schema = ", ".join(f"{c} STRING" for c in spec.columns)
    df = changes.withColumn("_prev", F.from_json(F.col("previous_data").cast("string"), prev_schema))
    entries = [
        F.struct(
            F.lit(c).alias("column_name"),
            F.when(F.col("change_type") == "I", F.lit(None)).otherwise(F.col(f"_prev.{c}")).alias("old_value"),
            F.when(F.col("change_type") == "D", F.lit(None))
             .otherwise(F.coalesce(F.col(c).cast("string"), F.col(f"_prev.{c}"))).alias("new_value"),
        )
        for c in spec.columns
    ]
    row_key = F.concat_ws("|", *[F.col(k).cast("string") for k in spec.keys])
    return (df.select("change_id", "change_type", "updated_by", "updated_at", row_key.alias("row_key"),
                      F.explode(F.array(*entries)).alias("e"))
              .select("change_id", F.lit(spec.name).alias("table_name"), "row_key", "e.column_name",
                      "e.old_value", "e.new_value", "change_type", "updated_by", "updated_at")
              .where(~F.col("old_value").eqNullSafe(F.col("new_value")))
              .withColumn("logged_at", F.current_timestamp()))


def write_changelog(spark: SparkSession, changes: DataFrame, changelog_table: str, spec: TableSpec) -> int:
    """Append changelog rows; idempotent (a re-processed change_id is not logged twice)."""
    spark.sql(CHANGELOG_DDL.format(table=changelog_table))
    if changes.isEmpty():
        return 0
    from delta.tables import DeltaTable
    rows = build_changelog(changes, spec)
    (DeltaTable.forName(spark, changelog_table).alias("t")
     .merge(rows.alias("s"), "t.change_id = s.change_id AND t.column_name = s.column_name "
                             f"AND t.table_name = '{spec.name}'")
     .whenNotMatchedInsertAll()
     .execute())
    return int(changes.agg(F.max("change_id")).first()[0])


class Progress:
    """How far each consumer got, per table. Used to decide what can be retired."""

    def __init__(self, spark: SparkSession, table: str):
        self.spark, self.table = spark, table
        spark.sql(PROGRESS_DDL.format(table=table))

    def record(self, stream: str, table_name: str, last_change_id: int) -> None:
        if not last_change_id:
            return
        self.spark.sql(f"""
            MERGE INTO {self.table} t
            USING (SELECT '{stream}' AS stream, '{table_name}' AS table_name,
                          CAST({int(last_change_id)} AS BIGINT) AS last_change_id) s
            ON t.stream = s.stream AND t.table_name = s.table_name
            WHEN MATCHED AND s.last_change_id > t.last_change_id
              THEN UPDATE SET last_change_id = s.last_change_id, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (stream, table_name, last_change_id, updated_at)
              VALUES (s.stream, s.table_name, s.last_change_id, current_timestamp())""")

    def read(self, table_name: str) -> dict[str, int]:
        rows = self.spark.sql(f"SELECT stream, last_change_id FROM {self.table} "
                              f"WHERE table_name = '{table_name}'").collect()
        return {r.stream: int(r.last_change_id) for r in rows}


def compare(expected: DataFrame, actual: DataFrame, spec: TableSpec) -> dict[str, int]:
    """Row-level comparison of the app view (expected) with Gold (actual)."""
    cols = [*spec.keys, *spec.columns]
    e, a = expected.select(*cols), actual.select(*cols)
    keys_e, keys_a = e.select(*spec.keys), a.select(*spec.keys)
    matched = e.alias("e").join(a.alias("a"), list(spec.keys))
    differs = " OR ".join(f"NOT (e.{c} <=> a.{c})" for c in spec.columns)
    return {
        "expected_rows": e.count(),
        "actual_rows": a.count(),
        "missing_in_actual": keys_e.subtract(keys_a).count(),
        "extra_in_actual": keys_a.subtract(keys_e).count(),
        "value_mismatches": matched.where(differs).count(),
    }
