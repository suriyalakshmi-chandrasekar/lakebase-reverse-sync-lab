# Databricks notebook source
# MAGIC %md
# MAGIC # Resolve Lakebase edits into Gold (or the changelog)
# MAGIC
# MAGIC Reads the Delta table that **Lakebase Change Data Feed** maintains for `app.store_inventory_changes`
# MAGIC and runs ONE consumer, chosen by the `consumer` parameter. Deploy it twice as two jobs
# MAGIC (`gold` and `changelog`): independent schedules, independent failures, independent checkpoints.
# MAGIC
# MAGIC Trigger: `availableNow` drains everything new and stops; a **continuous** job trigger relaunches it.

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath("..")
sys.path.insert(0, os.path.join(repo_root, "src"))

for name, default in [("consumer", "gold"), ("cdf_table", "lakebase_cdf.app.store_inventory_changes"),
                      ("gold_table", "main.gold.store_inventory"), ("changelog_table", "main.silver.ui_changelog"),
                      ("progress_table", "main.meta.reverse_sync_progress"),
                      ("checkpoint_root", "/Volumes/main/meta/checkpoints/reverse_sync")]:
    dbutils.widgets.text(name, default)  # noqa: F821
p = {k: dbutils.widgets.get(k) for k in  # noqa: F821
     ["consumer", "cdf_table", "gold_table", "changelog_table", "progress_table", "checkpoint_root"]}

# COMMAND ----------

from pyspark.sql import functions as F

from reversesync.changes import STORE_INVENTORY as SPEC
from reversesync.resolve import Progress, resolve_to_gold, write_changelog

progress = Progress(spark, p["progress_table"])  # noqa: F821


def handle(batch_df, batch_id):
    # Lakebase CDF rows for inserts into the append-only change table; ignore other change kinds
    changes = batch_df.where(F.col("_change_type").isin("insert", "update_postimage")) if \
        "_change_type" in batch_df.columns else batch_df
    if p["consumer"] == "gold":
        last = resolve_to_gold(spark, changes, p["gold_table"], SPEC)  # noqa: F821
    else:
        last = write_changelog(spark, changes, p["changelog_table"], SPEC)  # noqa: F821
    progress.record(p["consumer"], SPEC.name, last)


(spark.readStream.option("readChangeFeed", "true").table(p["cdf_table"])  # noqa: F821
      .writeStream.foreachBatch(handle)
      .option("checkpointLocation", f"{p['checkpoint_root']}/{p['consumer']}")
      .trigger(availableNow=True)
      .start()
      .awaitTermination())
