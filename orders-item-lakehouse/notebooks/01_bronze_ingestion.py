# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Auto Loader ingestion
# MAGIC
# MAGIC Reads item-level order CSVs landing hourly and writes them as-is (schema inferred,
# MAGIC `_ingest_ts` / `_source_file` added) into `orders_lake.bronze.raw_orders`.
# MAGIC
# MAGIC `LANDING_PATH` is set via pipeline configuration (`orders.landing_path`), so this
# MAGIC notebook is identical whether pointed at a UC Volume (Path B, Free Edition-safe)
# MAGIC or a mounted ADLS container (Path A, real Azure storage). Switching paths later is
# MAGIC a one-line config change in the pipeline settings, not a code change.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

# Set in the Lakeflow pipeline's "Configuration" section, e.g.:
#   Path B (Free Edition):  orders.landing_path = /Volumes/orders_lake/landing/raw_orders/
#   Path A (real Azure):    orders.landing_path = abfss://orders@<account>.dfs.core.windows.net/raw/orders/
LANDING_PATH = spark.conf.get("orders.landing_path")

# COMMAND ----------

@dlt.table(
    name="raw_orders",
    schema="bronze",
    comment="Raw item-level order lines as landed, schema inferred by Auto Loader.",
    table_properties={"quality": "bronze"},
)
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/orders_lake/landing/_schema/raw_orders")
        .option("header", "true")
        .load(LANDING_PATH)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
