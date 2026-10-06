# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — clean, dedup, quality-checked
# MAGIC
# MAGIC `orders_clean` drops rows that would corrupt revenue math (bad quantity/price)
# MAGIC and dedups on `order_line_id`. `orders_rejected` is the quarantine table that
# MAGIC keeps everything `orders_clean` dropped, for audit/reprocessing.
# MAGIC
# MAGIC Expectation choice matters here: `@dlt.expect_or_drop` is used for
# MAGIC quantity/price because a bad value there corrupts every downstream sum.
# MAGIC `@dlt.expect` (warn, keep row) is used for the email check because a
# MAGIC missing email shouldn't disqualify an order line from being counted in sales.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_timestamp

# COMMAND ----------

@dlt.table(
    name="orders_clean",
    schema="silver",
    comment="Deduplicated, typed, quality-checked order lines.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_price", "unit_price > 0")
@dlt.expect("has_email", "customer_email IS NOT NULL AND customer_email != ''")
def orders_clean():
    df = (
        dlt.read_stream("raw_orders")
        .dropDuplicates(["order_line_id"])
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("line_total", col("line_total").cast("double"))
    )
    return df

# COMMAND ----------

@dlt.table(
    name="orders_rejected",
    schema="silver",
    comment="Rows dropped by silver expectations, retained for audit.",
    table_properties={"quality": "silver"},
)
def orders_rejected():
    # No expectation decorator here on purpose: this table's whole job is to hold the
    # rows orders_clean rejected. (Note: the original spec attached
    # @dlt.expect_all_or_drop({"was_valid": "quantity > 0 AND unit_price > 0"}) to this
    # table — that condition is the *opposite* of what's already been filtered for, so
    # it would drop every single row and orders_rejected would always be empty. Fixed
    # here by just removing it.)
    return dlt.read_stream("raw_orders").filter("quantity <= 0 OR unit_price <= 0")
