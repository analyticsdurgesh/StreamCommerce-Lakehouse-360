# Databricks notebook source
dbutils.widgets.text("bronze_path", "s3://streamcommerce-lakehouse/bronze")
dbutils.widgets.text("silver_path", "s3://streamcommerce-lakehouse/silver")
dbutils.widgets.text("quarantine_path", "s3://streamcommerce-lakehouse/quarantine")

from pyspark.sql import Window
from pyspark.sql import functions as F

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
quarantine_path = dbutils.widgets.get("quarantine_path")
topics = ["customer_events", "orders", "payments", "inventory_updates", "product_catalog_updates", "shipment_events"]

# 01_bronze_ingestion keeps every field of the original event as a column and only
# ADDS metadata, so silver selects those columns. (It used to re-parse raw_json with
# `.rdd.map(...)`, which does not run on Databricks serverless compute, the compute
# Free Edition gives you: serverless has no RDD API.)
BRONZE_METADATA = {"raw_json", "ingestion_timestamp", "source_topic", "source_file", "batch_id", "processing_date"}

for topic in topics:
    bronze = spark.read.format("delta").load(f"{bronze_path}/{topic}")
    parsed = bronze.select(*[c for c in bronze.columns if c not in BRONZE_METADATA])
    for name in ("event_id", "event_timestamp", "price", "quantity"):
        if name not in parsed.columns:
            parsed = parsed.withColumn(name, F.lit(None).cast("string"))
    cleaned = (
        parsed.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
    )

    # One flag per row. A row with no event_id is a failure that can no longer go
    # missing: the old left_anti join on event_id could not match a NULL id, so
    # exactly those rows went to silver instead of quarantine. coalesce(..., false)
    # keeps the old meaning for a NULL price or quantity (not a failure).
    is_invalid = F.coalesce(
        F.col("event_id").isNull()
        | F.col("event_timestamp").isNull()
        | (F.col("price") < 0)
        | (F.col("quantity") < 1),
        F.lit(False),
    )
    flagged = cleaned.withColumn("_is_invalid", is_invalid)
    invalid = flagged.filter(F.col("_is_invalid")).drop("_is_invalid")

    # Deduplicate the valid rows only, keeping the latest version of each event.
    # (dropDuplicates(["event_id"]) kept an arbitrary copy and merged every
    # id-less row into one.)
    latest_first = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    valid = (
        flagged.filter(~F.col("_is_invalid"))
        .drop("_is_invalid")
        .withColumn("_row_number", F.row_number().over(latest_first))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("event_date").save(f"{silver_path}/{topic}")
    (
        invalid.withColumn("reason_code", F.lit("silver_validation_failed"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{quarantine_path}/dq_failures/{topic}")
    )
