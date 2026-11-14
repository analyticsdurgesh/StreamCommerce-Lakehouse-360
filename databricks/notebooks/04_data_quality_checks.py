# Databricks notebook source
dbutils.widgets.text("silver_path", "s3://streamcommerce-lakehouse/silver")
dbutils.widgets.text("gold_path", "s3://streamcommerce-lakehouse/gold")

from pyspark.sql import functions as F

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")
tables = ["customer_events", "orders", "payments", "inventory_updates", "product_catalog_updates", "shipment_events"]

results = []
for table in tables:
    df = spark.read.format("delta").load(f"{silver_path}/{table}")
    total = df.count()
    with_id = df.filter("event_id is not null")
    failures = {
        "event_id_not_null": total - with_id.count(),
        "event_timestamp_not_null": df.filter("event_timestamp is null").count(),
        # Rows that repeat an event_id. (This check used to be a copy of the
        # not-null check, so duplicates were never counted.)
        "event_id_unique": with_id.count() - with_id.select("event_id").distinct().count(),
    }
    for name, failed in failures.items():
        results.append((table, name, failed == 0, failed, total, 1.0 if total == 0 else (total - failed) / total))

scorecard = spark.createDataFrame(results, "table_name string, check_name string, passed boolean, failed_record_count long, total_record_count long, pass_percentage double")
scorecard = scorecard.withColumn("checked_at", F.current_timestamp())
scorecard.write.format("delta").mode("append").save(f"{gold_path}/data_quality_scorecard")
