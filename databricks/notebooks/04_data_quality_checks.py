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
    for name, condition in {
        "event_id_not_null": "event_id is not null",
        "event_timestamp_not_null": "event_timestamp is not null",
        "event_id_unique": "event_id is not null",
    }.items():
        failed = total - df.filter(condition).count()
        results.append((table, name, failed == 0, failed, total, 1.0 if total == 0 else (total - failed) / total))

scorecard = spark.createDataFrame(results, "table_name string, check_name string, passed boolean, failed_record_count long, total_record_count long, pass_percentage double")
scorecard = scorecard.withColumn("checked_at", F.current_timestamp())
scorecard.write.format("delta").mode("append").save(f"{gold_path}/data_quality_scorecard")
