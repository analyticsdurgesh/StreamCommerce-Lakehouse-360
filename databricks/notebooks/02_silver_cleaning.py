# Databricks notebook source
dbutils.widgets.text("bronze_path", "s3://streamcommerce-lakehouse/bronze")
dbutils.widgets.text("silver_path", "s3://streamcommerce-lakehouse/silver")
dbutils.widgets.text("quarantine_path", "s3://streamcommerce-lakehouse/quarantine")

from pyspark.sql import functions as F

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
quarantine_path = dbutils.widgets.get("quarantine_path")
topics = ["customer_events", "orders", "payments", "inventory_updates", "product_catalog_updates", "shipment_events"]

for topic in topics:
    df = spark.read.format("delta").load(f"{bronze_path}/{topic}")
    parsed = spark.read.json(df.select("raw_json").rdd.map(lambda r: r.raw_json))
    cleaned = (
        parsed.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .dropDuplicates(["event_id"])
    )
    invalid = cleaned.filter("event_id is null or event_timestamp is null or price < 0 or quantity < 1")
    valid = cleaned.join(invalid.select("event_id"), "event_id", "left_anti")
    valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("event_date").save(f"{silver_path}/{topic}")
    invalid.withColumn("reason_code", F.lit("silver_validation_failed")).write.format("delta").mode("append").save(f"{quarantine_path}/dq_failures/{topic}")
