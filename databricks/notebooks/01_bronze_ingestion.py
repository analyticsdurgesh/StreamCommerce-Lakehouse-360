# Databricks notebook source
dbutils.widgets.text("landing_path", "s3://streamcommerce-lakehouse/landing")
dbutils.widgets.text("bronze_path", "s3://streamcommerce-lakehouse/bronze")
dbutils.widgets.text("processing_date", "")

landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")
processing_date = dbutils.widgets.get("processing_date")

# In production this reads JSON from S3 landing and writes Delta Bronze with metadata.
from pyspark.sql import functions as F

topics = ["customer_events", "orders", "payments", "inventory_updates", "product_catalog_updates", "shipment_events"]
for topic in topics:
    source = f"{landing_path}/{topic}/"
    target = f"{bronze_path}/{topic}/"
    df = (
        spark.read.option("recursiveFileLookup", "true").json(source)
        .withColumn("raw_json", F.to_json(F.struct("*")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_topic", F.lit(topic))
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_id", F.expr("uuid()"))
        .withColumn("processing_date", F.coalesce(F.lit(processing_date), F.current_date().cast("string")))
    )
    df.write.format("delta").mode("append").partitionBy("processing_date").save(target)
