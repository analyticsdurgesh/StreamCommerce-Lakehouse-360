# Databricks notebook source
dbutils.widgets.text("silver_path", "s3://streamcommerce-lakehouse/silver")
dbutils.widgets.text("gold_path", "s3://streamcommerce-lakehouse/gold")

from pyspark.sql import functions as F

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

orders = spark.read.format("delta").load(f"{silver_path}/orders")
payments = spark.read.format("delta").load(f"{silver_path}/payments")

paid = payments.filter("payment_status = 'SUCCESS'").select("order_id").dropDuplicates()
paid_orders = orders.join(paid, "order_id")

daily_revenue = paid_orders.groupBy("event_date").agg(
    F.sum("amount").alias("total_revenue"),
    F.countDistinct("order_id").alias("total_orders"),
).withColumn("average_order_value", F.col("total_revenue") / F.col("total_orders"))

daily_revenue.write.format("delta").mode("overwrite").save(f"{gold_path}/daily_revenue")

paid_orders.groupBy("product_id", "category").agg(
    F.sum("quantity").alias("units_sold"),
    F.sum("amount").alias("revenue"),
    F.countDistinct("order_id").alias("orders"),
).write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance")
