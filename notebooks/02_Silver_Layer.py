# Databricks notebook source
orders = spark.table("orders_bronze")

customers = spark.table("customers_bronze")

payments = spark.table("payments_bronze")


orders_silver = orders.dropDuplicates()

customers_silver = customers.dropDuplicates()

payments_silver = payments.dropDuplicates()


from pyspark.sql.functions import *

orders_silver.select(
[
count(
when(col(c).isNull(), c)
).alias(c)

for c in orders_silver.columns
]
).show()
orders_silver.printSchema()


from pyspark.sql.functions import to_timestamp

orders_silver = orders_silver.withColumn(
    "order_purchase_timestamp",
    to_timestamp("order_purchase_timestamp")
)

orders_silver.write.mode("overwrite").saveAsTable(
    "orders_silver"
)

customers_silver.write.mode("overwrite").saveAsTable(
    "customers_silver"
)

payments_silver.write.mode("overwrite").saveAsTable(
    "payments_silver"
)