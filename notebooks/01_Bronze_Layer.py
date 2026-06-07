# Databricks notebook source
orders = spark.read.csv(
    "file:/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

display(orders)

import pandas as pd

orders_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_orders_dataset.csv"
)

orders_df.head()

orders = spark.createDataFrame(orders_df)

display(orders)
orders.printSchema()
print("Rows:", orders.count())



customers_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_customers_dataset.csv"
)

payments_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_order_payments_dataset.csv"
)
customers = spark.createDataFrame(customers_df)

payments = spark.createDataFrame(payments_df)
display(customers)

display(payments)



orders.write.mode("overwrite").saveAsTable("orders_bronze")

customers.write.mode("overwrite").saveAsTable("customers_bronze")

payments.write.mode("overwrite").saveAsTable("payments_bronze")

orders = spark.read.csv(
    "file:/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

display(orders)

import pandas as pd

orders_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_orders_dataset.csv"
)

orders_df.head()

orders = spark.createDataFrame(orders_df)

display(orders)
orders.printSchema()
print("Rows:", orders.count())



customers_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_customers_dataset.csv"
)

payments_df = pd.read_csv(
    "/Workspace/Users/pavi90425@gmail.com/Retail_Data_Lakehouse/olist_order_payments_dataset.csv"
)
customers = spark.createDataFrame(customers_df)

payments = spark.createDataFrame(payments_df)
display(customers)

display(payments)



orders.write.mode("overwrite").saveAsTable("orders_bronze")

customers.write.mode("overwrite").saveAsTable("customers_bronze")

payments.write.mode("overwrite").saveAsTable("payments_bronze")
spark.sql("SHOW TABLES").show()

# COMMAND ----------

display(monthly_revenue)