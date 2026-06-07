# Databricks notebook source
#spark.sql("SHOW TABLES").show()
from pyspark.sql.functions import month, year
orders = spark.table("orders_silver")

customers = spark.table("customers_silver")

payments = spark.table("payments_silver")

#1: Revenue By Order Status
sales = orders.join(
    payments,
    on="order_id",
    how="inner"
)
display(sales)

from pyspark.sql.functions import sum

revenue_status = sales.groupBy(
    "order_status"
).agg(
    sum("payment_value").alias("total_revenue")
)
display(revenue_status)
revenue_status.write.mode("overwrite").saveAsTable(
    "gold_revenue_status"
)
#2: Payment Type Analysis
from pyspark.sql.functions import count

payment_analysis = payments.groupBy(
    "payment_type"
).agg(
    count("*").alias("transactions")
)
display(payment_analysis)
payment_analysis.write.mode("overwrite").saveAsTable(
    "gold_payment_analysis"
)
#3: Top Customers
customer_sales = (
    orders
    .join(payments, "order_id")
    .join(customers, "customer_id")
)
from pyspark.sql.functions import sum

top_customers = customer_sales.groupBy(
    "customer_unique_id"
).agg(
    sum("payment_value").alias("total_spent")
)
top_customers = top_customers.orderBy(
    "total_spent",
    ascending=False
)
display(top_customers)
top_customers.write.mode("overwrite").saveAsTable(
    "gold_top_customers"
)

#4: Monthly Revenue
monthly_revenue = sales.groupBy(
    year("order_purchase_timestamp").alias("year"),
    month("order_purchase_timestamp").alias("month")
).agg(
    sum("payment_value").alias("revenue")
)
display(monthly_revenue)
monthly_revenue.write.mode("overwrite").saveAsTable(
    "gold_monthly_revenue"
)
spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

display(monthly_revenue)

# COMMAND ----------

display(payment_analysis)

# COMMAND ----------

display(revenue_status)

# COMMAND ----------

display(top_customers.limit(10))