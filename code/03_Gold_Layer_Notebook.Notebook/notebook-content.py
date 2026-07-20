# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22abdf99-fa4c-40c1-8ec2-ff82cadbea4e",
# META       "default_lakehouse_name": "walmart_project_Lakehouse",
# META       "default_lakehouse_workspace_id": "3f791e3d-d866-41af-b3fc-1e6edfc375d8",
# META       "known_lakehouses": [
# META         {
# META           "id": "22abdf99-fa4c-40c1-8ec2-ff82cadbea4e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *

# Read Silver tables
orders_df = (
    spark.table("silver_orders")
    .filter(
        (col("order_timestamp") >= trunc(add_months(current_date(), -1), "month")) &
        (col("order_timestamp") < trunc(current_date(), "month"))
    )
)
order_items_df = spark.table("silver_order_items")
products_df = spark.table("silver_products")
customers_df = spark.table("silver_customers")
stores_df = spark.table("silver_store")

# Create Fact Sales
fact_sales = (
    order_items_df.alias("oi")
    .join(
        orders_df.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )
    .join(
        products_df.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "inner"
    )
    .join(
        customers_df.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "inner"
    )
    .join(
        stores_df.alias("s"),
        col("o.store_id") == col("s.store_id"),
        "inner"
    )
    .withColumn(
    "data_source",
    when(col("o.batch_id").isNull(), lit("Real-Time"))
    .otherwise(lit("Historical"))
    )
        .withColumn(
    "date_key",
    date_format(
        col("o.order_timestamp"),
        "yyyyMMdd"
    ).cast("int")
    )
    .select(
        col("oi.order_item_id"),
        col("o.order_id"),
        col("o.customer_id"),
        col("o.store_id"),
        col("oi.product_id"),

        col("o.order_timestamp").alias("order_date"),
        col("date_key"),
        col("oi.quantity"),
        col("oi.unit_price"),
        col("oi.line_amount"),

        col("o.payment_method"),
        col("o.order_status"),
        col("data_source")
    )
)
fact_sales.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_fact_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Customer Dimension Table
dim_customer = spark.table("silver_customers")

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Product Dimension Table
dim_product = spark.table("silver_products")

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store Dimension Table
dim_store = spark.table("silver_store")

dim_store.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dim_store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_employee = spark.table("silver_employees")

dim_employee.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dim_employee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Date Dimension table
from pyspark.sql.functions import *

dim_date = (
    orders_df
     .select(
        to_date("order_timestamp").alias("date")
    )
    .distinct()
    .withColumn(
        "date_key",
        date_format("date","yyyyMMdd").cast("int")
    )    
    .distinct()
    .withColumn("year", year("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("month", month("date"))
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_name", date_format("date", "EEEE"))
)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
