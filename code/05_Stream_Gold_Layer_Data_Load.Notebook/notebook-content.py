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

# from pyspark.sql.functions import *

# # Read Silver tables
# orders_df = (
#     spark.table("silver_orders")
#     .filter(
#         (col("order_timestamp") >= trunc(add_months(current_date(), -1), "month")) &
#         (col("order_timestamp") < trunc(current_date(), "month"))
#     )
# )
# order_items_df = spark.table("silver_order_items")
# products_df = spark.table("silver_products")
# customers_df = spark.table("silver_customers")
# stores_df = spark.table("silver_store")

# # Create Fact Sales
# fact_sales = (
#     order_items_df.alias("oi")
#     .join(
#         orders_df.alias("o"),
#         col("oi.order_id") == col("o.order_id"),
#         "inner"
#     )
#     .join(
#         products_df.alias("p"),
#         col("oi.product_id") == col("p.product_id"),
#         "inner"
#     )
#     .join(
#         customers_df.alias("c"),
#         col("o.customer_id") == col("c.customer_id"),
#         "inner"
#     )
#     .join(
#         stores_df.alias("s"),
#         col("o.store_id") == col("s.store_id"),
#         "inner"
#     )
#     .withColumn(
#     "data_source",
#     when(col("o.batch_id").isNull(), lit("Real-Time"))
#     .otherwise(lit("Historical"))
#     )
#     .select(
#         col("oi.order_item_id"),
#         col("o.order_id"),
#         col("o.customer_id"),
#         col("o.store_id"),
#         col("oi.product_id"),

#         col("o.order_timestamp").alias("order_date"),

#         col("oi.quantity"),
#         col("oi.unit_price"),
#         col("oi.line_amount"),

#         col("o.payment_method"),
#         col("o.order_status"),
#         col("data_source")
#     )
# )
# fact_sales.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# -----------------------------
# Read Streaming Orders
# -----------------------------
orders_stream = (
    spark.readStream
         .table("silver_orders")
)

# -----------------------------
# Read Static Lookup Tables
# -----------------------------
order_items_df = spark.table("silver_order_items")
products_df = spark.table("silver_products")
customers_df = spark.table("silver_customers")
stores_df = spark.table("silver_store")

# -----------------------------
# Keep only Real-Time Orders
# -----------------------------
orders_stream = orders_stream.filter(col("batch_id").isNull())

# -----------------------------
# Create Fact Sales
# -----------------------------
fact_sales_stream = (
    orders_stream.alias("o")
    .join(
        order_items_df.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
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
    "date_key",
    date_format(
        col("order_timestamp"),
        "yyyyMMdd"
    ).cast("int")
    )
    .withColumn("data_source", lit("Real-Time"))
    .select(
        col("oi.order_item_id"),
        col("o.order_id"),
        col("o.customer_id"),
        col("o.store_id"),
        col("oi.product_id"),
        col("o.order_timestamp").alias("order_date"),
        col("order_date"),
        col("oi.quantity"),
        col("oi.unit_price"),
        col("oi.line_amount"),
        col("o.payment_method"),
        col("o.order_status"),
        col("data_source"),
        col("date_key"),

    )
)

# -----------------------------
# MERGE Function
# -----------------------------
gold_table = "gold_fact_sales"

def merge_to_gold(batch_df, batch_id):

    deltaTable = DeltaTable.forName(spark, gold_table)

    (
        deltaTable.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.order_item_id = s.order_item_id"
        )
        .whenMatchedUpdate(set={
            "order_id": "s.order_id",
            "customer_id": "s.customer_id",
            "store_id": "s.store_id",
            "product_id": "s.product_id",
            "order_date": "s.order_date",
            "quantity": "s.quantity",
            "unit_price": "s.unit_price",
            "line_amount": "s.line_amount",
            "payment_method": "s.payment_method",
            "order_status": "s.order_status",
            "data_source": "s.data_source",
            "date_key": "s.date_key",

        })
        .whenNotMatchedInsert(values={

            "order_item_id": "s.order_item_id",
            "order_id": "s.order_id",
            "customer_id": "s.customer_id",
            "store_id": "s.store_id",
            "product_id": "s.product_id",
            "order_date": "s.order_date",
            "date_key": "s.date_key",
            "quantity": "s.quantity",
            "unit_price": "s.unit_price",
            "line_amount": "s.line_amount",
            "payment_method": "s.payment_method",
            "order_status": "s.order_status",
            "data_source": "s.data_source"

        })

        # .whenNotMatchedInsertAll()
        .execute()
    )

# -----------------------------
# Start Streaming
# -----------------------------
(
    fact_sales_stream.writeStream
    .foreachBatch(merge_to_gold)
    .outputMode("update")
    .option(
        "checkpointLocation",
        "Files/checkpoints/gold_fact_sales"
    )
    .option("mergeShema","true")
    .start()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

orders_df = spark.table("silver_orders")
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
    .option("mergeSchema","true")\
    .saveAsTable("gold_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
