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

stream_bronze_df = spark.table("bronze_real_time_orders")

display(stream_bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stream_bronze_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select only order columns.
silver_stream_orders = (

    stream_bronze_df

    .select(
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "payment_method",
        "order_status",
        "total_amount",
        "created_timestamp",
        "updated_timestamp",
        "is_active"
    )
    .withColumn("store_id", col("store_id").cast("integer"))
    .withColumn("order_id",col("order_id").cast("Integer"))
    .withColumn("customer_id", col("customer_id").cast("integer"))
    .withColumn("order_timestamp", col("order_timestamp").cast("timestamp "))
    .withColumn("created_timestamp", col("created_timestamp").cast("timestamp "))
    .withColumn("updated_timestamp", col("updated_timestamp").cast("timestamp "))

    .dropDuplicates(["order_id"])

    .filter(col("order_id").isNotNull())

    .filter(col("is_active") == "Y")

    .filter(col("total_amount") >= 0)

    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Append into your existing silver_orders
silver_stream_orders.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_stream_order_items = (

    stream_bronze_df

    .select(
        "order_id",
        explode("order_items").alias("item")
    )

    .select(

        col("item.order_item_id"),

        col("order_id"),

        col("item.product_id"),

        col("item.quantity"),

        col("item.unit_price"),

        col("item.line_amount"),

        col("item.created_timestamp"),

        col("item.updated_timestamp"),

        col("item.is_active")
    )

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_stream_order_items.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_stream_order_items = (

    silver_stream_order_items
    .withColumn("order_item_id", col("order_item_id").cast("integer"))
    .withColumn("order_id", col("order_id").cast("integer"))
    .withColumn("product_id", col("product_id").cast("integer"))
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("created_timestamp", col("created_timestamp").cast("timestamp "))
    .withColumn("updated_timestamp", col("updated_timestamp").cast("timestamp "))

    .dropDuplicates(["order_item_id"])

    .filter(col("order_item_id").isNotNull())

    .filter(col("quantity") > 0)

    .filter(col("line_amount") >= 0)

    .filter(col("is_active") == "Y")

    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Appendding To Order-Items Table
silver_stream_order_items.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver_order_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
