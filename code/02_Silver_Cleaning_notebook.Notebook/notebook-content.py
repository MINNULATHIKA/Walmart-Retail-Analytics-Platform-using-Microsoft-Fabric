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

# Loading Delta tables from Bronze Layer
from pyspark.sql.functions import *
from delta.tables import DeltaTable

orders_df = spark.table("bronze_orders")
customer_df = spark.table("bronze_customers")
store_df = spark.table("bronze_stores")
employee_df = spark.table("bronze_employees")
order_items_df = spark.table("bronze_order_items")
product_df = spark.table("bronze_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# To Clean Newly arriving Data only , checking last Updated TimeStamp From Silver table, If it is First Table Creation Default date will pick
if spark.catalog.tableExists("silver_orders"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_orders
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Appending To silver Table

# Order table
order_df = orders_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)


order_silver_df = order_df.dropDuplicates(["order_id"])\
.filter(col('order_id').isNotNull())\
.filter((col('total_amount') >= 0 ) 
& (col('is_active') == 'Y'))\
.filter(col('order_timestamp') < current_timestamp())\
.join(
    store_df.select("store_id"),
    on = "store_id",
    how = "left_semi"
)


display(order_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not spark.catalog.tableExists("silver_orders"):

    order_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_orders")

else:

    order_silver_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Customers Table
if spark.catalog.tableExists("silver_customers"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_customers
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# --- filtering latest records

customer_df = customer_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)

# Cleaning new Records 

customer_silver_df = customer_df.dropDuplicates(["customer_id"])\
.filter(col('customer_id').isNotNull())\
.filter(col('is_active') == 'Y')


if not spark.catalog.tableExists("silver_customers"):

    customer_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_customers")

else:

    customer_silver_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Employees
if spark.catalog.tableExists("silver_employees"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_employees
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# --- filtering latest records

employees_df = employee_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)

# Cleaning new Records 

employees_silver_df = employees_df.dropDuplicates(["employee_id"])\
.filter(col('employee_id').isNotNull())\
.filter(col('is_active') == 'Y')\
.join(
    store_df.select("store_id"),
    on = "store_id",
    how = "left_semi"
)


if not spark.catalog.tableExists("silver_employees"):

    employees_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_employees")

else:

    employees_silver_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Order items
if spark.catalog.tableExists("silver_order_Items"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_order_items
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# --- filtering latest records

order_Items_df = order_items_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)

# Cleaning new Records 

order_Items_silver_df = order_items_df.dropDuplicates(["order_item_id"])\
.filter(col('order_item_id').isNotNull())\
.filter((col('is_active') == 'Y') & (col('unit_price') > 0) & (col('line_amount') > 0))\
.join(
    orders_df.select("order_id"),
    on = "order_id",
    how = "left_semi"
)\
.join(
    product_df.select("product_id"),
    on = "product_id",
    how = "left_semi"
)


if not spark.catalog.tableExists("silver_order_items"):

    order_Items_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_order_items")

else:

    order_Items_silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("silver_order_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store Table
if spark.catalog.tableExists("silver_store"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_store
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# --- filtering latest records

store_df = store_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)

# Cleaning new Records 

store_silver_df = store_df.dropDuplicates(["store_id"])\
.filter(col('store_id').isNotNull())\
.filter(col('is_active') == 'Y')

if not spark.catalog.tableExists("silver_store"):

    store_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_store")

else:

    store_silver_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Products Table
if spark.catalog.tableExists("silver_products"):
    
    last_timestamp = spark.sql("""
        SELECT MAX(updated_timestamp)
        FROM silver_products
    """).collect()[0][0]

else:
    last_timestamp = "1900-01-01 00:00:00"

print(last_timestamp)

# --- filtering latest records

product_df = product_df.filter(
    col("updated_timestamp") > lit(last_timestamp)
)

# Cleaning new Records 

products_silver_df = product_df.dropDuplicates(["product_id"])\
.filter(col('product_id').isNotNull())\
.filter((col('is_active') == 'Y') & (col('price') > 0))\



if not spark.catalog.tableExists("silver_products"):

    products_silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_products")

else:

    products_silver_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
