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

# Converting Csv files to delta tables
# Reading Csv files
customer_df = spark.read.format('csv').option("inferSchema","true").option("header","true").load('Files/RawData/RawDataCustomers/customers.csv')
display(customer_df);


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from uuid import uuid4


batch_id = str(uuid4())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = customer_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

customer_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

employee_df = spark.read.format('csv').option("inferSchema","true").option("header","true").load('Files/RawData/RawDataemployees/employees.csv')
employee_df = employee_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

employee_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Order-items Files to order-items Delta Table
from pyspark.sql.functions import *
order_items_df = spark.read.format('csv')\
.option("inferSchema","true")\
.option("header","true")\
.load("Files/RawData/RawDataorder_items/order_items.csv")

order_items_df = order_items_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

order_items_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_order_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Orders files to Orders Table
order_df = spark.read.format('csv')\
.option("inferSchema","true")\
.option("header","true")\
.load("Files/RawData/RawDataorders/orders.csv")


order_df = order_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

order_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df = spark.read.format('csv')\
.option("inferSchema","true")\
.option("header","true")\
.load("Files/RawData/RawDataproducts/products.csv")


products_df = products_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

products_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stores_df = spark.read.format('csv')\
.option("inferSchema","true")\
.option("header","true")\
.load("Files/RawData/RawDatastores/stores.csv")


stores_df = stores_df \
.withColumn("processed_timestamp", current_timestamp()) \
.withColumn("batch_id",  lit(batch_id))

stores_df.write.format("delta")\
.mode("append")\
.option("mergeSchema", "true") \
.saveAsTable("bronze_stores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
