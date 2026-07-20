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


%pip install azure-eventhub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create The producer
from azure.eventhub import EventHubProducerClient, EventData

# Reads from the Lakehouse Files area, which is NOT tracked by Git

with open("/lakehouse/default/Files/Keys/SAS_KEY.txt") as f:
    connection_string = f.read().strip()
print(connection_string)
producer = EventHubProducerClient.from_connection_string(conn_str=connection_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random

# Fetching Ids Customer, store,products from batch data
customer_ids = [
    r.customer_id
    for r in spark.table("silver_customers")
    .select("customer_id")
    .collect()
]

store_ids = [
    r.store_id
    for r in spark.table("silver_store")
    .select("store_id")
    .collect()
]

products = spark.table("silver_products") \
    .select("product_id","price") \
    .collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max
# Fetching Max orderid, orderitemid From existing Data
order_id = (
    spark.table("silver_orders")
    .agg(max("order_id"))
    .collect()[0][0]
) + 1

order_item_id = (
    spark.table("silver_order_items")
    .agg(max("order_item_id"))
    .collect()[0][0]
) + 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to generate one order
from datetime import datetime

def generate_order():

    global order_id
    global order_item_id

    payment = random.choice([
        "Cash",
        "Credit Card",
        "Debit Card",
        "UPI"
    ])

    status = random.choice([
        "Completed",
        "Pending",
        "Cancelled"
    ])

    customer = random.choice(customer_ids)

    store = random.choice(store_ids)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_amount = 0

    items = []

    number_of_items = random.randint(1,5)

    for i in range(number_of_items):

        product = random.choice(products)

        qty = random.randint(1,5)

        price = float(product.price)

        amount = round(price * qty,2)

        total_amount += amount

        items.append({

            "order_item_id": order_item_id,

            "product_id": product.product_id,

            "quantity": qty,

            "unit_price": price,

            "line_amount": amount,

            "created_timestamp": now,

            "updated_timestamp": now,

            "is_active":"Y"

        })

        order_item_id += 1

    event = {

        "order_id": order_id,

        "customer_id": customer,

        "store_id": store,

        "order_timestamp": now,

        "payment_method": payment,

        "order_status": status,

        "total_amount": round(total_amount,2),

        "created_timestamp": now,

        "updated_timestamp": now,

        "is_active":"Y",

        "order_items": items

    }

    order_id += 1

    return event

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Send one order
import json
from azure.eventhub import EventData

event = generate_order()

batch = producer.create_batch()

batch.add(EventData(json.dumps(event)))

producer.send_batch(batch)

print(json.dumps(event, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Simulate streaming
import time

for i in range(100):

    event = generate_order()

    batch = producer.create_batch()

    batch.add(EventData(json.dumps(event)))

    producer.send_batch(batch)

    print(f"Order {event['order_id']} sent")

    time.sleep(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
