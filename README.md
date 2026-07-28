# Walmart Retail Analytics Platform — Microsoft Fabric

An end-to-end retail analytics solution built on **Microsoft Fabric**. It processes historical Walmart transactional data through a medallion (Bronze → Silver → Gold) architecture and includes a near real-time streaming pipeline that simulates live retail order events, with results surfaced in Power BI.

---

## Overview

The platform answers six business questions:

1. **Store sales performance** — which stores generate the highest revenue and how sales vary across stores.
2. **Customer purchasing behavior** — which customers spend the most and how many orders each has placed.
3. **Product performance** — which products, categories, and brands drive the most sales.
4. **Order status monitoring** — the share of orders that are Completed, Pending, or Cancelled.
5. **Store performance dashboard** — which stores process the most orders and generate the highest average order value.
6. **Historical vs real-time comparison** — batch sales measured against live streamed orders.

---

## Architecture

Two independent data flows share the same Lakehouse:

- **Batch pipeline** — historical retail data through the medallion layers.
- **Streaming pipeline** — near real-time order events via Fabric Eventstream.

```
Batch:      GitHub CSV  ->  Data Factory Pipeline  ->  Lakehouse Files
                        ->  Bronze Delta  ->  Silver Delta  ->  Gold Star Schema  ->  Power BI

Streaming:  Fake Order Generator  ->  Fabric Eventstream  ->  Lakehouse Streaming Table
                        ->  Spark Processing  ->  Power BI Real-Time Dashboard
```

---
# Architectural Diagram

<img width="860" height="1454" alt="image" src="https://github.com/user-attachments/assets/221d2bfb-96d0-480d-a08f-ad279caa0ef1" />

## Datasets

Source CSV files are stored in a GitHub repository and ingested into the Lakehouse.

| Dataset | Rows |
|---|---|
| Customers | 2,000 |
| Orders | 10,000 |
| Order Items | 30,021 |
| Products | 500 |
| Employees | 250 |
| Stores | 25 |

---

## Batch Processing Pipeline

### Data ingestion
**Fabric Data Factory** ingests the CSV files from GitHub into the Lakehouse. Raw files land in the Lakehouse **Files** section unchanged.

### Bronze layer
Raw CSVs are converted into Delta tables using Fabric Spark notebooks, one table per source file:

`bronze_customers`, `bronze_orders`, `bronze_order_items`, `bronze_products`, `bronze_stores`, `bronze_employees`

### Silver layer
Spark notebooks clean and standardize the Bronze data:

- Remove duplicate records
- Handle null values
- Convert data types
- Standardize timestamps
- Filter invalid records
- Validate against reference tables
- Add audit columns

Cleaned output is written to Silver Delta tables (`silver_customers`, `silver_orders`, and so on). Incremental processing uses timestamps so only new or updated records from Bronze are reprocessed.

### Gold layer
The Gold layer holds business-ready models in a retail **star schema**.

**Fact table — `fact_sales`**
Order details, product details, quantity, sales amount, payment method, order status.

**Dimension tables**
`dim_customer`, `dim_product`, `dim_store`, `dim_employee`, `dim_date`

---

## Streaming Pipeline

A separate flow simulates real-time retail orders:

1. A fake order generator produces new order events.
2. **Fabric Eventstream** receives the events.
3. Events land in a Lakehouse streaming (Bronze) table.
4. Spark processes the streamed data.
5. A **Power BI real-time dashboard** displays live results.

Each streamed event carries: Order ID, Customer ID, Store ID, Product ID, Quantity, Amount, Payment Method, Order Status, and Event Timestamp.

---

## Semantic Model & Reporting

A semantic model is built over the Gold star schema to power Power BI reports covering store performance, top customers, product analysis, and order status, alongside the real-time streaming dashboard.

---

## Tech Stack

- Microsoft Fabric (Lakehouse, Data Factory, Eventstream)
- Spark notebooks (PySpark)
- Delta Lake
- Power BI
- GitHub (source data)

---

## Future Enhancements

**1. Real-time inventory monitoring and stock prediction**
Integrate inventory data with the sales platform to track availability in real time and predict stock needs — flagging low-stock products, high-demand products, and store-level inventory issues.

**2. Customer segmentation and personalized recommendations**
Classify customers by buying pattern (total spend, purchase frequency, product categories) into VIP, regular, and low-engagement segments to drive targeted recommendations.

---

## Author

MINNU LATHIKA
