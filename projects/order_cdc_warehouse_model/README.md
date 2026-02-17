# Order CDC Warehouse Model

## Overview

This project simulates a production-grade Change Data Capture (CDC) warehouse pipeline using SQL. It demonstrates how raw transactional updates are transformed into reliable, analytics-ready warehouse tables.

It also demonstrates incremental CDC processing using ingestion watermarks to simulate production pipeline behavior.

The pipeline handles real-world data engineering problems including duplicate records, late-arriving events, data corrections, and invalid data.

This project models a common warehouse architecture used in Snowflake, BigQuery, and modern analytics platforms.

---

## Architecture

The pipeline follows a layered warehouse design:

Raw Layer → Clean Layer → Fact Layer → Dimension Layer → Mart Layer → Incremental Processing Layer

---

## Pipeline Layers

### 1. Raw Layer (`01_raw_orders.sql`)

Simulates incoming CDC event data from source systems.

Includes realistic scenarios:

- duplicate records
- updated records
- late-arriving events
- invalid records (NULL keys, negative amounts)

Columns:

- order_id
- customer_id
- amount
- status
- event_ts (business event time)
- ingested_at (warehouse ingestion time)

---

### 2. Clean Layer (`02_valid_orders.sql`)

Removes invalid records to ensure data quality.

Rules applied:

- remove NULL customer_id
- remove negative order amounts

This produces a reliable staging dataset.

---

### 3. Fact Layer (`03_fact_orders.sql`)

Builds the current-state fact table using CDC deduplication logic.

Key technique:

ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC)

This ensures:

- only the latest version of each order is kept
- late updates correctly replace older records
- duplicates are eliminated

Output: one row per order_id.

---

### 4. Dimension Layer (`04_dim_customers.sql`)

Builds a customer dimension table derived from fact_orders.

Provides customer-level attributes:

- first_seen_date
- last_seen_date
- is_active flag

Output: one row per customer_id.

---

### 5. Mart Layer (`05_customer_metrics.sql`)

Creates business-ready analytics metrics.

Customer metrics include:

- order_count
- total_spend
- avg_order_value
- last_order_date
- recent spend metrics

This is the layer used by dashboards and business reports.

---

### 6. Daily Revenue Mart (`06_daily_revenue.sql`)

Builds daily revenue analytics using business event time.

Provides time-based revenue metrics:

- revenue_date
- order_count
- total_revenue
- avg_order_value

Uses event_ts instead of ingested_at to ensure revenue is attributed to the correct business date.

This ensures late-arriving events are correctly reflected in historical revenue reporting.

Output: one row per revenue_date.

---

### 7. Incremental CDC Load Simulation (`07_incremental_load_simulation.sql`)

Simulates incremental CDC pipeline processing using an ingestion watermark.

Demonstrates how production pipelines process only new records instead of reprocessing all historical data.

Key logic applied:

- track last_processed_ingested_at watermark
- filter records using ingested_at > watermark
- apply CDC deduplication using ROW_NUMBER()
- produce current-state records for incremental updates

This approach improves scalability, efficiency, and correctness in large-scale warehouse pipelines.

Output: incremental batch records and incremental current-state records.

---

## CDC Logic Explained

The pipeline distinguishes between:

event_ts → when the business event occurred  
ingested_at → when the warehouse received the event  

The latest ingested record represents the correct current state.

This approach ensures accurate analytics even when events arrive late or are corrected.

---

## Technologies Demonstrated

- SQL window functions
- CDC deduplication logic
- Warehouse fact and dimension modeling
- Data quality filtering
- Analytical aggregation patterns
- Incremental CDC processing using ingestion watermarks

Compatible with:

- Snowflake
- BigQuery
- DuckDB
- PostgreSQL

---

## Learning Goals

This project demonstrates core data engineering concepts:

- Change Data Capture handling
- Fact and dimension modeling
- Building analytics-ready data marts
- Designing layered warehouse pipelines
- Implementing incremental data processing logic

---

## Author

Jahnavi Reddy Avula  
Data Engineer
