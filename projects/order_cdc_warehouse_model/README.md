# Order CDC Warehouse Model

## Overview

This project simulates a production-grade Change Data Capture (CDC) warehouse pipeline using SQL. It demonstrates how raw transactional updates are transformed into reliable, analytics-ready warehouse tables.

The pipeline handles real-world data engineering problems including duplicate records, late-arriving events, data corrections, and invalid data.

This project models a common warehouse architecture used in Snowflake, BigQuery, and modern analytics platforms.

---

## Architecture

The pipeline follows a layered warehouse design:

Raw Layer → Clean Layer → Fact Layer → Dimension Layer → Mart Layer

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

---

## Author

Jahnavi Reddy Avula  
Data Engineer
