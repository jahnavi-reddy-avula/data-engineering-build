/*
Customer Dimension (Derived)
Goal: create a simple customer dimension from the current-state fact orders.
Outputs:
- first_seen_date: first order event date
- last_seen_date: most recent order event date
- is_active: seen in the last 30 days (example logic)
*/

WITH staged_orders AS (

  SELECT 101 AS order_id,
         'C1' AS customer_id,
         140.00 AS amount,
         'CREATED' AS status,
         TIMESTAMP '2026-02-10 09:00:00' AS event_ts,
         TIMESTAMP '2026-02-10 09:05:00' AS ingested_at
  UNION ALL
  SELECT 101, 'C1', 140.00, 'SHIPPED',
         TIMESTAMP '2026-02-10 09:00:00',
         TIMESTAMP '2026-02-10 12:00:00'
  UNION ALL
  SELECT 101, 'C1', 140.00, 'SHIPPED',
         TIMESTAMP '2026-02-10 09:00:00',
         TIMESTAMP '2026-02-10 12:00:00'
  UNION ALL
  SELECT 102, 'C2', 50.00, 'CREATED',
         TIMESTAMP '2026-02-10 11:00:00',
         TIMESTAMP '2026-02-10 11:02:00'
  UNION ALL
  SELECT 102, 'C2', 55.00, 'CREATED',
         TIMESTAMP '2026-02-10 11:00:00',
         TIMESTAMP '2026-02-10 13:30:00'
  UNION ALL
  SELECT 103, 'C3', 200.00, 'CREATED',
         TIMESTAMP '2026-02-09 16:00:00',
         TIMESTAMP '2026-02-11 01:00:00'
  UNION ALL
  SELECT 104, NULL, 75.00, 'CREATED',
         TIMESTAMP '2026-02-10 15:00:00',
         TIMESTAMP '2026-02-10 15:01:00'
  UNION ALL
  SELECT 105, 'C4', -20.00, 'CREATED',
         TIMESTAMP '2026-02-10 16:00:00',
         TIMESTAMP '2026-02-10 16:02:00'
),

valid_orders AS (
  SELECT *
  FROM staged_orders
  WHERE customer_id IS NOT NULL
    AND amount > 0
),

fact_orders_current AS (
  SELECT
    order_id,
    customer_id,
    amount,
    status,
    event_ts,
    ingested_at
  FROM valid_orders
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY ingested_at DESC, status DESC
  ) = 1
)

SELECT
  customer_id,
  MIN(CAST(event_ts AS DATE)) AS first_seen_date,
  MAX(CAST(event_ts AS DATE)) AS last_seen_date,
  CASE
    WHEN MAX(CAST(event_ts AS DATE)) >= DATE '2026-01-12' THEN TRUE
    ELSE FALSE
  END AS is_active
FROM fact_orders_current
GROUP BY customer_id
ORDER BY customer_id;
