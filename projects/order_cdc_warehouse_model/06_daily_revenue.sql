/*
Daily Revenue (Mart Layer)
Goal: daily revenue based on business event time (event_ts), using current-state orders.

Why event_ts:
- Revenue should be attributed to when the order happened, not when it was ingested.
- Late arriving events should still count toward the correct business day.
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
  CAST(event_ts AS DATE) AS revenue_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue,
  ROUND(AVG(amount), 2) AS avg_order_value
FROM fact_orders_current
GROUP BY 1
ORDER BY revenue_date;
