/*
Window Functions – Basics
Goal: show ranking, running totals, moving averages, and de-dup patterns.
*/

WITH sample_orders AS (
  SELECT 'C001' AS customer_id, DATE '2026-01-03' AS order_date, 140.00 AS order_amount UNION ALL
  SELECT 'C001', DATE '2026-01-10',  80.00 UNION ALL
  SELECT 'C001', DATE '2026-02-01', 200.00 UNION ALL
  SELECT 'C002', DATE '2026-01-05',  50.00 UNION ALL
  SELECT 'C002', DATE '2026-01-20',  75.00 UNION ALL
  SELECT 'C003', DATE '2026-01-07', 300.00 UNION ALL
  SELECT 'C003', DATE '2026-02-09',  40.00
)

-- 1) Order sequence per customer
SELECT
  customer_id,
  order_date,
  order_amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number
FROM sample_orders
ORDER BY customer_id, order_date;

-- 2) Ranking spend per customer
SELECT
  customer_id,
  order_date,
  order_amount,
  RANK() OVER (PARTITION BY customer_id ORDER BY order_amount DESC) AS amount_rank
FROM sample_orders
ORDER BY customer_id, amount_rank;

-- 3) Running total
SELECT
  customer_id,
  order_date,
  order_amount,
  SUM(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
  ) AS running_total
FROM sample_orders
ORDER BY customer_id, order_date;

-- 4) Moving average of last 2 orders
SELECT
  customer_id,
  order_date,
  order_amount,
  AVG(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
  ) AS moving_avg_last_2
FROM sample_orders
ORDER BY customer_id, order_date;

-- 5) Compare with previous order
SELECT
  customer_id,
  order_date,
  order_amount,
  LAG(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
  ) AS prev_amount
FROM sample_orders
ORDER BY customer_id, order_date;
