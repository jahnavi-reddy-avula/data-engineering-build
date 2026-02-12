/*
Join + Aggregation Example
Goal: calculate total spend per customer.
*/

WITH customers AS (
  SELECT 'C001' AS customer_id, 'Alice' AS name UNION ALL
  SELECT 'C002', 'Bob' UNION ALL
  SELECT 'C003', 'Carol'
),

orders AS (
  SELECT 'C001' AS customer_id, 140.00 AS order_amount UNION ALL
  SELECT 'C001', 80.00 UNION ALL
  SELECT 'C002', 50.00
)

SELECT
  c.customer_id,
  c.name,
  COALESCE(SUM(o.order_amount), 0) AS total_spend
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY c.customer_id;
