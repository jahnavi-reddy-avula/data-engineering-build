/*
Data Quality Checks
Goal: basic validations before loading into warehouse.
*/

WITH orders AS (
  SELECT 1 AS order_id, 'C001' AS customer_id, 140.00 AS amount UNION ALL
  SELECT 2, 'C002', -50.00 UNION ALL -- invalid negative
  SELECT 3, NULL, 75.00 UNION ALL -- missing customer
  SELECT 4, 'C003', 0.00
)

SELECT *
FROM orders
WHERE amount <= 0
   OR customer_id IS NULL;
