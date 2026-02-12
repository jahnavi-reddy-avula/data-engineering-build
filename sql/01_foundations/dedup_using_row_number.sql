/*
De-duplication using ROW_NUMBER()
Goal: keep the latest record per customer.
*/

WITH customer_updates AS (
  SELECT 'C001' AS customer_id, 'a@gmail.com' AS email, DATE '2026-01-01' AS updated_at UNION ALL
  SELECT 'C001', 'a@gmail.com', DATE '2026-01-05' UNION ALL
  SELECT 'C002', 'b@gmail.com', DATE '2026-01-02' UNION ALL
  SELECT 'C002', 'b@gmail.com', DATE '2026-01-03' UNION ALL
  SELECT 'C003', 'c@gmail.com', DATE '2026-01-04'
)

SELECT
  customer_id,
  email,
  updated_at
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY customer_id
           ORDER BY updated_at DESC
         ) AS row_no
  FROM customer_updates
)
WHERE row_no = 1
ORDER BY customer_id;
