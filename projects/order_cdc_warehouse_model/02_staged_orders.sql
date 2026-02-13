/*
Staged Orders
Goal: clean and standardize raw CDC records before modeling.
Rules:
- customer_id must not be NULL
- amount must be > 0
*/

WITH raw_orders AS (

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
)

SELECT *
FROM raw_orders
WHERE customer_id IS NOT NULL
  AND amount > 0
ORDER BY order_id, ingested_at;
