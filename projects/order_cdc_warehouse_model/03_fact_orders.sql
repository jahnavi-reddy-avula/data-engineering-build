/*
Fact Orders (Current State)
Goal: keep the latest valid record per order_id from the CDC stream.
Approach:
- Filter invalid rows (customer_id not null, amount > 0)
- Deduplicate by order_id using latest ingested_at
- If ties occur, break ties using status (stable ordering)
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
)

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
ORDER BY order_id;
