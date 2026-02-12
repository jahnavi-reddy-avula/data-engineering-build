/*
Raw CDC Orders Simulation
Simulates change data capture with updates, duplicates, and late arrivals.
Columns:
- event_ts: when the order event happened in the source system
- ingested_at: when our pipeline received the record
*/

WITH raw_orders AS (

  -- Order 101 created (on-time)
  SELECT 101 AS order_id,
         'C1' AS customer_id,
         140.00 AS amount,
         'CREATED' AS status,
         TIMESTAMP '2026-02-10 09:00:00' AS event_ts,
         TIMESTAMP '2026-02-10 09:05:00' AS ingested_at

  UNION ALL

  -- Order 101 status updated later (CDC update)
  SELECT 101,
         'C1',
         140.00,
         'SHIPPED',
         TIMESTAMP '2026-02-10 09:00:00',
         TIMESTAMP '2026-02-10 12:00:00'

  UNION ALL

  -- Duplicate replay of the same update (common in CDC pipelines)
  SELECT 101,
         'C1',
         140.00,
         'SHIPPED',
         TIMESTAMP '2026-02-10 09:00:00',
         TIMESTAMP '2026-02-10 12:00:00'

  UNION ALL

  -- Order 102 created (on-time)
  SELECT 102,
         'C2',
         50.00,
         'CREATED',
         TIMESTAMP '2026-02-10 11:00:00',
         TIMESTAMP '2026-02-10 11:02:00'

  UNION ALL

  -- Order 102 amount corrected later (CDC update)
  SELECT 102,
         'C2',
         55.00,
         'CREATED',
         TIMESTAMP '2026-02-10 11:00:00',
         TIMESTAMP '2026-02-10 13:30:00'

  UNION ALL

  -- Late arriving order (happened Feb 9, ingested Feb 11)
  SELECT 103,
         'C3',
         200.00,
         'CREATED',
         TIMESTAMP '2026-02-09 16:00:00',
         TIMESTAMP '2026-02-11 01:00:00'

  UNION ALL

  -- Bad record: missing customer_id
  SELECT 104,
         NULL,
         75.00,
         'CREATED',
         TIMESTAMP '2026-02-10 15:00:00',
         TIMESTAMP '2026-02-10 15:01:00'

  UNION ALL

  -- Bad record: negative amount
  SELECT 105,
         'C4',
         -20.00,
         'CREATED',
         TIMESTAMP '2026-02-10 16:00:00',
         TIMESTAMP '2026-02-10 16:02:00'
)

SELECT *
FROM raw_orders
ORDER BY order_id, ingested_at;
