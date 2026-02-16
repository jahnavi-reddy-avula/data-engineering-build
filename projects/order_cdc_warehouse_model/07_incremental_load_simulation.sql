/*
Incremental CDC Load Simulation
Goal: simulate processing only new CDC records based on ingested_at.

Concept:
- In production, we track a watermark (last_processed_ingested_at).
- Each run processes only records with ingested_at > watermark.
- Then we merge/deduplicate to refresh the current-state fact table.

This file simulates a "daily run" using a fixed watermark.
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

watermark AS (
  -- Imagine this is stored in a metadata table and updated each run
  SELECT TIMESTAMP '2026-02-10 12:00:00' AS last_processed_ingested_at
),

incremental_batch AS (
  -- Only process new records since the last run
  SELECT v.*
  FROM valid_orders v
  CROSS JOIN watermark w
  WHERE v.ingested_at > w.last_processed_ingested_at
),

incremental_current_state AS (
  -- Within the incremental batch, keep the latest record per order_id
  SELECT *
  FROM incremental_batch
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY ingested_at DESC, status DESC
  ) = 1
)

SELECT
  'WATERMARK' AS section,
  last_processed_ingested_at AS info_1,
  NULL::TIMESTAMP AS info_2
FROM watermark

UNION ALL

SELECT
  'INCREMENTAL_ROWS' AS section,
  ingested_at AS info_1,
  event_ts AS info_2
FROM incremental_batch

UNION ALL

SELECT
  'INCREMENTAL_CURRENT_STATE' AS section,
  ingested_at AS info_1,
  event_ts AS info_2
FROM incremental_current_state

ORDER BY section, info_1;
