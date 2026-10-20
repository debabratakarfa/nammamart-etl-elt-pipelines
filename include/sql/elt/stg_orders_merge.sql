-- Upsert one evaluated batch into stg_orders (current state, one row per order).
-- Rules:
--   * the newer snapshot wins (the 1 Oct increment overrides September);
--   * a failing row never overwrites a good one;
--   * extra copies of the same order_id inside one file are never merged.
-- Params: run_id, source_file, merged_at

-- 1. Log status changes (e.g. DELIVERED on 3 Sep -> RETURNED on 1 Oct) before overwriting
INSERT INTO stg_order_status_history
SELECT s.order_id, s.order_status, b.order_status, s._source_file, b._source_file,
       {{run_id}}, CAST({{merged_at}} AS TIMESTAMP)
FROM stg_order_batches b
JOIN stg_orders s USING (order_id)
WHERE b._run_id = {{run_id}} AND b._source_file = {{source_file}}
  AND NOT b.is_duplicate AND b.dq_status <> 'FAIL'
  AND b._snapshot_ts >= s._snapshot_ts
  AND b.order_status <> s.order_status
  AND s.dq_status <> 'FAIL';

-- 2. Upsert rows that passed (PASS / WARN)
INSERT INTO stg_orders
SELECT order_id, order_ts, order_date, customer_id, store_id, product_id, quantity,
       unit_price, discount_pct, order_amount, payment_mode, order_status, channel,
       delivery_minutes, dq_status, failure_reason, warning_reason,
       _source_file, _source_row, _snapshot_ts, _run_id, CAST({{merged_at}} AS TIMESTAMP)
FROM stg_order_batches
WHERE _run_id = {{run_id}} AND _source_file = {{source_file}}
  AND NOT is_duplicate AND order_id <> '' AND dq_status <> 'FAIL'
ON CONFLICT (order_id) DO UPDATE SET
    order_ts = excluded.order_ts, order_date = excluded.order_date,
    customer_id = excluded.customer_id, store_id = excluded.store_id,
    product_id = excluded.product_id, quantity = excluded.quantity,
    unit_price = excluded.unit_price, discount_pct = excluded.discount_pct,
    order_amount = excluded.order_amount, payment_mode = excluded.payment_mode,
    order_status = excluded.order_status, channel = excluded.channel,
    delivery_minutes = excluded.delivery_minutes, dq_status = excluded.dq_status,
    failure_reason = excluded.failure_reason, warning_reason = excluded.warning_reason,
    _source_file = excluded._source_file, _source_row = excluded._source_row,
    _snapshot_ts = excluded._snapshot_ts, _run_id = excluded._run_id,
    _merged_at = excluded._merged_at
WHERE excluded._snapshot_ts >= stg_orders._snapshot_ts;

-- 3. Record failing rows too (so staging shows them with dq_status = 'FAIL'),
--    but only where no good version of the order exists
INSERT INTO stg_orders
SELECT order_id, order_ts, order_date, customer_id, store_id, product_id, quantity,
       unit_price, discount_pct, order_amount, payment_mode, order_status, channel,
       delivery_minutes, dq_status, failure_reason, warning_reason,
       _source_file, _source_row, _snapshot_ts, _run_id, CAST({{merged_at}} AS TIMESTAMP)
FROM stg_order_batches
WHERE _run_id = {{run_id}} AND _source_file = {{source_file}}
  AND NOT is_duplicate AND order_id <> '' AND dq_status = 'FAIL'
ON CONFLICT (order_id) DO UPDATE SET
    order_ts = excluded.order_ts, order_date = excluded.order_date,
    customer_id = excluded.customer_id, store_id = excluded.store_id,
    product_id = excluded.product_id, quantity = excluded.quantity,
    unit_price = excluded.unit_price, discount_pct = excluded.discount_pct,
    order_amount = excluded.order_amount, payment_mode = excluded.payment_mode,
    order_status = excluded.order_status, channel = excluded.channel,
    delivery_minutes = excluded.delivery_minutes, dq_status = excluded.dq_status,
    failure_reason = excluded.failure_reason, warning_reason = excluded.warning_reason,
    _source_file = excluded._source_file, _source_row = excluded._source_row,
    _snapshot_ts = excluded._snapshot_ts, _run_id = excluded._run_id,
    _merged_at = excluded._merged_at
WHERE stg_orders.dq_status = 'FAIL' AND excluded._snapshot_ts >= stg_orders._snapshot_ts;
