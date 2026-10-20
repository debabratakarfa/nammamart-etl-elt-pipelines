-- Evaluate one raw orders batch (one file in one run) against every rule and
-- store the result in stg_order_batches. Nothing is dropped here: duplicates
-- and failures are kept with their reasons.
-- Params: run_id, source_file

DELETE FROM stg_order_batches WHERE _run_id = {{run_id}} AND _source_file = {{source_file}};

INSERT INTO stg_order_batches
WITH src AS (
    SELECT * FROM raw_orders WHERE _run_id = {{run_id}} AND _source_file = {{source_file}}
),
clean_stores    AS (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL'),
clean_products  AS (SELECT product_id, mrp FROM stg_products WHERE dq_status <> 'FAIL'),
clean_customers AS (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL'),
typed AS (
    SELECT
        trim(order_id)    AS order_id,
        trim(customer_id) AS customer_id,
        trim(store_id)    AS store_id,
        trim(product_id)  AS product_id,
        trim(channel)     AS channel,
        try_strptime(trim(order_ts), '%Y-%m-%d %H:%M:%S') AS order_ts,
        trim(order_status)        AS status_raw,
        upper(trim(order_status)) AS order_status,
        trim(payment_mode)        AS mode_raw,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cash' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
        END AS payment_mode,
        try_cast(nullif(trim(quantity), '') AS DOUBLE)         AS q,
        try_cast(nullif(trim(unit_price), '') AS DOUBLE)       AS u,
        try_cast(nullif(trim(discount_pct), '') AS DOUBLE)     AS d,
        try_cast(nullif(trim(order_amount), '') AS DOUBLE)     AS amt,
        try_cast(nullif(trim(delivery_minutes), '') AS DOUBLE) AS mins,
        _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
    FROM src
),
ranked AS (
    SELECT t.*, p.mrp,
           row_number() OVER (PARTITION BY t.order_id ORDER BY t._source_row) AS rn
    FROM typed t LEFT JOIN clean_products p USING (product_id)
),
flagged AS (
    SELECT *,
        [
          CASE WHEN order_id = '' THEN 'or_order_id_missing' END,
          CASE WHEN order_id <> '' AND rn > 1 THEN 'or_order_id_duplicate' END,
          CASE WHEN customer_id = '' THEN 'or_customer_id_missing' END,
          CASE WHEN order_ts IS NULL THEN 'or_order_ts_invalid' END,
          CASE WHEN order_ts > _snapshot_ts THEN 'or_order_ts_future' END,
          CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'or_status_invalid' END,
          CASE WHEN payment_mode IS NULL THEN 'or_payment_mode_invalid' END,
          CASE WHEN q IS NULL OR q <= 0 THEN 'or_quantity_invalid' END,
          CASE WHEN d IS NULL OR d < 0 OR d > 100 THEN 'or_discount_invalid' END,
          CASE WHEN NOT coalesce(abs(q * u * (1 - d / 100) - amt) <= 0.01, false) THEN 'or_amount_mismatch' END,
          CASE WHEN u > mrp + 0.001 THEN 'or_unit_price_above_mrp' END,
          CASE WHEN order_status = 'DELIVERED' AND mins IS NULL THEN 'or_delivery_missing' END,
          CASE WHEN mins <= 0 OR mins > 120 THEN 'or_delivery_implausible' END,
          CASE WHEN store_id NOT IN (SELECT store_id FROM clean_stores) THEN 'or_store_unknown' END,
          CASE WHEN product_id NOT IN (SELECT product_id FROM clean_products) THEN 'or_product_unknown' END,
          CASE WHEN customer_id <> '' AND customer_id NOT IN (SELECT customer_id FROM clean_customers)
               THEN 'or_customer_unknown' END
        ] AS crit,
        [
          CASE WHEN status_raw <> order_status AND order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')
               THEN 'or_status_casing' END,
          CASE WHEN payment_mode IS NOT NULL AND mode_raw <> payment_mode THEN 'or_payment_mode_nonstandard' END,
          CASE WHEN mins > 0 AND mins <= 120 AND (mins < 8 OR mins > 35) THEN 'or_delivery_outside_band' END
        ] AS warn
    FROM ranked
)
SELECT
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id,
    CAST(q AS INTEGER) AS quantity, u AS unit_price, d AS discount_pct, amt AS order_amount,
    payment_mode, order_status, channel, CAST(mins AS INTEGER) AS delivery_minutes,
    list_filter(crit, x -> x IS NOT NULL) || list_filter(warn, x -> x IS NOT NULL) AS failed_checks,
    CASE WHEN len(list_filter(crit, x -> x IS NOT NULL)) > 0 THEN 'FAIL'
         WHEN len(list_filter(warn, x -> x IS NOT NULL)) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(list_filter(crit, x -> x IS NOT NULL), '; '), '') AS failure_reason,
    nullif(array_to_string(list_filter(warn, x -> x IS NOT NULL), '; '), '') AS warning_reason,
    order_id <> '' AND rn > 1 AS is_duplicate,
    _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
FROM flagged;
