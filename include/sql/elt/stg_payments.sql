-- stg_payments: latest payment-gateway snapshot with quality flags.
-- A payment is orphaned only if its order_id never arrived in ANY orders file
-- (raw_orders keeps every file ever received).
CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT * FROM raw_payments
    WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_payments)
),
known_orders AS (SELECT DISTINCT trim(order_id) AS order_id FROM raw_orders),
typed AS (
    SELECT
        trim(payment_id)  AS payment_id,
        trim(order_id)    AS order_id,
        try_strptime(trim(payment_ts), '%Y-%m-%d %H:%M:%S') AS payment_ts,
        trim(payment_mode) AS mode_raw,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cash' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
        END AS payment_mode,
        try_cast(nullif(trim(amount_paid), '') AS DOUBLE) AS amount_paid,
        upper(trim(payment_status)) AS payment_status,
        trim(gateway_ref) AS gateway_ref,
        _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
    FROM src
),
flagged AS (
    SELECT *,
        [
          CASE WHEN payment_id = '' THEN 'pa_payment_id_missing' END,
          CASE WHEN payment_id <> '' AND row_number() OVER (
                    PARTITION BY payment_id ORDER BY _source_row) > 1
               THEN 'pa_payment_id_duplicate' END,
          CASE WHEN payment_ts IS NULL THEN 'pa_payment_ts_invalid' END,
          CASE WHEN payment_ts > _snapshot_ts THEN 'pa_payment_ts_future' END,
          CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'pa_status_invalid' END,
          CASE WHEN payment_mode IS NULL THEN 'pa_payment_mode_invalid' END,
          CASE WHEN amount_paid IS NULL OR amount_paid < 0
                    OR (amount_paid = 0 AND payment_status = 'SUCCESS') THEN 'pa_amount_invalid' END,
          CASE WHEN order_id NOT IN (SELECT order_id FROM known_orders) THEN 'pa_order_unknown' END
        ] AS crit,
        [
          CASE WHEN payment_mode IS NOT NULL AND mode_raw <> payment_mode THEN 'pa_payment_mode_nonstandard' END,
          CASE WHEN payment_mode IS DISTINCT FROM 'Cash on Delivery' AND gateway_ref = ''
               THEN 'pa_gateway_ref_missing' END
        ] AS warn
    FROM typed
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
    list_filter(crit, x -> x IS NOT NULL) || list_filter(warn, x -> x IS NOT NULL) AS failed_checks,
    CASE WHEN len(list_filter(crit, x -> x IS NOT NULL)) > 0 THEN 'FAIL'
         WHEN len(list_filter(warn, x -> x IS NOT NULL)) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(list_filter(crit, x -> x IS NOT NULL), '; '), '') AS failure_reason,
    nullif(array_to_string(list_filter(warn, x -> x IS NOT NULL), '; '), '') AS warning_reason,
    payment_id <> '' AND row_number() OVER (PARTITION BY payment_id ORDER BY _source_row) > 1 AS is_duplicate,
    _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
FROM flagged;
