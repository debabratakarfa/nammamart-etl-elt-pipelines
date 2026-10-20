-- Business Q4 + acceptance criterion 7: every order/payment mismatch, one row each.
-- Same rules as the ETL reconciliation, so the two can be compared.
-- Every unique payment is read (failing ones included): this table exists to
-- surface money that doesn't line up, not to hide it.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH pay_snapshot AS (SELECT max(_snapshot_ts) AS ts FROM stg_payments),
-- Orders placed after the payment export was taken can't have a payment yet:
-- a timing difference, not a mismatch.
o AS (SELECT * FROM stg_orders
      WHERE dq_status <> 'FAIL' AND order_ts < (SELECT ts FROM pay_snapshot)),
p AS (
    SELECT payment_id, order_id, payment_status, payment_mode, amount_paid,
           order_id IN (SELECT DISTINCT trim(order_id) FROM raw_orders) AS order_known
    FROM stg_payments WHERE NOT is_duplicate
),
order_side AS (
    SELECT o.order_id, p.payment_id, o.order_status, p.payment_status,
           o.payment_mode AS order_payment_mode, p.payment_mode AS payment_mode,
           o.order_amount, p.amount_paid,
           CASE
             WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL THEN 'DELIVERED_NO_PAYMENT'
             WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'FAILED' THEN 'DELIVERED_PAYMENT_FAILED'
             WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'REFUNDED' THEN 'DELIVERED_BUT_REFUNDED'
             WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'SUCCESS'
                  AND abs(coalesce(p.amount_paid, 0) - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
             WHEN o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS' THEN 'RETURNED_NOT_REFUNDED'
             WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN 'CANCELLED_BUT_CHARGED'
             WHEN p.payment_id IS NOT NULL AND p.payment_mode IS DISTINCT FROM o.payment_mode
                  THEN 'PAYMENT_MODE_MISMATCH'
           END AS mismatch_reason
    FROM o LEFT JOIN p USING (order_id)
),
payment_side AS (
    SELECT p.order_id, p.payment_id, NULL AS order_status, p.payment_status,
           NULL AS order_payment_mode, p.payment_mode, NULL AS order_amount, p.amount_paid,
           CASE WHEN p.order_known THEN 'ORDER_QUARANTINED' ELSE 'PAYMENT_WITHOUT_ORDER' END
    FROM p WHERE p.order_id NOT IN (SELECT order_id FROM o)
)
SELECT *,
       -- expected collection minus actual collection (positive = money short)
       round(CASE mismatch_reason
         WHEN 'DELIVERED_NO_PAYMENT' THEN order_amount
         WHEN 'DELIVERED_PAYMENT_FAILED' THEN order_amount
         WHEN 'DELIVERED_BUT_REFUNDED' THEN order_amount
         WHEN 'AMOUNT_MISMATCH' THEN order_amount - coalesce(amount_paid, 0)
         WHEN 'RETURNED_NOT_REFUNDED' THEN -amount_paid
         WHEN 'CANCELLED_BUT_CHARGED' THEN -amount_paid
         WHEN 'PAYMENT_MODE_MISMATCH' THEN 0
         ELSE CASE WHEN payment_status = 'SUCCESS' THEN -coalesce(amount_paid, 0) ELSE 0 END
       END, 2) AS amount_difference,
       {{run_id}} AS run_id
FROM (SELECT * FROM order_side WHERE mismatch_reason IS NOT NULL
      UNION ALL SELECT * FROM payment_side)
ORDER BY mismatch_reason, order_id;
