-- Business Q3: average delivery time per store and % of delivered orders within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
WITH o AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'),
s AS (SELECT * FROM stg_stores WHERE dq_status <> 'FAIL')
SELECT
    o.store_id, s.store_name, s.zone,
    count(*)                                                         AS delivered_orders,
    round(avg(o.delivery_minutes), 1)                                AS avg_delivery_minutes,
    count(*) FILTER (WHERE o.delivery_minutes <= 15)                 AS within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 1) AS pct_within_15_min
FROM o JOIN s USING (store_id)
GROUP BY ALL
ORDER BY pct_within_15_min DESC, o.store_id;
