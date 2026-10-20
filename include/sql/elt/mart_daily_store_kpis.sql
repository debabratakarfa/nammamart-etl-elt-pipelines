-- Business Q1 (net revenue by day, zone, store) + store capacity utilisation.
-- Only rows that passed every critical check (dq_status <> 'FAIL') are used.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
WITH o AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAIL'),
s AS (SELECT * FROM stg_stores WHERE dq_status <> 'FAIL'),
p AS (SELECT * FROM stg_products WHERE dq_status <> 'FAIL')
SELECT
    o.order_date, s.zone, o.store_id, s.store_name,
    count(*)                                                    AS total_orders,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED')        AS delivered_orders,
    count(*) FILTER (WHERE o.order_status = 'CANCELLED')        AS cancelled_orders,
    count(*) FILTER (WHERE o.order_status = 'RETURNED')         AS returned_orders,
    round(coalesce(sum(o.order_amount) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 2) AS net_revenue,
    round(coalesce(sum(o.order_amount - o.quantity * p.cost_price)
                   FILTER (WHERE o.order_status = 'DELIVERED'), 0), 2)                  AS gross_margin,
    round(avg(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 1)        AS avg_delivery_minutes,
    s.capacity_orders_per_day,
    round(100.0 * count(*) / s.capacity_orders_per_day, 1)      AS capacity_utilisation_pct
FROM o
JOIN s USING (store_id)
JOIN p USING (product_id)
GROUP BY o.order_date, s.zone, o.store_id, s.store_name, s.capacity_orders_per_day
ORDER BY o.order_date, s.zone, o.store_id;
