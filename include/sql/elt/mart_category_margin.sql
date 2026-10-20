-- Business Q2: revenue and gross margin by category (delivered orders only).
-- margin = revenue - quantity x cost_price
CREATE OR REPLACE TABLE mart_category_margin AS
WITH o AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'),
p AS (SELECT * FROM stg_products WHERE dq_status <> 'FAIL')
SELECT
    p.category,
    count(*)                                              AS delivered_orders,
    round(sum(o.order_amount), 2)                         AS revenue,
    round(sum(o.quantity * p.cost_price), 2)              AS cost,
    round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
    round(100 * sum(o.order_amount - o.quantity * p.cost_price) / sum(o.order_amount), 2) AS margin_pct
FROM o JOIN p USING (product_id)
GROUP BY p.category
ORDER BY revenue DESC;
