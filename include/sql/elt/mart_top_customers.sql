-- Business Q5: top 10 customers by delivered spend, with loyalty tier.
-- Only masked contact details are exposed.
CREATE OR REPLACE TABLE mart_top_customers AS
WITH o AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'),
c AS (SELECT * FROM stg_customers WHERE dq_status <> 'FAIL')
SELECT
    row_number() OVER (ORDER BY sum(o.order_amount) DESC, o.customer_id) AS spend_rank,
    o.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
    count(*)                       AS delivered_orders,
    round(sum(o.order_amount), 2)  AS total_spend
FROM o JOIN c USING (customer_id)
GROUP BY o.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY total_spend DESC, o.customer_id
LIMIT 10;
