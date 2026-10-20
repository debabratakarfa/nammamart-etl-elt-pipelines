-- stg_products: latest product catalogue snapshot with quality flags.
-- When a product_id appears twice, the row named "(old)" is the duplicate.
CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT * FROM raw_products
    WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_products)
),
typed AS (
    SELECT
        trim(product_id)   AS product_id,
        trim(product_name) AS product_name,
        trim(category)     AS category_raw,
        trim(sub_category) AS sub_category,
        trim(brand)        AS brand,
        try_cast(nullif(trim(mrp), '') AS DOUBLE)        AS mrp,
        try_cast(nullif(trim(cost_price), '') AS DOUBLE) AS cost_price,
        trim(unit)         AS unit,
        lower(trim(is_perishable)) IN ('true', 'y', 'yes', '1') AS is_perishable,
        try_strptime(trim(launch_date), '%Y-%m-%d')       AS launch_ts,
        CAST(lower(trim(product_name)) LIKE '%(old)%' AS INTEGER) AS is_old,
        _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
    FROM src
),
flagged AS (
    SELECT *,
        [
          CASE WHEN product_id = '' THEN 'pr_product_id_missing' END,
          CASE WHEN product_id <> '' AND row_number() OVER (
                    PARTITION BY product_id ORDER BY is_old, _source_row) > 1
               THEN 'pr_product_id_duplicate' END,
          CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'pr_mrp_invalid' END,
          CASE WHEN cost_price IS NULL OR cost_price <= 0 THEN 'pr_cost_price_invalid' END,
          CASE WHEN launch_ts IS NULL OR launch_ts > _snapshot_ts THEN 'pr_launch_date_future' END
        ] AS crit,
        [
          CASE WHEN category_raw = '' THEN 'pr_category_missing' END,
          CASE WHEN cost_price > mrp THEN 'pr_cost_above_mrp' END
        ] AS warn
    FROM typed
)
SELECT
    product_id, product_name,
    CASE WHEN category_raw = '' THEN 'Uncategorised' ELSE category_raw END AS category,
    sub_category, brand, mrp, cost_price, unit, is_perishable,
    CAST(launch_ts AS DATE) AS launch_date,
    list_filter(crit, x -> x IS NOT NULL) || list_filter(warn, x -> x IS NOT NULL) AS failed_checks,
    CASE WHEN len(list_filter(crit, x -> x IS NOT NULL)) > 0 THEN 'FAIL'
         WHEN len(list_filter(warn, x -> x IS NOT NULL)) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(list_filter(crit, x -> x IS NOT NULL), '; '), '') AS failure_reason,
    nullif(array_to_string(list_filter(warn, x -> x IS NOT NULL), '; '), '') AS warning_reason,
    _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
FROM flagged;
