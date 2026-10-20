-- Write this run's quality scorecard to dq_results: one row per check per file.
-- Params: run_id, dag_id, now
DELETE FROM dq_results WHERE run_id = {{run_id}} AND dag_id = {{dag_id}};

INSERT INTO dq_results
WITH evaluated AS (
    -- masters and payments: the snapshot evaluated in this run
    SELECT 'stores' AS entity, _source_file, failed_checks FROM stg_stores
    UNION ALL SELECT 'products', _source_file, failed_checks FROM stg_products
    UNION ALL SELECT 'customers', _source_file, failed_checks FROM stg_customers
    UNION ALL SELECT 'payments', _source_file, failed_checks FROM stg_payments
    -- orders: only batches that arrived in this run (unchanged files are skipped)
    UNION ALL SELECT 'orders', _source_file, failed_checks FROM stg_order_batches
              WHERE _run_id = {{run_id}}
),
files AS (SELECT entity, _source_file, count(*) AS rows_checked FROM evaluated GROUP BY ALL),
flat AS (SELECT _source_file, unnest(failed_checks) AS check_name FROM evaluated),
failures AS (
    SELECT _source_file, check_name, count(*) AS rows_failed FROM flat GROUP BY ALL
),
-- File-level observability checks for the orders feed
batch AS (
    SELECT b.source_file, b.row_count, b.previous_rows, b.snapshot_ts,
           (SELECT max(order_ts) FROM stg_order_batches s
             WHERE s._run_id = {{run_id}} AND s._source_file = b.source_file
               AND s.order_ts <= b.snapshot_ts) AS latest_ts
    FROM elt_batches b WHERE b.run_id = {{run_id}} AND b.entity = 'orders'
),
file_level AS (
    SELECT source_file, 'or_volume_drop' AS check_name, row_count AS rows_checked,
           CASE WHEN previous_rows > 0 AND row_count < previous_rows * (1 - 0.90)
                THEN row_count ELSE 0 END AS rows_failed
    FROM batch
    UNION ALL
    SELECT source_file, 'or_freshness_stale', row_count,
           CASE WHEN latest_ts IS NULL OR snapshot_ts - latest_ts > INTERVAL 1 DAY
                THEN row_count ELSE 0 END
    FROM batch
),
row_level AS (
    SELECT f._source_file AS source_file, c.check_name, f.rows_checked,
           coalesce(x.rows_failed, 0) AS rows_failed
    FROM files f
    JOIN dq_check_catalog c ON c.entity = f.entity
    LEFT JOIN failures x ON x._source_file = f._source_file AND x.check_name = c.check_name
    WHERE c.check_name NOT IN ('or_volume_drop', 'or_freshness_stale')
),
all_checks AS (SELECT * FROM row_level UNION ALL SELECT * FROM file_level)
SELECT {{run_id}}, {{dag_id}}, a.check_name, c.dimension, c.severity, a.rows_checked, a.rows_failed,
       CASE WHEN a.rows_checked = 0 THEN 'SKIPPED'
            WHEN a.rows_failed = 0 THEN 'PASS'
            WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       CAST({{now}} AS TIMESTAMP), a.source_file
FROM all_checks a JOIN dq_check_catalog c USING (check_name);
