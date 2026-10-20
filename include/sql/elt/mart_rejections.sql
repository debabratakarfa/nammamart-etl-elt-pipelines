-- Business Q6: rows rejected by this run, by file and reason.
-- Params: run_id, dag_id
CREATE OR REPLACE TABLE mart_rejections AS
SELECT run_id, source_file, failure_reason, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = {{run_id}} AND dag_id = {{dag_id}}
GROUP BY ALL
ORDER BY source_file, rows_rejected DESC;
