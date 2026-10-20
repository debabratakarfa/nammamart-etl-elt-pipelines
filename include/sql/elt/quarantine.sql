-- Copy this run's rejected rows into elt_quarantine, exactly as received
-- (joined back to raw_<entity>) with the reason.
-- Params: entity, key, raw_struct, run_id, dag_id, now, stg_table, batch_filter

DELETE FROM elt_quarantine WHERE run_id = {{run_id}} AND dag_id = {{dag_id}} AND entity = '{{entity}}';

INSERT INTO elt_quarantine
SELECT {{run_id}}, {{dag_id}}, '{{entity}}', s._source_file, s._source_row,
       r.{{key}}, s.failure_reason,
       to_json({{raw_struct}}),
       CAST({{now}} AS TIMESTAMP)
FROM {{stg_table}} s
JOIN raw_{{entity}} r
  ON r._loaded_at = s._loaded_at AND r._source_file = s._source_file AND r._source_row = s._source_row
WHERE s.dq_status = 'FAIL' {{batch_filter}};
