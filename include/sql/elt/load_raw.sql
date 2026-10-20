-- Load one source file into raw_<entity> exactly as received.
-- all_varchar keeps every value as text; the nullstr sentinel stops DuckDB
-- turning empty cells into NULL, so '' stays '' (true to the source).
-- Params: entity, columns, path, source_file, snapshot_ts, file_hash, run_id, loaded_at

-- A task retry for the same run replaces its own partial batch, nothing else.
DELETE FROM raw_{{entity}} WHERE _run_id = {{run_id}} AND _source_file = {{source_file}};

INSERT INTO raw_{{entity}}
SELECT {{columns}},
       {{source_file}}                       AS _source_file,
       CAST(row_number() OVER () AS INTEGER) AS _source_row,
       CAST({{snapshot_ts}} AS TIMESTAMP)     AS _snapshot_ts,
       {{file_hash}}                         AS _file_hash,
       {{run_id}}                            AS _run_id,
       CAST({{loaded_at}} AS TIMESTAMP)       AS _loaded_at
FROM read_csv({{path}}, header = true, all_varchar = true, delim = ',', quote = '"',
              nullstr = '__NAMMAMART_NEVER_NULL__');
