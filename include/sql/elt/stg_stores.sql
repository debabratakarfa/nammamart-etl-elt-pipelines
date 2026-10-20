-- stg_stores: latest raw snapshot of the store master, typed, standardised,
-- with quality flags. Rebuilt every run (masters are full snapshots).
CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT * FROM raw_stores
    WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_stores)
),
typed AS (
    SELECT
        trim(store_id)                                         AS store_id,
        trim(store_name)                                       AS store_name,
        upper(left(trim(zone), 1)) || lower(substr(trim(zone), 2)) AS zone_c,
        trim(area)                                             AS area,
        trim(pincode)                                          AS pincode,
        try_strptime(trim(open_date), '%Y-%m-%d')              AS open_ts,
        trim(manager_name)                                     AS manager_name,
        try_cast(nullif(trim(capacity_orders_per_day), '') AS DOUBLE) AS capacity,
        _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
    FROM src
),
flagged AS (
    SELECT *,
        [
          CASE WHEN store_id = '' THEN 'st_store_id_missing' END,
          CASE WHEN store_id <> '' AND row_number() OVER (PARTITION BY store_id ORDER BY _source_row) > 1
               THEN 'st_store_id_duplicate' END,
          CASE WHEN open_ts IS NULL OR open_ts > _snapshot_ts THEN 'st_open_date_future' END
        ] AS crit,
        [
          CASE WHEN zone_c = '' THEN 'st_zone_missing' END,
          CASE WHEN zone_c <> '' AND zone_c NOT IN ('North', 'South', 'East', 'West') THEN 'st_zone_invalid' END,
          CASE WHEN NOT regexp_full_match(pincode, '^[1-9][0-9]{5}$') THEN 'st_pincode_invalid' END,
          CASE WHEN capacity IS NULL OR capacity <= 0 THEN 'st_capacity_invalid' END
        ] AS warn
    FROM typed
)
SELECT
    store_id, store_name,
    CASE WHEN zone_c = '' THEN 'Unassigned' ELSE zone_c END AS zone,
    area, pincode, CAST(open_ts AS DATE) AS open_date, manager_name,
    CASE WHEN capacity > 0 THEN capacity END AS capacity_orders_per_day,
    list_filter(crit, x -> x IS NOT NULL) || list_filter(warn, x -> x IS NOT NULL) AS failed_checks,
    CASE WHEN len(list_filter(crit, x -> x IS NOT NULL)) > 0 THEN 'FAIL'
         WHEN len(list_filter(warn, x -> x IS NOT NULL)) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(list_filter(crit, x -> x IS NOT NULL), '; '), '') AS failure_reason,
    nullif(array_to_string(list_filter(warn, x -> x IS NOT NULL), '; '), '') AS warning_reason,
    _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
FROM flagged;
