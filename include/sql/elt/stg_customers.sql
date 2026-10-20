-- stg_customers: latest CRM snapshot. Raw email/phone/name never leave this
-- query: only masked versions are stored. Unmasked values stay in raw_customers,
-- which only the data owner role can read.
CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT * FROM raw_customers
    WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_customers)
),
typed AS (
    SELECT
        trim(customer_id) AS customer_id,
        trim(full_name)   AS full_name,
        trim(email)       AS email,
        trim(phone)       AS phone,
        trim(city)        AS city,
        trim(area)        AS area,
        trim(pincode)     AS pincode,
        try_strptime(trim(signup_date), '%Y-%m-%d') AS signup_ts,
        trim(loyalty_tier) AS tier_raw,
        upper(left(trim(loyalty_tier), 1)) || lower(substr(trim(loyalty_tier), 2)) AS tier,
        upper(trim(is_active)) = 'Y' AS is_active,
        _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
    FROM src
),
flagged AS (
    SELECT *,
        regexp_full_match(email, '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$') AS email_ok,
        [
          CASE WHEN customer_id = '' THEN 'cu_customer_id_missing' END,
          CASE WHEN customer_id <> '' AND row_number() OVER (
                    PARTITION BY customer_id ORDER BY _source_row) > 1
               THEN 'cu_customer_id_duplicate' END,
          CASE WHEN signup_ts IS NULL OR signup_ts > _snapshot_ts THEN 'cu_signup_date_future' END
        ] AS crit,
        [
          CASE WHEN NOT regexp_full_match(email, '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$') THEN 'cu_email_invalid' END,
          CASE WHEN NOT regexp_full_match(phone, '^[6-9][0-9]{9}$') THEN 'cu_phone_invalid' END,
          CASE WHEN NOT regexp_full_match(pincode, '^[1-9][0-9]{5}$') THEN 'cu_pincode_invalid' END,
          CASE WHEN city = '' THEN 'cu_city_missing' END,
          CASE WHEN tier_raw <> tier AND tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')
               THEN 'cu_loyalty_tier_casing' END,
          CASE WHEN tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'cu_loyalty_tier_invalid' END
        ] AS warn
    FROM typed
)
SELECT
    customer_id,
    -- Name: first name + initial of last name  (Asha Rao -> Asha R.)
    CASE WHEN full_name = '' THEN NULL
         WHEN len(string_split(full_name, ' ')) = 1 THEN full_name
         ELSE string_split(full_name, ' ')[1] || ' ' || left(string_split(full_name, ' ')[-1], 1) || '.'
    END AS customer_name_masked,
    -- Email: first two characters of the local part + domain (pr***@example.com)
    CASE WHEN email_ok THEN left(email, 2) || '***@' || split_part(email, '@', 2) END AS email_masked,
    -- Phone: first two and last two digits (97******21)
    CASE WHEN length(phone) >= 4 AND regexp_full_match(phone, '^[0-9]+$')
         THEN left(phone, 2) || repeat('*', length(phone) - 4) || right(phone, 2) END AS phone_masked,
    nullif(city, '') AS city, area, pincode, CAST(signup_ts AS DATE) AS signup_date,
    tier AS loyalty_tier, is_active,
    list_filter(crit, x -> x IS NOT NULL) || list_filter(warn, x -> x IS NOT NULL) AS failed_checks,
    CASE WHEN len(list_filter(crit, x -> x IS NOT NULL)) > 0 THEN 'FAIL'
         WHEN len(list_filter(warn, x -> x IS NOT NULL)) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(list_filter(crit, x -> x IS NOT NULL), '; '), '') AS failure_reason,
    nullif(array_to_string(list_filter(warn, x -> x IS NOT NULL), '; '), '') AS warning_reason,
    _source_file, _source_row, _snapshot_ts, _run_id, _loaded_at
FROM flagged;
