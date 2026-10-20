-- ELT tables that must exist before any load. Safe to run every time.

-- RAW: exact copy of every source file, all text, append-only.
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR,
    product_id VARCHAR, quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR,
    order_amount VARCHAR, payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP,
    _file_hash VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR,
    amount_paid VARCHAR, payment_status VARCHAR, gateway_ref VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP,
    _file_hash VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR,
    area VARCHAR, pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR,
    is_active VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP,
    _file_hash VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR,
    brand VARCHAR, mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR,
    launch_date VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP,
    _file_hash VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR,
    open_date VARCHAR, manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP,
    _file_hash VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP
);

-- One row per raw batch, used by the volume check.
CREATE TABLE IF NOT EXISTS elt_batches (
    run_id VARCHAR, entity VARCHAR, source_file VARCHAR, row_count BIGINT,
    previous_rows BIGINT, snapshot_ts TIMESTAMP, loaded_at TIMESTAMP
);

-- STAGING (orders): every evaluated row of every batch, duplicates included.
CREATE TABLE IF NOT EXISTS stg_order_batches (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR,
    store_id VARCHAR, product_id VARCHAR, quantity INTEGER, unit_price DOUBLE,
    discount_pct DOUBLE, order_amount DOUBLE, payment_mode VARCHAR, order_status VARCHAR,
    channel VARCHAR, delivery_minutes INTEGER,
    failed_checks VARCHAR[], dq_status VARCHAR, failure_reason VARCHAR, warning_reason VARCHAR,
    is_duplicate BOOLEAN,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP, _run_id VARCHAR,
    _loaded_at TIMESTAMP
);

-- STAGING (orders): current state, one row per order_id, maintained by upsert.
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR,
    store_id VARCHAR, product_id VARCHAR, quantity INTEGER, unit_price DOUBLE,
    discount_pct DOUBLE, order_amount DOUBLE, payment_mode VARCHAR, order_status VARCHAR,
    channel VARCHAR, delivery_minutes INTEGER,
    dq_status VARCHAR, failure_reason VARCHAR, warning_reason VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _snapshot_ts TIMESTAMP, _run_id VARCHAR,
    _merged_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_order_status_history (
    order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, old_source_file VARCHAR,
    new_source_file VARCHAR, run_id VARCHAR, changed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS elt_quarantine (
    run_id VARCHAR, dag_id VARCHAR, entity VARCHAR, source_file VARCHAR,
    source_row INTEGER, record_key VARCHAR, failure_reason VARCHAR,
    raw_record JSON, quarantined_at TIMESTAMP
);
