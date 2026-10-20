"""ETL pipeline: Extract -> Validate (pandas) -> Transform (pandas) -> Load (DuckDB).

Only rows that pass every Critical check reach the etl_* tables. The fact table
is loaded with an upsert on order_id, so re-running never double counts and a
later file (the daily increment) can correct an earlier order's status.
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

from . import common, db, validate
from .config import ETL_DAG_ID, SLA_TARGET_MINUTES, SOURCES, now_ist, run_dir
from .masking import mask_email, mask_name, mask_phone

DAG_ID = ETL_DAG_ID
QUARANTINE_TABLE = "etl_quarantine"
ENTITIES = ["orders", "payments", "customers", "products", "stores"]
EXPECTED_TASKS = (
    [f"extract_{e}" for e in ENTITIES]
    + [f"validate_{e}" for e in ["stores", "products", "customers", "orders", "payments"]]
    + ["transform", "load", "reconcile", "report"]
)

FACT_DDL = """
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE,
    customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes INTEGER, within_15_min BOOLEAN,
    net_revenue DOUBLE, cost_amount DOUBLE, gross_margin DOUBLE,
    dq_warnings VARCHAR, source_file VARCHAR, source_row INTEGER,
    snapshot_ts TIMESTAMP, run_id VARCHAR, loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_order_status_history (
    order_id VARCHAR, old_status VARCHAR, new_status VARCHAR,
    old_net_revenue DOUBLE, new_net_revenue DOUBLE, old_source_file VARCHAR,
    new_source_file VARCHAR, run_id VARCHAR, changed_at TIMESTAMP
);
"""
FACT_COLS = [
    "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
    "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
    "channel", "delivery_minutes", "within_15_min", "net_revenue", "cost_amount",
    "gross_margin", "dq_warnings", "source_file", "source_row", "snapshot_ts", "run_id",
    "loaded_at",
]


def _rd(run_id):
    return run_dir(DAG_ID, run_id)


def _save(obj, run_id, name):
    with open(_rd(run_id) / f"{name}.pkl", "wb") as fh:
        pickle.dump(obj, fh)


def _load(run_id, name):
    with open(_rd(run_id) / f"{name}.pkl", "rb") as fh:
        return pickle.load(fh)


# ------------------------------------------------------------------ extract
def extract(entity: str, run_id: str) -> list[dict]:
    # Only the orders feed is incremental; masters and payments are full snapshots
    return common.extract(entity, DAG_ID, run_id, skip_unchanged=(entity == "orders"))


# ------------------------------------------------------------------ validate
def _validate_file(entity, meta, fn, run_id, con, *args):
    cols = SOURCES[entity]["columns"]
    raw = validate.read_text_csv(_path(meta), entity)
    raw = common.keep_raw_copy(raw, cols)
    evaluated, results = fn(raw, *args)
    db.write_dq_results(con, run_id, DAG_ID, results)
    nq = common.quarantine(con, QUARANTINE_TABLE, DAG_ID, run_id, entity, evaluated, cols,
                           write_csv=True)
    return evaluated, nq


def _path(meta):
    from pathlib import Path
    return Path(meta["path"])


def validate_master(entity: str, metas: list[dict], run_id: str) -> dict:
    fn = {"stores": validate.validate_stores, "products": validate.validate_products,
          "customers": validate.validate_customers}[entity]
    meta = metas[-1]  # masters are single full snapshots
    with db.connect() as con:
        db.ensure_shared_tables(con)
        evaluated, nq = _validate_file(entity, meta, fn, run_id, con)
        db.record_metric(con, run_id, DAG_ID, f"validate_{entity}", rows_quarantined=nq)
    _save(evaluated, run_id, f"valid_{entity}")
    summary = evaluated.dq_status.value_counts().to_dict()
    print(f"[validate_{entity}] {summary}")
    return {"entity": entity, **summary}


def _clean(df):
    return df[df.dq_status != "FAIL"]


def validate_orders(metas: list[dict], run_id: str) -> dict:
    stores = _clean(_load(run_id, "valid_stores"))
    products = _clean(_load(run_id, "valid_products"))
    customers = _clean(_load(run_id, "valid_customers"))
    mrp = dict(zip(products.product_id, products.mrp))
    out, total_q = [], 0
    with db.connect() as con:
        db.ensure_shared_tables(con)
        for meta in metas:
            if meta["skip"]:
                print(f"[validate_orders] {meta['file']} unchanged since last load, skipped")
                continue
            evaluated, nq = _validate_file(
                "orders", meta, validate.validate_orders, run_id, con,
                set(customers.customer_id), set(stores.store_id), mrp, meta["previous_rows"],
            )
            total_q += nq
            _save(evaluated, run_id, f"valid_orders__{meta['file']}")
            out.append(meta["file"])
            print(f"[validate_orders] {meta['file']}: {evaluated.dq_status.value_counts().to_dict()}")
        db.record_metric(con, run_id, DAG_ID, "validate_orders", rows_quarantined=total_q)
    return {"files_validated": out, "rows_quarantined": total_q}


def validate_payments(order_metas: list[dict], pay_metas: list[dict], run_id: str) -> dict:
    # A payment is orphaned only if its order_id never arrived in ANY orders file
    all_ids = set()
    for m in order_metas:
        all_ids |= set(pd.read_csv(m["path"], dtype=str, usecols=["order_id"],
                                   keep_default_na=False).order_id.str.strip())
    with db.connect() as con:
        db.ensure_shared_tables(con)
        evaluated, nq = _validate_file("payments", pay_metas[-1], validate.validate_payments,
                                       run_id, con, all_ids)
        db.record_metric(con, run_id, DAG_ID, "validate_payments", rows_quarantined=nq)
    _save(evaluated, run_id, "valid_payments")
    _save(all_ids, run_id, "all_order_ids")
    return evaluated.dq_status.value_counts().to_dict()


# ------------------------------------------------------------------ transform
def transform(run_id: str, order_files: list[str]) -> dict:
    stores = _clean(_load(run_id, "valid_stores"))
    products = _clean(_load(run_id, "valid_products"))
    customers = _clean(_load(run_id, "valid_customers"))

    dim_store = stores[["store_id", "store_name", "zone", "area", "pincode", "open_date",
                        "manager_name", "capacity_orders_per_day"]].assign(
        dq_warnings=stores.warning_reason.values)
    dim_product = products[["product_id", "product_name", "category", "sub_category", "brand",
                            "mrp", "cost_price", "unit", "is_perishable", "launch_date"]].assign(
        dq_warnings=products.warning_reason.values)
    dim_customer = pd.DataFrame({
        "customer_id": customers.customer_id.values,
        "customer_name_masked": customers.full_name.map(mask_name).values,
        "email_masked": customers.email.map(mask_email).values,
        "phone_masked": customers.phone.map(mask_phone).values,
        "city": customers.city.replace("", None).values,
        "area": customers.area.values,
        "pincode": customers.pincode.values,
        "signup_date": customers.signup_date.values,
        "loyalty_tier": customers.loyalty_tier.values,
        "is_active": customers.is_active.values,
        "dq_warnings": customers.warning_reason.values,
    })
    _save({"dim_store": dim_store, "dim_product": dim_product, "dim_customer": dim_customer},
          run_id, "dims")

    cost = dict(zip(products.product_id, products.cost_price))
    fact_parts = {}
    for f in order_files:
        o = _clean(_load(run_id, f"valid_orders__{f}"))
        delivered = o.order_status == "DELIVERED"
        cost_amount = o.quantity * o.product_id.map(cost)  # rounded only when aggregated
        fact = pd.DataFrame({
            "order_id": o.order_id, "order_ts": o.order_ts, "order_date": o.order_date,
            "customer_id": o.customer_id, "store_id": o.store_id, "product_id": o.product_id,
            "quantity": o.quantity.astype(int), "unit_price": o.unit_price,
            "discount_pct": o.discount_pct, "order_amount": o.order_amount,
            "payment_mode": o.payment_mode, "order_status": o.order_status,
            "channel": o.channel, "delivery_minutes": o.delivery_minutes.astype("Int64"),
            "within_15_min": np.where(delivered, o.delivery_minutes <= SLA_TARGET_MINUTES, None),
            "net_revenue": np.where(delivered, o.order_amount, 0.0),
            "cost_amount": cost_amount,
            "gross_margin": np.where(delivered, o.order_amount - cost_amount, 0.0),
            "dq_warnings": o.warning_reason, "source_file": o._source_file,
            "source_row": o._source_row, "snapshot_ts": o._snapshot_ts,
        })
        fact_parts[f] = fact
        print(f"[transform] {f}: {len(fact)} fact rows")
    _save(fact_parts, run_id, "fact_parts")
    with db.connect() as con:
        db.ensure_shared_tables(con)
        db.record_metric(con, run_id, DAG_ID, "transform")
    return {"files": list(fact_parts), "rows": int(sum(len(v) for v in fact_parts.values()))}


# ------------------------------------------------------------------ load
def load(run_id: str, extract_metas: dict) -> dict:
    dims = _load(run_id, "dims")
    fact_parts = _load(run_id, "fact_parts")
    now = now_ist()
    inserted = updated = 0
    with db.connect() as con:
        db.ensure_shared_tables(con)
        con.execute(FACT_DDL)
        # Dimensions are full snapshots: replace atomically
        for name, df in dims.items():
            con.register("df_dim", df)
            con.execute(f"CREATE OR REPLACE TABLE etl_{name} AS SELECT * FROM df_dim")
            con.unregister("df_dim")
        set_clause = ", ".join(f"{c}=excluded.{c}" for c in FACT_COLS if c != "order_id")
        for f, fact in fact_parts.items():  # already in snapshot order
            fact = fact.assign(run_id=run_id, loaded_at=now)[FACT_COLS]
            con.register("df_fact", fact)
            existing = con.execute(
                "SELECT count(*) FROM df_fact d JOIN etl_fact_orders f USING (order_id)"
            ).fetchone()[0]
            # Keep an audit trail of status corrections (e.g. DELIVERED -> RETURNED)
            con.execute("""
                INSERT INTO etl_order_status_history
                SELECT f.order_id, f.order_status, d.order_status, f.net_revenue, d.net_revenue,
                       f.source_file, d.source_file, ?, ?
                FROM df_fact d JOIN etl_fact_orders f USING (order_id)
                WHERE d.order_status <> f.order_status AND d.snapshot_ts >= f.snapshot_ts
            """, [run_id, now])
            con.execute(f"""
                INSERT INTO etl_fact_orders SELECT * FROM df_fact
                ON CONFLICT (order_id) DO UPDATE SET {set_clause}
                WHERE excluded.snapshot_ts >= etl_fact_orders.snapshot_ts
            """)
            con.unregister("df_fact")
            inserted += len(fact) - existing
            updated += existing
            print(f"[load] {f}: {len(fact) - existing} inserted, {existing} upserted onto existing ids")
        # Record successfully loaded files so unchanged files are skipped next time
        for entity, metas in extract_metas.items():
            for m in metas:
                if m["skip"]:
                    continue
                con.execute("DELETE FROM load_manifest WHERE dag_id=? AND source_file=? AND run_id=?",
                            [DAG_ID, m["file"], run_id])
                con.execute("INSERT INTO load_manifest VALUES (?,?,?,?,?,?,?,?)",
                            [DAG_ID, entity, m["file"], m["hash"], m["rows"], m["snapshot_ts"],
                             run_id, now])
        n_fact = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        db.record_metric(con, run_id, DAG_ID, "load", rows_loaded=inserted + updated)
    print(f"[load] etl_fact_orders now holds {n_fact} rows")
    return {"inserted": inserted, "updated": updated, "fact_rows": n_fact}


# ------------------------------------------------------------------ reconcile
RECON_SQL = """
CREATE OR REPLACE TABLE etl_payment_reconciliation AS
-- Orders placed after the payment export was taken cannot have a payment row
-- yet; they are a timing difference, not a mismatch, so they are left out.
WITH o AS (SELECT * FROM etl_fact_orders WHERE order_ts < CAST(? AS TIMESTAMP)),
p AS (SELECT * FROM df_pay),
order_side AS (
    SELECT o.order_id, p.payment_id, o.order_status, p.payment_status,
           o.payment_mode AS order_payment_mode, p.payment_mode AS payment_mode,
           o.order_amount, p.amount_paid,
           CASE
             WHEN o.order_status='DELIVERED' AND p.payment_id IS NULL THEN 'DELIVERED_NO_PAYMENT'
             WHEN o.order_status='DELIVERED' AND p.payment_status='FAILED' THEN 'DELIVERED_PAYMENT_FAILED'
             WHEN o.order_status='DELIVERED' AND p.payment_status='REFUNDED' THEN 'DELIVERED_BUT_REFUNDED'
             WHEN o.order_status='DELIVERED' AND p.payment_status='SUCCESS'
                  AND abs(coalesce(p.amount_paid, 0) - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
             WHEN o.order_status='RETURNED' AND p.payment_status='SUCCESS' THEN 'RETURNED_NOT_REFUNDED'
             WHEN o.order_status='CANCELLED' AND p.payment_status='SUCCESS' THEN 'CANCELLED_BUT_CHARGED'
             WHEN p.payment_id IS NOT NULL AND p.payment_mode IS DISTINCT FROM o.payment_mode
                  THEN 'PAYMENT_MODE_MISMATCH'
           END AS mismatch_reason
    FROM o LEFT JOIN p USING (order_id)
),
payment_side AS (
    SELECT p.order_id, p.payment_id, NULL AS order_status, p.payment_status,
           NULL AS order_payment_mode, p.payment_mode, NULL AS order_amount, p.amount_paid,
           CASE WHEN p.order_known THEN 'ORDER_QUARANTINED' ELSE 'PAYMENT_WITHOUT_ORDER' END
    FROM p WHERE p.order_id NOT IN (SELECT order_id FROM o)
)
SELECT *,
       -- expected collection minus actual collection (positive = money short)
       round(CASE mismatch_reason
         WHEN 'DELIVERED_NO_PAYMENT' THEN order_amount
         WHEN 'DELIVERED_PAYMENT_FAILED' THEN order_amount
         WHEN 'DELIVERED_BUT_REFUNDED' THEN order_amount
         WHEN 'AMOUNT_MISMATCH' THEN order_amount - coalesce(amount_paid, 0)
         WHEN 'RETURNED_NOT_REFUNDED' THEN -amount_paid
         WHEN 'CANCELLED_BUT_CHARGED' THEN -amount_paid
         WHEN 'PAYMENT_MODE_MISMATCH' THEN 0
         ELSE CASE WHEN payment_status='SUCCESS' THEN -coalesce(amount_paid, 0) ELSE 0 END
       END, 2) AS amount_difference,
       ? AS run_id
FROM (SELECT * FROM order_side WHERE mismatch_reason IS NOT NULL
      UNION ALL SELECT * FROM payment_side)
ORDER BY mismatch_reason, order_id
"""


def reconcile(run_id: str) -> dict:
    pay = _load(run_id, "valid_payments")
    pay_snapshot = str(pay["_snapshot_ts"].iloc[0])
    all_ids = _load(run_id, "all_order_ids")
    # Reconciliation reads every unique payment (bad ones included): its job is
    # to surface money that doesn't line up, not to hide it.
    pay = pay[~pay.failure_reason.fillna("").str.contains("pa_payment_id_duplicate")]
    df_pay = pd.DataFrame({
        "payment_id": pay.payment_id, "order_id": pay.order_id,
        "payment_status": pay.payment_status, "payment_mode": pay.payment_mode,
        "amount_paid": pay.amount_paid, "order_known": pay.order_id.isin(all_ids),
    })
    with db.connect() as con:
        con.register("df_pay", df_pay)
        con.execute(RECON_SQL, [pay_snapshot, run_id])
        summary = con.execute("""
            SELECT mismatch_reason, count(*) n, round(sum(abs(amount_difference)), 2) amount
            FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC""").fetchdf()
        db.record_metric(con, run_id, DAG_ID, "reconcile")
    print("[reconcile]\n" + summary.to_string(index=False))
    return {"mismatches": int(summary.n.sum())}


# ------------------------------------------------------------------ report
def report(run_id: str) -> dict:
    with db.connect() as con:
        fact = con.execute("SELECT * FROM etl_fact_orders").fetchdf()
        stores = con.execute("SELECT store_id, zone FROM etl_dim_store").fetchdf()
        products = con.execute("SELECT product_id, category FROM etl_dim_product").fetchdf()
        d = fact[fact.order_status == "DELIVERED"].merge(stores, on="store_id").merge(products, on="product_id")
        zone = (d.groupby(["order_date", "zone"], as_index=False)
                 .agg(delivered_orders=("order_id", "count"), net_revenue=("net_revenue", "sum"))
                 .sort_values(["order_date", "zone"]))
        zone["net_revenue"] = zone.net_revenue.round(2)
        cat = (d.groupby("category", as_index=False)
                .agg(delivered_orders=("order_id", "count"), revenue=("net_revenue", "sum"),
                     cost=("cost_amount", "sum"), gross_margin=("gross_margin", "sum")))
        cat["margin_pct"] = (100 * cat.gross_margin / cat.revenue).round(2)
        cat = cat.round(2).sort_values("revenue", ascending=False)
        con.register("df_zone", zone)
        con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                       SELECT CAST(order_date AS DATE) AS order_date, zone, delivered_orders, net_revenue
                       FROM df_zone ORDER BY order_date, zone""")
        con.register("df_cat", cat)
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT * FROM df_cat")
        db.record_metric(con, run_id, DAG_ID, "report")
    totals = zone.groupby("zone").net_revenue.sum().round(2)
    print("[report] Net revenue by zone (all loaded days)\n" + totals.to_string())
    print("[report] Category margin\n" + cat.to_string(index=False))
    return {"net_revenue": float(zone.net_revenue.sum().round(2))}
