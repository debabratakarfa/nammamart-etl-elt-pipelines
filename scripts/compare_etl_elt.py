"""Prove the ETL and ELT pipelines agree (assignment Part B.4).

    python scripts/compare_etl_elt.py

Compares, for September and overall: fact rows, revenue by zone and day,
category margin, reconciliation, quarantine counts and dq_results.
Exit code 1 if anything differs.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402

from include.nammamart.config import ELT_DAG_ID, ETL_DAG_ID, WAREHOUSE_PATH  # noqa: E402

CHECKS = {
    "clean order rows (current state)": (
        """SELECT order_id, order_status, order_amount, quantity, store_id, product_id,
                  customer_id, payment_mode, delivery_minutes FROM etl_fact_orders""",
        """SELECT order_id, order_status, order_amount, quantity, store_id, product_id,
                  customer_id, payment_mode, delivery_minutes FROM stg_orders WHERE dq_status <> 'FAIL'""",
    ),
    "September net revenue by zone": (
        """SELECT d.zone, round(sum(f.net_revenue), 2) FROM etl_fact_orders f JOIN etl_dim_store d USING (store_id)
           WHERE f.order_date < DATE '2026-10-01' GROUP BY 1""",
        """SELECT zone, round(sum(net_revenue), 2) FROM mart_daily_store_kpis
           WHERE order_date < DATE '2026-10-01' GROUP BY 1""",
    ),
    "daily revenue by zone": (
        "SELECT order_date, zone, round(net_revenue, 2) FROM etl_daily_zone_revenue",
        """SELECT order_date, zone, round(sum(net_revenue), 2) FROM mart_daily_store_kpis
           GROUP BY 1, 2 HAVING sum(delivered_orders) > 0""",
    ),
    "category margin": (
        "SELECT category, delivered_orders, revenue, cost, gross_margin FROM etl_category_margin",
        "SELECT category, delivered_orders, revenue, cost, gross_margin FROM mart_category_margin",
    ),
    "payment reconciliation": (
        "SELECT order_id, payment_id, mismatch_reason, amount_difference FROM etl_payment_reconciliation",
        "SELECT order_id, payment_id, mismatch_reason, amount_difference FROM mart_payment_reconciliation",
    ),
    "masked customer dimension": (
        "SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier FROM etl_dim_customer",
        """SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier
           FROM stg_customers WHERE dq_status <> 'FAIL'""",
    ),
    "status-change history": (
        "SELECT order_id, old_status, new_status FROM etl_order_status_history",
        "SELECT order_id, old_status, new_status FROM stg_order_status_history",
    ),
}


def latest_run(con, dag_id):
    return con.execute("SELECT run_id FROM pipeline_runs WHERE dag_id=? ORDER BY end_time DESC LIMIT 1",
                       [dag_id]).fetchone()[0]


def main() -> int:
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    etl_run, elt_run = latest_run(con, ETL_DAG_ID), latest_run(con, ELT_DAG_ID)
    run_checks = dict(CHECKS)
    run_checks["quarantined rows by file and reason (latest runs)"] = (
        f"SELECT source_file, failure_reason, count(*) FROM etl_quarantine WHERE run_id='{etl_run}' "
        f"AND dag_id='{ETL_DAG_ID}' GROUP BY ALL",
        f"SELECT source_file, failure_reason, count(*) FROM elt_quarantine WHERE run_id='{elt_run}' "
        f"AND dag_id='{ELT_DAG_ID}' GROUP BY ALL",
    )
    run_checks["dq_results (latest runs)"] = (
        f"SELECT source_file, check_name, rows_checked, rows_failed, status FROM dq_results "
        f"WHERE run_id='{etl_run}' AND dag_id='{ETL_DAG_ID}'",
        f"SELECT source_file, check_name, rows_checked, rows_failed, status FROM dq_results "
        f"WHERE run_id='{elt_run}' AND dag_id='{ELT_DAG_ID}'",
    )
    ok = True
    for name, (a, b) in run_checks.items():
        etl_rows = set(con.execute(a).fetchall())
        elt_rows = set(con.execute(b).fetchall())
        only_etl, only_elt = etl_rows - elt_rows, elt_rows - etl_rows
        same = not only_etl and not only_elt
        ok &= same
        print(f"{'MATCH ' if same else 'DIFFER'}  {name}: {len(etl_rows)} rows")
        for r in sorted(only_etl, key=str)[:5]:
            print(f"        only in ETL: {r}")
        for r in sorted(only_elt, key=str)[:5]:
            print(f"        only in ELT: {r}")
    sept = con.execute("""SELECT round(sum(net_revenue), 2) FROM etl_fact_orders
                          WHERE order_date < DATE '2026-10-01'""").fetchone()[0]
    sept_elt = con.execute("""SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis
                              WHERE order_date < DATE '2026-10-01'""").fetchone()[0]
    print(f"\nSeptember net revenue  ETL: {sept:,.2f}   ELT: {sept_elt:,.2f}")
    print("ALL MATCH" if ok else "MISMATCHES FOUND")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
