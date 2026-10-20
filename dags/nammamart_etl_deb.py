"""
### NammaMart ETL pipeline (Deb)

**Extract -> Validate -> Transform -> Load**, Monday to Saturday at 09:00 IST.

* Five source files are extracted in parallel and frozen in a run-scoped landing folder.
* Every row is checked against the data quality catalog (`include/nammamart/checks.py`).
  Critical failures go to `include/quarantine/deb/<file>_failed_<run_id>.csv` and the
  `etl_quarantine` table, each with a `failure_reason`.
* Only rows that passed every Critical check reach the star schema
  (`etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`).
  Email and phone are masked.
* The fact table is upserted on `order_id`, so re-runs never double count and the daily
  increment file can correct older orders (DELIVERED -> RETURNED).
* `audit_run` always runs (`trigger_rule="all_done"`) and writes one row to `pipeline_runs`.

All logic lives in `include/nammamart/etl.py`; this file only wires tasks together.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from include.nammamart.alerts import on_failure_alert
from include.nammamart.config import ETL_DAG_ID

ENTITIES = ["orders", "payments", "customers", "products", "stores"]
MASTERS = ["stores", "products", "customers"]

default_args = {
    "owner": "deb_fde",
    "retries": 2,                           # transient issues: file still being copied, DB lock
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=8),
    "execution_timeout": timedelta(minutes=10),  # no task should take more than a few seconds
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id=ETL_DAG_ID,
    description="NammaMart ETL: validated, reconciled numbers for the 09:30 ops review",
    # 09:00, Monday (1) to Saturday (6). Interpreted in the start_date's timezone (IST).
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=default_args,
    doc_md=__doc__,
    tags=["nammamart", "etl", "fde", "deb"],
)
def nammamart_etl_deb():

    @task
    def extract(entity: str, **context) -> list[dict]:
        from include.nammamart import etl
        return etl.extract(entity, context["run_id"])

    @task
    def validate_master(entity: str, metas: list[dict], **context):
        from include.nammamart import etl
        return etl.validate_master(entity, metas, context["run_id"])

    @task
    def validate_orders(metas: list[dict], **context):
        from include.nammamart import etl
        return etl.validate_orders(metas, context["run_id"])

    @task
    def validate_payments(order_metas: list[dict], pay_metas: list[dict], **context):
        from include.nammamart import etl
        return etl.validate_payments(order_metas, pay_metas, context["run_id"])

    @task
    def transform(validated: dict, **context):
        from include.nammamart import etl
        return etl.transform(context["run_id"], validated["files_validated"])

    @task
    def load(transformed: dict, extract_metas: dict, **context):
        from include.nammamart import etl
        return etl.load(context["run_id"], extract_metas)

    @task
    def reconcile(**context):
        from include.nammamart import etl
        return etl.reconcile(context["run_id"])

    @task
    def report(**context):
        from include.nammamart import etl
        return etl.report(context["run_id"])

    # retries=0: it fails on purpose to mark the run failed; retrying only delays the alert.
    @task(trigger_rule="all_done", retries=0)
    def audit_run(**context):
        from include.nammamart import common, etl
        dag_run = context.get("dag_run")
        return common.audit_run(etl.DAG_ID, context["run_id"], etl.EXPECTED_TASKS,
                                getattr(dag_run, "start_date", None))

    # Extract: five parallel tasks
    metas = {e: extract.override(task_id=f"extract_{e}")(e) for e in ENTITIES}

    # Validate masters in parallel, then orders (needs clean masters), then payments
    masters = [validate_master.override(task_id=f"validate_{e}")(e, metas[e]) for e in MASTERS]
    v_orders = validate_orders(metas["orders"])
    masters >> v_orders
    v_payments = validate_payments(metas["orders"], metas["payments"])
    v_orders >> v_payments

    loaded = load(transform(v_orders), metas)
    rec = reconcile()
    rep = report()
    [loaded, v_payments] >> rec >> rep >> audit_run()


nammamart_etl_deb()
