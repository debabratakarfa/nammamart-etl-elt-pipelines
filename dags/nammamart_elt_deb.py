"""
### NammaMart ELT pipeline (Deb)

**Extract -> Load raw -> Transform in the warehouse with SQL**, Monday to Saturday at 09:00 IST.

| Layer | Tables | Rule |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Exact copy of each CSV, all text, plus `_loaded_at`, `_source_file`, `_run_id`. Append-only. |
| Staging | `stg_*` | SQL only: cast, standardise, dedupe, mask PII, quality flags (`dq_status`, `failure_reason`). `stg_orders` is upserted on `order_id`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | Business-ready, built only from rows with `dq_status <> 'FAIL'`. |

Every transformation is a file in `include/sql/elt/`. Python only fills in parameters and runs it.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from include.nammamart.alerts import on_failure_alert
from include.nammamart.config import ELT_DAG_ID

ENTITIES = ["orders", "payments", "customers", "products", "stores"]
MASTER_STAGING = ["stg_stores", "stg_products", "stg_customers"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers"]

default_args = {
    "owner": "deb_fde",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=8),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_alert,
}


@dag(
    dag_id=ELT_DAG_ID,
    description="NammaMart ELT: raw history kept forever, SQL staging and marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=default_args,
    doc_md=__doc__,
    tags=["nammamart", "elt", "fde", "deb"],
)
def nammamart_elt_deb():

    @task
    def extract(entity: str, **context) -> list[dict]:
        from include.nammamart import elt
        return elt.extract(entity, context["run_id"])

    @task
    def load_raw(entity: str, metas: list[dict], **context):
        from include.nammamart import elt
        return elt.load_raw(entity, metas, context["run_id"])

    @task
    def staging(step: str, **context):
        from include.nammamart import elt
        return elt.run_staging(step, context["run_id"])

    @task
    def mart(name: str, **context):
        from include.nammamart import elt
        return elt.run_mart(name, context["run_id"])

    @task
    def capture_dq(**context):
        from include.nammamart import elt
        return elt.capture_dq(context["run_id"])

    # retries=0: it fails on purpose to mark the run failed; retrying only delays the alert.
    @task(trigger_rule="all_done", retries=0)
    def audit_run(**context):
        from include.nammamart import common, elt
        dag_run = context.get("dag_run")
        return common.audit_run(elt.DAG_ID, context["run_id"], elt.EXPECTED_TASKS,
                                getattr(dag_run, "start_date", None))

    raw = [
        load_raw.override(task_id=f"load_raw_{e}")(e, extract.override(task_id=f"extract_{e}")(e))
        for e in ENTITIES
    ]
    masters = [staging.override(task_id=s)(s) for s in MASTER_STAGING]
    stg_orders = staging.override(task_id="stg_orders")("stg_orders")
    stg_payments = staging.override(task_id="stg_payments")("stg_payments")
    marts = [mart.override(task_id=m)(m) for m in MARTS]
    dq = capture_dq()

    for s in masters:          # every raw load before staging starts
        raw >> s
    masters >> stg_orders >> stg_payments
    for m in marts:            # marts run in parallel
        stg_payments >> m
        m >> dq
    dq >> audit_run()


nammamart_elt_deb()
