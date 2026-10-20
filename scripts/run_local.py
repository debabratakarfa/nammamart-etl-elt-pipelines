"""Run the ETL and/or ELT pipeline end to end without Airflow, in the same
task order as the DAGs. Handy for debugging logic before `astro dev start`.

    python scripts/run_local.py etl
    python scripts/run_local.py elt
    python scripts/run_local.py both --run-id manual_test_1
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.nammamart import common, elt, etl  # noqa: E402
from include.nammamart.config import now_ist  # noqa: E402

ENTITIES = ["orders", "payments", "customers", "products", "stores"]


def _with_audit(steps, module, run_id: str, start):
    """Mimic trigger_rule="all_done": write the pipeline_runs row even after a failure,
    then surface the original error."""
    error = None
    try:
        steps(run_id)
    except Exception as exc:
        error = exc
        print(f"[{module.DAG_ID}] TASK FAILED: {type(exc).__name__}: {exc}")
    try:
        common.audit_run(module.DAG_ID, run_id, module.EXPECTED_TASKS, start)
    except RuntimeError as exc:
        print(f"[audit] {exc}")
    if error:
        raise error


def run_etl(run_id: str):
    start = now_ist()
    _with_audit(_etl_steps, etl, run_id, start)


def _etl_steps(run_id: str):
    metas = {e: etl.extract(e, run_id) for e in ENTITIES}
    for e in ["stores", "products", "customers"]:
        etl.validate_master(e, metas[e], run_id)
    vo = etl.validate_orders(metas["orders"], run_id)
    etl.validate_payments(metas["orders"], metas["payments"], run_id)
    etl.transform(run_id, vo["files_validated"])
    etl.load(run_id, metas)
    etl.reconcile(run_id)
    etl.report(run_id)


def run_elt(run_id: str):
    start = now_ist()
    _with_audit(_elt_steps, elt, run_id, start)


def _elt_steps(run_id: str):
    metas = {e: elt.extract(e, run_id) for e in ENTITIES}
    for e in ENTITIES:
        elt.load_raw(e, metas[e], run_id)
    for step in elt.STAGING_STEPS:
        elt.run_staging(step, run_id)
    for mart in elt.MART_STEPS:
        elt.run_mart(mart, run_id)
    elt.capture_dq(run_id)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("pipeline", choices=["etl", "elt", "both"])
    ap.add_argument("--run-id", default=f"local__{datetime.now():%Y%m%dT%H%M%S}")
    a = ap.parse_args()
    if a.pipeline in ("etl", "both"):
        run_etl(a.run_id)
    if a.pipeline in ("elt", "both"):
        run_elt(a.run_id)
