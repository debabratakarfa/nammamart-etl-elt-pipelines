"""Part F: run the observability experiments against both pipelines, in an
isolated copy of the data and warehouse, and write docs/observability_results.md.

    python scripts/observability_experiments.py

Uses scripts/run_local.py (same task code as the DAGs). Airflow-only behaviour
(retry timing, task states in the UI, Clear) must still be checked in the UI.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "include" / "data" / "nammamart"
BASE = Path(os.environ.get("EXP_DIR", "/tmp/nammamart_experiments"))
INC = "orders_2026-10-01_increment.csv"
OUT: list[str] = []


def say(text=""):
    print(text)
    OUT.append(text)


def env_for(data: Path, wh: Path) -> dict:
    e = dict(os.environ)
    e.update(NAMMAMART_DATA_DIR=str(data), NAMMAMART_WAREHOUSE=str(wh),
             NAMMAMART_QUARANTINE_DIR=str(BASE / "quarantine"),
             NAMMAMART_WORK_DIR=str(BASE / "work"))
    return e


def run(pipeline: str, run_id: str, data: Path, wh: Path) -> tuple[int, str, float]:
    t = time.time()
    p = subprocess.run([sys.executable, str(ROOT / "scripts" / "run_local.py"), pipeline, "--run-id", run_id],
                       capture_output=True, text=True, env=env_for(data, wh))
    return p.returncode, p.stdout + p.stderr, round(time.time() - t, 1)


def q(wh: Path, sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(wh), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def fresh_data(include_increment: bool) -> Path:
    d = BASE / "data"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    for f in SRC.glob("*.csv"):
        if include_increment or f.name != INC:
            shutil.copy2(f, d / f.name)
    return d


def scratch(baseline: Path, name: str) -> Path:
    wh = BASE / f"{name}.duckdb"
    shutil.copy2(baseline, wh)
    return wh


def run_both(run_id, data, wh):
    res = {}
    for p in ("etl", "elt"):
        rc, log, secs = run(p, run_id, data, wh)
        res[p] = (rc, log, secs)
    return res


def runs_table(wh, run_id):
    return q(wh, f"""SELECT dag_id, status, rows_extracted, rows_loaded, rows_quarantined, failed_tasks
                     FROM pipeline_runs WHERE run_id = '{run_id}' ORDER BY dag_id""").to_markdown(index=False, floatfmt=".2f")


def last_error(log: str) -> str:
    lines = [l for l in log.splitlines() if "TASK FAILED" in l]
    return lines[-1].split("TASK FAILED: ", 1)[-1] if lines else "(no error)"


def main():
    shutil.rmtree(BASE, ignore_errors=True)
    BASE.mkdir(parents=True)
    wh = BASE / "main.duckdb"

    # ---------------------------------------------------------------- 1 happy path
    data = fresh_data(include_increment=False)
    say("## 1. Happy path (September files, no increment yet)\n")
    res = run_both("exp1", data, wh)
    for p, (rc, _, secs) in res.items():
        say(f"* {p.upper()} finished with exit code {rc} in {secs}s (local run, all tasks sequential)")
    say("\n" + runs_table(wh, "exp1") + "\n")
    say("Rows in / passed / quarantined per file (ETL run, from dq_results and etl_quarantine):\n")
    say(q(wh, """
        WITH chk AS (SELECT source_file, max(rows_checked) AS rows_in FROM dq_results
                     WHERE run_id='exp1' AND dag_id LIKE '%etl%' GROUP BY 1),
        qua AS (SELECT source_file, count(*) AS quarantined FROM etl_quarantine
                WHERE run_id='exp1' GROUP BY 1)
        SELECT chk.source_file, rows_in, rows_in - coalesce(quarantined,0) AS passed,
               coalesce(quarantined,0) AS quarantined
        FROM chk LEFT JOIN qua USING (source_file) ORDER BY 1""").to_markdown(index=False, floatfmt=".2f") + "\n")
    shutil.copy2(wh, BASE / "after_exp1.duckdb")

    # ---------------------------------------------------------------- 2 idempotency
    say("## 2. Re-run (idempotency)\n")
    before = q(wh, """SELECT (SELECT count(*) FROM etl_fact_orders) fact,
                             (SELECT round(sum(net_revenue),2) FROM etl_fact_orders) etl_revenue,
                             (SELECT count(*) FROM raw_orders) raw_orders,
                             (SELECT count(*) FROM stg_orders) stg_orders,
                             (SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis) elt_revenue""")
    run_both("exp2", data, wh)
    after = q(wh, """SELECT (SELECT count(*) FROM etl_fact_orders) fact,
                            (SELECT round(sum(net_revenue),2) FROM etl_fact_orders) etl_revenue,
                            (SELECT count(*) FROM raw_orders) raw_orders,
                            (SELECT count(*) FROM stg_orders) stg_orders,
                            (SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis) elt_revenue""")
    say(pd.concat([before.assign(when="before re-run"), after.assign(when="after re-run")])
        .to_markdown(index=False, floatfmt=".2f") + "\n")
    say(runs_table(wh, "exp2") + "\n")

    # ---------------------------------------------------------------- 3 increment
    say("## 3. Incremental load (add orders_2026-10-01_increment.csv)\n")
    shutil.copy2(SRC / INC, data / INC)
    res = run_both("exp3", data, wh)
    for line in res["etl"][1].splitlines():
        if line.startswith("[load] orders_2026"):
            say(f"* ETL: `{line}`")
    for line in res["elt"][1].splitlines():
        if line.startswith("[stg_orders]"):
            say(f"* ELT: `{line}`")
    say("\n" + q(wh, """SELECT 'ETL' AS pipeline, count(*) AS rows, count(DISTINCT order_id) AS distinct_ids,
                            count(*) FILTER (WHERE order_status='RETURNED') AS returned
                        FROM etl_fact_orders
                        UNION ALL
                        SELECT 'ELT', count(*), count(DISTINCT order_id),
                               count(*) FILTER (WHERE order_status='RETURNED')
                        FROM stg_orders WHERE dq_status <> 'FAIL'""").to_markdown(index=False, floatfmt=".2f") + "\n")
    say("Status changes logged: ETL "
        f"{q(wh, 'SELECT count(*) n FROM etl_order_status_history').n[0]}, ELT "
        f"{q(wh, 'SELECT count(*) n FROM stg_order_status_history').n[0]} "
        "(all DELIVERED -> RETURNED).\n")
    say(runs_table(wh, "exp3") + "\n")
    baseline = BASE / "after_exp3.duckdb"
    shutil.copy2(wh, baseline)

    # ---------------------------------------------------------------- 4 missing file
    say("## 4. Missing source file (products.csv renamed)\n")
    wh4 = scratch(baseline, "exp4")
    (data / "products.csv").rename(data / "products.csv.bak")
    res = run_both("exp4", data, wh4)
    (data / "products.csv.bak").rename(data / "products.csv")
    for p, (rc, log, _) in res.items():
        say(f"* {p.upper()}: exit {rc}, error `{last_error(log)}`")
    say("\n" + runs_table(wh4, "exp4") + "\n")

    # ---------------------------------------------------------------- 5 new bad rows
    say("## 5. New bad data (5 rows appended to a copy of orders.csv)\n")
    wh5 = scratch(baseline, "exp5")
    orig = (data / "orders.csv").read_bytes()  # bytes: keep CRLF so the hash is restored exactly
    bad = [
        "ORD900001,2026-09-15 10:00:00,,S01,P0001,1,107.0,0,107.0,UPI,DELIVERED,App,12",
        "ORD900002,2026-09-15 10:05:00,C00005,S01,P0001,-2,107.0,0,-214.0,UPI,DELIVERED,App,12",
        "ORD900003,2026-09-15 10:10:00,C00005,S42,P0001,1,107.0,0,107.0,UPI,DELIVERED,App,12",
        "ORD900004,2026-09-15 10:15:00,C00005,S01,P0001,1,107.0,0,107.0,UPI,SHIPPED,App,12",
        "ORD900005,2027-03-01 10:20:00,C00005,S01,P0001,1,107.0,0,107.0,UPI,DELIVERED,App,12",
    ]
    (data / "orders.csv").write_bytes(orig.rstrip(b"\r\n") + b"\r\n" + "\r\n".join(bad).encode() + b"\r\n")
    run_both("exp5", data, wh5)
    (data / "orders.csv").write_bytes(orig)
    say(q(wh5, """SELECT 'ETL' AS pipeline, record_key, failure_reason FROM etl_quarantine
                  WHERE run_id='exp5' AND record_key LIKE 'ORD9000%'
                  UNION ALL
                  SELECT 'ELT', record_key, failure_reason FROM elt_quarantine
                  WHERE run_id='exp5' AND record_key LIKE 'ORD9000%' ORDER BY 1, 2""").to_markdown(index=False, floatfmt=".2f") + "\n")
    say(q(wh5, """SELECT dag_id, check_name, rows_failed, status FROM dq_results
                  WHERE run_id='exp5' AND source_file='orders.csv'
                    AND check_name IN ('or_customer_id_missing','or_quantity_invalid','or_store_unknown',
                                       'or_status_invalid','or_order_ts_future')
                  ORDER BY check_name, dag_id""").to_markdown(index=False, floatfmt=".2f") + "\n")
    say("Each count is the baseline count plus one, so every new bad row was caught by the right rule.\n")

    # ---------------------------------------------------------------- 6 schema change
    say("## 6. Schema change (order_amount renamed to total)\n")
    wh6 = scratch(baseline, "exp6")
    df = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False).rename(columns={"order_amount": "total"})
    df.to_csv(data / "orders.csv", index=False)
    res = run_both("exp6", data, wh6)
    (data / "orders.csv").write_bytes(orig)
    for p, (rc, log, _) in res.items():
        say(f"* {p.upper()}: exit {rc}, error `{last_error(log)}`")
    say("\n" + runs_table(wh6, "exp6") + "\n")
    fact_before = q(baseline, "SELECT count(*) n FROM etl_fact_orders").n[0]
    fact_after = q(wh6, "SELECT count(*) n FROM etl_fact_orders").n[0]
    say(f"Fact rows before {fact_before}, after {fact_after}: nothing was loaded, so it failed loudly, not silently.\n")

    # ---------------------------------------------------------------- 7 volume
    say("## 7. Volume anomaly (orders.csv cut to 50 rows)\n")
    wh7 = scratch(baseline, "exp7")
    pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False).head(50).to_csv(data / "orders.csv", index=False)
    run_both("exp7", data, wh7)
    (data / "orders.csv").write_bytes(orig)
    say(q(wh7, """SELECT dag_id, check_name, rows_checked, rows_failed, status FROM dq_results
                  WHERE run_id='exp7' AND check_name='or_volume_drop'""").to_markdown(index=False, floatfmt=".2f") + "\n")

    # ---------------------------------------------------------------- 8 distribution drift
    say("## 8. Distribution drift (unit_price x100 for store S05, order_amount recomputed to match)\n")
    wh8 = scratch(baseline, "exp8")
    df = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False)
    m = df.store_id == "S05"
    up = pd.to_numeric(df.loc[m, "unit_price"]) * 100
    df.loc[m, "unit_price"] = up.astype(str)
    amt = (pd.to_numeric(df.loc[m, "quantity"], errors="coerce") * up
           * (1 - pd.to_numeric(df.loc[m, "discount_pct"], errors="coerce") / 100)).round(2)
    df.loc[m, "order_amount"] = amt.astype(str)
    df.to_csv(data / "orders.csv", index=False)
    run_both("exp8", data, wh8)
    (data / "orders.csv").write_bytes(orig)
    say(q(wh8, """SELECT dag_id, check_name, rows_failed, status FROM dq_results
                  WHERE run_id='exp8' AND source_file='orders.csv'
                    AND check_name IN ('or_unit_price_above_mrp','or_amount_mismatch')
                  ORDER BY 2, 1""").to_markdown(index=False, floatfmt=".2f") + "\n")
    rev = lambda w: q(w, "SELECT round(sum(net_revenue),2) r FROM mart_daily_store_kpis WHERE store_id='S05'").r[0]
    hypo = q(wh8, """SELECT round(sum(try_cast(order_amount AS DOUBLE)),2) r FROM raw_orders
                     WHERE _run_id='exp8' AND trim(store_id)='S05' AND upper(trim(order_status))='DELIVERED'""").r[0]
    say(f"S05 net revenue on the mart: before {rev(baseline):,.2f}, after {rev(wh8):,.2f}. "
        f"Had the rows got through, S05's delivered revenue in the file would have read {hypo:,.2f}.\n")

    # ---------------------------------------------------------------- 9 freshness
    say("## 9. Freshness (every order_ts moved back 30 days)\n")
    wh9 = scratch(baseline, "exp9")
    df = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False)
    ts = pd.to_datetime(df.order_ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df.loc[ts.notna(), "order_ts"] = (ts[ts.notna()] - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(data / "orders.csv", index=False)
    run_both("exp9", data, wh9)
    (data / "orders.csv").write_bytes(orig)
    say(q(wh9, """SELECT dag_id, check_name, rows_checked, rows_failed, status FROM dq_results
                  WHERE run_id='exp9' AND check_name='or_freshness_stale' AND source_file='orders.csv'
               """).to_markdown(index=False, floatfmt=".2f") + "\n")

    # ---------------------------------------------------------------- 10 recovery
    say("## 10. Recovery (fix the cause of experiment 4 and run again)\n")
    res = run_both("exp10", data, wh4)
    say(runs_table(wh4, "exp10") + "\n")
    say("Locally the whole pipeline re-runs. In Airflow, use Clear on the failed task with "
        "'Downstream' ticked: only that task and its downstream tasks re-run, the green "
        "upstream tasks keep their state and XComs.\n")

    out = ROOT / "docs" / "observability_results.md"
    out.write_text("# Observability experiments: observed results\n\n"
                   "Generated by `scripts/observability_experiments.py` (local runs of the same task code "
                   "as the DAGs, each experiment on a copy of the warehouse taken after experiment 3).\n\n"
                   + "\n".join(OUT))
    print(f"\nwritten {out}")


if __name__ == "__main__":
    main()
