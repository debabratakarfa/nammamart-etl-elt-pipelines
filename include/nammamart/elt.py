"""ELT pipeline: Extract -> Load raw (as received) -> Transform in DuckDB with SQL.

Every transformation lives in include/sql/elt/*.sql. Python only renders
parameters into the SQL and runs it.
"""
from __future__ import annotations

from datetime import datetime

from . import common, db
from .config import ELT_DAG_ID, SOURCES, SQL_DIR, now_ist

DAG_ID = ELT_DAG_ID
ENTITIES = ["orders", "payments", "customers", "products", "stores"]
STAGING_STEPS = ["stg_stores", "stg_products", "stg_customers", "stg_orders", "stg_payments"]
MART_STEPS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
              "mart_payment_reconciliation", "mart_top_customers"]
EXPECTED_TASKS = (
    [f"extract_{e}" for e in ENTITIES] + [f"load_raw_{e}" for e in ENTITIES]
    + STAGING_STEPS + MART_STEPS + ["capture_dq"]
)
ELT_SQL = SQL_DIR / "elt"


# ------------------------------------------------------------------ SQL helpers
def lit(value) -> str:
    """Render a Python value as a SQL literal (values come from our own config/run ids)."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        value = value.isoformat(sep=" ")
    return "'" + str(value).replace("'", "''") + "'"


def render(name: str, raw: dict | None = None, **params) -> str:
    """Load include/sql/elt/<name>.sql and fill {{param}} placeholders.
    params are rendered as SQL literals; raw values are inserted as-is
    (identifiers and SQL fragments built from our own config)."""
    sql = (ELT_SQL / f"{name}.sql").read_text()
    for k, v in (raw or {}).items():
        sql = sql.replace("{{" + k + "}}", v)
    for k, v in params.items():
        sql = sql.replace("{{" + k + "}}", lit(v))
    if "{{" in sql:
        raise ValueError(f"Unfilled parameter in {name}.sql")
    return sql


def run_sql(con, name: str, raw: dict | None = None, **params) -> None:
    print(f"[sql] running {name}.sql")
    con.execute(render(name, raw, **params))


def _setup(con):
    db.ensure_shared_tables(con)
    run_sql(con, "00_ddl")


# ------------------------------------------------------------------ extract + load raw
def extract(entity: str, run_id: str) -> list[dict]:
    # Raw is append-only: a file whose exact bytes were already loaded is skipped,
    # so re-runs don't pile up identical copies (raw grows only when data changes).
    return common.extract(entity, DAG_ID, run_id, skip_unchanged=True)


def load_raw(entity: str, metas: list[dict], run_id: str) -> dict:
    cols = ", ".join(f'"{c}"' for c in SOURCES[entity]["columns"])
    loaded = 0
    now = now_ist()
    with db.connect() as con:
        _setup(con)
        con.execute("SET threads = 1")  # deterministic _source_row = file line order
        for m in metas:
            if m["skip"]:
                print(f"[load_raw] {m['file']} unchanged since last load, skipped")
                continue
            run_sql(con, "load_raw", raw={"entity": entity, "columns": cols},
                    path=m["path"], source_file=m["file"], snapshot_ts=m["snapshot_ts"],
                    file_hash=m["hash"], run_id=run_id, loaded_at=now)
            n = con.execute(f"SELECT count(*) FROM raw_{entity} WHERE _run_id=? AND _source_file=?",
                            [run_id, m["file"]]).fetchone()[0]
            loaded += n
            con.execute("DELETE FROM elt_batches WHERE run_id=? AND source_file=?", [run_id, m["file"]])
            con.execute("INSERT INTO elt_batches VALUES (?,?,?,?,?,?,?)",
                        [run_id, entity, m["file"], n, m["previous_rows"], m["snapshot_ts"], now])
            con.execute("DELETE FROM load_manifest WHERE dag_id=? AND source_file=? AND run_id=?",
                        [DAG_ID, m["file"], run_id])
            con.execute("INSERT INTO load_manifest VALUES (?,?,?,?,?,?,?,?)",
                        [DAG_ID, entity, m["file"], m["hash"], n, m["snapshot_ts"], run_id, now])
            print(f"[load_raw] raw_{entity} += {n} rows from {m['file']}")
        total = con.execute(f"SELECT count(*) FROM raw_{entity}").fetchone()[0]
        db.record_metric(con, run_id, DAG_ID, f"load_raw_{entity}", rows_loaded=loaded)
    print(f"[load_raw] raw_{entity} now holds {total} rows")
    return {"appended": loaded, "total": total}


# ------------------------------------------------------------------ staging
def _quarantine(con, entity: str, stg_table: str, run_id: str, batch_filter: str = "") -> int:
    struct = "{" + ", ".join(f"'{c}': r.\"{c}\"" for c in SOURCES[entity]["columns"]) + "}"
    run_sql(con, "quarantine",
            raw={"entity": entity, "key": SOURCES[entity]["key"], "raw_struct": struct,
                 "stg_table": stg_table, "batch_filter": batch_filter},
            run_id=run_id, dag_id=DAG_ID, now=now_ist())
    return con.execute("SELECT count(*) FROM elt_quarantine WHERE run_id=? AND dag_id=? AND entity=?",
                       [run_id, DAG_ID, entity]).fetchone()[0]


def run_staging(step: str, run_id: str) -> dict:
    with db.connect() as con:
        _setup(con)
        if step == "stg_orders":
            files = con.execute(
                "SELECT DISTINCT _source_file, _snapshot_ts FROM raw_orders WHERE _run_id=? "
                "ORDER BY _snapshot_ts, _source_file", [run_id]).fetchall()
            if not files:
                print("[stg_orders] no new orders batch in this run, nothing to merge")
            for f, _ in files:  # oldest snapshot first, so corrections land last
                before = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
                run_sql(con, "stg_orders_evaluate", run_id=run_id, source_file=f)
                run_sql(con, "stg_orders_merge", run_id=run_id, source_file=f, merged_at=now_ist())
                after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
                stats = con.execute(
                    "SELECT dq_status, count(*) FROM stg_order_batches WHERE _run_id=? AND _source_file=? "
                    "GROUP BY 1 ORDER BY 1", [run_id, f]).fetchall()
                print(f"[stg_orders] {f}: {dict(stats)}; stg_orders {before} -> {after} rows")
            nq = _quarantine(con, "orders", "stg_order_batches", run_id,
                             batch_filter=f"AND s._run_id = {lit(run_id)}")
        else:
            run_sql(con, step)
            entity = step.replace("stg_", "")
            nq = _quarantine(con, entity, step, run_id)
            stats = con.execute(f"SELECT dq_status, count(*) FROM {step} GROUP BY 1 ORDER BY 1").fetchall()
            print(f"[{step}] {dict(stats)}")
        db.record_metric(con, run_id, DAG_ID, step, rows_quarantined=nq)
    return {"step": step, "quarantined": nq}


# ------------------------------------------------------------------ marts + dq
def run_mart(mart: str, run_id: str) -> dict:
    with db.connect() as con:
        _setup(con)
        if mart == "mart_payment_reconciliation":
            run_sql(con, mart, run_id=run_id)
        else:
            run_sql(con, mart)
        n = con.execute(f"SELECT count(*) FROM {mart}").fetchone()[0]
        preview = con.execute(f"SELECT * FROM {mart} LIMIT 12").fetchdf()
        db.record_metric(con, run_id, DAG_ID, mart)
    print(f"[{mart}] {n} rows\n{preview.to_string(index=False)}")
    return {"mart": mart, "rows": n}


def capture_dq(run_id: str) -> dict:
    with db.connect() as con:
        _setup(con)
        run_sql(con, "dq_results", run_id=run_id, dag_id=DAG_ID, now=now_ist())
        run_sql(con, "mart_rejections", run_id=run_id, dag_id=DAG_ID)
        summary = con.execute(
            "SELECT status, count(*) FROM dq_results WHERE run_id=? AND dag_id=? GROUP BY 1",
            [run_id, DAG_ID]).fetchall()
        db.record_metric(con, run_id, DAG_ID, "capture_dq")
    print(f"[capture_dq] check outcomes: {dict(summary)}")
    return dict(summary)
