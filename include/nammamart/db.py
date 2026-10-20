"""DuckDB helpers shared by both pipelines.

DuckDB allows one writer process at a time. Parallel Airflow tasks (and the
ETL and ELT DAGs both firing at 09:00) would collide on the file lock, so
connect() waits and retries instead of failing the task.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime

import duckdb

from . import checks
from .config import WAREHOUSE_PATH, now_ist

SHARED_DDL = """
CREATE TABLE IF NOT EXISTS dq_check_catalog (
    check_name VARCHAR PRIMARY KEY, entity VARCHAR, dimension VARCHAR,
    severity VARCHAR, description VARCHAR
);
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
    severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
    checked_at TIMESTAMP, source_file VARCHAR
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
    status VARCHAR, failed_tasks VARCHAR
);
CREATE TABLE IF NOT EXISTS run_task_metrics (
    run_id VARCHAR, dag_id VARCHAR, task_id VARCHAR, metric VARCHAR,
    value DOUBLE, recorded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS load_manifest (
    dag_id VARCHAR, entity VARCHAR, source_file VARCHAR, file_hash VARCHAR,
    row_count BIGINT, snapshot_ts TIMESTAMP, run_id VARCHAR, loaded_at TIMESTAMP
);
"""


@contextmanager
def connect(path=WAREHOUSE_PATH, wait_seconds: int = 300, read_only: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + wait_seconds
    delay = 0.5
    while True:
        try:
            con = duckdb.connect(str(path), read_only=read_only)
            break
        except duckdb.IOException as exc:  # "Could not set lock on file"
            if "lock" not in str(exc).lower() or time.time() > deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 5)
    try:
        yield con
    finally:
        con.close()


def ensure_shared_tables(con) -> None:
    con.execute(SHARED_DDL)
    con.execute("DELETE FROM dq_check_catalog")
    con.executemany("INSERT INTO dq_check_catalog VALUES (?, ?, ?, ?, ?)", checks.catalog_rows())


def record_metric(con, run_id: str, dag_id: str, task_id: str, **metrics) -> None:
    now = now_ist()
    con.execute(
        "DELETE FROM run_task_metrics WHERE run_id=? AND dag_id=? AND task_id=?",
        [run_id, dag_id, task_id],
    )
    rows = [(run_id, dag_id, task_id, k, float(v), now) for k, v in metrics.items()]
    rows.append((run_id, dag_id, task_id, "done", 1.0, now))
    con.executemany("INSERT INTO run_task_metrics VALUES (?, ?, ?, ?, ?, ?)", rows)


def write_dq_results(con, run_id: str, dag_id: str, results: list[dict]) -> None:
    """results: [{check_name, rows_checked, rows_failed, source_file}]"""
    now = now_ist()
    rows = []
    for r in results:
        _, dimension, severity, _ = checks.CHECKS[r["check_name"]]
        if r["rows_checked"] == 0:
            status = "SKIPPED"
        elif r["rows_failed"] == 0:
            status = "PASS"
        else:
            status = "FAIL" if severity == "Critical" else "WARN"
        rows.append((run_id, dag_id, r["check_name"], dimension, severity,
                     int(r["rows_checked"]), int(r["rows_failed"]), status, now,
                     r.get("source_file")))
    files = sorted({r.get("source_file") for r in results})
    names = sorted({r["check_name"] for r in results})
    # Idempotent per task retry: replace this run's rows for these checks/files
    for f in files:
        con.execute(
            f"DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND source_file IS NOT DISTINCT FROM ? "
            f"AND check_name IN ({','.join('?' * len(names))})",
            [run_id, dag_id, f, *names],
        )
    if rows:
        con.executemany("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?,?)", rows)


def previous_row_count(con, dag_id: str, source_file: str):
    """Row count of the most recent earlier load of the same file name."""
    row = con.execute(
        "SELECT row_count FROM load_manifest WHERE dag_id=? AND source_file=? "
        "ORDER BY loaded_at DESC LIMIT 1",
        [dag_id, source_file],
    ).fetchone()
    return row[0] if row else None


def already_loaded(con, dag_id: str, source_file: str, file_hash: str) -> bool:
    return con.execute(
        "SELECT count(*) FROM load_manifest WHERE dag_id=? AND source_file=? AND file_hash=?",
        [dag_id, source_file, file_hash],
    ).fetchone()[0] > 0
