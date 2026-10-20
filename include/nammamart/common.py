"""Steps shared by both pipelines: landing the source files, writing
quarantine rows, and the end-of-run audit."""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime

import pandas as pd

from . import db
from .config import (QUARANTINE_DIR, SOURCES, now_ist, run_dir, safe_run_id, snapshot_ts,
                     source_files, to_ist_naive)

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id VARCHAR, dag_id VARCHAR, entity VARCHAR, source_file VARCHAR,
    source_row INTEGER, record_key VARCHAR, failure_reason VARCHAR,
    raw_record JSON, quarantined_at TIMESTAMP
);
"""


def md5(path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def extract(entity: str, dag_id: str, run_id: str, skip_unchanged: bool) -> list[dict]:
    """Copy each source file into a run-scoped landing folder and fingerprint it.

    The landing copy freezes what this run saw, so a file being overwritten
    mid-run cannot produce half-old, half-new numbers. Files whose hash was
    already loaded are marked skip=True when skip_unchanged (orders feed only).
    """
    landing = run_dir(dag_id, run_id) / "landing"
    landing.mkdir(exist_ok=True)
    out, rows_new = [], 0
    with db.connect() as con:
        db.ensure_shared_tables(con)
        for src in source_files(entity):
            dst = landing / src.name
            shutil.copy2(src, dst)
            header = pd.read_csv(dst, nrows=0).columns.tolist()
            missing = [c for c in SOURCES[entity]["columns"] if c not in header]
            if missing:
                raise ValueError(f"Schema change in {src.name}: missing columns {missing}; got {header}")
            n = sum(1 for _ in open(dst, "rb")) - 1
            fhash = md5(dst)
            skip = skip_unchanged and db.already_loaded(con, dag_id, src.name, fhash)
            rows_new += 0 if skip else n
            out.append({
                "entity": entity, "file": src.name, "path": str(dst), "hash": fhash,
                "rows": n, "snapshot_ts": snapshot_ts(src.name).isoformat(sep=" "),
                "skip": skip,
                "previous_rows": db.previous_row_count(con, dag_id, src.name),
            })
            print(f"[extract] {src.name}: {n} rows, md5={fhash[:8]}, "
                  f"{'UNCHANGED, will skip' if skip else 'will process'}")
        db.record_metric(con, run_id, dag_id, f"extract_{entity}", rows_extracted=rows_new)
    return out


def quarantine(con, table: str, dag_id: str, run_id: str, entity: str,
               evaluated: pd.DataFrame, raw_columns: list[str], write_csv: bool) -> int:
    """Persist FAIL rows exactly as received, with the reason. Returns row count."""
    con.execute(QUARANTINE_DDL.format(table=table))
    failed = evaluated[evaluated.dq_status == "FAIL"]
    files = evaluated["_source_file"].unique().tolist()
    for f in files:
        con.execute(f"DELETE FROM {table} WHERE run_id=? AND dag_id=? AND source_file=?",
                    [run_id, dag_id, f])
    if failed.empty:
        return 0
    key = SOURCES[entity]["key"]
    now = now_ist()
    rows = []
    for _, r in failed.iterrows():
        raw = {c: r[f"_raw_{c}"] for c in raw_columns}
        rows.append((run_id, dag_id, entity, r["_source_file"], int(r["_source_row"]),
                     raw[key], r["failure_reason"], json.dumps(raw), now))
    con.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?)", rows)

    if write_csv:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        for f, part in failed.groupby("_source_file"):
            out = part[[f"_raw_{c}" for c in raw_columns]].copy()
            out.columns = raw_columns
            out.insert(0, "source_row", part["_source_row"].values)
            out["failure_reason"] = part["failure_reason"].values
            stem = f.rsplit(".", 1)[0]
            path = QUARANTINE_DIR / f"{stem}_failed_{safe_run_id(run_id)}.csv"
            out.to_csv(path, index=False)
            print(f"[quarantine] {len(out)} rows -> {path}")
    return len(failed)


def keep_raw_copy(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Keep the as-received text next to the cleaned columns (for quarantine)."""
    df = df.copy()
    for c in columns:
        df[f"_raw_{c}"] = df[c]
    return df


def audit_run(dag_id: str, run_id: str, expected_tasks: list[str], start_time) -> dict:
    """Write one row to pipeline_runs. Raises if any task did not finish, so the
    DAG run is marked failed even though this task uses trigger_rule=all_done."""
    with db.connect() as con:
        db.ensure_shared_tables(con)
        metrics = con.execute(
            "SELECT task_id, metric, value FROM run_task_metrics WHERE run_id=? AND dag_id=?",
            [run_id, dag_id],
        ).fetchdf()
        done = set(metrics.loc[metrics.metric == "done", "task_id"])
        failed = [t for t in expected_tasks if t not in done]
        total = lambda m: int(metrics.loc[metrics.metric == m, "value"].sum())
        row = {
            "run_id": run_id, "dag_id": dag_id,
            "start_time": to_ist_naive(start_time) or now_ist(), "end_time": now_ist(),
            "rows_extracted": total("rows_extracted"), "rows_loaded": total("rows_loaded"),
            "rows_quarantined": total("rows_quarantined"),
            "status": "failed" if failed else "success",
            "failed_tasks": ", ".join(failed) or None,
        }
        con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?)", list(row.values()))
    print(f"[audit] {json.dumps(row, default=str)}")
    if failed:
        raise RuntimeError(f"Run incomplete. Tasks that did not finish: {failed}")
    return {k: (str(v) if v is not None else "") for k, v in row.items()}  # XCom-safe
