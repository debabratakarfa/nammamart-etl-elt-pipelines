# NammaMart ETL & ELT pipelines (Deb)

A standalone Astro project: clone it and `astro dev start`. Built and tested on
Astro Runtime 3.3-2 (Airflow 3.3.0, Python 3.14, pandas 2.3.3, DuckDB 1.5.6).

```
Dockerfile, packages.txt, requirements.txt, airflow_settings.yaml, .astro/   Astro scaffolding
dags/nammamart_etl_deb.py, dags/nammamart_elt_deb.py                          the two DAGs
include/nammamart/          (Python package: config, checks, validate, etl, elt, ...)
include/sql/elt/            (all ELT SQL)
include/data/nammamart/     (the six CSVs, unedited)
scripts/                    (local runner, profiling, parity check, experiments)
docs/                       (profile, issues log, business questions, experiment results, screenshots)
SUBMISSION.md
```

`.gitignore` keeps the warehouse, quarantine files (raw customer PII), run scratch space and `.env` out of git.
To use another name, set `LEARNER_NAME` in `include/nammamart/config.py` and rename the two DAG files to match.

## 1. Run in Airflow

```bash
astro dev start
astro dev run dags list-import-errors        # should be empty
astro dev run dags test nammamart_etl_deb
astro dev run dags test nammamart_elt_deb
```

UI: the URL `astro dev start` prints (http://localhost:8080 by default). If port 8080 or 5432 is taken by
another project, run `astro config set webserver.port 8081` and `astro config set postgres.port 5433` first.
Both DAGs are scheduled for `0 9 * * 1-6` in Asia/Kolkata (03:30 UTC) and start paused; unpause them in the UI.

The warehouse is `include/warehouse_deb.duckdb`. Query it with VS Code SQLTools, or:

```bash
python -c "import duckdb; print(duckdb.connect('include/warehouse_deb.duckdb', read_only=True).sql('select * from pipeline_runs'))"
```

Close any open SQLTools connection before a DAG run: DuckDB allows one writer at a time. The tasks wait for the lock for up to 5 minutes, then fail.

### Experiment 3 (incremental load) in the UI

Both DAGs pick up `orders_*_increment.csv` automatically. To see the increment arrive as a separate run, follow these steps:

1. Move `orders_2026-10-01_increment.csv` out of `include/data/nammamart/`.
2. Delete `include/warehouse_deb.duckdb`.
3. Trigger both DAGs.
4. Put the increment file back.
5. Trigger both DAGs again.

### Optional Slack alert (bonus)

```bash
astro dev run variables set SLACK_WEBHOOK_URL https://hooks.slack.com/services/XXX
```

## 2. Run without Airflow (same task code)

```bash
python scripts/run_local.py both            # ETL then ELT
python scripts/compare_etl_elt.py           # proves ETL and ELT agree
python scripts/profile_data.py              # docs/data_profile.md + docs/data_issues_log.csv
python scripts/business_questions.py        # docs/business_questions.md
python scripts/observability_experiments.py # docs/observability_results.md (uses /tmp copies)
```

## How the code is laid out

| File | Role |
|---|---|
| `include/nammamart/config.py` | Paths, file specs, business rules, snapshot dates, IST time helpers |
| `include/nammamart/checks.py` | The single data quality catalog (54 checks, dimension and severity) |
| `include/nammamart/validate.py` | ETL rules in pandas |
| `include/nammamart/etl.py` | ETL tasks: validate, transform, load (upsert), reconcile, report |
| `include/nammamart/elt.py` | ELT tasks: load raw, run staging and mart SQL, capture dq |
| `include/nammamart/common.py` | Shared extract (landing + hash + schema check), quarantine, run audit |
| `include/nammamart/db.py` | DuckDB connection with lock wait; shared tables (`dq_results`, `pipeline_runs`, ...) |
| `include/nammamart/masking.py`, `alerts.py` | PII masking; Slack failure callback |
| `include/sql/elt/` | `00_ddl`, `load_raw`, `stg_*`, `stg_orders_evaluate` / `_merge`, `mart_*`, `quarantine`, `dq_results` |
