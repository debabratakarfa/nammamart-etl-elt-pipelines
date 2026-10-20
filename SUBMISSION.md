# NammaMart ETL & ELT: Submission

**FDE:** Debabrata Karfa (Deb) · FDAIE Module 2 · Cohort 5

| Item | Where |
|---|---|
| ETL DAG | `dags/nammamart_etl_deb.py` (logic in `include/nammamart/etl.py`, `validate.py`) |
| ELT DAG | `dags/nammamart_elt_deb.py` (SQL in `include/sql/elt/*.sql`, runner in `include/nammamart/elt.py`) |
| Quality rules (single catalog) | `include/nammamart/checks.py` |
| PII masking | `include/nammamart/masking.py` and `include/sql/elt/stg_customers.sql` |
| Data profile + Data Issues Log | `docs/data_profile.md`, `docs/data_issues_log.csv` (242 row-level lines) |
| Business questions (SQL + results, both pipelines) | `docs/business_questions.md` |
| Observability experiments (observed output) | `docs/observability_results.md` |
| ETL vs ELT parity proof | `python scripts/compare_etl_elt.py` |
| Screenshots | `docs/screenshots/` (see checklist at the end) |

Both DAGs parse with no import errors and complete green in the Airflow UI on **Astro Runtime 3.3-2** (Airflow 3.3.0+astro.2, Python 3.14.6, pandas 2.3.3, DuckDB 1.5.6), which is the `Dockerfile` in this repo. The code also supports Airflow 2.x imports.

---

## Headline results

| Metric | ETL | ELT |
|---|---|---|
| September net revenue (delivered, after discount, after 1 Oct corrections) | **₹11,18,449.50** | **₹11,18,449.50** |
| Clean orders in reporting tables | 2,431 | 2,431 |
| Rows quarantined in first run (all files) | 171 | 171 |
| Order/payment mismatches | 213 cases, ₹1,48,310.35 | 213 cases, ₹1,48,310.35 |
| Status corrections applied by the increment | 10 (DELIVERED → RETURNED) | 10 |

`scripts/compare_etl_elt.py` compares nine outputs row by row: fact rows, daily zone revenue, category margin, reconciliation, masked customers, status history, quarantine and `dq_results`. **All nine match.** Part B.4 is therefore answered: the mart totals equal the ETL totals, because both pipelines implement the same 54 rules from one catalog, with the same reason codes.

---

## Part 0: Discovery and data profiling

Full profile (row counts, empties, distinct values, min/max, referential integrity) is in `docs/data_profile.md`. The row-level log (file, source row, row key, column, value received, problem, dimension, rule, severity, action) is `docs/data_issues_log.csv`.

### Issues found, by file

| File | Issue | Rows | Dimension | Proposed rule | Severity |
|---|---|---|---|---|---|
| orders.csv | Exact duplicate rows (same `order_id`, all columns equal) | 12 | Uniqueness | Keep first, quarantine copies | Critical |
| orders.csv | `customer_id` empty | 18 | Completeness | Must be present | Critical |
| orders.csv | `order_ts` unparseable (`N/A`, blank, `2026-13-05`, `31/09/2026 10:00`) | 6 | Validity | Strict `YYYY-MM-DD HH:MM:SS` | Critical |
| orders.csv | `order_ts` after the export (21 Oct, 2 Nov 2026, 1 Jan 2027) | 7 | Timeliness | Not after file snapshot | Critical |
| orders.csv | `order_status` = `Pending??` | 3 | Validity | In DELIVERED/CANCELLED/RETURNED | Critical |
| orders.csv | `order_status` = `delivered` | 2 | Consistency | Upper-case, log | Warning |
| orders.csv | `payment_mode` variants `upi`, `UPI `, `CARD`, `cod`, `Cashh` | 8 | Consistency | Map to allowed list, log | Warning |
| orders.csv | `quantity` 0 or negative | 10 | Accuracy | quantity > 0 | Critical |
| orders.csv | `discount_pct` 120, 150, 250 | 3 | Validity | 0 to 100 | Critical |
| orders.csv | `order_amount` ≠ quantity × price × (1 − discount) (several exactly 10×) | 18 | Accuracy | Match within ₹0.01 | Critical |
| orders.csv | `store_id` S00, S17, S99 not in store master | 5 | Consistency | Must exist in clean master | Critical |
| orders.csv | `product_id` P9xxx not in catalogue | 9 | Consistency | Must exist in clean master | Critical |
| orders.csv | DELIVERED with no `delivery_minutes` | 8 | Completeness | Required when DELIVERED | Critical |
| orders.csv | `delivery_minutes` 480, 900, 1,440 | 6 | Accuracy | ≤ 120 | Critical |
| orders.csv | Orders for a quarantined master row (3 customers, P0109) | 29 | Consistency | Cascade from master | Critical |
| payments.csv | Exact duplicate payments | 4 | Uniqueness | Keep first | Critical |
| payments.csv | `order_id` ORD9xxxxx never seen in any orders file | 8 | Consistency | Must match an order | Critical |
| payments.csv | `amount_paid` zero or negative | 10 | Accuracy | > 0 for SUCCESS | Critical |
| payments.csv | `payment_ts` in the future | 7 | Timeliness | Not after snapshot | Critical |
| payments.csv | Mode casing/spelling variants | 8 | Consistency | Map, log | Warning |
| payments.csv | SUCCESS amount short of the order (₹10 to ₹100) | 13 | Accuracy (reconciliation) | Reported in reconciliation | n/a |
| payments.csv | DELIVERED orders with FAILED payment / no payment | 42 / 27 | Consistency (reconciliation) | Reported in reconciliation | n/a |
| customers.csv | Same `customer_id` twice with different phone and area | 5 | Uniqueness | Keep first, quarantine copy | Critical |
| customers.csv | `signup_date` in the future (Nov 2026 to Jan 2027) | 3 | Timeliness | Not after snapshot | Critical |
| customers.csv | Email `N/A`, missing `@` or TLD | 12 | Validity | Email format | Warning |
| customers.csv | Phone 7 to 9 digits | 7 | Validity | 10 digits, starts 6-9 | Warning |
| customers.csv | Pincode `5600XX`, `000000` | 5 | Validity | 6 digits, not starting 0 | Warning |
| customers.csv | City empty | 6 | Completeness | Present | Warning |
| customers.csv | `loyalty_tier` as `BRONZE`, `bronze`, `SILVER` | 8 | Consistency | Standardise casing | Warning |
| products.csv | `product_id` P0030 and P0140 twice (one row named "(old)") | 2 | Uniqueness | Keep current, quarantine "(old)" | Critical |
| products.csv | P0109 `mrp` = −203 | 1 | Accuracy | mrp > 0 | Critical |
| products.csv | `cost_price` > `mrp` (4 of them exactly 1.2 × MRP) | 5 | Accuracy | Log, margin will be negative | Warning |
| products.csv | `category` empty (P0051, P0060, P0068) | 3 | Completeness | Load as "Uncategorised", log | Warning |
| stores.csv | S07 `zone` empty | 1 | Completeness | Load as "Unassigned", log | Warning |
| stores.csv | S12 `capacity_orders_per_day` = 0 | 1 | Accuracy | > 0, log | Warning |
| increment | 150 new orders, 10 status corrections to RETURNED; 2 rows reference quarantined masters | 2 | Consistency | Same rules as orders | Critical |

**Severity principle.** A master row (customer, product, store) is rejected only when a key or a value that feeds a finance number is wrong. Descriptive gaps (zone, category, email) are warnings. Rejecting S07 for a missing zone would silently remove 149 real deliveries from revenue.

### Questions for the client before building

1. **Negative and zero quantities (10 orders, each with a matching negative payment):** are these returns recorded as negative orders, or data-entry errors? If they are returns, they need their own flow, not the order feed.
2. **Three customers with signup dates in 2026-11 to 2027-01:** a CRM typo, or test accounts? Today their 18 real orders are quarantined by cascade (₹ in quarantine, not in revenue). If the date is just wrong, we would rather null the date and keep the orders.
3. **P0109 has MRP −203 and cost ₹162.50, and sells at ₹203:** is it a sign error we may correct to 203? 11 orders are held back because of it.
4. **42 DELIVERED orders with a FAILED payment:** was the money collected some other way (cash on retry), or is this lost revenue? Finance needs to own this rule.
5. **S07 (BTM Layout) has no zone:** confirm it is South so the zone report is complete.

---

## Part A: ETL pipeline

```
extract_orders ─┐                    ┌─ validate_stores ──┐
extract_payments│  (5 in parallel)   ├─ validate_products ┼─ validate_orders ─ validate_payments ─┐
extract_customers ┼─────────────────►└─ validate_customers┘        │                             │
extract_products│                                                  transform ─ load ─────────────┴─ reconcile ─ report ─ audit_run (all_done)
extract_stores ─┘
```

* **Extract:** each file is copied to a run-scoped landing folder and MD5-hashed. The header is checked against the expected schema, and a missing column fails the task loudly. Orders files already loaded with the same hash are skipped (incremental feed).
* **Validate:** pandas rules from the shared catalog. Every row gets `dq_status` (PASS/WARN/FAIL), `failure_reason` and `warning_reason`. Orders are validated against the *clean* masters, so a bad master row cannot be joined into revenue.
* **Quarantine:** `include/quarantine/deb/<file>_failed_<run_id>.csv`, exactly as received, plus `source_row` and `failure_reason`. The same rows go to the `etl_quarantine` table with the raw record as JSON.
* **Transform:** trim, case-fix, map payment modes, dedupe masters, mask PII, join orders to product cost. Derives `order_date`, `net_revenue` (order_amount if DELIVERED else 0), `cost_amount`, `gross_margin` and `within_15_min`.
* **Load:** star schema `etl_fact_orders` + `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`.
  * Dimensions are replaced atomically.
  * The fact is upserted on `order_id` with `ON CONFLICT ... WHERE excluded.snapshot_ts >= snapshot_ts`, so the newer file wins.
  * Status changes are logged in `etl_order_status_history`.
* **Reconcile:** `etl_payment_reconciliation`, one row per mismatch with `mismatch_reason` and `amount_difference` (expected minus collected). Orders placed after the payment export are excluded as a timing difference.
* **Report:** `etl_daily_zone_revenue` and `etl_category_margin`, printed in the task log.

## Part B: ELT pipeline

| Layer | Tables | Built by |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | `load_raw.sql`: DuckDB `read_csv(all_varchar=true)` with a null sentinel so even empty cells stay `''`. Adds `_source_file`, `_source_row`, `_snapshot_ts`, `_file_hash`, `_run_id`, `_loaded_at`. Never cleaned, never deleted. |
| Staging | `stg_stores`, `stg_products`, `stg_customers`, `stg_payments`, `stg_order_batches`, `stg_orders` | `stg_*.sql`: cast, standardise, dedupe, mask PII, flag with `failed_checks`, `dq_status`, `failure_reason`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` (+ `mart_rejections`) | `mart_*.sql`: read only `dq_status <> 'FAIL'`. |

**Incremental MERGE.** `stg_orders_evaluate.sql` evaluates each new orders file. `stg_orders_merge.sql` then upserts it in snapshot order with three rules:

* The newer snapshot wins.
* A failing row never overwrites a good one.
* Duplicates within a file never merge.

Observed on the increment: **148 inserted, 10 updated, 2 rejected, 0 duplicates** (2,431 rows = 2,431 distinct ids).

Raw rows are kept as received, including the bad ones. `elt_quarantine` stores each rejected row's raw JSON by joining staging back to raw on `(_loaded_at, _source_file, _source_row)`.

---

## Part C: Scheduling

```python
schedule="0 9 * * 1-6",
start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
catchup=False, max_active_runs=1, dagrun_timeout=timedelta(minutes=25),
default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True,
              "max_retry_delay": timedelta(minutes=8), "execution_timeout": timedelta(minutes=10),
              "on_failure_callback": on_failure_alert}
```

* **Cron expression:** `0 9 * * 1-6` means minute 0, hour 9, Monday (1) to Saturday (6). Sunday is skipped.
* **What the UI shows in UTC:** **03:30 UTC**. IST is UTC+5:30 with no daylight saving. I verified this with the DAG's timetable: the next runs are Tue to Sat at 03:30 UTC, then Mon at 03:30 UTC, with no Sunday run. In the Airflow 3 UI the DAG header shows Next Run 03:30:00 with the UI clock set to UTC, and the scheduled run that fired was `scheduled__…T03:30:00+00:00`.
* **Why the Details tab says "Timezone: UTC":** that field is a display quirk of the Airflow 3 Details panel, not the DAG's real timezone. Inside the scheduler, `dag.timezone` and `dag.timetable.timezone` both return `Asia/Kolkata`, which is why the run fires at 03:30 UTC and not 09:00 UTC.
* **If `start_date` had been UTC:** the same cron would fire at 09:00 UTC = **14:30 IST**, five hours after the review it is meant to feed. The weekday filter would also follow UTC days. A tz-aware `start_date` is what makes Airflow read the cron in IST.
* **If the laptop or Docker is off at 9 AM:** nothing runs, and no alert fires, because the scheduler that would send it is down. When Docker comes back, `catchup=False` makes Airflow create **one** run for the most recent missed slot (today 09:00) straight away and skip older ones. I confirmed this in `CronTriggerTimetable`, and it happened for real during testing: Docker was off at 09:00 IST on 20 Oct, and when I started it the scheduler immediately created `scheduled__2026-10-20T03:30:00+00:00` for both DAGs. The ETL run started at 13:23 IST, four hours late, and no older runs were created (screenshot 04, `pipeline_runs`). The numbers arrive late. Production needs an always-on scheduler (Astro, or a server), plus an external heartbeat check that alerts when no `pipeline_runs` row exists by 09:15.
* **Why `catchup=False`:** each run reads whatever files are in the folder, not a partition for its logical date. Backfilling 15 past runs would re-process identical files 15 times and prove nothing. It would also flood the scheduler right after deployment.
* **Retries, retry delay and timeouts:** two retries with a 2-minute base delay and exponential backoff cover transient causes, such as a file still being copied or a DuckDB lock held by the other DAG. Airflow adds jitter to exponential backoff, so the observed gaps in experiment 4 were 3 min 10 s then 4 min 35 s (ETL) and 2 min 2 s then 7 min 56 s (ELT), not exactly 2 and 4. `execution_timeout` (10 min) stops a hung task; normal tasks take seconds. `dagrun_timeout` (25 min) guarantees a failed state and an alert before the 9:30 stand-up. `audit_run` has `retries=0`: it fails on purpose, and in the first experiment 4 run its inherited retries stretched the run to 18 min 44 s. With the fix the ETL run reached `failed` in 8 min 5 s, the retry waits of `extract_products` alone.
* **A contradiction to flag to Sameer:** acceptance criterion 1 says data ready *before* 9:00, but Part C asks for a 9:00 schedule. Runs take under 30 seconds, so data is ready about 9:01. I built 9:00 as asked and recommend 08:30, which leaves time for one retry cycle and a human look before the review.

---

## Part D: Data quality dimensions

All checks live in `include/nammamart/checks.py` (54 checks). They are implemented in pandas (`validate.py`) and SQL (`stg_*.sql`), with identical reason codes. Results are written every run to `dq_results`: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`, plus `source_file`. The check catalog itself is in `dq_check_catalog`.

| Dimension | Example checks (file) | Severity | First run result |
|---|---|---|---|
| Completeness | `or_customer_id_missing`, `or_delivery_missing` (orders); `pr_category_missing` (products); `st_zone_missing` (stores); `cu_city_missing` (customers) | Critical / Warning | 18, 8 / 3, 1, 6 |
| Uniqueness | `or_order_id_duplicate`, `pa_payment_id_duplicate`, `cu_customer_id_duplicate`, `pr_product_id_duplicate` | Critical | 12, 4, 5, 2 |
| Validity | `or_status_invalid`, `or_order_ts_invalid`, `or_discount_invalid` / `cu_email_invalid`, `cu_phone_invalid`, `cu_pincode_invalid`, `st_pincode_invalid` | Critical / Warning | 3, 6, 3 / 12, 7, 5, 0 |
| Accuracy | `or_quantity_invalid`, `or_amount_mismatch`, `or_delivery_implausible` (>120), `or_unit_price_above_mrp`, `pr_mrp_invalid`, `pa_amount_invalid` / `pr_cost_above_mrp` | Critical / Warning | 10, 18, 6, 0, 1, 10 / 5 |
| Consistency | `or_store_unknown`, `or_product_unknown`, `or_customer_unknown`, `pa_order_unknown` / `cu_loyalty_tier_casing`, `or_payment_mode_nonstandard` | Critical / Warning | 5, 20, 18, 8 / 8, 8 |
| Timeliness | `or_order_ts_future`, `pa_payment_ts_future`, `cu_signup_date_future` / `or_freshness_stale` | Critical / Warning | 7, 7, 3 / 0 |
| Volume (extra) | `or_volume_drop` (> 90% fewer rows than last load of the file) | Warning | 0 |

"Future" is measured against the file's **snapshot time** (1 Oct 00:00 for the September exports; the day after the date in an increment file's name). That keeps results reproducible whenever the DAG runs. It can be overridden with the Airflow Variable `nammamart_base_snapshot`.

```sql
-- Quality scorecard over time
SELECT run_id, dag_id, dimension, sum(rows_failed) AS failed, count(*) FILTER (WHERE status='FAIL') AS failing_checks
FROM dq_results GROUP BY ALL ORDER BY run_id, dag_id, dimension;
```

---

## Part E: Data governance

**1. Ownership.**

| Dataset | Owner |
|---|---|
| orders | Operations (COO Kavya's team, owners of the order management system) |
| payments | Finance (CFO) |
| customers | CRM / marketing, with the Data Protection Officer as approver for anything personal |
| products | Category / catalogue manager |
| stores | Operations |
| warehouse tables | The data team (FDE now, a NammaMart data owner later) as technical custodian; each mart has a business owner: KPIs to Ops, reconciliation and margin to Finance |

A change to a quality rule such as the 120-minute delivery limit is proposed as a pull request on `checks.py` and the SQL. It is approved by the owner of the KPI it affects (COO for delivery). If the rule changes which rows reach revenue, Finance co-signs.

**2. Data dictionary (reporting tables).**

| Table.column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| etl_fact_orders.order_id | VARCHAR (PK) | Order identifier | orders.order_id | `ORD` + 6 digits, unique | No |
| .order_ts / .order_date | TIMESTAMP / DATE | When the order was placed (IST) | orders.order_ts | ≤ file snapshot | No |
| .customer_id / .store_id / .product_id | VARCHAR (FK) | Keys to dimensions | orders | Must exist in clean dims | Pseudonymous id |
| .quantity | INTEGER | Units | orders.quantity | > 0 | No |
| .unit_price / .discount_pct | DOUBLE | Selling price; discount % | orders | price ≤ MRP; 0-100 | No |
| .order_amount | DOUBLE | quantity × price × (1 − discount/100) | orders | Matches formula ±0.01 | No |
| .payment_mode | VARCHAR | Standardised mode | orders | UPI, Card, Cash on Delivery, Wallet | No |
| .order_status | VARCHAR | Current status (latest snapshot) | orders + increments | DELIVERED, CANCELLED, RETURNED | No |
| .delivery_minutes / .within_15_min | INTEGER / BOOLEAN | Delivery time; SLA flag | orders | 1-120; NULL unless delivered | No |
| .net_revenue | DOUBLE | order_amount if DELIVERED else 0 | derived | ≥ 0 | No |
| .cost_amount / .gross_margin | DOUBLE | quantity × cost_price; net_revenue − cost (delivered only) | derived with products | | No |
| .dq_warnings / .source_file / .source_row / .snapshot_ts / .run_id | | Lineage and warnings | pipeline | | No |
| etl_dim_customer.customer_id | VARCHAR (PK) | Customer key | customers | unique | Pseudonymous |
| .customer_name_masked | VARCHAR | First name + last initial | customers.full_name | `Priya H.` | Yes (reduced) |
| .email_masked / .phone_masked | VARCHAR | `pr***@example.com`, `97******21`; NULL if invalid | customers | | Yes (masked) |
| .city / .area / .pincode | VARCHAR | Location | customers | | Quasi-identifier |
| .signup_date / .loyalty_tier / .is_active | DATE / VARCHAR / BOOLEAN | Profile | customers | Bronze/Silver/Gold/Platinum | No |
| etl_dim_product.* | | product_id (PK), name, category ("Uncategorised" if blank), sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date | products | mrp > 0 | No |
| etl_dim_store.* | | store_id (PK), store_name, zone ("Unassigned" if blank), area, pincode, open_date, manager_name, capacity_orders_per_day (NULL if ≤ 0) | stores | | manager_name: employee name |

The ELT staging and mart tables use the same column names and meanings.

**3. Lineage (traced example).** `mart_daily_store_kpis` shows **S04 Jayanagar, 1 Sep 2026: net_revenue ₹1,103.30, 1 delivered order**.

* *ELT trace:* `stg_orders` with that date and store has two rows. ORD000052 (PASS, ₹1,103.30) has `_source_file = orders.csv`, `_source_row = 2075` and a `_run_id`. ORD000020 is FAIL (`or_delivery_implausible`, 900 minutes) and is excluded. `raw_orders` row 2075 of `orders.csv` (matched by `_file_hash`) is the exact text received.
* *ETL trace:* the fact row carries `source_file` and `source_row`, so you can get back to the file line. But the warehouse does not keep the received text, only the run's landing copy in `include/tmp`, which is temporary.

ELT is easier because every layer is a SQL table you can join back to the original bytes.

**4. Personal data.**

* *Columns:* customers `full_name`, `email`, `phone`, plus `area` and `pincode`, which become identifying in combination. Stores `manager_name` is employee personal data.
* *Treatment:* email and phone are masked; the name is reduced to first name + initial.
* *Why mask, not delete:* masked values still let ops recognise a customer, de-duplicate and spot bad data, without exposing contact details. `customer_id` remains the join key, so the analysis works without the raw values.
* *Law:* India's **Digital Personal Data Protection Act, 2023** (and its Rules) applies to NammaMart as a Data Fiduciary. It expects processing only for the stated purpose (with consent or a legitimate use), minimisation, reasonable security safeguards, breach notification to the Data Protection Board and affected users, honouring access, correction and erasure requests, and erasure once the purpose is served.

**5. Access control.**

| Layer | Who reads |
|---|---|
| raw_* (incl. unmasked customer phone/email) | Pipeline service account and the named data owner only (DPO-approved, logged). No analyst access. |
| quarantine (CSV + tables; contains raw values) | Data team for triage, read-only to the source system owner. Same restriction as raw. |
| stg_* | Data team |
| mart_* / etl reporting tables | Ops, Finance, leadership (masked data only) |

Raw unmasked phone numbers should be seen by nobody in analytics. Only the CRM owner may access them, for a stated operational purpose. A DuckDB file has no row/column grants, so in production raw should sit in a separate file or schema with OS/database permissions. The `.gitignore` keeps warehouse and quarantine files out of git.

**6. Retention (proposal for legal sign-off).**

* *Raw orders and payments:* 8 years, because they are books of account under the Companies Act; GST needs 6 years. After that, delete or archive.
* *Raw customers with unmasked PII:* only while the account is active plus a short grace period (for example 12 months after closure). Then erase, per DPDP, and keep the masked dimension.
* *Quarantine files:* 90 days after the issue is resolved.
* *Masked customer data:* as long as the reporting needs it. The `include/tmp` landing copies are deleted after 7 days.

**7. Secrets.** `SLACK_WEBHOOK_URL` is read with `Variable.get()` in `include/nammamart/alerts.py`. It is set with `astro dev run variables set SLACK_WEBHOOK_URL ...`, or in a gitignored `.env` as `AIRFLOW_VAR_SLACK_WEBHOOK_URL`. A secrets backend is the production option. DAG files are committed and parsed by every Airflow component, and git history keeps a secret forever, even after it is "deleted". Anyone with repo access could then post into the company Slack.

**8. Audit: "show me a rejected order".** Example **ORD002305**: quantity 3 × ₹265 at 15% off should be ₹675.75, but the file says ₹6,757.50.

* *ETL:*
  * As received: `etl_quarantine.raw_record` (and `orders_failed_<run>.csv`, row 311).
  * Rule broken: `failure_reason = or_amount_mismatch`.
  * Proof it is not in revenue: `SELECT count(*) FROM etl_fact_orders WHERE order_id='ORD002305'` returns **0**.
* *ELT:*
  * As received: `raw_orders` (the text of line 311).
  * Rule broken: `stg_order_batches` / `stg_orders` show `dq_status='FAIL'` with the reason.
  * Proof: every mart filters `dq_status <> 'FAIL'`.
  * It also appears in `mart_payment_reconciliation` as `ORDER_QUARANTINED`, because the customer *was* charged ₹6,757.50. Finance should see that.

**9. Change history.** ORD000271 was DELIVERED on 4 Sep and RETURNED in the 1 Oct file; ten such orders are worth ₹4,193.70.

* *September revenue:* before the increment, both pipelines showed ₹11,22,643.20; after it, both show ₹11,18,449.50. Both restate September, because the fact/staging row holds the latest status.
* *What finance wants:* both views. The restated figure is for "what did we really earn in September"; the as-reported figure is for the closed books.
* *How each pipeline keeps both:*
  * ETL keeps the old status and revenue in `etl_order_status_history`.
  * ELT keeps every version in `raw_orders` (`_snapshot_ts`), so an "as at 30 Sep" query is a filter on `_snapshot_ts`.
  * The cleaner long-term model is a returns fact dated 1 Oct, so September is never rewritten after close.

---

## Part F: Observability tests

Run audit: `audit_run` (`trigger_rule="all_done"`) writes one row per run to `pipeline_runs`: `run_id`, `dag_id`, `start_time`, `end_time`, `rows_extracted`, `rows_loaded`, `rows_quarantined`, `status`, `failed_tasks`. It reads completion markers that each task writes to `run_task_metrics`, so it works on Airflow 2 and 3. If any task did not finish, it writes `status='failed'` and then raises. It has `retries=0`: it fails on purpose, so retrying it would only delay the alert. Without that, an `all_done` leaf task that succeeds would turn the whole DAG run green after an upstream failure.

**Definitions differ by design:**

* ETL `rows_loaded` = fact rows inserted or updated.
* ELT `rows_loaded` = raw rows appended.
* ETL re-reads masters and payments every run; ELT skips any file whose hash is already in raw.

Every experiment was run on **both DAGs in the Airflow UI** (Astro Runtime 3.3-2). For experiments 5 to 9 I changed the file in `include/data/nammamart/`, triggered both DAGs, then put the original file back and restored the warehouse to its post-increment state, so each experiment starts from the same baseline. `scripts/observability_experiments.py` reproduces the same numbers without Airflow (`docs/observability_results.md`).

| # | ETL run | ELT run |
|---|---|---|
| 4 / 10 | `manual__2026-10-20T08:19:45` (before the `retries=0` fix), `manual__2026-10-20T09:13:17` | `manual__2026-10-20T09:13:22` |
| 5 | `manual__2026-10-20T09:33:51` | `manual__2026-10-20T09:34:00` |
| 6 | `manual__2026-10-20T09:36:28` | `manual__2026-10-20T09:36:37` |
| 7 | `manual__2026-10-20T09:32:28` | `manual__2026-10-20T09:32:38` |
| 8 | `manual__2026-10-20T09:34:44` | `manual__2026-10-20T09:34:53` |
| 9 | `manual__2026-10-20T09:35:35` | `manual__2026-10-20T09:35:44` |

The runs between 09:27 and 09:31 in the Runs lists (screenshots 29–30) are a first attempt at 5, 7, 8 and 9 that I discarded: my test script restored the warehouse before ELT had finished, so ELT `audit_run` found its task markers missing and failed (`manual__…T09:27:25`, `manual__…T09:27:59`). That was the harness, not the pipelines; the runs above were repeated one at a time.

| # | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|
| 1 Happy path | All green; masters validate in parallel, then orders, then payments | ETL and ELT `success`; 5,212 rows in, 169 quarantined. orders 2,412 → 2,283 passed / 129 quarantined; payments 2,228 → 2,199 / 29; customers 405 → 397 / 8; products 152 → 149 / 3; stores 15 → 15 / 0. Under `airflow dags test`: ETL 17 s, ELT 24 s | `pipeline_runs`, `dq_results`, Graph view (*screenshot*) | Nothing; record task durations from the Gantt view |
| 2 Re-run | No change in facts | Fact 2,283 → 2,283; revenue ₹11,22,643.20 both times; `raw_orders` 2,412 → 2,412 (ELT skipped every file by hash); `rows_loaded` 0 | `pipeline_runs`, row counts | Correct as is. Raw grows only when the bytes change, so every distinct version is kept with no copies of identical files |
| 3 Increment | New orders added, 10 updated, no duplicates | ETL: 148 inserted, 10 upserted; ELT: `stg_orders` 2,400 → 2,550 (incl. FAIL rows), 2 rejected. Both: 2,431 rows = 2,431 distinct ids, 77 RETURNED, 10 history rows | Load task log, `*_status_history` | Alert finance when a closed month changes |
| 4 Missing products.csv | `extract_products` up_for_retry twice (≈2 min, then ≈4 min), then failed; downstream `upstream_failed`; other extracts green; audit failed; alert | Both DAGs in the Airflow UI: `FileNotFoundError: No source file for 'products'` on all 3 tries; other extracts (and ETL `validate_customers` / `validate_stores`, ELT raw loads) green; every downstream task `upstream_failed` (ETL 7, ELT 12); `pipeline_runs.status = failed` with `failed_tasks` listed; facts untouched. Retry gaps vary because Airflow adds jitter to exponential backoff: ETL 3 min 10 s then 4 min 35 s, ELT 2 min 2 s then 7 min 56 s. **audit_run no longer retries** (`retries=0`): it fails once on try 1, so the ETL run reached `failed` in 8 min 5 s (18 min 44 s before the fix) and ELT in 10 min 16 s | Task log; `pipeline_runs`; screenshots 08–10, 14–18 | Retrying cannot fix a missing file. Use a `FileSensor` with a 08:45 deadline so the alert names the real problem earlier |
| 5 Five bad rows | Each quarantined with the right reason | Both DAGs green. ORD900001 `or_customer_id_missing`, 002 `or_quantity_invalid`, 003 `or_store_unknown`, 004 `or_status_invalid`, 005 `or_order_ts_future`, identical in both. Each check rose by exactly 1 in `dq_results` (18→19, 10→11, 5→6, 3→4, 7→8) | `*_quarantine`, `dq_results`; screenshot 22 | Nothing |
| 6 order_amount → total | Fail loudly at extract | Both: `ValueError: Schema change in orders.csv: missing columns ['order_amount']` on all 3 tries of `extract_orders`; downstream `upstream_failed`; `pipeline_runs` failed, `rows_loaded` 0; fact rows 2,431 → 2,431. Runs took 9 min 57 s (ETL) and 8 min 45 s (ELT), almost all of it retry waits | Extract task log, `pipeline_runs`; screenshots 23–25 | Raise a non-retryable error for schema changes (`AirflowFailException`) so it alerts in seconds, and add a "schema changed" alert type for the owner (Ops) |
| 7 Only 50 orders | Volume warning | `or_volume_drop` WARN, 50 of 50, both DAGs green. The facts keep earlier orders (2,431 → 2,431): an absent row is never treated as a delete. **Side effect found in the UI run:** ETL quarantined 2,184 rows against ELT's 41, because ETL checks `payments.order_id` only against the orders files in the current run (`etl.py` `validate_payments`), so 2,172 genuine payments became `pa_order_unknown`. ELT checks against every order already loaded. Facts and reconciliation are unaffected | `dq_results`, `pipeline_runs`, `etl_quarantine`; screenshot 26 | Make a >90% drop Critical, halting before load. In ETL, also accept order ids already in `etl_fact_orders` when validating payments |
| 8 unit_price ×100 at S05 (amount recomputed so the formula still holds) | Caught | Both DAGs green. `or_unit_price_above_mrp` FAIL on 143 rows (0 at baseline, all S05); `or_amount_mismatch` unchanged at 18, so the formula check alone would **not** have caught it. S05 mart revenue unchanged at ₹86,001.25; without the MRP rule it would have read ₹80,27,875.00 | `dq_results`, `mart_daily_store_kpis`; screenshot 27 | Add a per-store average-order-value check against the trailing 7 days |
| 9 Latest order 30 days old | Freshness warning | `or_freshness_stale` WARN on all 2,412 rows in both DAGs; both runs green | `dq_results`; screenshot 28 | Send freshness warnings to Slack too; they are the earliest sign a source export is stuck |
| 10 Recovery | Only the failed task and its downstream re-run | Both DAGs, Clear on `extract_products` with Downstream ticked after restoring the file. ETL: the dialog listed 9 tasks; `validate_customers` and `validate_stores` kept their 08:20:00 start times and try 1 while `extract_products` passed on try 4; the run went green in 24 s. ELT: 14 of 22 tasks cleared; the 4 green extracts and their 4 raw loads kept their 09:13 start times; green in 18.8 s. The re-run rewrote `pipeline_runs` to `success` | Screenshots 11–13 (ETL), 19–21 (ELT) | Nothing. Tasks are idempotent per run (they delete their own run's rows before writing), so Clear is safe |

**Five pillars of data observability.**

| Pillar | How the pipelines cover it |
|---|---|
| Freshness | `or_freshness_stale`, plus `pipeline_runs.end_time` |
| Volume | `or_volume_drop`, rows in/out per run |
| Schema | Header check at extract, failing loudly |
| Distribution | Rule-based only: MRP ceiling, 120-minute cap, amount formula |
| Lineage | `_source_file`, `_source_row`, `_file_hash`, `_run_id` in every ELT layer; `source_file` and `source_row` on the ETL fact |

**Strongest:** schema and lineage. **Weakest:** distribution. There is no statistical baseline yet, so a believable shift (prices up 20%, deliveries 5 minutes slower) would pass. Next step: trailing-window z-score checks on average order value, delivery minutes and orders per store, computed from `dq_results` history.

---

## Part G: Recommendation to Sameer (CTO)

**To:** Sameer · **From:** Deb (FDE) · **Re:** ETL or ELT for NammaMart

**Recommendation: adopt ELT, with the quality gate enforced in staging, so finance only ever reads rows that passed.** I built both on the same data, and they produce the same numbers to the paisa (₹11,18,449.50 for September). Finance's guarantee that no bad row reaches the CFO's dashboard turned out not to need ETL. It needs a gate, and the gate works the same in SQL. Where the two differ is everything else.

* **Auditability.** ELT keeps every row exactly as received, every version of it, and which file and line it came from. When finance asks "show me order X as we received it, the rule it broke, and prove it is not in revenue", ELT answers with three SQL queries. ETL can show rejected rows but not accepted ones as received, and keeps no history beyond what I added by hand.
* **Data quality guarantees.** Equal. Both quarantine the same 171 rows for the same reasons and caught all five injected bad rows. Both caught a 100× price error that the amount formula missed. Both failed loudly on a schema change.
* **Ease of change.** ELT wins clearly. Suppose the client answers the open questions, for example "P0109's MRP is a sign error". In ELT I change one line of SQL and rebuild staging from raw, and September is corrected. In ETL the rejected rows were never loaded, so I would have to find and re-feed the old files.
* **Performance at 1000×** (about 2.4 million orders a month). The ETL path holds each file in pandas memory and passes pickles between tasks; that will be the first thing to break. The ELT path runs vectorised SQL inside DuckDB, which handles tens of millions of rows on one machine. Beyond that it moves to a server database without rewriting the logic, because the logic is already SQL.
* **Cost.** Both are open source. ELT stores more: raw history is about 0.5 MB a day today and roughly 0.5 GB a month at 1000×, less as Parquet. That is negligible next to an analyst spending half a day a week reconciling payments by hand.

**Top 3 risks of this design.**

1. **Raw tables hold unmasked personal data.** That carries DPDP exposure. DuckDB has no column-level permissions, so raw must live in a separately permissioned store with an erasure process.
2. **The gate is a convention.** A new mart written without `dq_status <> 'FAIL'` would leak bad rows. Mitigation: expose staging to analysts only through `clean_*` views, and add a test that fails CI if a mart reads `stg_*` directly.
3. **A single DuckDB file on one Docker host** is a single point of failure. It has one writer at a time, and if the host is off at 9:00 nobody is alerted. Before go-live: always-on scheduler, nightly backup of the warehouse, and an external "no run by 09:15" check.

---

## Bonus items included

* Slack alert on failure via the Airflow Variable `SLACK_WEBHOOK_URL` (`include/nammamart/alerts.py`). It logs instead when the variable is unset.
* Store-capacity utilisation: `mart_daily_store_kpis.capacity_utilisation_pct`. It surfaces S12's capacity of 0 as NULL.
* Status-change history in both pipelines (the change-history question in Part E).

## Screenshots (`docs/screenshots/`)

* [x] Both DAGs in Graph view after a green run: [01_etl_graph_green_run.jpg](docs/screenshots/01_etl_graph_green_run.jpg), [02_elt_graph_green_run.jpg](docs/screenshots/02_elt_graph_green_run.jpg)
* [x] DAG details showing schedule `0 9 * * 1-6`, timezone Asia/Kolkata and the next run (03:30 UTC): [03_etl_dag_details_schedule_next_run_utc.jpg](docs/screenshots/03_etl_dag_details_schedule_next_run_utc.jpg). The Details panel labels the timezone "UTC"; see the Part C note.
* [x] One quarantine file (orders, not customers: the customer file contains raw PII): [07_quarantine_orders_file.jpg](docs/screenshots/07_quarantine_orders_file.jpg)
* [x] Query results: `pipeline_runs` [04_pipeline_runs.jpg](docs/screenshots/04_pipeline_runs.jpg), `dq_results` [05_dq_results.jpg](docs/screenshots/05_dq_results.jpg), `etl_payment_reconciliation` / `mart_payment_reconciliation` [06_payment_reconciliation_etl_vs_elt.jpg](docs/screenshots/06_payment_reconciliation_etl_vs_elt.jpg)
* [x] Experiment 4 task states (up_for_retry, failed, upstream_failed), both DAGs:
  * ETL: [08_exp4_extract_products_up_for_retry.jpg](docs/screenshots/08_exp4_extract_products_up_for_retry.jpg), [09_exp4_failed_upstream_failed_graph.jpg](docs/screenshots/09_exp4_failed_upstream_failed_graph.jpg), [10_exp4_extract_products_log_3_tries.jpg](docs/screenshots/10_exp4_extract_products_log_3_tries.jpg), [17_exp4_etl_rerun_audit_run_try1_8min.jpg](docs/screenshots/17_exp4_etl_rerun_audit_run_try1_8min.jpg) (after the `retries=0` fix)
  * ELT: [14_exp4_elt_extract_products_up_for_retry.jpg](docs/screenshots/14_exp4_elt_extract_products_up_for_retry.jpg), [15_exp4_elt_failed_upstream_failed_graph.jpg](docs/screenshots/15_exp4_elt_failed_upstream_failed_graph.jpg), [16_exp4_elt_grid_before_clear_audit_run_try1.jpg](docs/screenshots/16_exp4_elt_grid_before_clear_audit_run_try1.jpg)
  * `pipeline_runs` failed with `failed_tasks`, both DAGs: [18_exp4_pipeline_runs_failed_both_dags.jpg](docs/screenshots/18_exp4_pipeline_runs_failed_both_dags.jpg)
* [x] Experiment 10 Grid before and after Clear, both DAGs:
  * ETL: [11_exp10_grid_before_clear.jpg](docs/screenshots/11_exp10_grid_before_clear.jpg), [12_exp10_clear_downstream_dialog.jpg](docs/screenshots/12_exp10_clear_downstream_dialog.jpg), [13_exp10_grid_after_clear.jpg](docs/screenshots/13_exp10_grid_after_clear.jpg)
  * ELT: [19_exp10_elt_clear_downstream_dialog.jpg](docs/screenshots/19_exp10_elt_clear_downstream_dialog.jpg), [20_exp10_elt_grid_after_clear.jpg](docs/screenshots/20_exp10_elt_grid_after_clear.jpg), [21_exp10_elt_untouched_tasks_keep_0913_times.jpg](docs/screenshots/21_exp10_elt_untouched_tasks_keep_0913_times.jpg)
* [x] Experiments 5–9 on both DAGs: [22_exp5_bad_rows_quarantined_both.jpg](docs/screenshots/22_exp5_bad_rows_quarantined_both.jpg), [23_exp6_etl_extract_orders_schema_error.jpg](docs/screenshots/23_exp6_etl_extract_orders_schema_error.jpg), [24_exp6_elt_extract_orders_schema_error.jpg](docs/screenshots/24_exp6_elt_extract_orders_schema_error.jpg), [25_exp6_pipeline_runs_facts_unchanged.jpg](docs/screenshots/25_exp6_pipeline_runs_facts_unchanged.jpg), [26_exp7_volume_drop_warn_both.jpg](docs/screenshots/26_exp7_volume_drop_warn_both.jpg), [27_exp8_price_x100_caught_both.jpg](docs/screenshots/27_exp8_price_x100_caught_both.jpg), [28_exp9_freshness_warn_both.jpg](docs/screenshots/28_exp9_freshness_warn_both.jpg)
* [x] Runs lists with every experiment run: [29_etl_runs_list_experiments_4_to_10.jpg](docs/screenshots/29_etl_runs_list_experiments_4_to_10.jpg), [30_elt_runs_list_experiments_4_to_10.jpg](docs/screenshots/30_elt_runs_list_experiments_4_to_10.jpg)
