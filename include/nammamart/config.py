"""Shared configuration for the NammaMart ETL and ELT pipelines.

Change LEARNER_NAME once and every path, table file and DAG id follows.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

LEARNER_NAME = os.environ.get("NAMMAMART_LEARNER", "deb")

INCLUDE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
SQL_DIR = INCLUDE_DIR / "sql"
QUARANTINE_DIR = Path(os.environ.get("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / LEARNER_NAME))
WORK_DIR = Path(os.environ.get("NAMMAMART_WORK_DIR", INCLUDE_DIR / "tmp" / LEARNER_NAME))
WAREHOUSE_PATH = Path(
    os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{LEARNER_NAME}.duckdb")
)

ETL_DAG_ID = f"nammamart_etl_{LEARNER_NAME}"
ELT_DAG_ID = f"nammamart_elt_{LEARNER_NAME}"

# One entry per source. "pattern" is a glob so the orders feed can pick up
# daily increment files (orders_YYYY-MM-DD_increment.csv) next to orders.csv.
SOURCES: dict[str, dict] = {
    "orders": {
        "patterns": ["orders.csv", "orders_*_increment.csv"],
        "key": "order_id",
        "columns": [
            "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
            "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
            "channel", "delivery_minutes",
        ],
    },
    "payments": {
        "patterns": ["payments.csv"],
        "key": "payment_id",
        "columns": [
            "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
            "payment_status", "gateway_ref",
        ],
    },
    "customers": {
        "patterns": ["customers.csv"],
        "key": "customer_id",
        "columns": [
            "customer_id", "full_name", "email", "phone", "city", "area", "pincode",
            "signup_date", "loyalty_tier", "is_active",
        ],
    },
    "products": {
        "patterns": ["products.csv"],
        "key": "product_id",
        "columns": [
            "product_id", "product_name", "category", "sub_category", "brand", "mrp",
            "cost_price", "unit", "is_perishable", "launch_date",
        ],
    },
    "stores": {
        "patterns": ["stores.csv"],
        "key": "store_id",
        "columns": [
            "store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
            "capacity_orders_per_day",
        ],
    },
}

# Business rules agreed with the client
VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONES = ["North", "South", "East", "West"]
# Raw value (lower-cased, trimmed) -> canonical payment mode
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cash": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}
DELIVERY_SLA_BAND = (8, 35)      # normal delivery window in minutes
DELIVERY_MAX_MINUTES = 120       # anything above is physically implausible for a dark store
SLA_TARGET_MINUTES = 15          # "% of delivered orders within 15 minutes"
VOLUME_DROP_THRESHOLD = 0.90     # warn when a file has >90% fewer rows than its last load
FRESHNESS_MAX_AGE = timedelta(days=1)
AMOUNT_TOLERANCE = 0.01
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

# The September exports were taken at midnight on 1 Oct 2026. A record dated
# after its file's snapshot time cannot be real, so "future" is measured
# against the snapshot, not the wall clock. That keeps results reproducible
# whenever the DAG runs. Override with the Airflow Variable below.
DEFAULT_BASE_SNAPSHOT = "2026-10-01 00:00:00"
INCREMENT_RE = re.compile(r"orders_(\d{4}-\d{2}-\d{2})_increment\.csv$")


def get_var(key: str, default=None):
    """Read an Airflow Variable; fall back to env var, then default.

    Works on Airflow 2 and 3, and outside Airflow (local tests).
    """
    try:
        try:
            from airflow.sdk import Variable  # Airflow 3
            return Variable.get(key, default=default)
        except ImportError:
            from airflow.models import Variable  # Airflow 2
            return Variable.get(key, default_var=default)
    except Exception:
        return os.environ.get(key.upper(), default)


def snapshot_ts(file_name: str) -> datetime:
    """When the export in this file was taken (records after this are 'future')."""
    m = INCREMENT_RE.search(file_name)
    if m:
        return datetime.strptime(m.group(1), DATE_FORMAT) + timedelta(days=1)
    base = get_var("nammamart_base_snapshot", DEFAULT_BASE_SNAPSHOT)
    return datetime.strptime(base, TS_FORMAT)


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)


def run_dir(dag_id: str, run_id: str) -> Path:
    d = WORK_DIR / dag_id / safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def source_files(entity: str) -> list[Path]:
    """Files for an entity, ordered by snapshot (base file first, then increments)."""
    files: list[Path] = []
    for pattern in SOURCES[entity]["patterns"]:
        files.extend(sorted(DATA_DIR.glob(pattern)))
    if not files:
        raise FileNotFoundError(
            f"No source file for '{entity}' in {DATA_DIR} (looked for {SOURCES[entity]['patterns']})"
        )
    return sorted(set(files), key=lambda p: (snapshot_ts(p.name), p.name))


IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    """Wall-clock time in IST, naive. Docker containers run in UTC, so never use datetime.now()."""
    return datetime.now(IST).replace(tzinfo=None)


def to_ist_naive(dt) -> datetime | None:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(IST).replace(tzinfo=None)
    return dt
