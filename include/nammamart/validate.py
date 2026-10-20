"""ETL data quality rules in pandas.

Each validate_* function takes the raw text DataFrame for one file and returns
(evaluated_df, dq_results). evaluated_df keeps every input row, adds cleaned
columns plus dq_status (PASS / WARN / FAIL), failure_reason (critical codes)
and warning_reason (warning codes). The SQL in include/sql/elt/ mirrors these
rules one for one.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from . import checks
from .config import (
    AMOUNT_TOLERANCE, DATE_FORMAT, DELIVERY_MAX_MINUTES, DELIVERY_SLA_BAND,
    FRESHNESS_MAX_AGE, PAYMENT_MODE_MAP, SOURCES, TS_FORMAT, VALID_LOYALTY_TIERS,
    VALID_ORDER_STATUS, VALID_PAYMENT_STATUS, VALID_ZONES, VOLUME_DROP_THRESHOLD,
    snapshot_ts,
)
from .masking import EMAIL_RE, PHONE_RE, PIN_RE


# ----------------------------------------------------------------- helpers
def read_text_csv(path: Path, entity: str) -> pd.DataFrame:
    """Read exactly as received: every column as text, empty stays empty."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    expected = SOURCES[entity]["columns"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        # Fail loudly on schema drift instead of loading wrong data silently
        raise ValueError(
            f"Schema change in {path.name}: missing columns {missing}; got {list(df.columns)}"
        )
    df = df[expected].copy()
    df["_source_file"] = path.name
    df["_source_row"] = np.arange(1, len(df) + 1)
    df["_snapshot_ts"] = snapshot_ts(path.name)
    return df


def _t(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(_t(s).replace("", np.nan), errors="coerce")


def _ts(s: pd.Series, fmt: str = TS_FORMAT) -> pd.Series:
    return pd.to_datetime(_t(s), format=fmt, errors="coerce")


def _dup(df: pd.DataFrame, key: str, order_cols: list[str]) -> pd.Series:
    """True for every row after the first of the same non-empty key."""
    ranked = df.sort_values(order_cols, kind="stable")
    rn = ranked.groupby(key, sort=False).cumcount() + 1
    rn = rn.reindex(df.index)
    return (df[key] != "") & (rn > 1)


def _finish(df: pd.DataFrame, flags: dict[str, pd.Series], entity: str,
            source_file: str | None = None) -> tuple[pd.DataFrame, list[dict]]:
    order = [c for c in checks.checks_for(entity) if c in flags]
    crit = [c for c in order if checks.is_critical(c)]
    warn = [c for c in order if not checks.is_critical(c)]

    def join(codes):
        if not codes:
            return pd.Series([None] * len(df), index=df.index, dtype=object)
        parts = pd.DataFrame({c: np.where(flags[c].fillna(False).to_numpy(), c, "") for c in codes},
                             index=df.index)
        out = parts.apply(lambda r: "; ".join(x for x in r if x), axis=1)
        return out.replace("", None)

    df = df.copy()
    df["failure_reason"] = join(crit)
    df["warning_reason"] = join(warn)
    df["dq_status"] = np.where(df["failure_reason"].notna(), "FAIL",
                               np.where(df["warning_reason"].notna(), "WARN", "PASS"))
    results = [
        {"check_name": c, "rows_checked": len(df),
         "rows_failed": int(flags[c].fillna(False).sum()),
         "source_file": source_file or (df["_source_file"].iloc[0] if len(df) else None)}
        for c in order
    ]
    return df, results


def _canon_mode(s: pd.Series) -> pd.Series:
    return _t(s).str.lower().map(PAYMENT_MODE_MAP)


# ----------------------------------------------------------------- stores
def validate_stores(raw: pd.DataFrame):
    df = raw.copy()
    snap = df["_snapshot_ts"]
    df["store_id"] = _t(df.store_id)
    zone = _t(df.zone).str.capitalize()
    df["zone"] = zone.where(zone != "", "Unassigned")
    df["store_name"] = _t(df.store_name)
    df["area"] = _t(df.area)
    df["pincode"] = _t(df.pincode)
    df["manager_name"] = _t(df.manager_name)
    open_date = _ts(df.open_date, DATE_FORMAT)
    df["open_date"] = open_date.dt.date
    cap = _num(df.capacity_orders_per_day)
    df["capacity_orders_per_day"] = cap.where(cap > 0)
    flags = {
        "st_store_id_missing": df.store_id == "",
        "st_store_id_duplicate": _dup(df, "store_id", ["_source_row"]),
        "st_zone_missing": zone == "",
        "st_zone_invalid": (zone != "") & ~zone.isin(VALID_ZONES),
        "st_pincode_invalid": ~df.pincode.str.fullmatch(PIN_RE.pattern),
        "st_capacity_invalid": cap.isna() | (cap <= 0),
        "st_open_date_future": open_date.isna() | (open_date > snap),
    }
    return _finish(df, flags, "stores")


# ----------------------------------------------------------------- products
def validate_products(raw: pd.DataFrame):
    df = raw.copy()
    snap = df["_snapshot_ts"]
    df["product_id"] = _t(df.product_id)
    df["product_name"] = _t(df.product_name)
    cat = _t(df.category)
    df["category"] = cat.where(cat != "", "Uncategorised")
    for c in ["sub_category", "brand", "unit"]:
        df[c] = _t(df[c])
    df["is_perishable"] = _t(df.is_perishable).str.lower().isin(["true", "y", "yes", "1"])
    mrp, cost = _num(df.mrp), _num(df.cost_price)
    df["mrp"], df["cost_price"] = mrp, cost
    launch = _ts(df.launch_date, DATE_FORMAT)
    df["launch_date"] = launch.dt.date
    # Prefer the current row over a row marked "(old)" when ids collide
    df["_is_old"] = df.product_name.str.lower().str.contains(r"\(old\)", regex=True).astype(int)
    flags = {
        "pr_product_id_missing": df.product_id == "",
        "pr_product_id_duplicate": _dup(df, "product_id", ["_is_old", "_source_row"]),
        "pr_category_missing": cat == "",
        "pr_mrp_invalid": mrp.isna() | (mrp <= 0),
        "pr_cost_price_invalid": cost.isna() | (cost <= 0),
        "pr_cost_above_mrp": (cost > mrp),
        "pr_launch_date_future": launch.isna() | (launch > snap),
    }
    df = df.drop(columns="_is_old")
    return _finish(df, flags, "products")


# ----------------------------------------------------------------- customers
def validate_customers(raw: pd.DataFrame):
    df = raw.copy()
    snap = df["_snapshot_ts"]
    df["customer_id"] = _t(df.customer_id)
    for c in ["full_name", "email", "phone", "city", "area", "pincode"]:
        df[c] = _t(df[c])
    raw_tier = _t(df.loyalty_tier)
    tier = raw_tier.str.capitalize()
    df["loyalty_tier"] = tier
    df["is_active"] = _t(df.is_active).str.upper().eq("Y")
    signup = _ts(df.signup_date, DATE_FORMAT)
    df["signup_date"] = signup.dt.date
    flags = {
        "cu_customer_id_missing": df.customer_id == "",
        "cu_customer_id_duplicate": _dup(df, "customer_id", ["_source_row"]),
        "cu_email_invalid": ~df.email.str.fullmatch(EMAIL_RE.pattern),
        "cu_phone_invalid": ~df.phone.str.fullmatch(PHONE_RE.pattern),
        "cu_pincode_invalid": ~df.pincode.str.fullmatch(PIN_RE.pattern),
        "cu_city_missing": df.city == "",
        "cu_loyalty_tier_casing": (raw_tier != tier) & tier.isin(VALID_LOYALTY_TIERS),
        "cu_loyalty_tier_invalid": ~tier.isin(VALID_LOYALTY_TIERS),
        "cu_signup_date_future": signup.isna() | (signup > snap),
    }
    return _finish(df, flags, "customers")


# ----------------------------------------------------------------- orders
def validate_orders(raw: pd.DataFrame, clean_customers: set, clean_stores: set,
                    product_mrp: dict, previous_rows: int | None):
    """product_mrp: {product_id: mrp} for products that passed their checks."""
    df = raw.copy()
    snap = df["_snapshot_ts"]
    for c in ["order_id", "customer_id", "store_id", "product_id", "channel"]:
        df[c] = _t(df[c])
    raw_status = _t(df.order_status)
    status = raw_status.str.upper()
    raw_mode = _t(df.payment_mode)
    mode = _canon_mode(df.payment_mode)
    ts = _ts(df.order_ts)
    q, u, d, amt = _num(df.quantity), _num(df.unit_price), _num(df.discount_pct), _num(df.order_amount)
    mins = _num(df.delivery_minutes)
    mrp = df.product_id.map(product_mrp)
    calc = q * u * (1 - d / 100)
    amount_ok = (calc - amt).abs() <= AMOUNT_TOLERANCE

    df["order_status"] = status
    df["payment_mode"] = mode
    df["order_ts"] = ts
    df["order_date"] = ts.dt.date
    df["quantity"], df["unit_price"], df["discount_pct"] = q, u, d
    df["order_amount"], df["delivery_minutes"] = amt, mins

    flags = {
        "or_order_id_missing": df.order_id == "",
        "or_order_id_duplicate": _dup(df, "order_id", ["_source_row"]),
        "or_customer_id_missing": df.customer_id == "",
        "or_order_ts_invalid": ts.isna(),
        "or_order_ts_future": ts > snap,
        "or_status_invalid": ~status.isin(VALID_ORDER_STATUS),
        "or_status_casing": (raw_status != status) & status.isin(VALID_ORDER_STATUS),
        "or_payment_mode_invalid": mode.isna(),
        "or_payment_mode_nonstandard": mode.notna() & (raw_mode != mode),
        "or_quantity_invalid": q.isna() | (q <= 0),
        "or_discount_invalid": d.isna() | (d < 0) | (d > 100),
        "or_amount_mismatch": ~amount_ok.fillna(False),
        "or_unit_price_above_mrp": u > mrp + 0.001,
        "or_delivery_missing": (status == "DELIVERED") & mins.isna(),
        "or_delivery_implausible": (mins <= 0) | (mins > DELIVERY_MAX_MINUTES),
        "or_delivery_outside_band": (mins > 0) & (mins <= DELIVERY_MAX_MINUTES)
                                    & ((mins < DELIVERY_SLA_BAND[0]) | (mins > DELIVERY_SLA_BAND[1])),
        "or_store_unknown": ~df.store_id.isin(clean_stores),
        "or_product_unknown": ~df.product_id.isin(product_mrp.keys()),
        "or_customer_unknown": (df.customer_id != "") & ~df.customer_id.isin(clean_customers),
    }
    df, results = _finish(df, flags, "orders")
    results += file_level_order_checks(df, ts, previous_rows)
    return df, results


def file_level_order_checks(df: pd.DataFrame, ts: pd.Series, previous_rows: int | None) -> list[dict]:
    source = df["_source_file"].iloc[0] if len(df) else None
    n = len(df)
    dropped = previous_rows is not None and previous_rows > 0 and n < previous_rows * (1 - VOLUME_DROP_THRESHOLD)
    snap = df["_snapshot_ts"].iloc[0] if n else None
    # Ignore impossible future timestamps: they are caught by or_order_ts_future
    latest = ts[ts <= snap].max() if n else None
    stale = bool(n and (pd.isna(latest) or (snap - latest) > FRESHNESS_MAX_AGE))
    return [
        {"check_name": "or_volume_drop", "rows_checked": n, "rows_failed": n if dropped else 0,
         "source_file": source},
        {"check_name": "or_freshness_stale", "rows_checked": n, "rows_failed": n if stale else 0,
         "source_file": source},
    ]


# ----------------------------------------------------------------- payments
def validate_payments(raw: pd.DataFrame, all_order_ids: set):
    df = raw.copy()
    snap = df["_snapshot_ts"]
    for c in ["payment_id", "order_id", "gateway_ref"]:
        df[c] = _t(df[c])
    raw_mode = _t(df.payment_mode)
    mode = _canon_mode(df.payment_mode)
    status = _t(df.payment_status).str.upper()
    ts = _ts(df.payment_ts)
    amt = _num(df.amount_paid)
    df["payment_mode"], df["payment_status"] = mode, status
    df["payment_ts"], df["amount_paid"] = ts, amt
    flags = {
        "pa_payment_id_missing": df.payment_id == "",
        "pa_payment_id_duplicate": _dup(df, "payment_id", ["_source_row"]),
        "pa_payment_ts_invalid": ts.isna(),
        "pa_payment_ts_future": ts > snap,
        "pa_status_invalid": ~status.isin(VALID_PAYMENT_STATUS),
        "pa_payment_mode_invalid": mode.isna(),
        "pa_payment_mode_nonstandard": mode.notna() & (raw_mode != mode),
        "pa_amount_invalid": amt.isna() | (amt < 0) | ((amt == 0) & (status == "SUCCESS")),
        "pa_order_unknown": ~df.order_id.isin(all_order_ids),
        "pa_gateway_ref_missing": (mode != "Cash on Delivery") & (df.gateway_ref == ""),
    }
    return _finish(df, flags, "payments")
