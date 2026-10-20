"""Part 0: profile all six files and write the Data Issues Log.

    python scripts/profile_data.py

Outputs
  docs/data_profile.md        row counts, nulls, duplicates, min/max, distinct values, RI
  docs/data_issues_log.csv    one line per issue: file, row key, column, problem,
                              dimension, proposed rule, severity
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from include.nammamart import checks, validate  # noqa: E402
from include.nammamart.config import DATA_DIR, SOURCES  # noqa: E402
from include.nammamart.masking import mask_name, mask_phone  # noqa: E402

DOCS = ROOT / "docs"
# Customer PII never leaves include/data/ unmasked: docs are committed.
PII_COLUMNS = {"full_name", "email", "phone"}
CATEGORICAL = {"order_status", "payment_mode", "loyalty_tier", "zone", "category", "channel",
               "payment_status", "unit", "is_active", "is_perishable", "city"}
NUMERIC = {"quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes",
           "amount_paid", "mrp", "cost_price", "capacity_orders_per_day"}
DATES = {"order_ts", "payment_ts", "signup_date", "launch_date", "open_date"}

# Column each check looks at (for the issues log)
CHECK_COLUMN = {
    "st_store_id_missing": "store_id", "st_store_id_duplicate": "store_id", "st_zone_missing": "zone",
    "st_zone_invalid": "zone", "st_pincode_invalid": "pincode",
    "st_capacity_invalid": "capacity_orders_per_day", "st_open_date_future": "open_date",
    "pr_product_id_missing": "product_id", "pr_product_id_duplicate": "product_id",
    "pr_category_missing": "category", "pr_mrp_invalid": "mrp", "pr_cost_price_invalid": "cost_price",
    "pr_cost_above_mrp": "cost_price", "pr_launch_date_future": "launch_date",
    "cu_customer_id_missing": "customer_id", "cu_customer_id_duplicate": "customer_id",
    "cu_email_invalid": "email", "cu_phone_invalid": "phone", "cu_pincode_invalid": "pincode",
    "cu_city_missing": "city", "cu_loyalty_tier_casing": "loyalty_tier",
    "cu_loyalty_tier_invalid": "loyalty_tier", "cu_signup_date_future": "signup_date",
    "or_order_id_missing": "order_id", "or_order_id_duplicate": "order_id",
    "or_customer_id_missing": "customer_id", "or_order_ts_invalid": "order_ts",
    "or_order_ts_future": "order_ts", "or_status_invalid": "order_status",
    "or_status_casing": "order_status", "or_payment_mode_invalid": "payment_mode",
    "or_payment_mode_nonstandard": "payment_mode", "or_quantity_invalid": "quantity",
    "or_discount_invalid": "discount_pct", "or_amount_mismatch": "order_amount",
    "or_unit_price_above_mrp": "unit_price", "or_delivery_missing": "delivery_minutes",
    "or_delivery_implausible": "delivery_minutes", "or_delivery_outside_band": "delivery_minutes",
    "or_store_unknown": "store_id", "or_product_unknown": "product_id",
    "or_customer_unknown": "customer_id",
    "pa_payment_id_missing": "payment_id", "pa_payment_id_duplicate": "payment_id",
    "pa_payment_ts_invalid": "payment_ts", "pa_payment_ts_future": "payment_ts",
    "pa_status_invalid": "payment_status", "pa_payment_mode_invalid": "payment_mode",
    "pa_payment_mode_nonstandard": "payment_mode", "pa_amount_invalid": "amount_paid",
    "pa_order_unknown": "order_id", "pa_gateway_ref_missing": "gateway_ref",
}


def redact(col: str, value: str) -> str:
    """Mask a customer PII value but keep what makes it invalid visible
    (missing '@' or TLD, wrong phone length). Placeholders like N/A pass through."""
    v = (value or "").strip()
    if col not in PII_COLUMNS or v in ("", "N/A"):
        return value
    if col == "email":
        return v[:2] + "***" + (v[v.index("@"):] if "@" in v else " (no @)")
    if col == "phone":
        return mask_phone(v) or "***"
    return mask_name(v) or "***"


def entity_of(name: str) -> str:
    return "orders" if name.startswith("orders") else name.replace(".csv", "")


def profile(df: pd.DataFrame, name: str) -> list[str]:
    ent = entity_of(name)
    key = SOURCES[ent]["key"]
    out = [f"### {name}", "",
           f"Rows: **{len(df)}**. Duplicate `{key}` values: **{df[key].duplicated().sum()}** "
           f"(exact duplicate rows: {df.duplicated().sum()}).", "",
           "| Column | Empty | Distinct | Min | Max | Values / sample |", "|---|---|---|---|---|---|"]
    for c in df.columns:
        s = df[c].str.strip()
        empty = int((s == "").sum())
        mn = mx = ""
        if c in NUMERIC:
            n = pd.to_numeric(s.replace("", None), errors="coerce")
            mn, mx = n.min(), n.max()
            bad = int(n.isna().sum() - empty)
            if bad:
                mn = f"{mn} ({bad} non-numeric)"
        elif c in DATES:
            ok = s[s != ""]
            mn, mx = ok.min(), ok.max()
        if c in CATEGORICAL or s.nunique() <= 10:
            vc = df[c].value_counts()
            vals = ", ".join(f"`{v!r}`: {n}" for v, n in vc.items())
        else:
            vals = ", ".join(f"`{redact(c, v)}`" for v in s.unique()[:3]) + " ..."
        out.append(f"| {c} | {empty} | {s.nunique()} | {mn} | {mx} | {vals} |")
    return out + [""]


def referential_integrity(frames: dict) -> list[str]:
    o = pd.concat([frames["orders.csv"]])
    c, p, s, pay = frames["customers.csv"], frames["products.csv"], frames["stores.csv"], frames["payments.csv"]
    rows = [
        ("orders.customer_id -> customers", o[(o.customer_id != "") & ~o.customer_id.isin(c.customer_id)].customer_id),
        ("orders.product_id -> products", o[~o.product_id.isin(p.product_id)].product_id),
        ("orders.store_id -> stores", o[~o.store_id.isin(s.store_id)].store_id),
        ("payments.order_id -> orders", pay[~pay.order_id.isin(o.order_id)].order_id),
    ]
    out = ["## Referential integrity (orders.csv)", "", "| Relationship | Orphans | Values |", "|---|---|---|"]
    for name, orphans in rows:
        out.append(f"| {name} | {len(orphans)} | {', '.join(sorted(orphans.unique()))} |")
    delivered = o[o.order_status.str.upper().str.strip() == "DELIVERED"]
    out += ["", f"DELIVERED orders with no payment row: "
                f"{(~delivered.order_id.isin(pay.order_id)).sum()}. Cancelled orders without payment are expected.", ""]
    return out


def issues_log(frames: dict) -> pd.DataFrame:
    """Apply every rule and list each failing row (one line per row per check)."""
    rows = []
    stores = validate.validate_stores(validate.read_text_csv(DATA_DIR / "stores.csv", "stores"))[0]
    products = validate.validate_products(validate.read_text_csv(DATA_DIR / "products.csv", "products"))[0]
    customers = validate.validate_customers(validate.read_text_csv(DATA_DIR / "customers.csv", "customers"))[0]
    clean = lambda d: d[d.dq_status != "FAIL"]
    mrp = dict(zip(clean(products).product_id, clean(products).mrp))
    evaluated = {"stores.csv": stores, "products.csv": products, "customers.csv": customers}
    all_ids = set()
    for f in sorted(DATA_DIR.glob("orders*.csv")):
        raw = validate.read_text_csv(f, "orders")
        all_ids |= set(raw.order_id.str.strip())
        evaluated[f.name] = validate.validate_orders(
            raw, set(clean(customers).customer_id), set(clean(stores).store_id), mrp, None)[0]
    evaluated["payments.csv"] = validate.validate_payments(
        validate.read_text_csv(DATA_DIR / "payments.csv", "payments"), all_ids)[0]

    for fname, ev in evaluated.items():
        raw = frames[fname]
        key = SOURCES[entity_of(fname)]["key"]
        for _, r in ev[ev.dq_status != "PASS"].iterrows():
            reasons = [x for x in (r.failure_reason, r.warning_reason) if isinstance(x, str)]
            codes = [c for c in "; ".join(reasons).split("; ") if c]
            for code in codes:
                ent, dim, sev, desc = checks.CHECKS[code]
                col = CHECK_COLUMN[code]
                value = redact(col, raw.iloc[int(r._source_row) - 1][col])
                rows.append({"file": fname, "source_row": int(r._source_row),
                             "row_key": raw.iloc[int(r._source_row) - 1][key], "column": col,
                             "value_received": value, "problem": desc, "dimension": dim,
                             "rule_code": code, "severity": sev,
                             "action": "quarantine" if sev == "Critical" else "load + log warning"})
    return pd.DataFrame(rows)


def main():
    frames = {f.name: pd.read_csv(f, dtype=str, keep_default_na=False) for f in sorted(DATA_DIR.glob("*.csv"))}
    md = ["# NammaMart data profile", "",
          "Generated by `scripts/profile_data.py` from the files in `include/data/nammamart/`.", ""]
    for name, df in frames.items():
        md += profile(df, name)
    md += referential_integrity(frames)
    log = issues_log(frames)
    summary = (log.groupby(["file", "rule_code", "column", "dimension", "severity"])
                  .size().reset_index(name="rows").sort_values(["file", "severity", "rows"],
                                                               ascending=[True, True, False]))
    md += ["## Issues summary (full row-level list in data_issues_log.csv)", "",
           summary.to_markdown(index=False), ""]
    DOCS.mkdir(exist_ok=True)
    (DOCS / "data_profile.md").write_text("\n".join(md))
    log.to_csv(DOCS / "data_issues_log.csv", index=False)
    print(summary.to_string(index=False))
    print(f"\n{len(log)} issue lines across {log.file.nunique()} files -> docs/data_issues_log.csv")


if __name__ == "__main__":
    main()
