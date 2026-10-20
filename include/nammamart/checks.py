"""Single catalog of data quality checks.

Both pipelines implement exactly these checks (pandas in ETL, SQL in ELT) and
use these codes in failure_reason, so their results can be compared row for row.

Severity
  Critical: the row is quarantined and never reaches reporting tables.
  Warning:  the row loads, the issue is logged in dq_results / dq_warnings.

Design principle for dimensions: a master row is rejected only when a value
that feeds a finance number or a join key is wrong. Descriptive gaps
(missing zone, category, bad email) are warnings, because quarantining the
master row would cascade and wrongly drop every real order that references it.
"""
from __future__ import annotations

# code: (entity, dimension, severity, description)
CHECKS: dict[str, tuple[str, str, str, str]] = {
    # ---------------- stores
    "st_store_id_missing":     ("stores", "Completeness", "Critical", "store_id is empty"),
    "st_store_id_duplicate":   ("stores", "Uniqueness", "Critical", "store_id appears more than once"),
    "st_zone_missing":         ("stores", "Completeness", "Warning", "zone is empty (loaded as 'Unassigned')"),
    "st_zone_invalid":         ("stores", "Validity", "Warning", "zone not in North/South/East/West"),
    "st_pincode_invalid":      ("stores", "Validity", "Warning", "pincode is not 6 digits"),
    "st_capacity_invalid":     ("stores", "Accuracy", "Warning", "capacity_orders_per_day is missing or <= 0"),
    "st_open_date_future":     ("stores", "Timeliness", "Critical", "open_date missing, unparseable or after the export snapshot"),
    # ---------------- products
    "pr_product_id_missing":   ("products", "Completeness", "Critical", "product_id is empty"),
    "pr_product_id_duplicate": ("products", "Uniqueness", "Critical", "product_id appears more than once (the '(old)' row is rejected)"),
    "pr_category_missing":     ("products", "Completeness", "Warning", "category is empty (loaded as 'Uncategorised')"),
    "pr_mrp_invalid":          ("products", "Accuracy", "Critical", "mrp is missing or <= 0"),
    "pr_cost_price_invalid":   ("products", "Accuracy", "Critical", "cost_price is missing or <= 0"),
    "pr_cost_above_mrp":       ("products", "Accuracy", "Warning", "cost_price > mrp (negative margin on every sale)"),
    "pr_launch_date_future":   ("products", "Timeliness", "Critical", "launch_date missing, unparseable or after the export snapshot"),
    # ---------------- customers
    "cu_customer_id_missing":  ("customers", "Completeness", "Critical", "customer_id is empty"),
    "cu_customer_id_duplicate":("customers", "Uniqueness", "Critical", "customer_id appears more than once (first row kept)"),
    "cu_email_invalid":        ("customers", "Validity", "Warning", "email is missing or not a valid address"),
    "cu_phone_invalid":        ("customers", "Validity", "Warning", "phone is not a 10-digit Indian mobile number"),
    "cu_pincode_invalid":      ("customers", "Validity", "Warning", "pincode is not a valid 6-digit PIN"),
    "cu_city_missing":         ("customers", "Completeness", "Warning", "city is empty"),
    "cu_loyalty_tier_casing":  ("customers", "Consistency", "Warning", "loyalty_tier casing differs (standardised)"),
    "cu_loyalty_tier_invalid": ("customers", "Validity", "Warning", "loyalty_tier not in Bronze/Silver/Gold/Platinum"),
    "cu_signup_date_future":   ("customers", "Timeliness", "Critical", "signup_date is missing, unparseable or after the export snapshot"),
    # ---------------- orders
    "or_order_id_missing":     ("orders", "Completeness", "Critical", "order_id is empty"),
    "or_order_id_duplicate":   ("orders", "Uniqueness", "Critical", "order_id repeated in the same file (first row kept)"),
    "or_customer_id_missing":  ("orders", "Completeness", "Critical", "customer_id is empty"),
    "or_order_ts_invalid":     ("orders", "Validity", "Critical", "order_ts missing or not YYYY-MM-DD HH:MM:SS"),
    "or_order_ts_future":      ("orders", "Timeliness", "Critical", "order_ts is after the export snapshot"),
    "or_status_invalid":       ("orders", "Validity", "Critical", "order_status not in DELIVERED/CANCELLED/RETURNED"),
    "or_status_casing":        ("orders", "Consistency", "Warning", "order_status casing differs (standardised)"),
    "or_payment_mode_invalid": ("orders", "Validity", "Critical", "payment_mode cannot be mapped to an allowed value"),
    "or_payment_mode_nonstandard": ("orders", "Consistency", "Warning", "payment_mode spelling/casing differs (standardised)"),
    "or_quantity_invalid":     ("orders", "Accuracy", "Critical", "quantity is missing or <= 0"),
    "or_discount_invalid":     ("orders", "Validity", "Critical", "discount_pct missing or outside 0-100"),
    "or_amount_mismatch":      ("orders", "Accuracy", "Critical", "order_amount != quantity x unit_price x (1 - discount/100)"),
    "or_unit_price_above_mrp": ("orders", "Accuracy", "Critical", "unit_price above the product MRP (illegal in India; catches price drift)"),
    "or_delivery_missing":     ("orders", "Completeness", "Critical", "DELIVERED order has no delivery_minutes"),
    "or_delivery_implausible": ("orders", "Accuracy", "Critical", "delivery_minutes <= 0 or above 120"),
    "or_delivery_outside_band":("orders", "Accuracy", "Warning", "delivery_minutes outside the normal 8-35 band"),
    "or_store_unknown":        ("orders", "Consistency", "Critical", "store_id not in the clean store master"),
    "or_product_unknown":      ("orders", "Consistency", "Critical", "product_id not in the clean product master"),
    "or_customer_unknown":     ("orders", "Consistency", "Critical", "customer_id not in the clean customer master"),
    "or_volume_drop":          ("orders", "Volume", "Warning", "file has >90% fewer rows than the previous load of the same file"),
    "or_freshness_stale":      ("orders", "Timeliness", "Warning", "latest order_ts is more than 1 day older than the export snapshot"),
    # ---------------- payments
    "pa_payment_id_missing":   ("payments", "Completeness", "Critical", "payment_id is empty"),
    "pa_payment_id_duplicate": ("payments", "Uniqueness", "Critical", "payment_id repeated (first row kept)"),
    "pa_payment_ts_invalid":   ("payments", "Validity", "Critical", "payment_ts missing or unparseable"),
    "pa_payment_ts_future":    ("payments", "Timeliness", "Critical", "payment_ts is after the export snapshot"),
    "pa_status_invalid":       ("payments", "Validity", "Critical", "payment_status not in SUCCESS/FAILED/REFUNDED"),
    "pa_payment_mode_invalid": ("payments", "Validity", "Critical", "payment_mode cannot be mapped to an allowed value"),
    "pa_payment_mode_nonstandard": ("payments", "Consistency", "Warning", "payment_mode spelling/casing differs (standardised)"),
    "pa_amount_invalid":       ("payments", "Accuracy", "Critical", "amount_paid missing, negative, or zero on a SUCCESS payment"),
    "pa_order_unknown":        ("payments", "Consistency", "Critical", "order_id never received in any orders file"),
    "pa_gateway_ref_missing":  ("payments", "Completeness", "Warning", "non-cash payment has no gateway_ref"),
}

FILE_LEVEL_CHECKS = {"or_volume_drop", "or_freshness_stale"}


def checks_for(entity: str) -> list[str]:
    return [c for c, v in CHECKS.items() if v[0] == entity]


def is_critical(code: str) -> bool:
    return CHECKS[code][2] == "Critical"


def catalog_rows() -> list[tuple]:
    return [(code, *v) for code, v in CHECKS.items()]
