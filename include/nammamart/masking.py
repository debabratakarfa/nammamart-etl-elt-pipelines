"""PII masking. The SQL versions in include/sql/elt/stg_customers.sql produce
identical output, so ETL and ELT dimensions match.

Email  asha.rao12@example.com -> as***@example.com  (NULL if invalid)
Phone  9876543210             -> 98******10          (NULL if not digits)
Name   Asha Rao               -> Asha R.
(Made-up examples: no real customer values in code.)
"""
from __future__ import annotations

import re

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
PHONE_RE = re.compile(r"^[6-9][0-9]{9}$")
PIN_RE = re.compile(r"^[1-9][0-9]{5}$")


def mask_email(email: str | None) -> str | None:
    email = (email or "").strip()
    if not EMAIL_RE.fullmatch(email):
        return None
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone: str | None) -> str | None:
    phone = (phone or "").strip()
    if len(phone) < 4 or not phone.isdigit():
        return None
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]


def mask_name(name: str | None) -> str | None:
    parts = (name or "").split()
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][0]}."
