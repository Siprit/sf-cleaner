"""Score lead record completeness based on field presence and quality."""
from __future__ import annotations

import re

# Weights must sum to 100
_FIELD_WEIGHTS: dict[str, int] = {
    "Email": 30,
    "Phone": 20,
    "Company": 20,
    "Title": 15,
    "FirstName": 8,
    "LastName": 7,
}

_EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
_PHONE_RE = re.compile(r"[\d\s\-\(\)\+]{7,}")


def _has_value(field: str, value: str | None) -> bool:
    if not value or not value.strip():
        return False
    if field == "Email":
        return bool(_EMAIL_RE.match(value.strip()))
    if field == "Phone":
        return bool(_PHONE_RE.search(value))
    return True


def score_completeness(raw_lead: dict, reconciled: dict | None) -> float:
    """
    Return completeness score 0–100.

    Uses reconciled email/phone (post-enrichment) so the score reflects the
    actual data quality *after* the enrichment run, not just the stale SF state.
    """
    merged = {**raw_lead}
    if reconciled:
        # reconciled dict uses lowercase keys; map to SF field names
        if reconciled.get("email"):
            merged["Email"] = reconciled["email"]
        if reconciled.get("phone"):
            merged["Phone"] = reconciled["phone"]

    score = 0.0
    for field, weight in _FIELD_WEIGHTS.items():
        if _has_value(field, merged.get(field)):
            score += weight

    return round(score, 1)
