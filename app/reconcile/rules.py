"""Rule-based reconciliation engine. Falls back to Ollama only on genuine conflict."""

import re

from app.reconcile.llm_fallback import llm_resolve

_EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
_PHONE_RE = re.compile(r"[\d\s\-\(\)\+]{7,}")


def _valid_email(v: str | None) -> bool:
    return bool(v and _EMAIL_RE.match(v.strip()))


def _valid_phone(v: str | None) -> bool:
    return bool(v and _PHONE_RE.search(v))


async def _resolve_field(
    field: str,
    li_val: str | None,
    zi_val: str | None,
    lead: dict,
    validator,
    zi_preferred: bool = True,
    apollo_val: str | None = None,
) -> tuple[str | None, float]:
    """
    Returns (chosen_value, confidence).

    Priority: LinkedIn > ZoomInfo > Apollo.

    Rules (in order):
    1. Only one source (across L/Z/A) has the value → use it (confidence 0.90)
    2. All valid sources agree                       → use it (confidence 1.00)
    3. L/Z conflict, both valid:
       - email → prefer ZoomInfo (confidence 0.80)
       - phone → prefer LinkedIn (confidence 0.80)
    4. L/Z both invalid/missing, Apollo has a value  → use Apollo (confidence 0.75)
    5. Genuine multi-source conflict                 → escalate to Ollama
    """
    li_ok = validator(li_val)
    zi_ok = validator(zi_val)
    ap_ok = validator(apollo_val)

    # Collect valid values with source labels (priority order)
    valid_sources = [(li_val, li_ok), (zi_val, zi_ok), (apollo_val, ap_ok)]
    valid_vals = [v for v, ok in valid_sources if ok]

    if not valid_vals:
        # Nothing valid from any source — try Ollama if any raw value exists
        candidates = list(filter(None, [li_val, zi_val, apollo_val]))
        if candidates:
            chosen, confidence = await llm_resolve(field, li_val, zi_val, lead)
            return chosen, confidence
        return None, 0.0

    # All valid values agree
    if len(set(valid_vals)) == 1:
        return valid_vals[0], 1.0 if len(valid_vals) > 1 else 0.90

    # LinkedIn and ZoomInfo both valid but differ — deterministic preference
    if li_ok and zi_ok:
        preferred = zi_val if zi_preferred else li_val
        return preferred, 0.80

    # L/Z missing/invalid — fall through to Apollo
    if ap_ok and not li_ok and not zi_ok:
        return apollo_val, 0.75

    # Remaining edge case: one of L/Z valid, Apollo also valid but differs
    if li_ok:
        return li_val, 0.85
    if zi_ok:
        return zi_val, 0.85

    return valid_vals[0], 0.75


async def reconcile(
    lead: dict,
    linkedin: dict | None,
    zoominfo: dict | None,
    apollo: dict | None = None,
) -> tuple[dict, float]:
    """
    Merge LinkedIn, ZoomInfo, and Apollo data into a single result dict.
    Returns (merged_dict, overall_confidence).
    Priority: LinkedIn > ZoomInfo > Apollo.
    """
    li = linkedin or {}
    zi = zoominfo or {}
    ap = apollo or {}

    email, email_conf = await _resolve_field(
        "email",
        li.get("email"),
        zi.get("email"),
        lead,
        _valid_email,
        zi_preferred=True,   # ZoomInfo more authoritative for email
        apollo_val=ap.get("email"),
    )

    phone, phone_conf = await _resolve_field(
        "phone",
        li.get("phone"),
        zi.get("phone"),
        lead,
        _valid_phone,
        zi_preferred=False,  # LinkedIn more authoritative for phone
        apollo_val=ap.get("phone"),
    )

    merged: dict = {}
    if email:
        merged["email"] = email
    if phone:
        merged["phone"] = phone

    # Firmographic fields — waterfall: LinkedIn → ZoomInfo → Apollo (first non-None wins)
    for field in ("company_size", "industry", "annual_revenue", "tech_stack"):
        value = li.get(field) or zi.get(field) or ap.get(field)
        if value:
            merged[field] = value

    if not merged:
        return {}, 0.0

    confs = [c for c in [email_conf if email else None, phone_conf if phone else None] if c is not None]
    overall = sum(confs) / len(confs) if confs else 0.0

    return merged, overall
