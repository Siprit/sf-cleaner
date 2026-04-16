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
) -> tuple[str | None, float]:
    """
    Returns (chosen_value, confidence).

    Rules (in priority order):
    1. Only one source has the value → use it (confidence 0.9)
    2. Both agree                    → use it (confidence 1.0)
    3. One is empty                  → use the non-empty one (confidence 0.85)
    4. Both conflict, both valid     → prefer ZoomInfo for email, LinkedIn for phone (confidence 0.80)
    5. Neither heuristic applies     → escalate to Ollama (variable confidence)
    """
    li_ok = validator(li_val)
    zi_ok = validator(zi_val)

    # Rule 1 & 3: only one source
    if li_ok and not zi_ok:
        return li_val, 0.90
    if zi_ok and not li_ok:
        return zi_val, 0.90

    # Rule 2: both agree
    if li_ok and zi_ok and li_val == zi_val:
        return li_val, 1.0

    # Rule 4: both valid but differ → deterministic preference
    if li_ok and zi_ok:
        preferred = zi_val if zi_preferred else li_val
        return preferred, 0.80

    # Rule 5: no valid value from either source → try Ollama
    if li_val or zi_val:
        chosen, confidence = await llm_resolve(field, li_val, zi_val, lead)
        return chosen, confidence

    return None, 0.0


async def reconcile(
    lead: dict,
    linkedin: dict | None,
    zoominfo: dict | None,
) -> tuple[dict, float]:
    """
    Merge LinkedIn and ZoomInfo data into a single result dict.
    Returns (merged_dict, overall_confidence).
    """
    li = linkedin or {}
    zi = zoominfo or {}

    email, email_conf = await _resolve_field(
        "email",
        li.get("email"),
        zi.get("email"),
        lead,
        _valid_email,
        zi_preferred=True,   # ZoomInfo more authoritative for email
    )

    phone, phone_conf = await _resolve_field(
        "phone",
        li.get("phone"),
        zi.get("phone"),
        lead,
        _valid_phone,
        zi_preferred=False,  # LinkedIn more authoritative for phone
    )

    merged = {}
    if email:
        merged["email"] = email
    if phone:
        merged["phone"] = phone

    if not merged:
        return {}, 0.0

    # Overall confidence = weighted mean of present fields
    confs = [c for c in [email_conf if email else None, phone_conf if phone else None] if c is not None]
    overall = sum(confs) / len(confs) if confs else 0.0

    return merged, overall
