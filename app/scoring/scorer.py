"""Composite lead scorer. Combines completeness, activity, and MC engagement."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.scoring.activity import fetch_activity_score
from app.scoring.completeness import score_completeness
from app.scoring.marketing import fetch_mc_score

# Signal weights — must sum to 1.0
# completeness: 30% — reflects enrichment quality
# activity:     35% — reflects SDR engagement depth
# marketing:    35% — reflects inbound intent signals
_WEIGHTS = {
    "completeness": 0.30,
    "activity": 0.35,
    "marketing": 0.35,
}

# Max raw values for normalization (completeness is already 0–100)
_ACTIVITY_MAX = 35.0
_MARKETING_MAX = 35.0


@dataclass
class LeadScore:
    total: float        # 0–100 composite (weighted, normalized)
    completeness: float  # 0–100
    activity: float      # 0–35 raw signal
    marketing: float     # 0–35 raw signal


async def compute_score(
    lead_id: str,
    raw_lead: dict,
    reconciled: dict | None,
    access_token: str,
    instance_url: str,
) -> LeadScore:
    """
    Compute the composite lead score.

    Completeness is calculated locally (no I/O).
    Activity and MC engagement are fetched concurrently via asyncio.gather.
    """
    completeness = score_completeness(raw_lead, reconciled)

    email = (reconciled or {}).get("email") or raw_lead.get("Email")

    activity, marketing = await asyncio.gather(
        fetch_activity_score(lead_id, access_token, instance_url),
        fetch_mc_score(email),
    )

    activity_norm = (activity / _ACTIVITY_MAX) * 100
    marketing_norm = (marketing / _MARKETING_MAX) * 100

    total = (
        completeness * _WEIGHTS["completeness"]
        + activity_norm * _WEIGHTS["activity"]
        + marketing_norm * _WEIGHTS["marketing"]
    )

    return LeadScore(
        total=round(total, 1),
        completeness=completeness,
        activity=activity,
        marketing=marketing,
    )
