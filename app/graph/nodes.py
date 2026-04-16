"""LangGraph node functions — one per workflow step."""

import asyncio
import os

import structlog

from app.enrichment.apollo import get_apollo_adapter
from app.enrichment.linkedin import LinkedInAdapter
from app.enrichment.verify import CONFIDENCE_DELTAS, verify_email
from app.enrichment.zoominfo import get_zoominfo_adapter
from app.graph.state import LeadState
from app.reconcile.rules import reconcile
from app.scoring.scorer import compute_score
from app.vector.cache import VectorCache

log = structlog.get_logger()


def _get_cache() -> VectorCache:
    return VectorCache()


def _get_linkedin() -> LinkedInAdapter:
    return LinkedInAdapter(access_token=os.environ["LINKEDIN_ACCESS_TOKEN"])


# ── Nodes ─────────────────────────────────────────────────────────────────────

async def check_cache(state: LeadState) -> LeadState:
    cache = _get_cache()
    lead = state["raw_lead"]
    cached = await cache.lookup(lead)
    if cached:
        log.info("cache_hit", lead_id=state["lead_id"])
        return {**state, "cache_hit": True, "reconciled": cached, "action": "update"}
    return {**state, "cache_hit": False}


async def identify_source(state: LeadState) -> LeadState:
    """No-op routing node — both sources are always attempted in parallel."""
    return {**state, "linkedin_data": None, "zoominfo_data": None}


async def linkedin_enrich(state: LeadState) -> LeadState:
    adapter = _get_linkedin()
    try:
        result = await adapter.enrich(state["raw_lead"])
        return {**state, "linkedin_data": dict(result) if result else None}
    except Exception as exc:
        log.warning("linkedin_enrich_failed", lead_id=state["lead_id"], error=str(exc))
        return {**state, "linkedin_data": None}
    finally:
        await adapter.aclose()


async def zoominfo_enrich(state: LeadState) -> LeadState:
    adapter = get_zoominfo_adapter()
    try:
        result = await adapter.enrich(state["raw_lead"])
        return {**state, "zoominfo_data": dict(result) if result else None}
    except Exception as exc:
        log.warning("zoominfo_enrich_failed", lead_id=state["lead_id"], error=str(exc))
        return {**state, "zoominfo_data": None}


async def apollo_enrich(state: LeadState) -> LeadState:
    adapter = get_apollo_adapter()
    try:
        result = await adapter.enrich(state["raw_lead"])
        return {**state, "apollo_data": dict(result) if result else None}
    except Exception as exc:
        log.warning("apollo_enrich_failed", lead_id=state["lead_id"], error=str(exc))
        return {**state, "apollo_data": None}


async def reconcile_data(state: LeadState) -> LeadState:
    merged, confidence = await reconcile(
        lead=state["raw_lead"],
        linkedin=state.get("linkedin_data"),
        zoominfo=state.get("zoominfo_data"),
        apollo=state.get("apollo_data"),
    )
    return {**state, "reconciled": merged, "confidence": confidence}


async def verify_email_node(state: LeadState) -> LeadState:
    """Verify the reconciled email and adjust confidence accordingly."""
    reconciled = state.get("reconciled") or {}
    email = reconciled.get("email") or (state.get("raw_lead") or {}).get("Email")

    if not email:
        return {**state, "email_verification": None}

    try:
        status = await verify_email(email)
        delta = CONFIDENCE_DELTAS[status]
        current_conf = state.get("confidence", 0.0)
        new_conf = max(0.0, min(1.0, current_conf + delta))
        log.info(
            "email_verified",
            lead_id=state["lead_id"],
            status=status.value,
            confidence_delta=delta,
        )
        return {**state, "email_verification": status.value, "confidence": new_conf}
    except Exception as exc:
        log.warning("email_verify_failed", lead_id=state["lead_id"], error=str(exc))
        return {**state, "email_verification": None}


async def score_lead(state: LeadState) -> LeadState:
    """
    Compute the three-signal lead score (completeness + activity + MC engagement).

    Requires sf_access_token and sf_instance_url to be present in state so the
    activity sub-scorer can query Task/Event via the SF REST API. If credentials
    are absent (e.g. during unit tests), the node logs a warning and sets
    lead_score to None rather than crashing the workflow.
    """
    access_token = state.get("sf_access_token")
    instance_url = state.get("sf_instance_url")

    if not access_token or not instance_url:
        log.warning("score_lead_skipped", lead_id=state["lead_id"], reason="no_sf_credentials")
        return {**state, "lead_score": None, "score_breakdown": None}

    try:
        result = await compute_score(
            lead_id=state["lead_id"],
            raw_lead=state["raw_lead"],
            reconciled=state.get("reconciled"),
            access_token=access_token,
            instance_url=instance_url,
        )
        log.info(
            "lead_scored",
            lead_id=state["lead_id"],
            total=result.total,
            completeness=result.completeness,
            activity=result.activity,
            marketing=result.marketing,
        )
        return {
            **state,
            "lead_score": result.total,
            "score_breakdown": {
                "completeness": result.completeness,
                "activity": result.activity,
                "marketing": result.marketing,
            },
        }
    except Exception as exc:
        log.warning("score_lead_failed", lead_id=state["lead_id"], error=str(exc))
        return {**state, "lead_score": None, "score_breakdown": None}


async def confidence_score(state: LeadState) -> LeadState:
    """Confidence is already set by reconcile_data; this node handles routing."""
    threshold = float(os.getenv("CONFIDENCE_THRESHOLD", "0.80"))
    action: str
    if state.get("reconciled") is None:
        action = "skip"
    elif state.get("confidence", 0.0) >= threshold:
        action = "update"
    else:
        action = "review"
    return {**state, "action": action}


async def update_cache(state: LeadState) -> LeadState:
    if state.get("reconciled") and not state.get("cache_hit"):
        cache = _get_cache()
        await cache.store(state["raw_lead"], state["reconciled"])
    return state


async def route_output(state: LeadState) -> LeadState:
    """Terminal node — action field drives post-graph handling in the task."""
    log.info(
        "lead_routed",
        lead_id=state["lead_id"],
        action=state.get("action"),
        confidence=state.get("confidence"),
        cache_hit=state.get("cache_hit"),
    )
    return state
