"""LangGraph node functions — one per workflow step."""

import asyncio
import os

import structlog

from app.enrichment.linkedin import LinkedInAdapter
from app.enrichment.zoominfo import get_zoominfo_adapter
from app.graph.state import LeadState
from app.reconcile.rules import reconcile
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


async def reconcile_data(state: LeadState) -> LeadState:
    merged, confidence = await reconcile(
        lead=state["raw_lead"],
        linkedin=state.get("linkedin_data"),
        zoominfo=state.get("zoominfo_data"),
    )
    return {**state, "reconciled": merged, "confidence": confidence}


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
