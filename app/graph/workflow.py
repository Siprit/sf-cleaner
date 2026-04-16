"""Assemble and compile the LangGraph enrichment workflow."""

from langgraph.graph import END, START, StateGraph

from app.graph.nodes import (
    check_cache,
    confidence_score,
    linkedin_enrich,
    reconcile_data,
    route_output,
    update_cache,
    zoominfo_enrich,
)
from app.graph.state import LeadState


def _cache_router(state: LeadState) -> str:
    return "route_output" if state.get("cache_hit") else "linkedin_enrich"


def build_graph():
    g = StateGraph(LeadState)

    g.add_node("check_cache", check_cache)
    g.add_node("linkedin_enrich", linkedin_enrich)
    g.add_node("zoominfo_enrich", zoominfo_enrich)
    g.add_node("reconcile_data", reconcile_data)
    g.add_node("confidence_score", confidence_score)
    g.add_node("update_cache", update_cache)
    g.add_node("route_output", route_output)

    g.add_edge(START, "check_cache")
    g.add_conditional_edges("check_cache", _cache_router)

    # LinkedIn and ZoomInfo run in parallel after cache miss
    g.add_edge("linkedin_enrich", "zoominfo_enrich")   # sequential fallback; swap to fan-out if needed
    g.add_edge("zoominfo_enrich", "reconcile_data")

    g.add_edge("reconcile_data", "confidence_score")
    g.add_edge("confidence_score", "update_cache")
    g.add_edge("update_cache", "route_output")
    g.add_edge("route_output", END)

    return g.compile()


# Module-level compiled graph (reused across Celery task invocations)
enrichment_graph = build_graph()
