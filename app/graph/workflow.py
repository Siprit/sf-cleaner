"""Assemble and compile the LangGraph enrichment workflow."""

from langgraph.graph import END, START, StateGraph

from app.graph.nodes import (
    apollo_enrich,
    check_cache,
    confidence_score,
    linkedin_enrich,
    reconcile_data,
    route_output,
    score_lead,
    update_cache,
    verify_email_node,
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
    g.add_node("apollo_enrich", apollo_enrich)
    g.add_node("reconcile_data", reconcile_data)
    g.add_node("verify_email_node", verify_email_node)
    g.add_node("score_lead", score_lead)
    g.add_node("confidence_score", confidence_score)
    g.add_node("update_cache", update_cache)
    g.add_node("route_output", route_output)

    g.add_edge(START, "check_cache")
    g.add_conditional_edges("check_cache", _cache_router)

    # Waterfall: LinkedIn → ZoomInfo → Apollo (each runs regardless; reconcile picks winner)
    g.add_edge("linkedin_enrich", "zoominfo_enrich")
    g.add_edge("zoominfo_enrich", "apollo_enrich")
    g.add_edge("apollo_enrich", "reconcile_data")

    # verify_email adjusts confidence before routing decision
    g.add_edge("reconcile_data", "verify_email_node")
    g.add_edge("verify_email_node", "score_lead")
    g.add_edge("score_lead", "confidence_score")
    g.add_edge("confidence_score", "update_cache")
    g.add_edge("update_cache", "route_output")
    g.add_edge("route_output", END)

    return g.compile()


# Module-level compiled graph (reused across Celery task invocations)
enrichment_graph = build_graph()
