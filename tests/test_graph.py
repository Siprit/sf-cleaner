"""Integration tests for the LangGraph enrichment workflow (mocked adapters)."""

from unittest.mock import AsyncMock, patch

import pytest

from app.graph.state import LeadState
from app.graph.workflow import enrichment_graph

LEAD = {
    "id": "00Q000001",
    "first_name": "Jane",
    "last_name": "Doe",
    "email": None,
    "phone": None,
    "company": "Acme Corp",
    "title": "VP Sales",
}


@pytest.mark.asyncio
async def test_full_workflow_auto_update():
    """Happy path: both sources return data → action=update."""
    with (
        patch("app.graph.nodes._get_cache") as mock_cache_factory,
        patch("app.graph.nodes._get_linkedin") as mock_li_factory,
        patch("app.enrichment.zoominfo.get_zoominfo_adapter") as mock_zi_factory,
    ):
        mock_cache = AsyncMock()
        mock_cache.lookup.return_value = None   # cache miss
        mock_cache.store.return_value = None
        mock_cache_factory.return_value = mock_cache

        mock_li = AsyncMock()
        mock_li.enrich.return_value = {"email": "jane@acme.com", "phone": None, "source": "linkedin"}
        mock_li.aclose.return_value = None
        mock_li_factory.return_value = mock_li

        mock_zi = AsyncMock()
        mock_zi.enrich.return_value = {"email": "jane@acme.com", "phone": "+18005550100", "source": "zoominfo"}
        mock_zi_factory.return_value = mock_zi

        initial: LeadState = {"lead_id": LEAD["id"], "raw_lead": LEAD, "cache_hit": False, "confidence": 0.0}
        result = await enrichment_graph.ainvoke(initial)

    assert result["action"] == "update"
    assert result["reconciled"]["email"] == "jane@acme.com"
    assert result["confidence"] >= 0.80


@pytest.mark.asyncio
async def test_cache_hit_skips_enrichment():
    """Cache hit → workflow short-circuits to route_output."""
    with patch("app.graph.nodes._get_cache") as mock_cache_factory:
        mock_cache = AsyncMock()
        mock_cache.lookup.return_value = {"email": "cached@acme.com", "phone": "+1234567890"}
        mock_cache_factory.return_value = mock_cache

        initial: LeadState = {"lead_id": LEAD["id"], "raw_lead": LEAD, "cache_hit": False, "confidence": 0.0}
        result = await enrichment_graph.ainvoke(initial)

    assert result["cache_hit"] is True
    assert result["action"] == "update"
    assert result["reconciled"]["email"] == "cached@acme.com"


@pytest.mark.asyncio
async def test_low_confidence_goes_to_review():
    """Low confidence result → action=review."""
    with (
        patch("app.graph.nodes._get_cache") as mock_cache_factory,
        patch("app.graph.nodes._get_linkedin") as mock_li_factory,
        patch("app.enrichment.zoominfo.get_zoominfo_adapter") as mock_zi_factory,
        patch("app.reconcile.llm_fallback.llm_resolve", new_callable=AsyncMock) as mock_llm,
    ):
        mock_cache = AsyncMock()
        mock_cache.lookup.return_value = None
        mock_cache.store.return_value = None
        mock_cache_factory.return_value = mock_cache

        mock_li = AsyncMock()
        mock_li.enrich.return_value = {"email": "jane1@acme.com", "phone": None, "source": "linkedin"}
        mock_li.aclose.return_value = None
        mock_li_factory.return_value = mock_li

        mock_zi = AsyncMock()
        mock_zi.enrich.return_value = {"email": "jane2@acme.com", "phone": None, "source": "zoominfo"}
        mock_zi_factory.return_value = mock_zi

        # Ollama returns low confidence
        mock_llm.return_value = ("jane1@acme.com", 0.50)

        initial: LeadState = {"lead_id": LEAD["id"], "raw_lead": LEAD, "cache_hit": False, "confidence": 0.0}
        result = await enrichment_graph.ainvoke(initial)

    assert result["action"] == "review"
    assert result["confidence"] < 0.80
