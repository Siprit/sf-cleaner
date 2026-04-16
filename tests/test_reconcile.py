"""Unit tests for the rule-based reconciliation engine."""

import pytest

from app.reconcile.rules import reconcile

LEAD = {"id": "abc", "first_name": "Jane", "last_name": "Doe", "company": "Acme"}


@pytest.mark.asyncio
async def test_only_linkedin_has_email():
    merged, conf = await reconcile(
        LEAD,
        linkedin={"email": "jane@acme.com", "phone": None},
        zoominfo={"email": None, "phone": None},
    )
    assert merged["email"] == "jane@acme.com"
    assert conf >= 0.85


@pytest.mark.asyncio
async def test_both_sources_agree():
    merged, conf = await reconcile(
        LEAD,
        linkedin={"email": "jane@acme.com", "phone": None},
        zoominfo={"email": "jane@acme.com", "phone": None},
    )
    assert merged["email"] == "jane@acme.com"
    assert conf == 1.0


@pytest.mark.asyncio
async def test_email_conflict_prefers_zoominfo():
    merged, conf = await reconcile(
        LEAD,
        linkedin={"email": "jane.doe@acme.com", "phone": None},
        zoominfo={"email": "jdoe@acme.com", "phone": None},
    )
    assert merged["email"] == "jdoe@acme.com"
    assert conf == 0.80


@pytest.mark.asyncio
async def test_phone_conflict_prefers_linkedin():
    merged, conf = await reconcile(
        LEAD,
        linkedin={"email": None, "phone": "+1-800-555-0001"},
        zoominfo={"email": None, "phone": "+1-800-555-0002"},
    )
    assert merged["phone"] == "+1-800-555-0001"
    assert conf == 0.80


@pytest.mark.asyncio
async def test_no_data_returns_empty():
    merged, conf = await reconcile(LEAD, linkedin=None, zoominfo=None)
    assert merged == {}
    assert conf == 0.0


@pytest.mark.asyncio
async def test_both_sources_missing():
    merged, conf = await reconcile(
        LEAD,
        linkedin={"email": None, "phone": None},
        zoominfo={"email": None, "phone": None},
    )
    assert merged == {}
    assert conf == 0.0
