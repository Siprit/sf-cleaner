"""Unit tests for the Apollo enrichment adapter."""

import pytest

from app.enrichment.apollo import ApolloAdapter, ApolloStubAdapter, _headcount_bucket


LEAD = {"first_name": "Jane", "last_name": "Doe", "company": "Acme Corp"}

APOLLO_PERSON_RESPONSE = {
    "person": {
        "email": "jane@acme.com",
        "email_statuses": [{"email": "jane@acme.com", "status": "verified"}],
        "phone_numbers": [{"sanitized_number": "+18005550100"}],
        "title": "VP Sales",
        "employment_history": [{"organization_name": "Acme Corp"}],
        "organization": {
            "estimated_num_employees": 120,
            "industry": "Software",
            "annual_revenue_printed": "$5M",
            "technology_names": ["Salesforce", "HubSpot", "AWS"],
        },
    }
}


@pytest.mark.asyncio
async def test_apollo_adapter_happy_path():
    from unittest.mock import AsyncMock

    adapter = ApolloAdapter("fakekey")
    adapter._client.post = AsyncMock(return_value=_mock_resp(APOLLO_PERSON_RESPONSE))

    result = await adapter.enrich(LEAD)
    await adapter.aclose()

    assert result is not None
    assert result["email"] == "jane@acme.com"
    assert result["phone"] == "+18005550100"
    assert result["source"] == "apollo"
    assert result["company_size"] == "51-200"
    assert result["industry"] == "Software"
    assert result["tech_stack"] == "Salesforce, HubSpot, AWS"


@pytest.mark.asyncio
async def test_apollo_adapter_not_found():
    from unittest.mock import AsyncMock

    adapter = ApolloAdapter("fakekey")
    adapter._client.post = AsyncMock(return_value=_mock_resp({"person": None}))

    result = await adapter.enrich(LEAD)
    await adapter.aclose()

    assert result is None


@pytest.mark.asyncio
async def test_apollo_stub_returns_none():
    stub = ApolloStubAdapter()
    result = await stub.enrich(LEAD)
    assert result is None


@pytest.mark.asyncio
async def test_apollo_skips_empty_name():
    adapter = ApolloAdapter("fakekey")
    result = await adapter.enrich({"first_name": "", "last_name": "", "company": "Acme"})
    assert result is None


def test_headcount_bucket():
    assert _headcount_bucket(5) == "1-10"
    assert _headcount_bucket(120) == "51-200"
    assert _headcount_bucket(10000) == "5000+"
    assert _headcount_bucket(None) is None


# ── Helpers ───────────────────────────────────────────────────────────────────

class _mock_resp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._data
