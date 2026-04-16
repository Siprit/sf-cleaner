"""Apollo.io People Match enrichment adapter."""

from __future__ import annotations

import os

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.enrichment.base import EnrichedContact, EnrichmentSource


class ApolloAdapter(EnrichmentSource):
    BASE_URL = "https://api.apollo.io/v1"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            headers={"Cache-Control": "no-cache", "Content-Type": "application/json"},
            timeout=10.0,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def enrich(self, lead: dict) -> EnrichedContact | None:
        first = lead.get("first_name", "")
        last = lead.get("last_name", "")
        company = lead.get("company", "")

        if not (first or last) or not company:
            return None

        resp = await self._client.post(
            f"{self.BASE_URL}/people/match",
            json={
                "api_key": self._api_key,
                "first_name": first,
                "last_name": last,
                "organization_name": company,
                "reveal_personal_emails": True,
            },
        )

        if resp.status_code == 404:
            return None
        resp.raise_for_status()

        person = resp.json().get("person")
        if not person:
            return None

        emails = person.get("email_statuses") or []
        email = next((e["email"] for e in emails if e.get("status") == "verified"), None)
        if not email:
            email = person.get("email")

        phone_numbers = person.get("phone_numbers") or []
        phone = phone_numbers[0].get("sanitized_number") if phone_numbers else None

        employment = (person.get("employment_history") or [{}])[0]

        return EnrichedContact(
            email=email,
            phone=phone,
            title=person.get("title"),
            company=employment.get("organization_name") or company,
            # Firmographic fields
            company_size=_headcount_bucket(person.get("organization", {}).get("estimated_num_employees")),
            industry=person.get("organization", {}).get("industry"),
            annual_revenue=person.get("organization", {}).get("annual_revenue_printed"),
            tech_stack=_extract_tech_stack(person.get("organization", {})),
            source="apollo",
        )

    async def aclose(self) -> None:
        await self._client.aclose()


class ApolloStubAdapter(EnrichmentSource):
    """Drop-in stub for development — returns None for every lead."""

    async def enrich(self, lead: dict) -> EnrichedContact | None:
        return None


def get_apollo_adapter() -> EnrichmentSource:
    enabled = os.getenv("APOLLO_ENABLED", "false").lower() == "true"
    if not enabled:
        return ApolloStubAdapter()
    return ApolloAdapter(api_key=os.environ["APOLLO_API_KEY"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _headcount_bucket(num: int | None) -> str | None:
    if num is None:
        return None
    for threshold, label in [(10, "1-10"), (50, "11-50"), (200, "51-200"),
                              (500, "201-500"), (1000, "501-1000"), (5000, "1001-5000")]:
        if num <= threshold:
            return label
    return "5000+"


def _extract_tech_stack(org: dict) -> str | None:
    techs = org.get("technology_names") or []
    return ", ".join(techs[:10]) if techs else None
