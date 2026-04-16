"""LinkedIn Sales Navigator People Search adapter."""

import asyncio

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.enrichment.base import EnrichedContact, EnrichmentSource


class LinkedInAdapter(EnrichmentSource):
    BASE_URL = "https://api.linkedin.com/v2"

    def __init__(self, access_token: str):
        self._token = access_token
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=10.0,
        )
        # Simple token bucket: 100 req/min
        self._semaphore = asyncio.Semaphore(5)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def enrich(self, lead: dict) -> EnrichedContact | None:
        name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()
        company = lead.get("company", "")

        if not name:
            return None

        async with self._semaphore:
            resp = await self._client.get(
                f"{self.BASE_URL}/salesNavigatorSearch",
                params={"keywords": f"{name} {company}", "type": "PEOPLE"},
            )

        if resp.status_code == 404:
            return None
        resp.raise_for_status()

        results = resp.json().get("elements", [])
        if not results:
            return None

        top = results[0]
        return EnrichedContact(
            email=top.get("emailAddress"),
            phone=top.get("phoneNumbers", [{}])[0].get("number"),
            title=top.get("headline"),
            company=top.get("company", {}).get("name"),
            source="linkedin",
        )

    async def aclose(self) -> None:
        await self._client.aclose()
