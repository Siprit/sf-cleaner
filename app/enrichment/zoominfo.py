"""ZoomInfo Enrich API adapter (stub until credentials are available)."""

import os

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.enrichment.base import EnrichedContact, EnrichmentSource


class ZoomInfoAdapter(EnrichmentSource):
    BASE_URL = "https://api.zoominfo.com/enrich"

    def __init__(self, client_id: str, private_key: str):
        self._client_id = client_id
        self._private_key = private_key
        self._jwt_token: str | None = None
        self._client = httpx.AsyncClient(timeout=10.0)

    def _get_token(self) -> str:
        """Exchange client_id + private_key for a short-lived JWT."""
        resp = httpx.post(
            "https://api.zoominfo.com/authenticate",
            json={"username": self._client_id, "password": self._private_key},
        )
        resp.raise_for_status()
        return resp.json()["jwt"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def enrich(self, lead: dict) -> EnrichedContact | None:
        if not self._jwt_token:
            self._jwt_token = self._get_token()

        payload = {
            "matchPersonInput": [
                {
                    "firstName": lead.get("first_name", ""),
                    "lastName": lead.get("last_name", ""),
                    "companyName": lead.get("company", ""),
                }
            ],
            "outputFields": ["email", "phone", "jobTitle", "companyName"],
        }

        resp = await self._client.post(
            f"{self.BASE_URL}/person",
            headers={"Authorization": f"Bearer {self._jwt_token}"},
            json=payload,
        )

        if resp.status_code == 401:
            # Token expired — refresh once
            self._jwt_token = self._get_token()
            return await self.enrich(lead)

        if resp.status_code == 404:
            return None

        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            return None

        person = data[0].get("outputFields", {})
        return EnrichedContact(
            email=person.get("email"),
            phone=person.get("phone"),
            title=person.get("jobTitle"),
            company=person.get("companyName"),
            source="zoominfo",
        )

    async def aclose(self) -> None:
        await self._client.aclose()


class ZoomInfoStubAdapter(EnrichmentSource):
    """Drop-in stub for development — returns None for every lead."""

    async def enrich(self, lead: dict) -> EnrichedContact | None:
        return None


def get_zoominfo_adapter() -> EnrichmentSource:
    enabled = os.getenv("ZOOMINFO_ENABLED", "false").lower() == "true"
    if not enabled:
        return ZoomInfoStubAdapter()
    return ZoomInfoAdapter(
        client_id=os.environ["ZOOMINFO_CLIENT_ID"],
        private_key=os.environ["ZOOMINFO_PRIVATE_KEY"],
    )
