from abc import ABC, abstractmethod
from typing import TypedDict


class EnrichedContact(TypedDict, total=False):
    email: str | None
    phone: str | None
    title: str | None
    company: str | None
    source: str
    # Firmographic (returned by Apollo + LinkedIn where available)
    company_size: str | None        # headcount bucket e.g. "51-200"
    industry: str | None
    annual_revenue: str | None
    tech_stack: str | None          # comma-separated top technologies


class EnrichmentSource(ABC):
    """All data source adapters implement this interface."""

    @abstractmethod
    async def enrich(self, lead: dict) -> EnrichedContact | None:
        """
        Return normalized contact data for the given lead, or None if not found.

        Args:
            lead: dict with keys: first_name, last_name, email, phone, company, title
        """
