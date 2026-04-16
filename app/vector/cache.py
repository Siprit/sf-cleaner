"""Enrichment cache backed by the vector store."""

import hashlib
import os
from datetime import datetime, timedelta, timezone

from app.vector.embeddings import embed, lead_identity_text
from app.vector.store import get_vector_store


class VectorCache:
    def __init__(self):
        self._store = get_vector_store()
        self._threshold = float(os.getenv("CACHE_SIMILARITY_THRESHOLD", "0.92"))
        self._ttl_days = int(os.getenv("CACHE_TTL_DAYS", "90"))

    def _vector_id(self, lead: dict) -> str:
        text = lead_identity_text(lead)
        return hashlib.sha256(text.encode()).hexdigest()

    async def lookup(self, lead: dict) -> dict | None:
        """Return cached enrichment result if a fresh match exists, else None."""
        identity = lead_identity_text(lead)
        embedding = await embed(identity)

        results = await self._store.query(embedding, top_k=1, min_score=self._threshold)
        if not results:
            return None

        match = results[0]
        metadata = match["metadata"]

        enriched_at_str = metadata.get("enriched_at")
        if enriched_at_str:
            enriched_at = datetime.fromisoformat(enriched_at_str)
            if datetime.now(timezone.utc) - enriched_at > timedelta(days=self._ttl_days):
                return None  # TTL expired

        return {k: v for k, v in metadata.items() if k != "enriched_at"}

    async def store(self, lead: dict, enriched: dict) -> None:
        identity = lead_identity_text(lead)
        embedding = await embed(identity)
        vector_id = self._vector_id(lead)
        metadata = {
            **enriched,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        await self._store.upsert(vector_id, embedding, metadata)
