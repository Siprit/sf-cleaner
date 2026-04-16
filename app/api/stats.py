"""Redis-backed enrichment metrics — read path for /stats endpoint."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import redis.asyncio as aioredis

# ── Key schema ────────────────────────────────────────────────────────────────
# Counters:  sfcleaner:cache_hits:24h / 7d
#            sfcleaner:total:24h / 7d
#            sfcleaner:updated:last_run
#            sfcleaner:review_queue_size
# Hashes:    sfcleaner:provider_hits   {linkedin, zoominfo, apollo} → count
#            sfcleaner:provider_attempts
# Sorted set: sfcleaner:confidence_dist  member=bucket score=count
# String:    sfcleaner:last_run_at  (ISO timestamp)

_P = "sfcleaner"


def _k(*parts: str) -> str:
    return ":".join([_P, *parts])


def _redis() -> aioredis.Redis:
    return aioredis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )


# ── Write helpers (called by Celery tasks) ────────────────────────────────────

class MetricsWriter:
    """Thin async wrapper. Instantiate once per Celery task, then aclose()."""

    def __init__(self):
        self._r = _redis()

    async def record_batch_result(self, total: int, updated: int, review: int) -> None:
        async with self._r.pipeline(transaction=False) as pipe:
            pipe.incrby(_k("total", "24h"), total)
            pipe.incrby(_k("total", "7d"), total)
            pipe.incrby(_k("updated", "last_run"), updated)
            pipe.incrby(_k("review_queue_size"), review)
            await pipe.execute()

    async def record_cache_hit(self) -> None:
        async with self._r.pipeline(transaction=False) as pipe:
            pipe.incr(_k("cache_hits", "24h"))
            pipe.incr(_k("cache_hits", "7d"))
            await pipe.execute()

    async def record_provider_attempt(self, provider: str) -> None:
        await self._r.hincrby(_k("provider_attempts"), provider, 1)

    async def record_provider_hit(self, provider: str) -> None:
        await self._r.hincrby(_k("provider_hits"), provider, 1)

    async def record_confidence(self, confidence: float) -> None:
        await self._r.zincrby(_k("confidence_dist"), 1, _confidence_bucket(confidence))

    async def set_last_run_timestamp(self) -> None:
        await self._r.set(_k("last_run_at"), datetime.now(timezone.utc).isoformat())

    async def reset_last_run_counters(self) -> None:
        """Call at the start of each Airflow DAG run."""
        await self._r.set(_k("updated", "last_run"), 0)

    async def aclose(self) -> None:
        await self._r.aclose()


# ── Read path ─────────────────────────────────────────────────────────────────

async def fetch_stats() -> dict:
    r = _redis()
    try:
        mget = await r.mget(
            _k("cache_hits", "24h"),
            _k("total", "24h"),
            _k("cache_hits", "7d"),
            _k("total", "7d"),
            _k("updated", "last_run"),
            _k("review_queue_size"),
            _k("last_run_at"),
        )
        (raw_hits_24h, raw_total_24h, raw_hits_7d, raw_total_7d,
         raw_updated, raw_review, last_run_at) = mget

        provider_hits = await r.hgetall(_k("provider_hits"))
        provider_attempts = await r.hgetall(_k("provider_attempts"))
        conf_dist = await r.zrange(_k("confidence_dist"), 0, -1, withscores=True)
    finally:
        await r.aclose()

    def _i(v) -> int:
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    total_24h = _i(raw_total_24h)
    total_7d = _i(raw_total_7d)

    match_rates = {}
    for provider in ("linkedin", "zoominfo", "apollo"):
        attempts = _i(provider_attempts.get(provider))
        hits = _i(provider_hits.get(provider))
        match_rates[provider] = round(hits / attempts, 3) if attempts else 0.0

    return {
        "cache_hit_rate": {
            "24h": round(_i(raw_hits_24h) / total_24h, 3) if total_24h else 0.0,
            "7d": round(_i(raw_hits_7d) / total_7d, 3) if total_7d else 0.0,
        },
        "leads_processed": {
            "24h": total_24h,
            "7d": total_7d,
        },
        "leads_enriched_last_run": _i(raw_updated),
        "review_queue_size": _i(raw_review),
        "per_provider_match_rate": match_rates,
        "confidence_distribution": {member: int(score) for member, score in conf_dist},
        "last_run_at": last_run_at,
    }


# ── Helper ────────────────────────────────────────────────────────────────────

def _confidence_bucket(confidence: float) -> str:
    if confidence >= 0.95:
        return "0.95-1.00"
    if confidence >= 0.90:
        return "0.90-0.95"
    if confidence >= 0.80:
        return "0.80-0.90"
    if confidence >= 0.60:
        return "0.60-0.80"
    return "0.00-0.60"
