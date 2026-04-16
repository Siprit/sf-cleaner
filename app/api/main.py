"""FastAPI service — health checks, run stats, manual trigger, intent webhook, observability."""

import hashlib
import hmac
import os

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from app.salesforce.client import SalesforceClient
from app.tasks.enrichment_task import enrich_lead_batch

app = FastAPI(title="sf-cleaner", version="0.1.0")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


# ── Manual trigger ────────────────────────────────────────────────────────────

class TriggerRequest(BaseModel):
    limit: int = 1000
    soql_override: str | None = None


@app.post("/trigger")
def trigger_enrichment(req: TriggerRequest):
    """Manually kick off an enrichment run (useful for testing)."""
    from app.salesforce.client import STALE_LEADS_SOQL

    soql = req.soql_override or STALE_LEADS_SOQL
    if req.limit and "LIMIT" not in soql.upper():
        soql = soql + f"\nLIMIT {req.limit}"

    client = SalesforceClient()
    batch: list[dict] = []
    task_ids: list[str] = []

    for lead in client.iter_stale_leads(soql):
        batch.append(lead.model_dump())
        if len(batch) >= 1000:
            task_ids.append(enrich_lead_batch.delay(batch).id)
            batch = []

    if batch:
        task_ids.append(enrich_lead_batch.delay(batch).id)

    return {"queued_tasks": len(task_ids), "task_ids": task_ids}


# ── Task status ───────────────────────────────────────────────────────────────

@app.get("/tasks/{task_id}")
def task_status(task_id: str):
    from celery.result import AsyncResult
    from app.tasks.celery_app import celery_app

    result = AsyncResult(task_id, app=celery_app)
    return {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }


# ── Intent webhook ────────────────────────────────────────────────────────────

class IntentPayload(BaseModel):
    lead_id: str
    signal: str   # job_change | funding | web_visit
    source: str


_VALID_SIGNALS = {"job_change", "funding", "web_visit"}


def _verify_hmac(body: bytes, signature: str) -> bool:
    secret = os.getenv("INTENT_WEBHOOK_SECRET", "")
    if not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.removeprefix("sha256="))


@app.post("/webhook/intent")
async def intent_webhook(
    request: Request,
    x_signature: str = Header(alias="X-Signature"),
):
    """
    Receives an intent signal for a lead (job change, funding, web visit).
    Validates HMAC-SHA256, invalidates the vector cache for the lead,
    then re-queues enrichment at high priority (9).
    """
    body = await request.body()

    if not _verify_hmac(body, x_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = IntentPayload.model_validate_json(body)

    if payload.signal not in _VALID_SIGNALS:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown signal '{payload.signal}'. Valid: {sorted(_VALID_SIGNALS)}",
        )

    # Invalidate vector cache so the next enrichment run fetches fresh data
    await _invalidate_cache(payload.lead_id)

    # Re-enrich this lead at high priority (Celery priority 9 = highest)
    task = enrich_lead_batch.apply_async(
        args=[[{"id": payload.lead_id}]],
        priority=9,
    )

    return {"accepted": True, "lead_id": payload.lead_id, "task_id": task.id}


async def _invalidate_cache(lead_id: str) -> None:
    """Remove the vector cache entry for a single lead by its SF ID."""
    import hashlib

    from app.vector.store import get_vector_store

    # The cache key is sha256 of the lead identity text; for direct-ID invalidation
    # we store a secondary id→vector_id mapping. As a simpler approach we store the
    # SF lead ID as the vector_id directly when the lead has no name/company yet.
    store = get_vector_store()
    # pgvector: delete by id; Pinecone: delete by id
    try:
        if hasattr(store, "_conn"):
            # pgvector
            with store._conn.cursor() as cur:
                cur.execute("DELETE FROM lead_vectors WHERE id = %s", (lead_id,))
            store._conn.commit()
        else:
            # Pinecone
            store._index.delete(ids=[lead_id])
    except Exception:
        pass  # Cache invalidation is best-effort


# ── Observability ─────────────────────────────────────────────────────────────

@app.get("/stats")
async def get_stats():
    """Return enrichment metrics from Redis."""
    from app.api.stats import fetch_stats
    return await fetch_stats()


@app.get("/stats/export")
async def export_stats():
    """Download enrichment metrics as CSV."""
    import csv
    import io

    from fastapi.responses import StreamingResponse

    from app.api.stats import fetch_stats

    data = await fetch_stats()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["metric", "value"])
    for key, value in data.items():
        if isinstance(value, dict):
            for subkey, subval in value.items():
                writer.writerow([f"{key}.{subkey}", subval])
        else:
            writer.writerow([key, value])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sf-cleaner-stats.csv"},
    )
