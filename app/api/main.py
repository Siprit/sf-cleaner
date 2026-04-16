"""FastAPI service — health checks, run stats, manual trigger."""

from fastapi import FastAPI, HTTPException
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
    if req.limit:
        # Inject LIMIT if not already present
        if "LIMIT" not in soql.upper():
            soql = soql + f"\nLIMIT {req.limit}"

    client = SalesforceClient()
    batch: list[dict] = []
    task_ids: list[str] = []

    batch_size = 1000
    for lead in client.iter_stale_leads(soql):
        batch.append(lead.model_dump())
        if len(batch) >= batch_size:
            result = enrich_lead_batch.delay(batch)
            task_ids.append(result.id)
            batch = []

    if batch:
        result = enrich_lead_batch.delay(batch)
        task_ids.append(result.id)

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
