"""Celery task: enrich a batch of Salesforce leads."""

import asyncio
import logging

from app.graph.workflow import enrichment_graph
from app.graph.state import LeadState
from app.salesforce.client import SalesforceClient
from app.salesforce.models import LeadUpdate
from app.tasks.celery_app import celery_app

log = logging.getLogger(__name__)


async def _process_lead(lead: dict, sf_access_token: str, sf_instance_url: str) -> LeadState:
    initial: LeadState = {
        "lead_id": lead["id"],
        "raw_lead": lead,
        "cache_hit": False,
        "confidence": 0.0,
        "sf_access_token": sf_access_token,
        "sf_instance_url": sf_instance_url,
    }
    return await enrichment_graph.ainvoke(initial)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def enrich_lead_batch(self, leads: list[dict]) -> dict:
    """
    Process a batch of leads through the LangGraph enrichment workflow.

    Args:
        leads: list of lead dicts with keys matching LeadState.raw_lead

    Returns:
        Summary dict with counts of update / review / skip / error actions.
    """
    sf_client = SalesforceClient()
    # Authenticate once up front so the token is available for all per-lead
    # activity queries inside the score_lead graph node.
    access_token = sf_client.access_token
    instance_url = sf_client.instance_url

    updates: list[LeadUpdate] = []
    review: list[str] = []
    skipped: int = 0
    errors: int = 0

    async def run_batch():
        nonlocal skipped, errors
        tasks = [_process_lead(lead, access_token, instance_url) for lead in leads]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for lead, result in zip(leads, results):
            if isinstance(result, Exception):
                log.error("lead_processing_error", extra={"lead_id": lead["id"], "error": str(result)})
                errors += 1
                continue

            action = result.get("action", "skip")
            if action == "update":
                reconciled = result.get("reconciled") or {}
                updates.append(
                    LeadUpdate(
                        id=lead["id"],
                        email=reconciled.get("email"),
                        phone=reconciled.get("phone"),
                        lead_score=result.get("lead_score"),
                    )
                )
            elif action == "review":
                review.append(lead["id"])
            else:
                skipped += 1

    try:
        asyncio.run(run_batch())
        sf_client.bulk_update_leads(updates)
    except Exception as exc:
        raise self.retry(exc=exc)

    return {
        "total": len(leads),
        "updated": len(updates),
        "review": len(review),
        "skipped": skipped,
        "errors": errors,
        "review_ids": review,
    }
