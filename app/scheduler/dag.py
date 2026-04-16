"""Airflow DAG: nightly Salesforce lead enrichment."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "sf-cleaner",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

BATCH_SIZE = 1000


def extract_stale_leads(**context):
    """Pull stale leads from Salesforce and publish batches to the Celery queue."""
    from app.salesforce.client import SalesforceClient
    from app.tasks.enrichment_task import enrich_lead_batch

    client = SalesforceClient()
    batch: list[dict] = []
    task_ids: list[str] = []

    for lead in client.iter_stale_leads():
        batch.append(lead.model_dump())
        if len(batch) >= BATCH_SIZE:
            result = enrich_lead_batch.delay(batch)
            task_ids.append(result.id)
            batch = []

    if batch:
        result = enrich_lead_batch.delay(batch)
        task_ids.append(result.id)

    context["ti"].xcom_push(key="task_ids", value=task_ids)
    context["ti"].xcom_push(key="total_batches", value=len(task_ids))
    return {"queued_batches": len(task_ids)}


def wait_for_enrichment(**context):
    """Poll until all Celery tasks complete (simple sensor pattern)."""
    import time
    from celery.result import AsyncResult
    from app.tasks.celery_app import celery_app

    task_ids: list[str] = context["ti"].xcom_pull(key="task_ids")
    timeout = 3 * 3600  # 3 hours max
    poll_interval = 30
    elapsed = 0

    pending = set(task_ids)
    while pending and elapsed < timeout:
        done = {tid for tid in pending if AsyncResult(tid, app=celery_app).ready()}
        pending -= done
        if pending:
            time.sleep(poll_interval)
            elapsed += poll_interval

    if pending:
        raise RuntimeError(f"{len(pending)} enrichment tasks did not complete within timeout")


def report_results(**context):
    """Aggregate task results and log a summary."""
    import logging
    from celery.result import AsyncResult
    from app.tasks.celery_app import celery_app

    log = logging.getLogger(__name__)
    task_ids: list[str] = context["ti"].xcom_pull(key="task_ids")

    totals = {"total": 0, "updated": 0, "review": 0, "skipped": 0, "errors": 0}
    for tid in task_ids:
        result = AsyncResult(tid, app=celery_app).result or {}
        for k in totals:
            totals[k] += result.get(k, 0)

    log.info("enrichment_run_complete", extra=totals)
    return totals


with DAG(
    dag_id="sf_lead_enrichment",
    default_args=DEFAULT_ARGS,
    description="Nightly Salesforce lead enrichment via LinkedIn + ZoomInfo",
    schedule="0 2 * * *",   # 2am daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["salesforce", "enrichment"],
) as dag:

    extract = PythonOperator(
        task_id="extract_stale_leads",
        python_callable=extract_stale_leads,
    )

    wait = PythonOperator(
        task_id="wait_for_enrichment",
        python_callable=wait_for_enrichment,
    )

    report = PythonOperator(
        task_id="report_results",
        python_callable=report_results,
    )

    extract >> wait >> report
