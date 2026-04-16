# sf-cleaner — Claude Code instructions

This is a Python pipeline enriching stale Salesforce Lead records via LinkedIn Sales Navigator,
ZoomInfo, LangGraph, Celery, and Salesforce Bulk API 2.0.

## Active uplift tasks (work in priority order)

### 1. Email verification — `app/enrichment/verify.py`
- ZeroBounce primary, NeverBounce fallback
- New LangGraph node after `reconcile_data`, before `confidence_score`
- Score deltas: valid +0.10 / invalid -0.20 / catch_all ±0 / unknown -0.05
- Add ZEROBOUNCE_API_KEY to .env.example
- Tests: mock ZeroBounce API responses

### 2. Apollo enrichment adapter — `app/enrichment/apollo.py`
- Same interface as existing LinkedIn/ZoomInfo adapters
- Apollo People Match endpoint, keyed on name + company domain
- APOLLO_ENABLED=false default; runs as third fallback in waterfall
- Priority in reconcile: LinkedIn > ZoomInfo > Apollo

### 3. Firmographic fields
- SF custom fields: Company_Size__c, Industry_Vertical__c, Annual_Revenue__c, Tech_Stack__c
- Add field metadata under force-app/main/default/objects/Lead/fields/
- Extend LinkedIn + Apollo adapters to return these fields
- Update Bulk API write-back to include them

### 4. Intent webhook — `POST /webhook/intent`
- Accepts lead_id, signal (job_change|funding|web_visit), source
- Invalidates vector cache for that lead
- Dispatches Celery task at priority=9
- HMAC-SHA256 signature validation via INTENT_WEBHOOK_SECRET

### 5. Observability — `GET /stats` and `GET /stats/export`
- Metrics stored in Redis (incremented by Celery tasks)
- cache_hit_rate (24h + 7d), confidence_distribution, per_provider_match_rate,
  review_queue_size, leads_enriched_last_run
- CSV export endpoint for non-engineers

## Code standards
- Always run `ruff check .` and `mypy app` before finishing
- Every new module needs a corresponding test in `tests/`
- New env vars go in `.env.example` with a comment
- Follow the existing pluggable adapter interface in `app/enrichment/`
