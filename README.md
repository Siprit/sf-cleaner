# sf-cleaner

Enriches stale Salesforce lead records (email, phone, firmographics) at scale using LinkedIn Sales Navigator, ZoomInfo, and Apollo as sources of truth. Designed to process 500k+ leads nightly with near-zero LLM API cost.

---

## How it works

```
Airflow DAG (nightly 2am)
    │
    ▼
Salesforce Bulk API 2.0  ──  read stale leads (LastModifiedDate > 180d OR missing email/phone)
    │
    ▼
Redis + Celery  ──  fan out into batches of 1,000 leads
    │
    ▼  (per lead, in parallel workers)
LangGraph Workflow
    ├── check_cache          vector DB lookup — skip if recently enriched (90-day TTL)
    ├── linkedin_enrich      LinkedIn Sales Navigator People Search
    ├── zoominfo_enrich      ZoomInfo Enrich API (pluggable; stub until credentials available)
    ├── apollo_enrich        Apollo People Match (third-priority fallback)
    ├── reconcile_data       rule engine → Ollama fallback only for genuine conflicts (~10%)
    ├── verify_email_node    ZeroBounce / NeverBounce email verification
    ├── score_lead           composite lead score (completeness + activity + marketing)
    ├── confidence_score     score ≥ 0.80 → auto-update │ score < 0.80 → human review queue
    └── update_cache         store result in vector DB for future runs
    │
    ▼
Salesforce Bulk API 2.0  ──  write updated fields back to Lead records
    (Email, Phone, Lead_Score__c, Company_Size__c, Industry_Vertical__c,
     Annual_Revenue__c, Tech_Stack__c)
```

### Cost control

| Strategy | Effect |
|---|---|
| Vector cache (90-day TTL, cosine ≥ 0.92) | Skip re-enriching the same person across runs |
| Cross-lead dedup via vector similarity | Skip enriching duplicate SF records |
| Rule-based reconciliation first | Handles ~90% of conflicts with zero LLM calls |
| Ollama `llama3.1:8b` for the remaining ~10% | Self-hosted — zero API cost |

**Estimated LLM API spend: $0.**

---

## Tech stack

| Layer | Technology |
|---|---|
| Workflow | LangGraph |
| Vector DB | pgvector (default) or Pinecone |
| Embeddings | OpenAI `text-embedding-3-small` |
| LLM fallback | Ollama (`llama3.1:8b`) — self-hosted |
| Task queue | Celery + Redis |
| Scheduler | Apache Airflow |
| Salesforce | `simple-salesforce` + Bulk API 2.0 |
| LinkedIn | Sales Navigator People Search API |
| ZoomInfo | Enrich API (stub until keys available) |
| Apollo | People Match API (third-priority fallback) |
| Email verification | ZeroBounce (primary) + NeverBounce (fallback) |
| API | FastAPI |

---

## Project structure

```
sf-cleaner/
├── app/
│   ├── salesforce/       # OAuth JWT Bearer client + Bulk API 2.0 read/write
│   ├── enrichment/       # Pluggable source adapters (LinkedIn, ZoomInfo, Apollo, email verify)
│   ├── graph/            # LangGraph workflow (nodes, state, compiled graph)
│   ├── vector/           # Vector store abstraction, embeddings, cache
│   ├── reconcile/        # Rule engine + Ollama fallback
│   ├── scoring/          # Composite lead score (completeness, activity, marketing)
│   ├── tasks/            # Celery app + enrichment task
│   ├── scheduler/        # Airflow DAG
│   └── api/              # FastAPI service (health, trigger, stats, webhook)
├── force-app/            # Salesforce custom field metadata (Lead_Score__c, firmographics)
├── tests/
├── docker-compose.yml    # Redis, pgvector, Ollama, API, Celery worker
├── pyproject.toml
└── .env.example
```

---

## Getting started

### 1. Configure environment

```bash
cp .env.example .env
```

Fill in:
- `SF_USERNAME`, `SF_CONSUMER_KEY`, `SF_PRIVATE_KEY_PATH` — Salesforce Connected App (JWT Bearer flow)
- `LINKEDIN_ACCESS_TOKEN` — LinkedIn Sales Navigator long-lived token
- `OPENAI_API_KEY` — for `text-embedding-3-small` embeddings
- `ZEROBOUNCE_API_KEY` — email verification (optional; NeverBounce as fallback)

Optional adapters (disabled by default):
- `ZOOMINFO_ENABLED=true` + credentials
- `APOLLO_ENABLED=true` + `APOLLO_API_KEY`

### 2. Salesforce Connected App setup

Create a Connected App with **JWT Bearer** flow:
1. In Setup → App Manager → New Connected App
2. Enable OAuth, add scopes: `api`, `refresh_token`, `offline_access`
3. Enable **Use digital signatures**, upload your RSA public key
4. Note the Consumer Key → `SF_CONSUMER_KEY`

Custom fields required on the Lead object (metadata in `force-app/`):
- `Lead_Score__c` (Number) — composite lead quality score 0–100
- `Company_Size__c` (Picklist) — headcount bucket
- `Industry_Vertical__c` (Text) — industry vertical
- `Annual_Revenue__c` (Text) — annual revenue string
- `Tech_Stack__c` (Long Text Area) — comma-separated technologies

Generate a key pair if needed:
```bash
mkdir -p certs
openssl genrsa -out certs/sf_private_key.pem 2048
openssl req -new -x509 -key certs/sf_private_key.pem -out certs/sf_certificate.crt -days 365
```

### 3. Start services

```bash
docker compose up -d
```

This starts: Redis, pgvector (PostgreSQL), Ollama, Celery worker, FastAPI API.

### 4. Pull the local LLM

```bash
docker compose exec ollama ollama pull llama3.1:8b
```

### 5. Install and test

```bash
pip install -e ".[dev]"
pytest
```

---

## Running enrichment

**Automatic** — the Airflow DAG (`sf_lead_enrichment`) runs nightly at 2am.

**Manual trigger** via API:

```bash
# Trigger enrichment for up to 1,000 leads
curl -X POST http://localhost:8000/trigger \
  -H "Content-Type: application/json" \
  -d '{"limit": 1000}'

# Check task status
curl http://localhost:8000/tasks/<task_id>
```

---

## Intent webhook

External systems can push real-time signals (job changes, funding events, web visits) to trigger immediate re-enrichment of a lead:

```bash
# Compute HMAC-SHA256 signature
BODY='{"lead_id": "00Q000001", "signal": "job_change", "source": "clearbit"}'
SIG=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$INTENT_WEBHOOK_SECRET" | awk '{print $2}')

curl -X POST http://localhost:8000/webhook/intent \
  -H "Content-Type: application/json" \
  -H "X-Signature: sha256=$SIG" \
  -d "$BODY"
```

Supported signals: `job_change`, `funding`, `web_visit`.

The webhook invalidates the vector cache for that lead and dispatches a high-priority (9) Celery task.

---

## Observability

```bash
# JSON metrics (cache hit rate, confidence distribution, provider match rates)
curl http://localhost:8000/stats

# CSV export for non-engineers
curl -o stats.csv http://localhost:8000/stats/export
```

Metrics tracked in Redis:
- `cache_hit_rate` (24h + 7d rolling)
- `confidence_distribution` (bucketed: 0.95–1.00, 0.90–0.95, 0.80–0.90, 0.60–0.80, 0.00–0.60)
- `per_provider_match_rate` (linkedin, zoominfo, apollo)
- `review_queue_size`
- `leads_enriched_last_run`

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `VECTOR_BACKEND` | `pgvector` | `pgvector` or `pinecone` |
| `CACHE_TTL_DAYS` | `90` | Days before a cached enrichment expires |
| `CACHE_SIMILARITY_THRESHOLD` | `0.92` | Cosine similarity required for a cache hit |
| `CONFIDENCE_THRESHOLD` | `0.80` | Score cutoff for auto-updating Salesforce |
| `BATCH_SIZE` | `1000` | Leads per Celery task |
| `OLLAMA_MODEL` | `llama3.1:8b` | Local model for conflict resolution |
| `ZOOMINFO_ENABLED` | `false` | Set `true` once ZoomInfo credentials are available |
| `APOLLO_ENABLED` | `false` | Set `true` once Apollo API key is available |
| `INTENT_WEBHOOK_SECRET` | — | HMAC-SHA256 shared secret for webhook validation |

---

## Reconciliation logic

Three-source waterfall: **LinkedIn > ZoomInfo > Apollo**.

When sources return different values for the same field:

1. All valid sources agree → use it (confidence 1.00)
2. Only one source has a valid value → use it (confidence 0.90)
3. LinkedIn + ZoomInfo conflict, both valid → prefer ZoomInfo for email, LinkedIn for phone (confidence 0.80)
4. LinkedIn + ZoomInfo both miss, Apollo has a value → use Apollo (confidence 0.75)
5. None of the above → ask Ollama to decide (variable confidence)

After reconciliation, **email verification** adjusts confidence:
- Valid: +0.10
- Invalid: −0.20
- Catch-all: ±0.00
- Unknown: −0.05

Leads where the final confidence falls below `CONFIDENCE_THRESHOLD` are written to a **review queue** for human validation rather than auto-applied.

Firmographic fields (`company_size`, `industry`, `annual_revenue`, `tech_stack`) use a simple first-non-null waterfall: LinkedIn → ZoomInfo → Apollo.

---

## Lead scoring

Each lead receives a composite score (0–100) written to `Lead_Score__c`:

| Signal | Weight | Source |
|---|---|---|
| Completeness | 30% | Local calculation — how many fields are populated post-enrichment |
| Activity | 35% | Salesforce Task/Event queries via REST API |
| Marketing engagement | 35% | Marketing Cloud email engagement metrics |

---

## Development

```bash
# Lint
ruff check .

# Type check
mypy app

# Run tests
pytest --tb=short -v
```
