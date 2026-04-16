# sf-cleaner

Enriches stale Salesforce lead records (email, phone) at scale using LinkedIn Sales Navigator and ZoomInfo as sources of truth. Designed to process 500k+ leads nightly with near-zero LLM API cost.

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
    ├── check_cache       vector DB lookup — skip if recently enriched (90-day TTL)
    ├── linkedin_enrich   LinkedIn Sales Navigator People Search
    ├── zoominfo_enrich   ZoomInfo Enrich API (pluggable; stub until credentials available)
    ├── reconcile_data    rule engine → Ollama fallback only for genuine conflicts (~10% of leads)
    ├── confidence_score  score ≥ 0.80 → auto-update │ score < 0.80 → human review queue
    └── update_cache      store result in vector DB for future runs
    │
    ▼
Salesforce Bulk API 2.0  ──  write updated email/phone back to Lead records
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
| API | FastAPI |

---

## Project structure

```
sf-cleaner/
├── app/
│   ├── salesforce/       # OAuth JWT Bearer client + Bulk API 2.0 read/write
│   ├── enrichment/       # Pluggable source adapters (LinkedIn, ZoomInfo)
│   ├── graph/            # LangGraph workflow (nodes, state, compiled graph)
│   ├── vector/           # Vector store abstraction, embeddings, cache
│   ├── reconcile/        # Rule engine + Ollama fallback
│   ├── tasks/            # Celery app + enrichment task
│   ├── scheduler/        # Airflow DAG
│   └── api/              # FastAPI service (health, trigger, task status)
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

ZoomInfo is disabled by default (`ZOOMINFO_ENABLED=false`). Set to `true` and add credentials when available.

### 2. Salesforce Connected App setup

Create a Connected App with **JWT Bearer** flow:
1. In Setup → App Manager → New Connected App
2. Enable OAuth, add scopes: `api`, `refresh_token`, `offline_access`
3. Enable **Use digital signatures**, upload your RSA public key
4. Note the Consumer Key → `SF_CONSUMER_KEY`

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

---

## Reconciliation logic

When LinkedIn and ZoomInfo return different values for the same field:

1. Only one source has the field → use it (confidence 0.90)
2. Both sources agree → use it (confidence 1.00)
3. One source is empty → use the non-empty one (confidence 0.85)
4. Both conflict, both valid → prefer ZoomInfo for email, LinkedIn for phone (confidence 0.80)
5. None of the above → ask Ollama to decide (variable confidence)

Leads where the final confidence falls below `CONFIDENCE_THRESHOLD` are written to a **review queue** for human validation rather than auto-applied.

---

## Development

```bash
# Lint
ruff check .

# Type check
mypy app

# Run tests with coverage
pytest --tb=short -v
```
