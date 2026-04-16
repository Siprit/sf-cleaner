"""Salesforce Bulk API 2.0 client with JWT Bearer OAuth."""

import csv
import io
import time
from typing import Iterator

import httpx
import jwt
from pydantic_settings import BaseSettings

from app.salesforce.models import Lead, LeadUpdate


class SalesforceSettings(BaseSettings):
    sf_username: str
    sf_consumer_key: str
    sf_private_key_path: str
    sf_domain: str = "login"

    model_config = {"env_file": ".env", "extra": "ignore"}


STALE_LEADS_SOQL = """
SELECT Id, FirstName, LastName, Email, Phone, Company, Title
FROM Lead
WHERE LastModifiedDate < LAST_N_DAYS:180
   OR Email = null
   OR Phone = null
LIMIT 500000
""".strip()


class SalesforceClient:
    def __init__(self, settings: SalesforceSettings | None = None):
        self._settings = settings or SalesforceSettings()
        self._access_token: str | None = None
        self._instance_url: str | None = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _build_jwt(self) -> str:
        with open(self._settings.sf_private_key_path, "rb") as f:
            private_key = f.read()

        now = int(time.time())
        payload = {
            "iss": self._settings.sf_consumer_key,
            "sub": self._settings.sf_username,
            "aud": f"https://{self._settings.sf_domain}.salesforce.com",
            "exp": now + 300,
        }
        return jwt.encode(payload, private_key, algorithm="RS256")

    def authenticate(self) -> None:
        token = self._build_jwt()
        resp = httpx.post(
            f"https://{self._settings.sf_domain}.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": token,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._instance_url = data["instance_url"]

    @property
    def access_token(self) -> str:
        """Return the bearer token, authenticating lazily if needed."""
        if not self._access_token:
            self.authenticate()
        return self._access_token  # type: ignore[return-value]

    @property
    def instance_url(self) -> str:
        """Return the instance URL, authenticating lazily if needed."""
        if not self._instance_url:
            self.authenticate()
        return self._instance_url  # type: ignore[return-value]

    @property
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ── Bulk API 2.0 Read ─────────────────────────────────────────────────────

    def iter_stale_leads(self, soql: str = STALE_LEADS_SOQL) -> Iterator[Lead]:
        """Yield Lead objects from a Bulk API 2.0 query job."""
        job_id = self._create_query_job(soql)
        self._wait_for_job(job_id)
        yield from self._stream_results(job_id)

    def _create_query_job(self, soql: str) -> str:
        resp = httpx.post(
            f"{self._instance_url}/services/data/v60.0/jobs/query",
            headers=self._headers,
            json={"operation": "queryAll", "query": soql},
        )
        resp.raise_for_status()
        return resp.json()["id"]

    def _wait_for_job(self, job_id: str, poll_interval: int = 5) -> None:
        url = f"{self._instance_url}/services/data/v60.0/jobs/query/{job_id}"
        while True:
            resp = httpx.get(url, headers=self._headers)
            resp.raise_for_status()
            state = resp.json()["state"]
            if state == "JobComplete":
                return
            if state in ("Failed", "Aborted"):
                raise RuntimeError(f"Bulk query job {job_id} ended with state: {state}")
            time.sleep(poll_interval)

    def _stream_results(self, job_id: str) -> Iterator[Lead]:
        url = f"{self._instance_url}/services/data/v60.0/jobs/query/{job_id}/results"
        params: dict = {}
        while True:
            resp = httpx.get(url, headers={**self._headers, "Content-Type": "text/csv"}, params=params)
            resp.raise_for_status()

            reader = csv.DictReader(io.StringIO(resp.text))
            for row in reader:
                yield Lead(id=row["Id"], **{k: v or None for k, v in row.items() if k != "Id"})

            locator = resp.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                break
            params = {"locator": locator}

    # ── Bulk API 2.0 Write ────────────────────────────────────────────────────

    def bulk_update_leads(self, updates: list[LeadUpdate]) -> None:
        """Write email, phone, and lead score updates back to Salesforce in one bulk job.

        Lead_Score__c must exist as a Number field on the Lead object in your SF org.
        Create it via Setup → Object Manager → Lead → Fields & Relationships → New.
        """
        if not updates:
            return

        rows = [
            {
                "Id": u.id,
                "Email": u.email or "",
                "Phone": u.phone or "",
                "Lead_Score__c": str(u.lead_score) if u.lead_score is not None else "",
            }
            for u in updates
        ]
        csv_body = self._to_csv(rows, fieldnames=["Id", "Email", "Phone", "Lead_Score__c"])

        job_id = self._create_ingest_job()
        self._upload_csv(job_id, csv_body)
        self._close_job(job_id)

    def _create_ingest_job(self) -> str:
        resp = httpx.post(
            f"{self._instance_url}/services/data/v60.0/jobs/ingest",
            headers=self._headers,
            json={"object": "Lead", "operation": "update", "contentType": "CSV"},
        )
        resp.raise_for_status()
        return resp.json()["id"]

    def _upload_csv(self, job_id: str, csv_body: str) -> None:
        resp = httpx.put(
            f"{self._instance_url}/services/data/v60.0/jobs/ingest/{job_id}/batches",
            headers={**self._headers, "Content-Type": "text/csv"},
            content=csv_body.encode(),
        )
        resp.raise_for_status()

    def _close_job(self, job_id: str) -> None:
        resp = httpx.patch(
            f"{self._instance_url}/services/data/v60.0/jobs/ingest/{job_id}",
            headers=self._headers,
            json={"state": "UploadComplete"},
        )
        resp.raise_for_status()

    @staticmethod
    def _to_csv(rows: list[dict], fieldnames: list[str]) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue()
