"""Score lead based on Salesforce activity history (Task + Event)."""
from __future__ import annotations

import re
from datetime import date, datetime

import httpx
import structlog

log = structlog.get_logger()

# Salesforce IDs are 15 or 18 alphanumeric characters — validate before embedding in SOQL
_SF_ID_RE = re.compile(r"^[a-zA-Z0-9]{15,18}$")

# Points awarded per activity type (before recency decay)
_TYPE_POINTS: dict[str, float] = {
    "Call": 3.0,
    "Email": 2.0,
    "Meeting": 3.0,
}
_DEFAULT_POINTS = 1.0

# Recency decay buckets: (max_age_days, multiplier)
_DECAY_BUCKETS: list[tuple[int, float]] = [
    (30, 1.0),
    (90, 0.7),
    (180, 0.4),
    (365, 0.2),
]

MAX_SCORE = 35.0


def _decay(activity_date: date) -> float:
    age_days = (date.today() - activity_date).days
    for max_age, multiplier in _DECAY_BUCKETS:
        if age_days <= max_age:
            return multiplier
    return 0.0


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        # Task.ActivityDate → "YYYY-MM-DD"
        # Event.ActivityDateTime → "YYYY-MM-DDTHH:MM:SS.000+0000"
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


async def fetch_activity_score(
    lead_id: str,
    access_token: str,
    instance_url: str,
) -> float:
    """
    Query Task and Event records for the lead over the last 365 days.
    Returns activity score 0–35.

    Raises ValueError if lead_id is not a valid Salesforce ID (prevents SOQL injection).
    """
    if not _SF_ID_RE.match(lead_id):
        raise ValueError(f"Invalid Salesforce ID: {lead_id!r}")

    headers = {"Authorization": f"Bearer {access_token}"}
    query_url = f"{instance_url}/services/data/v60.0/query"

    queries: list[tuple[str, str]] = [
        (
            f"SELECT ActivityDate, Type FROM Task "
            f"WHERE WhoId = '{lead_id}' AND ActivityDate >= LAST_N_DAYS:365",
            "ActivityDate",
        ),
        (
            f"SELECT ActivityDateTime, Type FROM Event "
            f"WHERE WhoId = '{lead_id}' AND ActivityDateTime >= LAST_N_DAYS:365",
            "ActivityDateTime",
        ),
    ]

    raw_score = 0.0
    async with httpx.AsyncClient(timeout=10.0) as client:
        for soql, date_field in queries:
            try:
                resp = await client.get(query_url, headers=headers, params={"q": soql})
                resp.raise_for_status()
                for rec in resp.json().get("records", []):
                    act_date = _parse_date(rec.get(date_field))
                    if not act_date:
                        continue
                    points = _TYPE_POINTS.get(rec.get("Type") or "", _DEFAULT_POINTS)
                    raw_score += points * _decay(act_date)
            except Exception as exc:
                log.warning("activity_fetch_failed", lead_id=lead_id, soql_field=date_field, error=str(exc))

    return round(min(raw_score, MAX_SCORE), 1)
