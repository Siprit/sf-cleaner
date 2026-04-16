"""Marketing Cloud engagement scorer. Disabled by default (MC_ENABLED=false).

To enable:
  MC_ENABLED=true
  MC_CLIENT_ID=<installed package client id>
  MC_CLIENT_SECRET=<installed package client secret>
  MC_SUBDOMAIN=<tenant-specific subdomain, e.g. mc563885gzs27c5t9-63k636ttgq>

The scorer uses the MC REST API to pull per-subscriber open/click counts and
translates them into a 0–35 engagement score. It is intentionally stubbed here
so the rest of the scoring pipeline works without MC credentials.
"""
from __future__ import annotations

import os

import httpx
import structlog

log = structlog.get_logger()

MAX_SCORE = 35.0


def _is_enabled() -> bool:
    return os.getenv("MC_ENABLED", "false").lower() == "true"


async def fetch_mc_score(email: str | None) -> float:
    """
    Return MC engagement score 0–35.
    Returns 0.0 immediately when MC_ENABLED is false or email is absent.
    """
    if not _is_enabled() or not email:
        return 0.0

    subdomain = os.environ["MC_SUBDOMAIN"]
    auth_url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"
    rest_base = f"https://{subdomain}.rest.marketingcloudapis.com"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Step 1: client-credentials token
            token_resp = await client.post(auth_url, json={
                "grant_type": "client_credentials",
                "client_id": os.environ["MC_CLIENT_ID"],
                "client_secret": os.environ["MC_CLIENT_SECRET"],
            })
            token_resp.raise_for_status()
            headers = {"Authorization": f"Bearer {token_resp.json()['access_token']}"}

            # Step 2: look up subscriber engagement
            # MC contacts/v1 returns engagement attributes for a known email address
            search_resp = await client.post(
                f"{rest_base}/contacts/v1/contacts/actions/search",
                headers=headers,
                json={"email": email},
            )
            search_resp.raise_for_status()
            items = search_resp.json().get("items", [])
            if not items:
                return 0.0

            contact = items[0]
            opens = contact.get("emailOpenCount") or 0
            clicks = contact.get("emailClickCount") or 0

            # opens: up to 20 pts (2 pts each, max 10 opens counted)
            # clicks: up to 15 pts (3 pts each, max 5 clicks counted)
            score = min(opens * 2, 20) + min(clicks * 3, 15)
            return round(min(score, MAX_SCORE), 1)

    except Exception as exc:
        log.warning("mc_score_failed", email=email, error=str(exc))
        return 0.0
