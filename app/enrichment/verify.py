"""Email verification — ZeroBounce primary, NeverBounce fallback."""

from __future__ import annotations

import os
from enum import Enum

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class VerifyStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    CATCH_ALL = "catch_all"
    UNKNOWN = "unknown"


# Confidence deltas applied to reconciliation confidence after verification
CONFIDENCE_DELTAS: dict[VerifyStatus, float] = {
    VerifyStatus.VALID: +0.10,
    VerifyStatus.INVALID: -0.20,
    VerifyStatus.CATCH_ALL: 0.0,
    VerifyStatus.UNKNOWN: -0.05,
}


class ZeroBounceVerifier:
    BASE_URL = "https://api.zerobounce.net/v2"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = httpx.AsyncClient(timeout=10.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
    async def verify(self, email: str) -> VerifyStatus:
        resp = await self._client.get(
            f"{self.BASE_URL}/validate",
            params={"api_key": self._api_key, "email": email},
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown").lower()
        return _map_zerobounce_status(status)

    async def aclose(self) -> None:
        await self._client.aclose()


class NeverBounceVerifier:
    BASE_URL = "https://api.neverbounce.com/v4"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = httpx.AsyncClient(timeout=10.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
    async def verify(self, email: str) -> VerifyStatus:
        resp = await self._client.get(
            f"{self.BASE_URL}/single/check",
            params={"key": self._api_key, "email": email},
        )
        resp.raise_for_status()
        result_code = resp.json().get("result", "unknown")
        return _map_neverbounce_status(result_code)

    async def aclose(self) -> None:
        await self._client.aclose()


def _map_zerobounce_status(status: str) -> VerifyStatus:
    return {
        "valid": VerifyStatus.VALID,
        "invalid": VerifyStatus.INVALID,
        "catch-all": VerifyStatus.CATCH_ALL,
        "spamtrap": VerifyStatus.INVALID,
        "abuse": VerifyStatus.INVALID,
        "do_not_mail": VerifyStatus.INVALID,
    }.get(status, VerifyStatus.UNKNOWN)


def _map_neverbounce_status(code) -> VerifyStatus:
    # NeverBounce returns integer codes: 0=valid, 1=invalid, 2=disposable, 3=catchall, 4=unknown
    return {
        0: VerifyStatus.VALID,
        1: VerifyStatus.INVALID,
        2: VerifyStatus.INVALID,
        3: VerifyStatus.CATCH_ALL,
        4: VerifyStatus.UNKNOWN,
    }.get(int(code) if str(code).isdigit() else -1, VerifyStatus.UNKNOWN)


async def verify_email(email: str) -> VerifyStatus:
    """
    Verify an email address. ZeroBounce is attempted first; on failure,
    NeverBounce is used as a fallback. Returns UNKNOWN if both are unavailable.
    """
    zb_key = os.getenv("ZEROBOUNCE_API_KEY")
    nb_key = os.getenv("NEVERBOUNCE_API_KEY")

    if zb_key:
        verifier = ZeroBounceVerifier(zb_key)
        try:
            return await verifier.verify(email)
        except Exception:
            pass
        finally:
            await verifier.aclose()

    if nb_key:
        verifier = NeverBounceVerifier(nb_key)
        try:
            return await verifier.verify(email)
        except Exception:
            pass
        finally:
            await verifier.aclose()

    return VerifyStatus.UNKNOWN
