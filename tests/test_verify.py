"""Unit tests for email verification."""

from unittest.mock import AsyncMock, patch

import pytest

from app.enrichment.verify import (
    NeverBounceVerifier,
    VerifyStatus,
    ZeroBounceVerifier,
    verify_email,
)


@pytest.mark.asyncio
async def test_zerobounce_valid():
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.__aenter__ = AsyncMock()
        verifier = ZeroBounceVerifier("fakekey")
        verifier._client.get = AsyncMock(return_value=_mock_resp({"status": "valid"}))
        result = await verifier.verify("jane@acme.com")
        await verifier.aclose()
    assert result == VerifyStatus.VALID


@pytest.mark.asyncio
async def test_zerobounce_invalid():
    verifier = ZeroBounceVerifier("fakekey")
    verifier._client.get = AsyncMock(return_value=_mock_resp({"status": "invalid"}))
    result = await verifier.verify("bad@nowhere.com")
    await verifier.aclose()
    assert result == VerifyStatus.INVALID


@pytest.mark.asyncio
async def test_zerobounce_catch_all():
    verifier = ZeroBounceVerifier("fakekey")
    verifier._client.get = AsyncMock(return_value=_mock_resp({"status": "catch-all"}))
    result = await verifier.verify("info@acme.com")
    await verifier.aclose()
    assert result == VerifyStatus.CATCH_ALL


@pytest.mark.asyncio
async def test_neverbounce_valid():
    verifier = NeverBounceVerifier("fakekey")
    verifier._client.get = AsyncMock(return_value=_mock_resp({"result": 0}))
    result = await verifier.verify("jane@acme.com")
    await verifier.aclose()
    assert result == VerifyStatus.VALID


@pytest.mark.asyncio
async def test_neverbounce_catchall():
    verifier = NeverBounceVerifier("fakekey")
    verifier._client.get = AsyncMock(return_value=_mock_resp({"result": 3}))
    result = await verifier.verify("all@acme.com")
    await verifier.aclose()
    assert result == VerifyStatus.CATCH_ALL


@pytest.mark.asyncio
async def test_verify_email_uses_zerobounce_first():
    with patch.dict("os.environ", {"ZEROBOUNCE_API_KEY": "zbkey", "NEVERBOUNCE_API_KEY": "nbkey"}):
        with patch("app.enrichment.verify.ZeroBounceVerifier") as MockZB:
            instance = AsyncMock()
            instance.verify = AsyncMock(return_value=VerifyStatus.VALID)
            instance.aclose = AsyncMock()
            MockZB.return_value = instance

            result = await verify_email("jane@acme.com")

    assert result == VerifyStatus.VALID
    instance.verify.assert_called_once_with("jane@acme.com")


@pytest.mark.asyncio
async def test_verify_email_falls_back_to_neverbounce():
    with patch.dict("os.environ", {"ZEROBOUNCE_API_KEY": "zbkey", "NEVERBOUNCE_API_KEY": "nbkey"}):
        with (
            patch("app.enrichment.verify.ZeroBounceVerifier") as MockZB,
            patch("app.enrichment.verify.NeverBounceVerifier") as MockNB,
        ):
            zb = AsyncMock()
            zb.verify = AsyncMock(side_effect=Exception("ZB down"))
            zb.aclose = AsyncMock()
            MockZB.return_value = zb

            nb = AsyncMock()
            nb.verify = AsyncMock(return_value=VerifyStatus.VALID)
            nb.aclose = AsyncMock()
            MockNB.return_value = nb

            result = await verify_email("jane@acme.com")

    assert result == VerifyStatus.VALID


@pytest.mark.asyncio
async def test_verify_email_returns_unknown_when_no_keys():
    with patch.dict("os.environ", {}, clear=True):
        result = await verify_email("jane@acme.com")
    assert result == VerifyStatus.UNKNOWN


# ── Helpers ───────────────────────────────────────────────────────────────────

class _mock_resp:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data
