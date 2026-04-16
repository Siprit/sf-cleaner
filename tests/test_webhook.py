"""Tests for the intent webhook endpoint."""

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.main import app

client = TestClient(app)

_SECRET = "test-secret-key"
_LEAD_ID = "00Q0000001"


def _sign(body: bytes, secret: str = _SECRET) -> str:
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def _post_intent(payload: dict, secret: str = _SECRET, bad_sig: bool = False):
    body = json.dumps(payload).encode()
    sig = "sha256=badsig" if bad_sig else _sign(body, secret)
    return client.post("/webhook/intent", content=body, headers={"X-Signature": sig})


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("INTENT_WEBHOOK_SECRET", _SECRET)


def test_valid_intent_accepted():
    with (
        patch("app.api.main._invalidate_cache", new_callable=AsyncMock),
        patch("app.api.main.enrich_lead_batch") as mock_task,
    ):
        mock_result = AsyncMock()
        mock_result.id = "task-123"
        mock_task.apply_async.return_value = mock_result

        resp = _post_intent({"lead_id": _LEAD_ID, "signal": "job_change", "source": "clearbit"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] is True
    assert data["lead_id"] == _LEAD_ID
    mock_task.apply_async.assert_called_once()
    # priority=9 should be set
    _, kwargs = mock_task.apply_async.call_args
    assert kwargs.get("priority") == 9


def test_invalid_signature_rejected():
    resp = _post_intent({"lead_id": _LEAD_ID, "signal": "funding", "source": "x"}, bad_sig=True)
    assert resp.status_code == 401


def test_unknown_signal_rejected():
    with patch("app.api.main._invalidate_cache", new_callable=AsyncMock):
        resp = _post_intent({"lead_id": _LEAD_ID, "signal": "unknown_signal", "source": "x"})
    assert resp.status_code == 422


def test_all_valid_signals_accepted():
    for signal in ("job_change", "funding", "web_visit"):
        with (
            patch("app.api.main._invalidate_cache", new_callable=AsyncMock),
            patch("app.api.main.enrich_lead_batch") as mock_task,
        ):
            mock_result = AsyncMock()
            mock_result.id = "task-x"
            mock_task.apply_async.return_value = mock_result
            resp = _post_intent({"lead_id": _LEAD_ID, "signal": signal, "source": "test"})
        assert resp.status_code == 200, f"signal '{signal}' should be accepted"
