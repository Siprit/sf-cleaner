"""Tests for the stats read path and MetricsWriter."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.stats import MetricsWriter, _confidence_bucket, fetch_stats


# ── confidence bucket ─────────────────────────────────────────────────────────

def test_confidence_bucket_high():
    assert _confidence_bucket(0.97) == "0.95-1.00"

def test_confidence_bucket_medium():
    assert _confidence_bucket(0.83) == "0.80-0.90"

def test_confidence_bucket_low():
    assert _confidence_bucket(0.30) == "0.00-0.60"

def test_confidence_bucket_boundary():
    assert _confidence_bucket(0.95) == "0.95-1.00"
    assert _confidence_bucket(0.80) == "0.80-0.90"


# ── MetricsWriter ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_metrics_writer_record_batch():
    # pipeline() is a sync method in redis.asyncio — use MagicMock, not AsyncMock
    mock_pipe = AsyncMock()
    mock_pipe.incrby = MagicMock()
    mock_pipe.execute = AsyncMock()

    mock_pipeline_ctx = MagicMock()
    mock_pipeline_ctx.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipeline_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_r = AsyncMock()
    mock_r.pipeline = MagicMock(return_value=mock_pipeline_ctx)
    mock_r.aclose = AsyncMock()

    with patch("app.api.stats._redis", return_value=mock_r):
        writer = MetricsWriter()
        await writer.record_batch_result(total=100, updated=80, review=10)
        await writer.aclose()

    mock_r.pipeline.assert_called_once_with(transaction=False)
    assert mock_pipe.incrby.call_count == 4  # total 24h, total 7d, updated last_run, review_queue


# ── fetch_stats ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_stats_returns_expected_shape():
    mock_r = AsyncMock()
    mock_r.mget = AsyncMock(return_value=["50", "200", "300", "1000", "80", "15", "2024-01-01T02:00:00+00:00"])
    mock_r.hgetall = AsyncMock(side_effect=[
        {"linkedin": "120", "zoominfo": "90", "apollo": "30"},  # hits
        {"linkedin": "200", "zoominfo": "180", "apollo": "100"},  # attempts
    ])
    mock_r.zrange = AsyncMock(return_value=[("0.80-0.90", 60.0), ("0.90-0.95", 40.0)])
    mock_r.aclose = AsyncMock()

    with patch("app.api.stats._redis", return_value=mock_r):
        stats = await fetch_stats()

    assert "cache_hit_rate" in stats
    assert "24h" in stats["cache_hit_rate"]
    assert "7d" in stats["cache_hit_rate"]
    assert stats["leads_processed"]["24h"] == 200
    assert stats["leads_enriched_last_run"] == 80
    assert stats["review_queue_size"] == 15
    assert "linkedin" in stats["per_provider_match_rate"]
    assert "confidence_distribution" in stats
    assert stats["cache_hit_rate"]["24h"] == round(50 / 200, 3)
