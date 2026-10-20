from __future__ import annotations

from api.query_service import QueryService
from api.tools import TOOL_SPECS, ToolRegistry


def registry() -> ToolRegistry:
    return ToolRegistry(QueryService())


def test_exposes_only_three_read_only_tools() -> None:
    names = [spec["function"]["name"] for spec in TOOL_SPECS]
    assert names == ["get_catalog_metadata", "query_metrics", "query_timeseries"]
    assert all("sql" not in name for name in names)


def test_query_metrics_returns_numbers_and_sql() -> None:
    result = registry().execute("query_metrics", {"symbol": "BTCUSDT", "window": "24h"})
    assert result.ok is True
    assert result.payload["symbol"] == "BTCUSDT"
    assert result.payload["current_price"] > 0
    assert result.payload["vwap"] > 0
    assert result.sql and "serving.realtime_metrics" in result.sql
    assert result.provenance == "SPEED_LAYER"


def test_query_timeseries_returns_candles() -> None:
    result = registry().execute(
        "query_timeseries",
        {"symbol": "ETHUSDT", "interval": "15m", "limit": 10},
    )
    assert result.ok is True
    assert len(result.payload["candles"]) == 10
    assert result.payload["candles"][0]["open"] > 0
    assert "LIMIT 10" in (result.sql or "")
    assert result.cutoff_time


def test_catalog_metadata_contains_owner_and_lineage() -> None:
    result = registry().execute("get_catalog_metadata", {"dataset": "market_ohlcv"})
    assert result.ok is True
    assert "Data Engineering" in result.payload["owner"]
    assert result.payload["lineage"]
    assert result.provenance == "BATCH_LAYER"


def test_rejects_unknown_tool() -> None:
    result = registry().execute("run_shell", {"command": "whoami"})
    assert result.ok is False
    assert "no autorizada" in (result.error or "")


def test_rejects_invalid_arguments() -> None:
    result = registry().execute("query_timeseries", {"symbol": "BTCUSDT", "interval": "15m", "limit": 500})
    assert result.ok is False
    assert "invÃ¡lidos" in (result.error or "")
