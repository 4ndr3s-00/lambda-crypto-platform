"""Typed, read-only tools exposed to Qwen2.5 (US-29).

Only three capabilities exist:
    - get_catalog_metadata
    - query_metrics
    - query_timeseries

There is deliberately no shell, filesystem, DDL or free-form SQL tool.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from .query_service import QueryService, QueryServiceError

WINDOWS = ("1h", "4h", "24h", "7d")
INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d", "1D", "7D")


class CatalogArgs(BaseModel):
    dataset: str = Field(default="market_ohlcv", min_length=1, max_length=80)


class MetricsArgs(BaseModel):
    symbol: str = Field(default="BTCUSDT", min_length=3, max_length=30)
    window: Literal["1h", "4h", "24h", "7d"] = "24h"


class TimeseriesArgs(BaseModel):
    symbol: str = Field(default="BTCUSDT", min_length=3, max_length=30)
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d", "1D", "7D"] = "15m"
    limit: int = Field(default=96, ge=1, le=100)


TOOL_SPECS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_catalog_metadata",
            "description": (
                "Devuelve owner, schema version, retención, particiones y linaje de un dataset "
                "del catálogo de gobierno. Usar para preguntas de catálogo/linaje, no para precios."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "enum": ["market_ohlcv", "market_metrics"],
                        "description": "Dataset del catálogo.",
                    }
                },
                "required": ["dataset"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_metrics",
            "description": (
                "Calcula métricas agregadas de mercado (precio actual, retorno, volumen, VWAP, "
                "varianza) para un símbolo y ventana. Solo lectura."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "enum": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
                    "window": {"type": "string", "enum": list(WINDOWS)},
                },
                "required": ["symbol", "window"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_timeseries",
            "description": (
                "Devuelve velas OHLCV recientes para un símbolo, intervalo y límite <= 100. "
                "Usar para precios, máximos/mínimos, velas y series temporales."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "enum": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
                    "interval": {"type": "string", "enum": list(INTERVALS)},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                "required": ["symbol", "interval", "limit"],
                "additionalProperties": False,
            },
        },
    },
]


@dataclass
class ToolResult:
    name: str
    arguments: dict[str, Any]
    payload: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    source: str = ""
    cutoff_time: str = ""
    provenance: str = "SERVING_LAYER"
    sql: str | None = None
    ok: bool = True
    error: str | None = None


def _money(value: float) -> str:
    return f"${value:,.2f}"


class ToolRegistry:
    def __init__(self, query_service: QueryService) -> None:
        self.query_service = query_service

    @property
    def specs(self) -> list[dict[str, Any]]:
        return TOOL_SPECS

    def execute(self, name: str, arguments: dict[str, Any] | None = None) -> ToolResult:
        args = arguments or {}
        try:
            if name == "get_catalog_metadata":
                return self._catalog(CatalogArgs.model_validate(args))
            if name == "query_metrics":
                return self._metrics(MetricsArgs.model_validate(args))
            if name == "query_timeseries":
                return self._timeseries(TimeseriesArgs.model_validate(args))
            return ToolResult(
                name=name,
                arguments=args,
                ok=False,
                error=f"herramienta no autorizada: {name}",
            )
        except ValidationError as exc:
            return ToolResult(
                name=name,
                arguments=args,
                ok=False,
                error=f"argumentos inválidos: {exc.errors()}",
            )
        except (QueryServiceError, ValueError) as exc:
            return ToolResult(name=name, arguments=args, ok=False, error=str(exc))

    def _catalog(self, args: CatalogArgs) -> ToolResult:
        payload = self.query_service.get_catalog_metadata(args.dataset)
        return ToolResult(
            name="get_catalog_metadata",
            arguments=args.model_dump(),
            payload=payload,
            summary=(
                f"{payload.get('dataset', args.dataset)} · owner={payload.get('owner', 'n/d')} · "
                f"schema={payload.get('schema_version', 'n/d')}"
            ),
            source=str(payload.get("source", "governance.dataset_catalog")),
            cutoff_time=str(payload.get("cutoff_time", "")),
            provenance="BATCH_LAYER",
            sql=(
                "SELECT dataset_name, owner_team, schema_version, storage_uri, "
                "upstream_topics, downstream_consumers, last_profiled_at\n"
                "FROM governance.dataset_catalog\n"
                f"WHERE dataset_name = '{args.dataset}'\n"
                "ORDER BY last_profiled_at DESC\n"
                "LIMIT 1;"
            ),
        )

    def _metrics(self, args: MetricsArgs) -> ToolResult:
        payload = self.query_service.get_metrics(args.symbol, window=args.window)
        return ToolResult(
            name="query_metrics",
            arguments=args.model_dump(),
            payload=payload,
            summary=(
                f"{payload['symbol']} · precio={_money(payload['current_price'])} · "
                f"retorno={payload['return_24h']:+.2f}% · VWAP={_money(payload['vwap'])}"
            ),
            source=str(payload.get("source", "serving.realtime_metrics")),
            cutoff_time=str(payload.get("cutoff_time", "")),
            provenance="SPEED_LAYER",
            sql=(
                "SELECT symbol, last_price, rolling_volume_5m, pct_change_5m, event_timestamp\n"
                "FROM serving.realtime_metrics\n"
                f"WHERE symbol = '{args.symbol}'\n"
                "ORDER BY event_timestamp DESC\n"
                "LIMIT 1;"
            ),
        )

    def _timeseries(self, args: TimeseriesArgs) -> ToolResult:
        payload = self.query_service.get_ohlcv(args.symbol, args.interval, args.limit)
        candles = payload.get("candles", [])
        last = candles[-1] if candles else {}
        return ToolResult(
            name="query_timeseries",
            arguments=args.model_dump(),
            payload=payload,
            summary=(
                f"{payload['symbol']} · {payload['interval']} · {len(candles)} velas · "
                f"último close={last.get('close', 'n/d')}"
            ),
            source=str(payload.get("source", "serving.ohlcv_reconciled")),
            cutoff_time=str(payload.get("cutoff_time", "")),
            provenance="SPEED_LAYER",
            sql=(
                "SELECT bucket_start, open_price, high_price, low_price, close_price, volume\n"
                "FROM serving.ohlcv_reconciled\n"
                f"WHERE symbol = '{args.symbol}' AND timeframe = '{args.interval}'\n"
                "ORDER BY bucket_start DESC\n"
                f"LIMIT {args.limit};"
            ),
        )
