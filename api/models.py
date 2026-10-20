"""Pydantic contracts for the serving API and the AI chat."""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

Provenance = Literal["SPEED_LAYER", "BATCH_LAYER", "SERVING_LAYER", "RAG"]
SourceKind = Literal["rag", "tool", "database"]


class AssetInfo(BaseModel):
    symbol: str
    base: str
    quote: str
    source: str
    exchange: str
    active: bool = True


class AssetsResponse(BaseModel):
    assets: list[AssetInfo]
    cutoff_time: str


class Candle(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class OHLCVResponse(BaseModel):
    symbol: str
    interval: str
    limit: int
    candles: list[Candle]
    source: str
    provenance: Provenance = "SPEED_LAYER"
    watermark: str = "event-time · 2s"
    cutoff_time: str


class MetricsResponse(BaseModel):
    symbol: str
    window: str
    current_price: float
    return_24h: float
    volume_24h: float
    volume_base: float
    vwap: float
    price_variance: float
    open: float
    high: float
    low: float
    source: str
    provenance: Provenance = "SPEED_LAYER"
    cutoff_time: str
    freshness_seconds: float


class KafkaStatus(BaseModel):
    status: str
    consumer_lag: int
    topics: list[str]
    ingestion_rate: str
    freshness_seconds: float


class SparkStatus(BaseModel):
    status: str
    window: str
    watermark: str
    micro_batch_ms: int
    late_arrivals: int


class AirflowStatus(BaseModel):
    status: str
    dag_id: str
    last_run: str
    failed_tasks: int


class MinioStatus(BaseModel):
    status: str
    path: str
    last_sync: str
    parquet_size: str


class DLQStatus(BaseModel):
    status: str
    rejected_events: int


class PipelineStatusResponse(BaseModel):
    kafka: KafkaStatus
    spark: SparkStatus
    airflow: AirflowStatus
    minio: MinioStatus
    dlq: DLQStatus
    timestamp: str
    freshness_seconds: float


class HealthServices(BaseModel):
    serving_api: str
    postgres: str
    minio: str
    ollama: str
    rag: str


class HealthResponse(BaseModel):
    status: str
    application: str
    version: str
    environment: str
    services: HealthServices
    serving_latency_ms: float
    timestamp: str


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(
        min_length=1,
        max_length=4000,
        validation_alias=AliasChoices("message", "query"),
    )
    symbol: str = "BTCUSDT"
    interval: str = "15m"
    include_sql: bool = True


class SourceReference(BaseModel):
    source: str
    title: str | None = None
    snippet: str | None = None
    score: float | None = None
    kind: SourceKind = "rag"


class ToolCallRecord(BaseModel):
    name: str
    arguments: dict[str, object]
    result_summary: str


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    provenance: Provenance = "SERVING_LAYER"
    asset: str | None = None
    source: str | None = None
    sources: list[SourceReference] = Field(default_factory=list)
    cutoff_time: str
    disclaimer: str
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    rag_used: bool = False
    latency_ms: float
