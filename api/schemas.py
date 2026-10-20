"""
Modelos Pydantic y esquemas de validación para la Serving API (US-22).
Garantiza contratos de respuesta tipados y validación estricta de parámetros de entrada.
"""

from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator
import re

SYMBOL_REGEX = re.compile(r"^[A-Z0-9_-]{2,20}$")

class IntervalEnum(str, Enum):
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"

class HealthResponse(BaseModel):
    status: str = Field(..., description="Estado de salud general (healthy, degraded, unhealthy)")
    service: str = Field(default="serving-api", description="Nombre del servicio")
    architecture: str = Field(default="Lambda (Batch + Speed + Serving)", description="Patrón arquitectónico")
    dependencies: Dict[str, str] = Field(..., description="Estado de conectividad de servicios dependientes")
    timestamp_utc: datetime = Field(..., description="Marca temporal UTC de la verificación")

class AssetItem(BaseModel):
    symbol: str = Field(..., description="Símbolo del par (ej. BTCUSDT)")
    name: str = Field(..., description="Nombre descriptivo del activo")
    exchange: str = Field(default="binance", description="Exchange de cotización")
    base_asset: str = Field(..., description="Activo base (ej. BTC)")
    quote_asset: str = Field(..., description="Activo cotizado (ej. USDT)")
    status: str = Field(default="ACTIVE", description="Estado operativo (ACTIVE, INACTIVE, DELISTED)")
    is_active: bool = Field(default=True, description="Indicador booleano de operatividad")
    created_at: Optional[datetime] = Field(None, description="Fecha de registro")

class AssetsResponse(BaseModel):
    total: int = Field(..., description="Total de activos que coinciden con los filtros")
    page: int = Field(..., description="Número de página actual")
    page_size: int = Field(..., description="Cantidad de elementos por página")
    items: List[AssetItem] = Field(..., description="Lista de activos")

class OHLCVItem(BaseModel):
    symbol: str = Field(..., description="Símbolo del par")
    timeframe: str = Field(..., description="Temporalidad de la vela (ej. 1h)")
    bucket_start: datetime = Field(..., description="Inicio de la ventana temporal")
    bucket_end: datetime = Field(..., description="Fin de la ventana temporal")
    open_price: float = Field(..., description="Precio de apertura")
    high_price: float = Field(..., description="Precio máximo observado")
    low_price: float = Field(..., description="Precio mínimo observado")
    close_price: float = Field(..., description="Precio de cierre")
    volume: float = Field(..., description="Volumen acumulado")
    trades_count: int = Field(default=0, description="Número de transacciones en la ventana")
    source_layer: str = Field(..., description="Capa de procedencia: 'batch' o 'speed'")
    is_consolidated: bool = Field(default=False, description="True si fue consolidado por la Batch Layer")
    updated_at: Optional[datetime] = Field(None, description="Marca temporal de actualización")

class OHLCVResponse(BaseModel):
    symbol: str = Field(..., description="Símbolo consultado")
    interval: str = Field(..., description="Temporalidad consultada")
    count: int = Field(..., description="Cantidad de velas devueltas")
    cutoff_timestamp: Optional[datetime] = Field(None, description="Fecha de corte de consolidación batch")
    data: List[OHLCVItem] = Field(..., description="Series de velas OHLCV ordenadas cronológicamente")

class MetricItem(BaseModel):
    symbol: str = Field(..., description="Símbolo del activo")
    last_price: float = Field(..., description="Último precio observado en el mercado")
    rolling_volume_5m: Optional[float] = Field(None, description="Volumen móvil acumulado en 5 minutos")
    pct_change_5m: Optional[float] = Field(None, description="Variación porcentual en 5 minutos")
    event_timestamp: datetime = Field(..., description="Marca temporal de generación del evento en el exchange")
    ingestion_timestamp: Optional[datetime] = Field(None, description="Marca temporal de captura")
    last_updated_utc: datetime = Field(..., description="Momento de cálculo / reconciliación")
    source: str = Field(default="Speed Layer (Spark Structured Streaming)", description="Origen del cálculo")

class MetricsResponse(BaseModel):
    count: int = Field(..., description="Cantidad de métricas")
    last_updated_utc: datetime = Field(..., description="Marca temporal de la respuesta")
    items: List[MetricItem] = Field(..., description="Lista de métricas operativas")

class PipelineStatusItem(BaseModel):
    component: str = Field(..., description="Nombre del componente del pipeline")
    last_run: Optional[datetime] = Field(None, description="Última ejecución registrada")
    status: str = Field(..., description="Estado del componente: SUCCESS, RUNNING, FAILED, WARNING")
    consumer_lag: int = Field(default=0, description="Lag de consumidores Kafka en mensajes")
    metrics_summary: Optional[Dict[str, Any]] = Field(None, description="Resumen numérico operativo")
    updated_at: datetime = Field(..., description="Momento de reporte")

class PipelineStatusResponse(BaseModel):
    cluster_status: str = Field(..., description="Estado consolidado de la plataforma")
    timestamp_utc: datetime = Field(..., description="Marca temporal de auditoría")
    freshness_seconds: Optional[float] = Field(None, description="Segundos transcurridos desde el último dato ingerido")
    components: List[PipelineStatusItem] = Field(..., description="Estado individual de cada componente")
