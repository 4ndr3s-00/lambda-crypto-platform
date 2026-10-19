-- Script de inicialización de PostgreSQL para la Plataforma Lambda
-- Crea base de datos para Airflow y esquema relacional para Serving Layer

CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- Conexión a la base de datos market_platform
\c market_platform;

-- Esquema de la Serving Layer para consultas de baja latencia
CREATE SCHEMA IF NOT EXISTS serving;

-- Tabla de activos / símbolos registrados
CREATE TABLE IF NOT EXISTS serving.assets (
    symbol VARCHAR(30) PRIMARY KEY,
    name VARCHAR(100),
    asset_type VARCHAR(20) DEFAULT 'crypto',
    base_asset VARCHAR(10),
    quote_asset VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de reconciliación de velas OHLCV (fusión Batch + Speed)
CREATE TABLE IF NOT EXISTS serving.ohlcv_reconciled (
    symbol VARCHAR(30) NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- 1m, 5m, 1h, 1d
    bucket_start TIMESTAMP WITH TIME ZONE NOT NULL,
    bucket_end TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(24, 8) NOT NULL,
    high_price NUMERIC(24, 8) NOT NULL,
    low_price NUMERIC(24, 8) NOT NULL,
    close_price NUMERIC(24, 8) NOT NULL,
    volume NUMERIC(28, 8) NOT NULL,
    trades_count BIGINT DEFAULT 0,
    source_layer VARCHAR(10) NOT NULL, -- 'batch' o 'speed'
    is_consolidated BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, timeframe, bucket_start)
);

-- Tabla de métricas operativas recientes (Speed Layer)
CREATE TABLE IF NOT EXISTS serving.realtime_metrics (
    symbol VARCHAR(30) PRIMARY KEY,
    last_price NUMERIC(24, 8) NOT NULL,
    rolling_volume_5m NUMERIC(28, 8),
    pct_change_5m NUMERIC(10, 4),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Estado de pipeline y observabilidad
CREATE TABLE IF NOT EXISTS serving.pipeline_status (
    component VARCHAR(50) PRIMARY KEY,
    last_run TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20),
    consumer_lag BIGINT DEFAULT 0,
    metrics_summary JSONB,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Inserción inicial de pares cripto estándar
INSERT INTO serving.assets (symbol, name, base_asset, quote_asset)
VALUES 
    ('BTCUSDT', 'Bitcoin / Tether', 'BTC', 'USDT'),
    ('ETHUSDT', 'Ethereum / Tether', 'ETH', 'USDT'),
    ('SOLUSDT', 'Solana / Tether', 'SOL', 'USDT')
ON CONFLICT (symbol) DO NOTHING;
