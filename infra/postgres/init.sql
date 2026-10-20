-- ==============================================================================
-- SCRIPT DE INICIALIZACIÓN DE POSTGRESQL PARA LA PLATAFORMA LAMBDA (US-04)
-- Responsable: Samuel Roncancio (Storage & Database Lead)
-- ==============================================================================

-- 1. Creación de Bases de Datos Independientes (Criterio 1 BDD)
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

SELECT 'CREATE DATABASE serving_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'serving_db')\gexec

SELECT 'CREATE DATABASE metastore_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metastore_db')\gexec

SELECT 'CREATE DATABASE openmetadata_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db')\gexec

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- 2. Creación de Usuarios de Servicio con Mínimo Privilegio (Criterio 1 BDD)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
        CREATE USER airflow_user WITH ENCRYPTED PASSWORD 'airflow_secure_pass123';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'serving_user') THEN
        CREATE USER serving_user WITH ENCRYPTED PASSWORD 'serving_secure_pass123';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metastore_user') THEN
        CREATE USER metastore_user WITH ENCRYPTED PASSWORD 'metastore_secure_pass123';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata_user') THEN
        CREATE USER openmetadata_user WITH ENCRYPTED PASSWORD 'openmetadata_secure_pass123';
    END IF;
END $$;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE serving_db TO serving_user;
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO metastore_user;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE serving_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO postgres;

-- 3. Estructura Relacional en serving_db (Criterio 2 BDD)
\c serving_db;

CREATE SCHEMA IF NOT EXISTS serving;
GRANT ALL ON SCHEMA serving TO postgres;
GRANT ALL ON SCHEMA serving TO serving_user;
GRANT USAGE ON SCHEMA serving TO serving_user;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA serving 
    GRANT ALL ON TABLES TO serving_user;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA serving 
    GRANT ALL ON SEQUENCES TO serving_user;

-- 3.1 Dimensión de Activos (dim_assets)
CREATE TABLE IF NOT EXISTS serving.dim_assets (
    symbol VARCHAR(30) PRIMARY KEY,
    exchange VARCHAR(30) NOT NULL DEFAULT 'binance',
    asset_name VARCHAR(100) NOT NULL,
    base_asset VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    is_active BOOLEAN GENERATED ALWAYS AS (status = 'ACTIVE') STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_assets_exchange 
    ON serving.dim_assets (exchange);

CREATE INDEX IF NOT EXISTS idx_dim_assets_status 
    ON serving.dim_assets (status);

CREATE INDEX IF NOT EXISTS idx_dim_assets_base_quote 
    ON serving.dim_assets (base_asset, quote_asset);

CREATE OR REPLACE VIEW serving.assets AS
SELECT 
    symbol, 
    asset_name AS name, 
    'crypto'::VARCHAR(20) AS asset_type,
    base_asset, 
    quote_asset, 
    (status = 'ACTIVE') AS is_active, 
    created_at
FROM serving.dim_assets;

-- 3.2 Registro de Ejecución y Auditoría de Pipelines (pipeline_execution_log)
CREATE TABLE IF NOT EXISTS serving.pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    latency_ms INTEGER,
    cutoff_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    records_processed BIGINT DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_exec_name_time 
    ON serving.pipeline_execution_log (pipeline_name, execution_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_exec_cutoff 
    ON serving.pipeline_execution_log (cutoff_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_exec_status 
    ON serving.pipeline_execution_log (status);

-- 3.3 Reconciliación de velas OHLCV (fusión Batch + Speed de US-21)
CREATE TABLE IF NOT EXISTS serving.ohlcv_reconciled (
    symbol VARCHAR(30) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    bucket_start TIMESTAMP WITH TIME ZONE NOT NULL,
    bucket_end TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(24, 8) NOT NULL,
    high_price NUMERIC(24, 8) NOT NULL,
    low_price NUMERIC(24, 8) NOT NULL,
    close_price NUMERIC(24, 8) NOT NULL,
    volume NUMERIC(28, 8) NOT NULL,
    trades_count BIGINT DEFAULT 0,
    source_layer VARCHAR(10) NOT NULL,
    is_consolidated BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, timeframe, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_tf_bucket 
    ON serving.ohlcv_reconciled (symbol, timeframe, bucket_start DESC);

-- 3.4 Métricas operativas recientes (Speed Layer)
CREATE TABLE IF NOT EXISTS serving.realtime_metrics (
    symbol VARCHAR(30) PRIMARY KEY,
    last_price NUMERIC(24, 8) NOT NULL,
    rolling_volume_5m NUMERIC(28, 8),
    pct_change_5m NUMERIC(10, 4),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3.5 Estado del pipeline y observabilidad
CREATE TABLE IF NOT EXISTS serving.pipeline_status (
    component VARCHAR(50) PRIMARY KEY,
    last_run TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20),
    consumer_lag BIGINT DEFAULT 0,
    metrics_summary JSONB,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL ON ALL TABLES IN SCHEMA serving TO serving_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA serving TO serving_user;

-- 3.6 Inserción inicial de pares cripto estándar
INSERT INTO serving.dim_assets (symbol, exchange, asset_name, base_asset, quote_asset, status)
VALUES 
    ('BTCUSDT', 'binance', 'Bitcoin / Tether', 'BTC', 'USDT', 'ACTIVE'),
    ('ETHUSDT', 'binance', 'Ethereum / Tether', 'ETH', 'USDT', 'ACTIVE'),
    ('SOLUSDT', 'binance', 'Solana / Tether', 'SOL', 'USDT', 'ACTIVE')
ON CONFLICT (symbol) DO UPDATE 
SET asset_name = EXCLUDED.asset_name,
    exchange = EXCLUDED.exchange,
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO serving.pipeline_execution_log (pipeline_name, execution_timestamp, latency_ms, cutoff_timestamp, records_processed, status)
VALUES
    ('batch_recompute', CURRENT_TIMESTAMP - INTERVAL '1 hour', 4250, CURRENT_TIMESTAMP - INTERVAL '1 hour', 1000, 'SUCCESS')
ON CONFLICT DO NOTHING;

-- 4. Replicación en market_platform para retrocompatibilidad
\c market_platform;

CREATE SCHEMA IF NOT EXISTS serving;
GRANT ALL ON SCHEMA serving TO postgres;

CREATE TABLE IF NOT EXISTS serving.dim_assets (
    symbol VARCHAR(30) PRIMARY KEY,
    exchange VARCHAR(30) NOT NULL DEFAULT 'binance',
    asset_name VARCHAR(100) NOT NULL,
    base_asset VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    is_active BOOLEAN GENERATED ALWAYS AS (status = 'ACTIVE') STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_assets_exchange ON serving.dim_assets (exchange);
CREATE INDEX IF NOT EXISTS idx_dim_assets_status ON serving.dim_assets (status);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'serving' AND table_name = 'assets') THEN
        EXECUTE 'CREATE VIEW serving.assets AS SELECT symbol, asset_name AS name, ''crypto''::VARCHAR(20) AS asset_type, base_asset, quote_asset, (status = ''ACTIVE'') AS is_active, created_at FROM serving.dim_assets';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS serving.pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    latency_ms INTEGER,
    cutoff_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    records_processed BIGINT DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipe_exec_name_time 
    ON serving.pipeline_execution_log (pipeline_name, execution_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pipe_exec_cutoff 
    ON serving.pipeline_execution_log (cutoff_timestamp DESC);

CREATE TABLE IF NOT EXISTS serving.ohlcv_reconciled (
    symbol VARCHAR(30) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    bucket_start TIMESTAMP WITH TIME ZONE NOT NULL,
    bucket_end TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(24, 8) NOT NULL,
    high_price NUMERIC(24, 8) NOT NULL,
    low_price NUMERIC(24, 8) NOT NULL,
    close_price NUMERIC(24, 8) NOT NULL,
    volume NUMERIC(28, 8) NOT NULL,
    trades_count BIGINT DEFAULT 0,
    source_layer VARCHAR(10) NOT NULL,
    is_consolidated BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, timeframe, bucket_start)
);

CREATE TABLE IF NOT EXISTS serving.realtime_metrics (
    symbol VARCHAR(30) PRIMARY KEY,
    last_price NUMERIC(24, 8) NOT NULL,
    rolling_volume_5m NUMERIC(28, 8),
    pct_change_5m NUMERIC(10, 4),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    reconciled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS serving.pipeline_status (
    component VARCHAR(50) PRIMARY KEY,
    last_run TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20),
    consumer_lag BIGINT DEFAULT 0,
    metrics_summary JSONB,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO serving.dim_assets (symbol, exchange, asset_name, base_asset, quote_asset, status)
VALUES 
    ('BTCUSDT', 'binance', 'Bitcoin / Tether', 'BTC', 'USDT', 'ACTIVE'),
    ('ETHUSDT', 'binance', 'Ethereum / Tether', 'ETH', 'USDT', 'ACTIVE'),
    ('SOLUSDT', 'binance', 'Solana / Tether', 'SOL', 'USDT', 'ACTIVE')
ON CONFLICT (symbol) DO NOTHING;
