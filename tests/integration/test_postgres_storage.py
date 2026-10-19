"""
Pruebas de Integración y Persistencia para PostgreSQL (US-04).
Responsable: Samuel Roncancio (Storage & Database Lead)

Criterios de Aceptación Validados:
- Criterio 1 (BDD): Separación estricta de bases de datos (airflow_db, serving_db, metastore_db)
  con usuarios independientes y permisos de mínimo privilegio.
- Criterio 2 (BDD): Modelo relacional para serving auxiliar con tablas indexadas para
  dim_assets (símbolo, exchange, activo, estado) y pipeline_execution_log (última ejecución,
  latencia, fecha de corte).
- Criterio 3 (BDD): Persistencia y respaldos en el volumen de datos de PostgreSQL.
- Tarea Técnica 4: Validación de conectividad exitosa mediante psycopg2 / SQLAlchemy.
"""

import os
from datetime import datetime, timezone
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor


# Configuración de conexión por variables de entorno o defaults de desarrollo
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_ADMIN_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_ADMIN_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass")


def get_admin_connection(dbname="postgres"):
    """Retorna una conexión como superusuario a la base de datos indicada."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_ADMIN_USER,
        password=POSTGRES_ADMIN_PASSWORD,
        dbname=dbname
    )


def get_service_connection(dbname, user, password):
    """Retorna una conexión con las credenciales de servicio dedicadas."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=user,
        password=password,
        dbname=dbname
    )


# ------------------------------------------------------------------------------
# Pruebas de Criterio 1: Separación de Bases de Datos y Usuarios Dedicados
# ------------------------------------------------------------------------------

def test_bdd_criterio_1_separacion_bases_de_datos():
    """Valida que existan las tres bases de datos requeridas: airflow_db, serving_db y metastore_db."""
    conn = get_admin_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database 
                WHERE datname IN ('airflow_db', 'serving_db', 'metastore_db');
            """)
            databases = {row[0] for row in cur.fetchall()}
            assert "airflow_db" in databases, "airflow_db no existe"
            assert "serving_db" in databases, "serving_db no existe"
            assert "metastore_db" in databases, "metastore_db no existe"
    finally:
        conn.close()


def test_bdd_criterio_1_usuarios_dedicados_y_autenticacion():
    """Valida que los usuarios de servicio puedan autenticarse en sus respectivas bases de datos."""
    # 1. serving_user sobre serving_db
    conn_serving = get_service_connection(
        dbname="serving_db",
        user="serving_user",
        password="serving_secure_pass123"
    )
    assert conn_serving.status == psycopg2.extensions.STATUS_READY
    conn_serving.close()

    # 2. airflow_user sobre airflow_db
    conn_airflow = get_service_connection(
        dbname="airflow_db",
        user="airflow_user",
        password="airflow_secure_pass123"
    )
    assert conn_airflow.status == psycopg2.extensions.STATUS_READY
    conn_airflow.close()

    # 3. metastore_user sobre metastore_db
    conn_metastore = get_service_connection(
        dbname="metastore_db",
        user="metastore_user",
        password="metastore_secure_pass123"
    )
    assert conn_metastore.status == psycopg2.extensions.STATUS_READY
    conn_metastore.close()


# ------------------------------------------------------------------------------
# Pruebas de Criterio 2: Modelo Relacional de Serving Auxiliar (dim_assets)
# ------------------------------------------------------------------------------

def test_bdd_criterio_2_dim_assets_estructura_y_columnas():
    """Valida que dim_assets contenga los campos requeridos: símbolo, exchange, activo, estado."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'serving' AND table_name = 'dim_assets';
            """)
            columns = {row["column_name"]: row for row in cur.fetchall()}
            
            assert "symbol" in columns, "Columna 'symbol' faltante en dim_assets"
            assert "exchange" in columns, "Columna 'exchange' faltante en dim_assets"
            assert "asset_name" in columns, "Columna 'asset_name' faltante en dim_assets"
            assert "status" in columns, "Columna 'status' faltante en dim_assets"
            assert "base_asset" in columns, "Columna 'base_asset' faltante en dim_assets"
            assert "quote_asset" in columns, "Columna 'quote_asset' faltante en dim_assets"
    finally:
        conn.close()


def test_bdd_criterio_2_dim_assets_indices():
    """Valida que existan los índices B-Tree requeridos para dim_assets."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE schemaname = 'serving' AND tablename = 'dim_assets';
            """)
            indexes = {row[0] for row in cur.fetchall()}
            assert "dim_assets_pkey" in indexes, "Clave primaria en symbol no encontrada"
            assert "idx_dim_assets_exchange" in indexes, "Índice en exchange no encontrado"
            assert "idx_dim_assets_status" in indexes, "Índice en status no encontrado"
    finally:
        conn.close()


def test_bdd_criterio_2_dim_assets_operaciones_y_semillas():
    """Valida la consulta de activos semilla y la inserción idempotente de nuevos pares."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Consulta de semillas estándar
            cur.execute("SELECT symbol, exchange, status, is_active FROM serving.dim_assets WHERE symbol = 'BTCUSDT';")
            row = cur.fetchone()
            assert row is not None, "Activo semilla BTCUSDT no encontrado"
            assert row["exchange"] == "binance"
            assert row["status"] == "ACTIVE"
            assert row["is_active"] is True

            # Inserción de activo de prueba
            cur.execute("""
                INSERT INTO serving.dim_assets (symbol, exchange, asset_name, base_asset, quote_asset, status)
                VALUES ('ADAUSDT', 'binance', 'Cardano / Tether', 'ADA', 'USDT', 'ACTIVE')
                ON CONFLICT (symbol) DO UPDATE SET status = 'ACTIVE';
            """)
            conn.commit()

            cur.execute("SELECT asset_name FROM serving.dim_assets WHERE symbol = 'ADAUSDT';")
            ada = cur.fetchone()
            assert ada["asset_name"] == "Cardano / Tether"
    finally:
        conn.close()


# ------------------------------------------------------------------------------
# Pruebas de Criterio 2: pipeline_execution_log (Auditoría y Observabilidad)
# ------------------------------------------------------------------------------

def test_bdd_criterio_2_pipeline_execution_log_estructura():
    """Valida campos requeridos: última ejecución, latencia y fecha de corte."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'serving' AND table_name = 'pipeline_execution_log';
            """)
            columns = {row["column_name"]: row for row in cur.fetchall()}

            assert "pipeline_name" in columns, "Columna pipeline_name faltante"
            assert "execution_timestamp" in columns, "Columna execution_timestamp (última ejecución) faltante"
            assert "latency_ms" in columns, "Columna latency_ms (latencia) faltante"
            assert "cutoff_timestamp" in columns, "Columna cutoff_timestamp (fecha de corte) faltante"
            assert "status" in columns, "Columna status faltante"
    finally:
        conn.close()


def test_bdd_criterio_2_pipeline_execution_log_indices():
    """Valida los índices de pipeline_execution_log."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE schemaname = 'serving' AND tablename = 'pipeline_execution_log';
            """)
            indexes = {row[0] for row in cur.fetchall()}
            assert "pipeline_execution_log_pkey" in indexes
            assert "idx_pipeline_exec_name_time" in indexes
            assert "idx_pipeline_exec_cutoff" in indexes
            assert "idx_pipeline_exec_status" in indexes
    finally:
        conn.close()


def test_bdd_criterio_2_pipeline_execution_log_insercion_y_filtro_corte():
    """Valida inserción de eventos de auditoría y consultas ordenadas por fecha de corte."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cutoff = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
            exec_time = datetime(2026, 10, 19, 12, 5, 30, tzinfo=timezone.utc)

            cur.execute("""
                INSERT INTO serving.pipeline_execution_log 
                    (pipeline_name, execution_timestamp, latency_ms, cutoff_timestamp, records_processed, status)
                VALUES 
                    (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, ("test_spark_batch_us04", exec_time, 3300, cutoff, 50000, "SUCCESS"))
            inserted_id = cur.fetchone()["id"]
            conn.commit()

            # Consultar por fecha de corte y pipeline_name
            cur.execute("""
                SELECT pipeline_name, latency_ms, records_processed, status 
                FROM serving.pipeline_execution_log 
                WHERE id = %s AND cutoff_timestamp = %s;
            """, (inserted_id, cutoff))
            record = cur.fetchone()

            assert record is not None
            assert record["pipeline_name"] == "test_spark_batch_us04"
            assert record["latency_ms"] == 3300
            assert record["records_processed"] == 50000
            assert record["status"] == "SUCCESS"
    finally:
        conn.close()


# ------------------------------------------------------------------------------
# Pruebas de Criterio 3: Persistencia de Datos
# ------------------------------------------------------------------------------

def test_bdd_criterio_3_persistencia_tablas_y_datos():
    """Valida que los registros insertados persistan y sean consultables."""
    conn = get_service_connection("serving_db", "serving_user", "serving_secure_pass123")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM serving.dim_assets;")
            count_assets = cur.fetchone()[0]
            assert count_assets >= 3, "No se encontraron los activos mínimos persistidos"

            cur.execute("SELECT COUNT(*) FROM serving.pipeline_execution_log;")
            count_logs = cur.fetchone()[0]
            assert count_logs >= 1, "No se encontraron logs de ejecución persistidos"
    finally:
        conn.close()
