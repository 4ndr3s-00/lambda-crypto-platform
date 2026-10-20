"""
Script de Automatización de Catálogo, Profiling y Reglas de Calidad para OpenMetadata (US-25).
Responsable: Samuel Roncancio (Data Architect & Storage Lead)

Cumple con los Criterios de Aceptación:
- Criterio 1 (BDD): Registro completo de datasets con owners, descripciones, frecuencia y SLA.
- Criterio 2 (BDD): Generación de perfiles estadísticos (min, max, mean, nulls, cardinalidad).
- Criterio 3 (BDD): Ejecución de pruebas de calidad automatizadas:
  * No nulidad de event_time
  * No nulidad de price
  * Unicidad de event_id
  * Valores de price > 0
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# Metadatos canónicos de gobernanza (Criterio 1 BDD)
GOVERNANCE_METADATA = {
    "version": "1.0",
    "service_owners": {
        "technical_lead": {
            "name": "Samuel Roncancio",
            "email": "samueroncacio@gmail.com",
            "role": "Data Architect & Storage Lead"
        },
        "processing_lead": {
            "name": "Andrés Blanco",
            "email": "andresblancocenteno2@gmail.com",
            "role": "Big Data Processing Engineer"
        },
        "streaming_lead": {
            "name": "Samuel Benavidez",
            "email": "samuel.benavidez@riwi.io",
            "role": "Streaming & Ingestion Engineer"
        }
    },
    "catalog_datasets": [
        {
            "name": "market_events_raw",
            "fullyQualifiedName": "minio_market_lake.market-lake.historical.events",
            "type": "datalake_table",
            "source": "MinIO S3 (s3://market-lake/historical/)",
            "owner": "Samuel Roncancio",
            "tier": "Tier 1",
            "frequency": "Continuo (micro-lotes) / Diario batch",
            "sla": "Frescura <= 5 minutos",
            "description": "Eventos crudos de transacciones y ticks de mercado cripto normalizados",
            "columns": [
                {"name": "event_id", "type": "string", "description": "Identificador UUID único del evento"},
                {"name": "source", "type": "string", "description": "Proveedor de origen (binance, coingecko)"},
                {"name": "exchange", "type": "string", "description": "Exchange emisor de la cotización"},
                {"name": "symbol", "type": "string", "description": "Par de activo negociado (ej. BTCUSDT)"},
                {"name": "event_time", "type": "timestamp", "description": "Marca de tiempo del trade en UTC"},
                {"name": "ingestion_time", "type": "timestamp", "description": "Marca de tiempo de captura en UTC"},
                {"name": "price", "type": "float", "description": "Precio unitario ejecutado (> 0)"},
                {"name": "quantity", "type": "float", "description": "Volumen o cantidad del trade (>= 0)"}
            ]
        },
        {
            "name": "batch_ohlcv_consolidated",
            "fullyQualifiedName": "minio_market_lake.market-lake.batch-results.ohlcv",
            "type": "datalake_table",
            "source": "MinIO S3 (s3://market-lake/batch-results/metric=ohlcv/)",
            "owner": "Andrés Blanco",
            "tier": "Tier 1",
            "frequency": "Diario a las 00:00 UTC",
            "sla": "Consolidación <= 30 minutos tras cierre",
            "description": "Vistas maestras históricas de velas OHLCV recalculadas de forma idempotente",
            "columns": [
                {"name": "symbol", "type": "string", "description": "Par cripto"},
                {"name": "timeframe", "type": "string", "description": "Granularidad temporal (1m, 5m, 1h, 1d)"},
                {"name": "bucket_start", "type": "timestamp", "description": "Inicio de la ventana"},
                {"name": "bucket_end", "type": "timestamp", "description": "Fin de la ventana"},
                {"name": "open", "type": "float", "description": "Precio de apertura"},
                {"name": "high", "type": "float", "description": "Precio máximo alcanzado"},
                {"name": "low", "type": "float", "description": "Precio mínimo alcanzado"},
                {"name": "close", "type": "float", "description": "Precio de cierre"},
                {"name": "volume", "type": "float", "description": "Volumen total transaccionado"}
            ]
        },
        {
            "name": "postgres_dim_assets",
            "fullyQualifiedName": "postgres_serving_layer.serving_db.serving.dim_assets",
            "type": "relational_table",
            "source": "PostgreSQL (serving_db.serving.dim_assets)",
            "owner": "Samuel Roncancio",
            "tier": "Tier 1",
            "frequency": "Bajo demanda / Actualización diaria",
            "sla": "Disponibilidad 99.9%",
            "description": "Tabla de dimensión maestra de activos disponibles e indexados",
            "columns": [
                {"name": "symbol", "type": "string", "description": "Clave primaria del activo"},
                {"name": "exchange", "type": "string", "description": "Exchange asociado"},
                {"name": "asset_name", "type": "string", "description": "Nombre legible del par"},
                {"name": "status", "type": "string", "description": "Estado operativo (ACTIVE/INACTIVE)"}
            ]
        },
        {
            "name": "kafka_market_events_topic",
            "fullyQualifiedName": "kafka_market_events.market.events",
            "type": "kafka_topic",
            "source": "Apache Kafka (broker: kafka:9092, topic: market.events)",
            "owner": "Samuel Benavidez",
            "tier": "Tier 1",
            "frequency": "Streaming continuo en tiempo real",
            "sla": "Latencia de entrega < 1 segundo",
            "description": "Bus central de mensajería distribuida particionado por clave exchange|symbol",
            "columns": [
                {"name": "key", "type": "string", "description": "Clave compuesta exchange|symbol"},
                {"name": "value", "type": "json", "description": "Payload validado bajo market_event.schema.json"}
            ]
        }
    ]
}


def calculate_data_profile(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcula perfiles estadísticos (Criterio 2 BDD):
    - Mínimos
    - Máximos
    - Medias
    - Recuento de nulos
    - Cardinalidad de símbolos
    """
    if not records:
        return {"row_count": 0, "columns": {}}

    row_count = len(records)
    prices = []
    quantities = []
    symbols = set()
    null_counts = {
        "event_id": 0,
        "event_time": 0,
        "price": 0,
        "quantity": 0,
        "symbol": 0
    }

    for r in records:
        # Conteo de nulos
        for col in null_counts.keys():
            if r.get(col) is None:
                null_counts[col] += 1

        # Precios
        p = r.get("price")
        if p is not None and isinstance(p, (int, float)):
            prices.append(float(p))

        # Cantidades
        q = r.get("quantity")
        if q is not None and isinstance(q, (int, float)):
            quantities.append(float(q))

        # Símbolos
        s = r.get("symbol")
        if s:
            symbols.add(str(s))

    profile = {
        "profile_timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "columns": {
            "price": {
                "min": min(prices) if prices else None,
                "max": max(prices) if prices else None,
                "mean": round(sum(prices) / len(prices), 4) if prices else None,
                "null_count": null_counts["price"],
                "null_proportion": round(null_counts["price"] / row_count, 4)
            },
            "quantity": {
                "min": min(quantities) if quantities else None,
                "max": max(quantities) if quantities else None,
                "mean": round(sum(quantities) / len(quantities), 4) if quantities else None,
                "null_count": null_counts["quantity"],
                "null_proportion": round(null_counts["quantity"] / row_count, 4)
            },
            "symbol": {
                "cardinality": len(symbols),
                "distinct_values": sorted(list(symbols)),
                "null_count": null_counts["symbol"]
            },
            "event_time": {
                "null_count": null_counts["event_time"],
                "null_proportion": round(null_counts["event_time"] / row_count, 4)
            },
            "event_id": {
                "null_count": null_counts["event_id"],
                "distinct_count": len({r.get("event_id") for r in records if r.get("event_id") is not None})
            }
        }
    }
    return profile


def run_data_quality_tests(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Ejecuta las pruebas de calidad de datos requeridas (Criterio 3 BDD):
    1. No nulidad en event_time
    2. No nulidad en price
    3. Unicidad de event_id
    4. Valores de price > 0
    """
    results = {
        "suite_name": "market_event_contract_test_suite",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "total_records": len(records),
        "tests": []
    }

    # Test 1: No nulidad en event_time
    null_event_time = sum(1 for r in records if r.get("event_time") is None)
    passed_test_1 = (null_event_time == 0)
    results["tests"].append({
        "test_name": "columnValuesToBeNotNull_event_time",
        "column": "event_time",
        "status": "PASSED" if passed_test_1 else "FAILED",
        "null_count": null_event_time,
        "assertion": "event_time IS NOT NULL"
    })

    # Test 2: No nulidad en price
    null_price = sum(1 for r in records if r.get("price") is None)
    passed_test_2 = (null_price == 0)
    results["tests"].append({
        "test_name": "columnValuesToBeNotNull_price",
        "column": "price",
        "status": "PASSED" if passed_test_2 else "FAILED",
        "null_count": null_price,
        "assertion": "price IS NOT NULL"
    })

    # Test 3: Unicidad de event_id
    event_ids = [r.get("event_id") for r in records if r.get("event_id") is not None]
    unique_ids_count = len(set(event_ids))
    duplicate_count = len(event_ids) - unique_ids_count
    passed_test_3 = (duplicate_count == 0) and (len(event_ids) == len(records))
    results["tests"].append({
        "test_name": "columnValuesToBeUnique_event_id",
        "column": "event_id",
        "status": "PASSED" if passed_test_3 else "FAILED",
        "duplicate_count": duplicate_count,
        "unique_records": unique_ids_count,
        "assertion": "event_id IS UNIQUE"
    })

    # Test 4: Valores de price > 0
    invalid_prices = sum(
        1 for r in records 
        if r.get("price") is None or not isinstance(r.get("price"), (int, float)) or r.get("price") <= 0
    )
    passed_test_4 = (invalid_prices == 0)
    results["tests"].append({
        "test_name": "columnValuesToBeGreaterThan_price_0",
        "column": "price",
        "status": "PASSED" if passed_test_4 else "FAILED",
        "invalid_count": invalid_prices,
        "assertion": "price > 0.0"
    })

    results["all_passed"] = all(t["status"] == "PASSED" for t in results["tests"])
    return results


def export_catalog_report(output_dir: str = "metadata/openmetadata/catalog") -> str:
    """Exporta el reporte consolidado de catálogo, owners, SLAs, profiling y quality tests."""
    os.makedirs(output_dir, exist_ok=True)
    report_file = os.path.join(output_dir, "openmetadata_catalog_report.json")

    # Muestra de datos canónicos para profiling inicial
    sample_events = [
        {"event_id": f"evt-btc-00{i}", "source": "binance", "exchange": "binance", "symbol": "BTCUSDT",
         "event_time": f"2026-10-20T07:00:0{i}Z", "ingestion_time": f"2026-10-20T07:00:0{i}.100Z",
         "price": 68000.0 + (i * 12.5), "quantity": 0.45 + (i * 0.1)}
        for i in range(1, 6)
    ] + [
        {"event_id": f"evt-eth-00{i}", "source": "binance", "exchange": "binance", "symbol": "ETHUSDT",
         "event_time": f"2026-10-20T07:00:0{i}Z", "ingestion_time": f"2026-10-20T07:00:0{i}.100Z",
         "price": 2650.0 + (i * 4.2), "quantity": 2.1 + (i * 0.3)}
        for i in range(1, 6)
    ]

    profile_data = calculate_data_profile(sample_events)
    quality_tests = run_data_quality_tests(sample_events)

    report_payload = {
        "metadata_header": GOVERNANCE_METADATA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profiling": profile_data,
        "quality_tests": quality_tests
    }

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    return report_file


if __name__ == "__main__":
    report_path = export_catalog_report()
    print(f"[OpenMetadata Ingestion] Reporte de catálogo y calidad generado en: {report_path}")
