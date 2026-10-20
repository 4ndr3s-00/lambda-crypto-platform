"""
Tests Unitarios para el Módulo y Grafo OpenLineage (US-26).
Responsable: Samuel Roncancio (Data Architect & Storage Lead)

Verifica:
1. Generación de eventos RunEvent con especificación OpenLineage válida (START, COMPLETE, FAIL).
2. Construcción correcta de Datasets de entrada y salida con namespace y facetas.
3. Coherencia y conectividad del grafo de linaje de extremo a extremo de la plataforma:
   (Fuentes REST/WS -> Kafka -> Spark Batch/Streaming -> MinIO -> Serving API -> Consumidores).
"""

from metadata.openlineage.lineage_emitter import OpenLineageEmitter, get_platform_lineage_graph


def test_openlineage_emitter_run_event_structure():
    """Criterio 1: Los eventos deben emitirse con estructura RunEvent estándar."""
    emitter = OpenLineageEmitter(namespace="lambda-crypto-platform", endpoint_url="http://mock-openmetadata:8585")

    input_dataset = emitter.build_dataset("s3://market-lake/historical/source=binance/")
    output_dataset = emitter.build_dataset("s3://market-lake/batch-results/metric=ohlcv/")

    start_event = emitter.create_run_event(
        event_type="START",
        job_name="spark_batch_ohlcv_job",
        inputs=[input_dataset],
        outputs=[output_dataset],
    )

    assert start_event["eventType"] == "START"
    assert start_event["job"]["name"] == "spark_batch_ohlcv_job"
    assert start_event["job"]["namespace"] == "lambda-crypto-platform"
    assert len(start_event["inputs"]) == 1
    assert start_event["inputs"][0]["name"] == "s3://market-lake/historical/source=binance/"
    assert len(start_event["outputs"]) == 1
    assert start_event["outputs"][0]["name"] == "s3://market-lake/batch-results/metric=ohlcv/"
    assert "eventTime" in start_event
    assert "runId" in start_event["run"]

    # Validar evento COMPLETE
    complete_event = emitter.create_run_event(
        event_type="COMPLETE",
        job_name="spark_batch_ohlcv_job",
        run_id=start_event["run"]["runId"],
        inputs=[input_dataset],
        outputs=[output_dataset],
    )
    assert complete_event["eventType"] == "COMPLETE"
    assert complete_event["run"]["runId"] == start_event["run"]["runId"]


def test_openlineage_fail_event_has_error_facet():
    """Verifica que los eventos FAIL adjunten la faceta de mensaje de error."""
    emitter = OpenLineageEmitter()
    fail_event = emitter.create_run_event(
        event_type="FAIL",
        job_name="spark_streaming_job",
        error_message="Kafka Broker timeout connection failure",
    )

    assert fail_event["eventType"] == "FAIL"
    assert "errorMessage" in fail_event["run"]["facets"]
    assert fail_event["run"]["facets"]["errorMessage"]["message"] == "Kafka Broker timeout connection failure"


def test_platform_lineage_graph_end_to_end():
    """Criterio 2: El grafo de linaje debe conectar de forma continua desde las fuentes hasta la Serving API y UI."""
    graph = get_platform_lineage_graph()
    nodes = {n["id"]: n for n in graph["nodes"]}
    edges = [(e["from"], e["to"]) for e in graph["edges"]]

    # Verificar existencia de nodos clave
    assert "binance_rest" in nodes
    assert "kafka_market_events" in nodes
    assert "minio_historical" in nodes
    assert "spark_batch" in nodes
    assert "minio_batch_results" in nodes
    assert "serving_api" in nodes
    assert "frontend_dashboard" in nodes

    # Verificar trazabilidad en aristas
    assert ("binance_rest", "kafka_market_events") in edges
    assert ("kafka_market_events", "minio_historical") in edges
    assert ("minio_historical", "spark_batch") in edges
    assert ("spark_batch", "minio_batch_results") in edges
    assert ("minio_batch_results", "serving_api") in edges
    assert ("serving_api", "frontend_dashboard") in edges


def test_emitter_dry_run():
    """Verifica el modo seguro dry_run sin requerir servidor HTTP activo."""
    emitter = OpenLineageEmitter(endpoint_url="mock://localhost")
    event = emitter.create_run_event("START", "test_job")
    success = emitter.emit(event, dry_run=True)
    assert success is True
