"""
Pruebas unitarias para el compactador de archivos y DAG compact_files (US-16).
"""

from unittest.mock import MagicMock, patch

from spark.compact_partition import calculate_optimal_partitions
from dags.compact_files import scan_small_file_partitions


def test_calculate_optimal_partitions():
    """Valida la estimación del número óptimo de archivos para alcanzar ~128MB."""
    target_128mb = 128 * 1024 * 1024

    # Menos de 128MB -> Siempre al menos 1 archivo
    assert calculate_optimal_partitions(0, target_128mb) == 1
    assert calculate_optimal_partitions(10 * 1024 * 1024, target_128mb) == 1
    assert calculate_optimal_partitions(100 * 1024 * 1024, target_128mb) == 1

    # 300 MB -> 2 archivos de ~150 MB
    assert calculate_optimal_partitions(300 * 1024 * 1024, target_128mb) == 2

    # 1 GB -> 8 archivos de 128 MB
    assert calculate_optimal_partitions(1024 * 1024 * 1024, target_128mb) == 8


@patch("dags.compact_files.get_s3_client")
def test_scan_small_file_partitions_detection(mock_get_s3):
    """Valida que solo se seleccionen particiones con >= 20 archivos pequeños (< 16MB)."""
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()

    # Partición 1: 22 archivos pequeños de 1 MB -> DEBE ser seleccionada
    contents_part_1 = [
        {"Key": f"historical/source=binance/date=2026-10-18/part_{i}.json", "Size": 1024 * 1024}
        for i in range(22)
    ]
    # Partición 2: solo 5 archivos pequeños de 1 MB -> NO debe ser seleccionada
    contents_part_2 = [
        {"Key": f"historical/source=binance/date=2026-10-19/part_{i}.json", "Size": 1024 * 1024}
        for i in range(5)
    ]
    # Partición 3: 25 archivos grandes de 30 MB -> NO debe ser seleccionada
    contents_part_3 = [
        {"Key": f"batch-results/metric=ohlcv/symbol=BTCUSDT/part_{i}.parquet", "Size": 30 * 1024 * 1024}
        for i in range(25)
    ]

    def mock_paginate(Bucket, Prefix):
        if "historical" in Prefix:
            return [{"Contents": contents_part_1 + contents_part_2}]
        elif "batch-results" in Prefix:
            return [{"Contents": contents_part_3}]
        return [{"Contents": []}]

    mock_paginator.paginate.side_effect = mock_paginate
    mock_s3.get_paginator.return_value = mock_paginator
    mock_get_s3.return_value = mock_s3

    context = {
        "params": {
            "bucket": "market-lake",
            "prefixes": "historical/,batch-results/",
            "min_file_count": 20,
            "size_threshold_mb": 16,
        }
    }

    candidates = scan_small_file_partitions(**context)

    # Solo la partición 1 cumple tener >= 20 archivos < 16 MB
    assert len(candidates) == 1
    assert candidates[0]["partition_dir"] == "historical/source=binance/date=2026-10-18"
    assert candidates[0]["file_count"] == 22
    assert candidates[0]["format"] == "json"
