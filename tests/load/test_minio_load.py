"""
Load and Stress Testing for MinIO S3 Storage (US-03).
Simulates concurrent ingestion of market events and batch reads using boto3.
"""

import time
import concurrent.futures
import pytest
import boto3
from moto import mock_aws


NUM_EVENTS = 100
BATCH_SIZE = 10


@pytest.fixture
def load_s3_env():
    """Entorno simulado para pruebas de carga."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="market-lake")
        yield client


def test_concurrent_put_and_get_load(load_s3_env):
    """Prueba de concurrencia y carga simulada sobre s3://market-lake/historical/."""
    def write_event(idx: int):
        key = f"historical/source=binance/date=2026-10-19/event_{idx:05d}.json"
        body = f'{{"event_id": "evt_{idx}", "symbol": "BTCUSDT", "price": {65000 + idx}, "timestamp": {int(time.time())}}}'
        load_s3_env.put_object(
            Bucket="market-lake",
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )
        return key

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        written_keys = list(executor.map(write_event, range(NUM_EVENTS)))
    duration = time.time() - start_time

    assert len(written_keys) == NUM_EVENTS
    assert duration < 5.0, f"La carga de {NUM_EVENTS} eventos tomó demasiado tiempo: {duration:.2f}s"

    # Lectura concurrente
    def read_event(key: str):
        res = load_s3_env.get_object(Bucket="market-lake", Key=key)
        return len(res["Body"].read())

    start_read = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        read_lengths = list(executor.map(read_event, written_keys))
    read_duration = time.time() - start_read

    assert len(read_lengths) == NUM_EVENTS
    assert all(length > 0 for length in read_lengths)
    assert read_duration < 5.0, f"La lectura de {NUM_EVENTS} eventos tomó demasiado tiempo: {read_duration:.2f}s"
