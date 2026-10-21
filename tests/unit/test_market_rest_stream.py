from ingestion.connectors.market_rest_stream import build_event, price_url
from ingestion.contracts.validator import validate_event


def test_build_event_matches_contract() -> None:
    event = build_event("BTCUSDT", 84000.0, 7)

    validate_event(event)
    assert event["symbol"] == "BTCUSDT"
    assert event["price"] == 84000.0
    assert event["source"] == "binance_rest"
    assert event["interval"] == "tick"
    assert event["schema_version"] == "2.0.0"


def test_price_url_uses_public_mirror(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_API_BASE", "https://data-api.binance.vision/")

    assert price_url("BTCUSDT") == (
        "https://data-api.binance.vision/api/v3/ticker/price"
    )
