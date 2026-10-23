from __future__ import annotations

import pytest

from api.sql_guardrails import SQLGuardrailError, assert_safe_sql, validate_sql


def test_accepts_select_with_limit() -> None:
    result = validate_sql(
        "SELECT symbol, close_price FROM serving.ohlcv_reconciled "
        "WHERE symbol = 'BTCUSDT' ORDER BY bucket_start DESC LIMIT 10"
    )
    assert result.ok is True
    assert result.limit == 10
    assert result.tables == ("serving.ohlcv_reconciled",)
    assert result.timeout_seconds == 5.0
    assert "LIMIT 10" in result.sql


def test_injects_limit_when_missing() -> None:
    result = validate_sql("SELECT symbol FROM serving.assets")
    assert result.ok is True
    assert result.limit == 100
    assert "LIMIT 100" in result.sql


def test_rejects_limit_above_100() -> None:
    result = validate_sql("SELECT symbol FROM serving.assets LIMIT 101")
    assert result.ok is False
    assert "LIMIT" in (result.reason or "")


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM serving.assets",
        "UPDATE serving.assets SET symbol = 'X'",
        "DROP TABLE serving.ohlcv_reconciled",
        "ALTER TABLE serving.assets ADD COLUMN x INT",
        "SELECT * FROM serving.assets; DROP TABLE serving.assets",
        "SELECT * FROM pg_catalog.pg_shadow",
        "SELECT * FROM secret_table",
        "SELECT pg_sleep(10)",
        "SELECT * FROM serving.assets FOR UPDATE",
    ],
)
def test_rejects_destructive_or_unauthorized_sql(sql: str) -> None:
    result = validate_sql(sql)
    assert result.ok is False
    assert result.reason


def test_accepts_cte_over_allowlisted_table() -> None:
    result = validate_sql(
        "WITH recent AS ("
        "SELECT symbol, close_price, bucket_start FROM serving.ohlcv_reconciled "
        "WHERE symbol = 'BTCUSDT' LIMIT 50"
        ") SELECT symbol, close_price FROM recent LIMIT 20"
    )
    assert result.ok is True
    assert result.limit == 20
    assert "serving.ohlcv_reconciled" in result.tables


def test_assert_safe_sql_raises() -> None:
    with pytest.raises(SQLGuardrailError):
        assert_safe_sql("DELETE FROM serving.assets")
