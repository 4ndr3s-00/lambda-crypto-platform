from __future__ import annotations

import pytest

from api.config import get_settings
from api.rag_indexer import RagIndex


@pytest.fixture(scope="module")
def index() -> RagIndex:
    settings = get_settings()
    rag = RagIndex(settings.rag_index_path, settings.rag_docs_dir, settings.glossary_path)
    rag.ensure_index()
    return rag


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("¿Qué es el watermark en Spark Streaming?", "watermark"),
        ("¿Qué guardrails protege el asistente?", "guardrail"),
        ("¿Quién es el owner del dataset OHLCV?", "owner"),
        ("¿Cómo se calcula el VWAP?", "vwap"),
    ],
)
def test_faq_questions_retrieve_relevant_context(index: RagIndex, question: str, expected: str) -> None:
    hits = index.query(question, k=4)
    assert hits, f"sin recuperación para: {question}"
    combined = " ".join(hit.text.lower() for hit in hits)
    assert expected in combined
    assert any(hit.source for hit in hits)
