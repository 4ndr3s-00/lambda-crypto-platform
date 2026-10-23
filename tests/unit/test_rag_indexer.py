from __future__ import annotations

import json

from api.rag_indexer import RagIndex, chunk_text, load_documents


def test_chunk_text_respects_overlap() -> None:
    text = " ".join(f"palabra{i}" for i in range(400))
    chunks = chunk_text(text, max_chars=200, overlap=20)
    assert len(chunks) > 1
    assert all(len(chunk) <= 200 for chunk in chunks)


def test_build_and_query_index(tmp_path) -> None:
    docs_dir = tmp_path / "metadata"
    docs_dir.mkdir()
    (docs_dir / "faq.md").write_text(
        "La capa Speed usa Kafka y Spark Structured Streaming con watermark de 5 segundos. "
        "El VWAP se calcula ponderando por volumen.",
        encoding="utf-8",
    )
    glossary = tmp_path / "glossary.json"
    glossary.write_text(
        json.dumps({"terms": {"DLQ": "Cola de eventos rechazados."}}),
        encoding="utf-8",
    )
    index_path = tmp_path / "rag_index.json"

    index = RagIndex(index_path, docs_dir, glossary)
    stats = index.build(force=True)
    assert stats["documents"] >= 1
    assert index_path.exists()

    hits = index.query("¿Cuál es el watermark de la capa Speed?", k=3)
    assert hits
    assert any("watermark" in hit.text.lower() for hit in hits)
    assert any(hit.source for hit in hits)

    status = index.status()
    assert status["ready"] is True
    assert isinstance(status["documents"], (int, type(None)))


def test_load_documents_skips_its_own_index(tmp_path) -> None:
    docs_dir = tmp_path / "metadata"
    docs_dir.mkdir()
    (docs_dir / "faq.md").write_text("contenido", encoding="utf-8")
    (docs_dir / "rag_index.json").write_text("{}", encoding="utf-8")
    documents = load_documents(docs_dir)
    assert all("rag_index" not in doc.source for doc in documents)
