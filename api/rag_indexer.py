"""RAG index for platform documentation and financial glossary (US-28).

Backend selection:
    - ChromaDB persistent collection when `chromadb` is installed.
    - Deterministic TF-IDF/cosine fallback that ships in the base image.

The hashing embedding function is intentionally local and dependency-free:
no model download, no external API, and stable vectors across restarts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

TOKEN_RE = re.compile(r"[a-záéíóúüñ0-9]{2,}", re.IGNORECASE)
STOPWORDS = frozenset(
    {
        "de",
        "la",
        "el",
        "los",
        "las",
        "un",
        "una",
        "unos",
        "unas",
        "y",
        "o",
        "que",
        "en",
        "por",
        "para",
        "con",
        "del",
        "al",
        "se",
        "es",
        "son",
        "como",
        "the",
        "and",
        "or",
        "of",
        "to",
        "in",
        "for",
        "with",
        "on",
        "is",
        "are",
        "a",
        "an",
    }
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text) if token.lower() not in STOPWORDS]


def chunk_text(text: str, max_chars: int = 900, overlap: int = 120) -> list[str]:
    """Split on word boundaries with a small overlap so answers keep context."""
    clean = re.sub(r"\s+", " ", text).strip()
    if not clean:
        return []
    if len(clean) <= max_chars:
        return [clean]

    chunks: list[str] = []
    start = 0
    length = len(clean)
    while start < length:
        end = min(length, start + max_chars)
        if end < length:
            boundary = clean.rfind(" ", start + int(max_chars * 0.6), end)
            if boundary > start:
                end = boundary
        chunk = clean[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= length:
            break
        start = max(end - overlap, start + 1)
    return chunks


@dataclass
class RagDocument:
    id: str
    text: str
    source: str
    title: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RagHit:
    text: str
    source: str
    title: str
    score: float
    backend: str

    def as_source(self) -> dict[str, Any]:
        snippet = self.text[:280].strip()
        return {
            "source": self.source,
            "title": self.title,
            "snippet": snippet,
            "score": round(self.score, 4),
            "kind": "rag",
        }


class HashingEmbeddingFunction:
    """Stable, dependency-free embedding function for ChromaDB."""

    def __init__(self, dimensions: int = 384) -> None:
        self.dimensions = dimensions

    def _embed(self, text: str) -> list[float]:
        vector = [0.0] * self.dimensions
        tokens = tokenize(text)
        if not tokens:
            return vector
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimensions
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            vector[index] += sign
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]

    def __call__(self, input: Iterable[str]) -> list[list[float]]:  # Chroma modern API
        return [self._embed(text) for text in input]

    def embed_documents(self, texts: Iterable[str]) -> list[list[float]]:
        return [self._embed(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)

    def name(self) -> str:
        return "iq-hashing-384"


def _flatten_json(value: Any, prefix: str = "") -> list[str]:
    lines: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            label = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(child, (dict, list)):
                lines.extend(_flatten_json(child, label))
            else:
                lines.append(f"{label}: {child}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            lines.extend(_flatten_json(child, f"{prefix}[{index}]"))
    else:
        lines.append(f"{prefix}: {value}")
    return lines


def load_documents(docs_dir: Path, glossary_path: Path | None = None) -> list[RagDocument]:
    documents: list[RagDocument] = []
    root = docs_dir.parent
    candidates: list[Path] = []
    if docs_dir.exists():
        candidates.extend(sorted(docs_dir.rglob("*.md")))
        candidates.extend(sorted(docs_dir.rglob("*.txt")))
        candidates.extend(sorted(docs_dir.rglob("*.json")))
    readme = root / "README.md"
    if readme.exists() and readme not in candidates:
        candidates.insert(0, readme)

    for path in candidates:
        if path.name == "rag_index.json" or ".chroma" in path.parts:
            continue
        if glossary_path and path.resolve() == glossary_path.resolve():
            continue
        try:
            raw = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        title = path.name
        if path.suffix.lower() == ".json":
            try:
                payload = json.loads(raw)
                raw = "\n".join(_flatten_json(payload))
            except json.JSONDecodeError:
                pass
        for index, chunk in enumerate(chunk_text(raw)):
            documents.append(
                RagDocument(
                    id=f"{path.stem}-{index}",
                    text=chunk,
                    source=str(path.relative_to(root)) if root in path.parents else str(path),
                    title=title,
                    metadata={"path": str(path), "chunk": index},
                )
            )

    # Glossary is loaded explicitly even if it lives outside docs_dir.
    if glossary_path and glossary_path.exists():
        try:
            payload = json.loads(glossary_path.read_text(encoding="utf-8"))
            terms = payload.get("terms", payload) if isinstance(payload, dict) else payload
            if isinstance(terms, dict):
                for term, definition in terms.items():
                    text = f"{term}: {definition}"
                    documents.append(
                        RagDocument(
                            id=f"glossary-{term}",
                            text=text,
                            source=str(glossary_path.name),
                            title="Glosario financiero",
                            metadata={"term": term},
                        )
                    )
        except Exception:  # noqa: BLE001 - a malformed glossary must not kill the API
            pass

    return documents


class RagIndex:
    def __init__(
        self,
        index_path: Path,
        docs_dir: Path,
        glossary_path: Path | None = None,
    ) -> None:
        self.index_path = index_path
        self.docs_dir = docs_dir
        self.glossary_path = glossary_path
        self._backend: str | None = None
        self._documents: list[dict[str, Any]] = []
        self._idf: dict[str, float] = {}
        self._collection: Any = None
        self._client: Any = None

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, force: bool = False) -> dict[str, Any]:
        documents = load_documents(self.docs_dir, self.glossary_path)
        if not documents:
            raise RuntimeError(f"No hay documentos para indexar en {self.docs_dir}")

        self.index_path.parent.mkdir(parents=True, exist_ok=True)

        chroma_stats = self._build_chroma(documents, force)
        if chroma_stats is not None:
            return chroma_stats

        return self._build_tfidf(documents)

    def _build_chroma(self, documents: list[RagDocument], force: bool) -> dict[str, Any] | None:
        try:
            import chromadb  # type: ignore[import-not-found]
        except Exception:
            return None

        try:
            chroma_dir = self.index_path.with_suffix(".chroma")
            client = chromadb.PersistentClient(path=str(chroma_dir))
            if force:
                try:
                    client.delete_collection("iq_analyzer_docs")
                except Exception:  # noqa: BLE001
                    pass
            collection = client.get_or_create_collection(
                name="iq_analyzer_docs",
                embedding_function=HashingEmbeddingFunction(),
                metadata={"hnsw:space": "cosine"},
            )
            existing = collection.count()
            if existing == 0 or force:
                batch_size = 64
                for start in range(0, len(documents), batch_size):
                    batch = documents[start : start + batch_size]
                    collection.add(
                        ids=[doc.id for doc in batch],
                        documents=[doc.text for doc in batch],
                        metadatas=[
                            {"source": doc.source, "title": doc.title, **doc.metadata}
                            for doc in batch
                        ],
                    )
            stats = {
                "backend": "chromadb",
                "documents": len(documents),
                "chunks": collection.count(),
                "index_path": str(self.index_path),
                "built_at": utc_now_iso(),
            }
            self.index_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
            self._backend = "chromadb"
            self._collection = collection
            self._client = client
            return stats
        except Exception:  # noqa: BLE001 - fall back to TF-IDF
            return None

    def _build_tfidf(self, documents: list[RagDocument]) -> dict[str, Any]:
        tokenized = [tokenize(doc.text) for doc in documents]
        document_frequency: dict[str, int] = {}
        for tokens in tokenized:
            for token in set(tokens):
                document_frequency[token] = document_frequency.get(token, 0) + 1

        total = len(documents)
        idf = {
            token: math.log((total + 1) / (frequency + 1)) + 1.0
            for token, frequency in document_frequency.items()
        }

        records: list[dict[str, Any]] = []
        for doc, tokens in zip(documents, tokenized):
            frequencies: dict[str, int] = {}
            for token in tokens:
                frequencies[token] = frequencies.get(token, 0) + 1
            vector = {
                token: (1 + math.log(count)) * idf.get(token, 1.0)
                for token, count in frequencies.items()
            }
            norm = math.sqrt(sum(value * value for value in vector.values())) or 1.0
            records.append(
                {
                    "id": doc.id,
                    "text": doc.text,
                    "source": doc.source,
                    "title": doc.title,
                    "metadata": doc.metadata,
                    "vector": vector,
                    "norm": norm,
                }
            )

        payload = {
            "backend": "tfidf",
            "documents": records,
            "idf": idf,
            "built_at": utc_now_iso(),
        }
        self.index_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        self._backend = "tfidf"
        self._documents = records
        self._idf = idf
        return {
            "backend": "tfidf",
            "documents": len(documents),
            "chunks": len(records),
            "terms": len(idf),
            "index_path": str(self.index_path),
            "built_at": payload["built_at"],
        }

    # ------------------------------------------------------------------
    # Load / query
    # ------------------------------------------------------------------

    def ensure_index(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return self.build()
        try:
            manifest = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return self.build(force=True)
        backend = manifest.get("backend")
        if backend == "chromadb":
            try:
                import chromadb  # type: ignore[import-not-found]

                client = chromadb.PersistentClient(path=str(self.index_path.with_suffix(".chroma")))
                self._collection = client.get_collection(
                    name="iq_analyzer_docs",
                    embedding_function=HashingEmbeddingFunction(),
                )
                self._client = client
                self._backend = "chromadb"
                return manifest
            except Exception:  # noqa: BLE001
                return self.build(force=True)
        if backend == "tfidf":
            self._documents = manifest.get("documents", [])
            self._idf = manifest.get("idf", {})
            self._backend = "tfidf"
            return manifest
        return self.build(force=True)

    def query(self, text: str, k: int = 4) -> list[RagHit]:
        if not text.strip():
            return []
        self.ensure_index()
        if self._backend == "chromadb" and self._collection is not None:
            try:
                result = self._collection.query(
                    query_texts=[text],
                    n_results=max(1, min(k, 10)),
                    include=["documents", "metadatas", "distances"],
                )
                documents = (result.get("documents") or [[]])[0]
                metadatas = (result.get("metadatas") or [[]])[0]
                distances = (result.get("distances") or [[]])[0]
                hits: list[RagHit] = []
                for doc, metadata, distance in zip(documents, metadatas, distances):
                    meta = metadata or {}
                    score = 1.0 - float(distance)
                    hits.append(
                        RagHit(
                            text=str(doc),
                            source=str(meta.get("source", "document")),
                            title=str(meta.get("title", "Documento")),
                            score=score,
                            backend="chromadb",
                        )
                    )
                return hits
            except Exception:  # noqa: BLE001 - degrade to TF-IDF
                self._backend = None
                try:
                    self._build_tfidf(load_documents(self.docs_dir, self.glossary_path))
                except Exception:  # noqa: BLE001
                    return []
                return self._query_tfidf(text, k)

        return self._query_tfidf(text, k)

    def _query_tfidf(self, text: str, k: int) -> list[RagHit]:
        if not self._documents:
            try:
                manifest = json.loads(self.index_path.read_text(encoding="utf-8"))
                self._documents = manifest.get("documents", [])
                self._idf = manifest.get("idf", {})
                self._backend = "tfidf"
            except Exception:  # noqa: BLE001
                return []

        tokens = tokenize(text)
        if not tokens:
            return []
        frequencies: dict[str, int] = {}
        for token in tokens:
            frequencies[token] = frequencies.get(token, 0) + 1
        query_vector = {
            token: (1 + math.log(count)) * self._idf.get(token, 1.0)
            for token, count in frequencies.items()
        }
        query_norm = math.sqrt(sum(value * value for value in query_vector.values())) or 1.0

        scored: list[tuple[float, dict[str, Any]]] = []
        for record in self._documents:
            vector = record.get("vector", {})
            dot = sum(
                weight * vector.get(token, 0.0)
                for token, weight in query_vector.items()
            )
            denominator = query_norm * float(record.get("norm", 1.0)) or 1.0
            score = dot / denominator
            if score > 0:
                scored.append((score, record))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [
            RagHit(
                text=str(record.get("text", "")),
                source=str(record.get("source", "document")),
                title=str(record.get("title", "Documento")),
                score=score,
                backend="tfidf",
            )
            for score, record in scored[: max(1, k)]
        ]

    def status(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return {"ready": False, "backend": None, "index_path": str(self.index_path)}
        try:
            manifest = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {"ready": False, "backend": None, "index_path": str(self.index_path)}
        raw_documents = manifest.get("documents")
        raw_chunks = manifest.get("chunks")
        document_count = len(raw_documents) if isinstance(raw_documents, list) else raw_documents
        chunk_count = len(raw_chunks) if isinstance(raw_chunks, list) else raw_chunks
        return {
            "ready": True,
            "backend": manifest.get("backend"),
            "documents": document_count,
            "chunks": chunk_count,
            "built_at": manifest.get("built_at"),
            "index_path": str(self.index_path),
        }


def build_default_index(force: bool = False) -> dict[str, Any]:
    try:
        from .config import get_settings
    except ImportError:  # Docker / uvicorn main:app
        from config import get_settings

    settings = get_settings()
    index = RagIndex(settings.rag_index_path, settings.rag_docs_dir, settings.glossary_path)
    return index.build(force=force)


def main() -> None:
    parser = argparse.ArgumentParser(description="Indexa documentación y glosario para RAG")
    parser.add_argument("--docs", type=Path, help="Directorio de documentación (default: metadata/)")
    parser.add_argument("--glossary", type=Path, help="JSON de glosario")
    parser.add_argument("--index", type=Path, help="Ruta del índice JSON")
    parser.add_argument("--rebuild", action="store_true", help="Reconstruir desde cero")
    args = parser.parse_args()

    try:
        from .config import get_settings
    except ImportError:  # Docker / uvicorn main:app
        from config import get_settings

    settings = get_settings()
    index = RagIndex(
        args.index or settings.rag_index_path,
        args.docs or settings.rag_docs_dir,
        args.glossary or settings.glossary_path,
    )
    stats = index.build(force=args.rebuild)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
