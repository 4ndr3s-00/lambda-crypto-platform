"""Local semantic router for IQ Analyzer.

The router runs before any paid LLM call. It embeds the user message with a
local Ollama embedding model (default: bge-m3, multilingual) and compares it
against small intent prototypes. Greetings, identity, farewells, thanks and
out-of-scope questions are answered locally, without touching Groq.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Sequence

import httpx

LOGGER = logging.getLogger("iq_analyzer.router")

EmbedFn = Callable[[Sequence[str]], Awaitable[list[list[float]]]]


@dataclass(frozen=True)
class Intent:
    name: str
    kind: str
    phrases: tuple[str, ...]
    response_es: str = ""
    response_en: str = ""


GREETING_ES = (
    "¡Hola! Soy el asistente inteligente de IQ Analyzer. Estoy conectado "
    "directamente a los datos de tu empresa para ayudarte con consultas en "
    "lenguaje natural, análisis, métricas y comparaciones. ¿Qué información "
    "deseas analizar hoy?"
)
GREETING_EN = (
    "Hello! I am the IQ Analyzer intelligent assistant. I am connected directly "
    "to your company data to help with natural-language queries, analysis, "
    "metrics and comparisons. What would you like to analyze today?"
)
FAREWELL_ES = (
    "¡Hasta luego! Quedo a tu disposición en IQ Analyzer cuando necesites "
    "realizar nuevas consultas o análisis de datos."
)
FAREWELL_EN = (
    "Goodbye! I will be here in IQ Analyzer whenever you need new data queries "
    "or analysis."
)
THANKS_ES = (
    "¡De nada! Estoy aquí para ayudarte con métricas, comparaciones, series "
    "temporales y análisis de los datos de IQ Analyzer."
)
THANKS_EN = (
    "You're welcome! I am here to help with metrics, comparisons, time series "
    "and analysis of your IQ Analyzer data."
)
OUT_OF_SCOPE_ES = (
    "Disculpa, no tengo información sobre ese tema. Como asistente exclusivo de "
    "IQ Analyzer, mi función es ayudarte únicamente con consultas, comparaciones, "
    "métricas y análisis basados en los datos de la empresa. ¡Pídeme cualquier "
    "análisis sobre tu información y con gusto te ayudaré!"
)
OUT_OF_SCOPE_EN = (
    "Sorry, I do not have information about that topic. As the exclusive IQ "
    "Analyzer assistant, I can only help with queries, comparisons, metrics and "
    "analysis based on your company data. Ask me for any analysis of your "
    "information and I will be glad to help!"
)


INTENTS: tuple[Intent, ...] = (
    Intent(
        "greeting",
        "greeting",
        (
            "Hola",
            "Buenos días",
            "Buenas tardes",
            "Buenas noches",
            "Hello",
            "Hi",
            "Good morning",
            "Good afternoon",
            "Good evening",
            "¿Cómo estás?",
            "How are you?",
            "Qué tal",
            "What's up?",
        ),
        GREETING_ES,
        GREETING_EN,
    ),
    Intent(
        "identity",
        "identity",
        (
            "¿Quién eres?",
            "Cuéntame sobre ti",
            "Who are you?",
            "Tell me about yourself",
            "¿Qué haces?",
            "What do you do?",
            "¿Para qué sirves?",
            "What can you do?",
            "¿Eres un bot?",
            "Are you a bot?",
        ),
        GREETING_ES,
        GREETING_EN,
    ),
    Intent(
        "farewell",
        "farewell",
        (
            "Adiós",
            "Chao",
            "Hasta luego",
            "Nos vemos",
            "Bye",
            "Goodbye",
            "See you",
            "Hasta mañana",
        ),
        FAREWELL_ES,
        FAREWELL_EN,
    ),
    Intent(
        "thanks",
        "thanks",
        (
            "Gracias",
            "Muchas gracias",
            "Te agradezco",
            "Thanks",
            "Thank you",
            "Thanks a lot",
        ),
        THANKS_ES,
        THANKS_EN,
    ),
    Intent(
        "out_of_scope",
        "out_of_scope",
        (
            "¿Quién es Albert Einstein?",
            "Who is Albert Einstein?",
            "¿Cuál es la capital de Francia?",
            "What is the capital of France?",
            "Cuéntame un chiste",
            "Tell me a joke",
            "Hazme un código en Python",
            "Write Python code",
            "¿Cómo se hace una pizza?",
            "How do I make a pizza?",
            "Cuéntame la historia de Roma",
            "Tell me the history of Rome",
            "¿Quién ganó el mundial?",
            "What is the weather today?",
            "Escríbeme un poema",
            "Write me a poem",
        ),
        OUT_OF_SCOPE_ES,
        OUT_OF_SCOPE_EN,
    ),
    Intent(
        "analytics",
        "analytics",
        (
            "¿Cuál es el precio de Bitcoin?",
            "Muéstrame las métricas de Ethereum",
            "Compara BTC y ETH",
            "Analiza el volumen de SOL en 24 horas",
            "Dame un gráfico de velas de BTC",
            "¿Cuál es el retorno de ETH?",
            "Consulta la base de datos",
            "Quiero un análisis de tendencia",
            "Muéstrame el VWAP",
            "¿Cómo viene el mercado?",
            "Estado del pipeline",
            "Owner del dataset",
            "Consulta el catálogo",
            "What is the price of Bitcoin?",
            "Show me Ethereum metrics",
            "Compare BTC and ETH",
            "Analyze SOL volume",
            "Give me a candlestick chart",
            "What is the 24h return?",
            "Query the database",
            "Show me the trend",
            "What is the VWAP?",
            "How is the market?",
            "Pipeline status",
            "Dataset owner",
            "Catalog metadata",
        ),
    ),
)

# Fast safety net. Semantic routing is the primary classifier; these patterns
# make sure a known off-domain question never reaches Groq.
OUT_OF_SCOPE_PATTERNS = re.compile(
    r"(albert\s+einstein|capital\s+de\s+francia|capital\s+of\s+france|"
    r"cu[eé]ntame\s+un\s+chiste|tell\s+me\s+a\s+joke|"
    r"c[oó]digo\s+en\s+python|python\s+code|"
    r"c[oó]mo\s+se\s+hace\s+una\s+pizza|make\s+a\s+pizza|"
    r"historia\s+de\s+roma|history\s+of\s+rome|"
    r"qui[eé]n\s+gan[oó]\s+el\s+mundial|"
    r"what\s+is\s+the\s+weather|clima\s+de\s+hoy|"
    r"escr[ií]beme\s+un\s+poema|write\s+me\s+a\s+poem)",
    re.IGNORECASE,
)

GREETING_PATTERNS = re.compile(
    r"^\s*(hola|buenas|buenos d[i?]as|buenas tardes|buenas noches|"
    r"hello|hi|hey|good morning|good afternoon|good evening|qu[e?] tal)\b",
    re.IGNORECASE,
)
IDENTITY_PATTERNS = re.compile(
    r"(qui[e?]n eres|cu[e?]ntame sobre ti|who are you|tell me about yourself|"
    r"qu[e?] haces|what do you do|para qu[e?] sirves|what can you do)",
    re.IGNORECASE,
)
FAREWELL_PATTERNS = re.compile(
    r"\b(chao|adi[o?]s|hasta luego|nos vemos|bye|goodbye|see you|hasta ma?ana)\b",
    re.IGNORECASE,
)
THANKS_PATTERNS = re.compile(
    r"\b(gracias|muchas gracias|te agradezco|thanks|thank you|thanks a lot)\b",
    re.IGNORECASE,
)

ANALYTICS_HINTS = re.compile(
    r"(precio|price|m[eé]trica|metric|retorno|return|volumen|volume|vwap|"
    r"vela|candle|gr[aá]fico|chart|tendencia|trend|an[aá]lisis|analysis|"
    r"compara|compare|consulta|query|base\s+de\s+datos|database|pipeline|"
    r"kafka|spark|airflow|minio|dataset|catalog|cat[aá]logo|owner|linaje|"
    r"btc|eth|sol|bnb|mercado|market)",
    re.IGNORECASE,
)


@dataclass
class RouterDecision:
    kind: str
    intent: str
    response: str | None
    score: float
    matched: str | None
    backend: str
    latency_ms: float

    @classmethod
    def analytics(cls, score: float = 0.0, backend: str = "fallback") -> "RouterDecision":
        return cls("analytics", "analytics", None, score, None, backend, 0.0)


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    return dot / (norm_a * norm_b) if norm_a and norm_b else 0.0


def _looks_english(text: str) -> bool:
    return bool(
        re.search(
            r"\b(hello|hi|who|what|how|tell|goodbye|bye|thanks|thank|price|"
            r"show|compare|market|analysis|query|database|the|is|are|you)\b",
            text,
            re.IGNORECASE,
        )
    )


class SemanticRouter:
    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "bge-m3",
        threshold: float = 0.72,
        timeout_seconds: float = 20.0,
        cache_path: Path | None = None,
        embed_fn: EmbedFn | None = None,
        enabled: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.threshold = threshold
        self.timeout_seconds = timeout_seconds
        self.cache_path = cache_path
        self._embed_fn = embed_fn
        self.enabled = enabled
        self._vectors: dict[str, list[list[float]]] = {}
        self._lock = asyncio.Lock()
        self._backend = "not-loaded"

    def _fingerprint(self) -> str:
        payload = json.dumps(
            [(intent.name, list(intent.phrases)) for intent in INTENTS],
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def _embed(self, texts: Sequence[str]) -> list[list[float]]:
        if self._embed_fn is not None:
            result = self._embed_fn(texts)
            if inspect.isawaitable(result):
                result = await result
            return result

        embeddings: list[list[float]] = []
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            for start in range(0, len(texts), 8):
                batch = list(texts[start : start + 8])
                try:
                    response = await client.post(
                        f"{self.base_url}/api/embed",
                        json={"model": self.model, "input": batch},
                    )
                    if response.status_code < 400:
                        payload = response.json()
                        batch_vectors = payload.get("embeddings")
                        if isinstance(batch_vectors, list) and len(batch_vectors) == len(batch):
                            embeddings.extend(batch_vectors)
                            continue
                except Exception:  # noqa: BLE001 - try legacy endpoint
                    pass

                # Older Ollama builds expose /api/embeddings one input at a time.
                for text in batch:
                    response = await client.post(
                        f"{self.base_url}/api/embeddings",
                        json={"model": self.model, "prompt": text},
                    )
                    response.raise_for_status()
                    embeddings.append(response.json()["embedding"])
        return embeddings

    async def _ensure_vectors(self) -> None:
        if self._vectors:
            return
        async with self._lock:
            if self._vectors:
                return

            fingerprint = self._fingerprint()
            if self.cache_path and self.cache_path.exists():
                try:
                    cached = json.loads(self.cache_path.read_text(encoding="utf-8"))
                    if (
                        cached.get("model") == self.model
                        and cached.get("fingerprint") == fingerprint
                        and isinstance(cached.get("vectors"), dict)
                    ):
                        self._vectors = cached["vectors"]
                        self._backend = "ollama-cache"
                        return
                except Exception:  # noqa: BLE001
                    pass

            phrases = [phrase for intent in INTENTS for phrase in intent.phrases]
            vectors = await self._embed(phrases)
            offset = 0
            built: dict[str, list[list[float]]] = {}
            for intent in INTENTS:
                built[intent.name] = vectors[offset : offset + len(intent.phrases)]
                offset += len(intent.phrases)
            self._vectors = built
            self._backend = "ollama" if self._embed_fn is None else "injected"

            if self.cache_path:
                try:
                    self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                    self.cache_path.write_text(
                        json.dumps(
                            {
                                "model": self.model,
                                "fingerprint": fingerprint,
                                "vectors": built,
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("No se pudo guardar cache del router: %s", exc)

    async def route(self, text: str) -> RouterDecision:
        started = time.perf_counter()
        message = (text or "").strip()
        if not message:
            return RouterDecision.analytics(backend="empty")

        # Known off-domain questions are blocked even if embeddings are down.
        if OUT_OF_SCOPE_PATTERNS.search(message):
            response = OUT_OF_SCOPE_EN if _looks_english(message) else OUT_OF_SCOPE_ES
            return RouterDecision(
                "out_of_scope",
                "out_of_scope",
                response,
                1.0,
                "lexical",
                "lexical",
                round((time.perf_counter() - started) * 1000, 2),
            )

        # If the message contains a clear data/analysis request, never answer
        # it with a greeting just because "hola" is semantically close.
        if ANALYTICS_HINTS.search(message):
            return RouterDecision(
                "analytics",
                "analytics",
                None,
                1.0,
                "analytics-hint",
                "lexical",
                round((time.perf_counter() - started) * 1000, 2),
            )

        # Short conversational messages are high-confidence even when the
        # embedding score sits just below the threshold (e.g. "chao" ~0.71).
        lexical_intents = (
            (FAREWELL_PATTERNS, "farewell", FAREWELL_ES, FAREWELL_EN),
            (THANKS_PATTERNS, "thanks", THANKS_ES, THANKS_EN),
            (IDENTITY_PATTERNS, "identity", GREETING_ES, GREETING_EN),
            (GREETING_PATTERNS, "greeting", GREETING_ES, GREETING_EN),
        )
        for pattern, kind, response_es, response_en in lexical_intents:
            if pattern.search(message):
                return RouterDecision(
                    kind,
                    kind,
                    response_en if _looks_english(message) else response_es,
                    1.0,
                    "lexical-intent",
                    "lexical",
                    round((time.perf_counter() - started) * 1000, 2),
                )

        if not self.enabled:
            return RouterDecision.analytics(backend="disabled")

        try:
            await self._ensure_vectors()
            query_vector = (await self._embed([message]))[0]
        except Exception as exc:  # noqa: BLE001 - never block chat on router
            LOGGER.warning("Router semántico no disponible, se delega a Groq: %s", exc)
            return RouterDecision.analytics(backend="unavailable")

        best_intent: Intent | None = None
        best_score = -1.0
        best_phrase: str | None = None
        for intent in INTENTS:
            for phrase, vector in zip(intent.phrases, self._vectors.get(intent.name, [])):
                score = _cosine(query_vector, vector)
                if score > best_score:
                    best_score = score
                    best_intent = intent
                    best_phrase = phrase

        latency = round((time.perf_counter() - started) * 1000, 2)
        if best_intent is None or best_score < self.threshold:
            return RouterDecision(
                "analytics",
                "analytics",
                None,
                round(best_score, 4),
                best_phrase,
                self._backend,
                latency,
            )

        if best_intent.kind == "analytics":
            return RouterDecision(
                "analytics",
                best_intent.name,
                None,
                round(best_score, 4),
                best_phrase,
                self._backend,
                latency,
            )

        response = best_intent.response_en if _looks_english(message) else best_intent.response_es
        return RouterDecision(
            best_intent.kind,
            best_intent.name,
            response,
            round(best_score, 4),
            best_phrase,
            self._backend,
            latency,
        )

    def status(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "backend": self._backend,
            "model": self.model,
            "threshold": self.threshold,
            "intents": [intent.name for intent in INTENTS],
            "vectors_loaded": bool(self._vectors),
        }
