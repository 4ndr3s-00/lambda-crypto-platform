"""Groq inference, local semantic routing, tool orchestration and fallback (US-28/29/30)."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

import httpx

try:
    from .config import DISCLAIMER, Settings, get_settings
    from .models import ChatChart, ChatRequest, ChatResponse, SourceReference, ToolCallRecord
    from .query_service import QueryService
    from .rag_indexer import RagHit, RagIndex
    from .semantic_router import ANALYTICS_HINTS, RouterDecision, SemanticRouter, _looks_english
    from .sql_guardrails import validate_sql
    from .tools import TOOL_SPECS, ToolRegistry, ToolResult
except ImportError:  # Docker / uvicorn main:app
    from config import DISCLAIMER, Settings, get_settings
    from models import ChatChart, ChatRequest, ChatResponse, SourceReference, ToolCallRecord
    from query_service import QueryService
    from rag_indexer import RagHit, RagIndex
    from semantic_router import ANALYTICS_HINTS, RouterDecision, SemanticRouter, _looks_english
    from sql_guardrails import validate_sql
    from tools import TOOL_SPECS, ToolRegistry, ToolResult

LOGGER = logging.getLogger("iq_analyzer.llm")

SYSTEM_PROMPT = """<system_prompt>
<role>
Eres el Asistente Analítico Oficial de IQ Analyzer. Estás conectado a la base de datos, al contexto RAG y a las herramientas de solo lectura del sistema.
</role>
<directives>
1. Responde en texto plano, natural y breve. No uses Markdown: nada de **, __, backticks, encabezados, tablas dibujadas a mano ni bloques de código.
2. Si necesitas enumerar, usa líneas simples que empiecen con "- ".
3. Responde en el idioma del usuario, español o inglés.
4. Para cifras de mercado usa get_catalog_metadata, query_metrics o query_timeseries antes de responder. Nunca inventes números ni SQL.
5. Basa conclusiones solo en los datos de las herramientas o en el contexto RAG. Si no hay contexto suficiente, dilo claramente; no inventes.
6. Si la pregunta es de cultura general o fuera de IQ Analyzer, responde con el guardrail de dominio y no consultes la base de datos.
7. No repitas el SQL ni el JSON de las herramientas en la respuesta; el sistema ya muestra la trazabilidad por separado.
</directives>
</system_prompt>"""


class OllamaUnavailable(RuntimeError):
    """Ollama isn't running or the model isn't pulled yet."""


class GroqUnavailable(RuntimeError):
    """Groq is not configured, rate-limited or unreachable."""


def _strip_code_fence(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _clean_answer_text(text: str) -> str:
    """Remove Markdown artifacts so the UI receives clean prose."""
    cleaned = text.strip()
    cleaned = re.sub(r"```.*?```", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.replace("**", "").replace("__", "").replace("`", "")
    lines: list[str] = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if re.fullmatch(r"\|?[\s:|-]+\|?", line):
            continue
        if line.count("|") >= 2:
            cells = [cell.strip() for cell in line.strip("|").split("|") if cell.strip()]
            line = " · ".join(cells)
        line = re.sub(r"^#{1,6}\s*", "", line)
        if line.startswith("* "):
            line = "- " + line[2:]
        lines.append(line)
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _parse_model_json(text: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(text)
    if not cleaned:
        return None
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            try:
                payload = json.loads(cleaned[start : end + 1])
                return payload if isinstance(payload, dict) else None
            except json.JSONDecodeError:
                return None
    return None


def _detect_symbol(message: str, fallback: str) -> str:
    upper = message.upper()
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        if symbol in upper or symbol.replace("USDT", "") in upper:
            return symbol
    candidate = (fallback or "BTCUSDT").upper()
    return candidate if candidate in {"BTCUSDT", "ETHUSDT", "SOLUSDT"} else "BTCUSDT"


def _detect_interval(message: str, fallback: str) -> str:
    lower = message.lower()
    if re.search(r"\b(semana|week|7 d[ií]as|siete d[ií]as|diaria|daily)\b", lower):
        return "1d"
    if re.search(r"\b(4 horas|4 hours|4h)\b", lower):
        return "4h"
    match = re.search(r"\b(1m|5m|15m|1h|4h|1d|7d)\b", message, re.IGNORECASE)
    if match:
        return match.group(1)
    candidate = (fallback or "15m").strip()
    return candidate if candidate in {"1m", "5m", "15m", "1h", "4h", "1d", "1D", "7D"} else "15m"


def _detect_window(message: str) -> str:
    lower = message.lower()
    if "7d" in lower or "7 días" in lower or "semana" in lower:
        return "7d"
    if "4h" in lower or "4 horas" in lower:
        return "4h"
    if "1h" in lower or "1 hora" in lower:
        return "1h"
    return "24h"


def _money(value: float) -> str:
    return f"${value:,.2f}"


class LLMService:
    def __init__(
        self,
        query_service: QueryService,
        rag_index: RagIndex,
        settings: Settings | None = None,
        router: SemanticRouter | None = None,
    ) -> None:
        self.query_service = query_service
        self.rag_index = rag_index
        self.settings = settings or get_settings()
        self.tools = ToolRegistry(query_service)
        self.router = router or SemanticRouter(
            base_url=self.settings.ollama_base_url,
            model=self.settings.ollama_embedding_model,
            threshold=self.settings.semantic_router_threshold,
            cache_path=self.settings.semantic_router_cache_path,
            enabled=self.settings.semantic_router_enabled,
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def answer(self, request: ChatRequest) -> ChatResponse:
        started = time.perf_counter()
        message = request.message.strip()
        symbol = _detect_symbol(message, request.symbol)
        interval = _detect_interval(message, request.interval)

        decision = RouterDecision.analytics()
        if self.settings.semantic_router_enabled and self.settings.groq_api_key:
            try:
                decision = await self.router.route(message)
            except Exception as exc:  # noqa: BLE001 - router must never break chat
                LOGGER.warning("Router semántico falló, se continúa con Groq: %s", exc)

        if decision.kind != "analytics" and decision.response:
            return ChatResponse(
                answer=decision.response,
                sql=None,
                provenance="RAG",
                asset=None,
                source="semantic_router",
                sources=[],
                cutoff_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                disclaimer=DISCLAIMER,
                tool_calls=[],
                rag_used=False,
                intent=decision.intent,
                router_score=decision.score,
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
            )

        rag_hits: list[RagHit] = []
        try:
            rag_hits = self.rag_index.query(message, k=4)
        except Exception as exc:  # noqa: BLE001 - RAG is best-effort
            LOGGER.warning("RAG query failed: %s", exc)

        if not rag_hits and not ANALYTICS_HINTS.search(message):
            return ChatResponse(
                answer=(
                    "No tengo información suficiente en la base documental para responder eso. "
                    "Puedo ayudarte con métricas, velas, comparaciones, catálogo o estado del pipeline de IQ Analyzer."
                ),
                sql=None,
                provenance="RAG",
                asset=None,
                source="rag_guard",
                sources=[],
                cutoff_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                disclaimer=DISCLAIMER,
                tool_calls=[],
                rag_used=False,
                intent="rag_guard",
                router_score=decision.score,
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
            )

        model_result: dict[str, Any] | None = None
        pipeline_query = re.search(
            r"\b(pipeline|dlq|lag|salud|estado|status|health|consumer)\b",
            message,
            re.IGNORECASE,
        )
        history_query = re.search(
            r"\b(semana|week|7 d[ií]as|siete d[ií]as|historial|history)\b",
            message,
            re.IGNORECASE,
        )
        if pipeline_query or history_query:
            # Pipeline snapshots and multi-day history are resolved with the
            # local read-only tools. This avoids the model choosing 15m/24
            # when the user asked for a week.
            model_result = self._fallback(message, symbol, interval)
        elif self.settings.groq_api_key:
            try:
                model_result = await self._run_groq(message, symbol, interval, rag_hits)
            except GroqUnavailable as exc:
                LOGGER.info("Groq no disponible, usando fallback determinista: %s", exc)
            except Exception as exc:  # noqa: BLE001 - never 500 the chat
                LOGGER.warning("Fallo de Groq, usando fallback: %s", exc)
        else:
            try:
                model_result = await self._run_ollama(message, symbol, interval, rag_hits)
            except OllamaUnavailable as exc:
                LOGGER.info("Ollama no disponible, usando fallback determinista: %s", exc)
            except Exception as exc:  # noqa: BLE001 - never 500 the chat
                LOGGER.warning("Fallo de inferencia, usando fallback: %s", exc)

        tool_results: list[ToolResult] = []
        tool_records: list[ToolCallRecord] = []
        if model_result:
            tool_results = model_result.get("tool_results", [])
            tool_records = model_result.get("tool_records", [])
            answer_text = str(model_result.get("answer", "")).strip()
            sql = model_result.get("sql")
            provenance = str(model_result.get("provenance") or "RAG")
        else:
            fallback = self._fallback(message, symbol, interval)
            tool_results = fallback["tool_results"]
            tool_records = fallback["tool_records"]
            answer_text = fallback["answer"]
            sql = fallback["sql"]
            provenance = fallback["provenance"]

        if not answer_text:
            fallback = self._fallback(message, symbol, interval)
            tool_results = fallback["tool_results"]
            tool_records = fallback["tool_records"]
            answer_text = fallback["answer"]
            sql = fallback["sql"]
            provenance = fallback["provenance"]

        cutoff_time = next(
            (result.cutoff_time for result in tool_results if result.cutoff_time),
            "",
        ) or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        asset = next(
            (str(result.payload.get("symbol")) for result in tool_results if result.payload.get("symbol")),
            symbol,
        )
        source = next((result.source for result in tool_results if result.source), None)

        chart: ChatChart | None = None
        metrics_result = next(
            (result for result in tool_results if result.name == "query_metrics" and result.ok),
            None,
        )
        if metrics_result is not None:
            metrics_payload = metrics_result.payload
            metrics_symbol = str(metrics_payload.get("symbol") or symbol)
            metrics_window = str(metrics_payload.get("window") or "24h")
            chart_interval = "1h" if metrics_window in {"24h", "7d"} else "15m"
            chart_limit = 168 if metrics_window == "7d" else 24 if metrics_window == "24h" else 4
            try:
                chart_payload = self.query_service.get_market_ohlcv(
                    metrics_symbol, interval=chart_interval, limit=chart_limit
                )
                chart_candles = chart_payload.get("candles", [])
                if chart_candles:
                    chart = ChatChart(
                        symbol=metrics_symbol,
                        interval=chart_interval,
                        candles=chart_candles,
                        source=str(chart_payload.get("source") or ""),
                        data_source=str(chart_payload.get("data_source") or "no_data_available"),
                        is_live=bool(chart_payload.get("is_live", True)),
                        cutoff_time=str(chart_payload.get("cutoff_time") or ""),
                    )
            except Exception as exc:  # noqa: BLE001 - chart is best-effort
                LOGGER.warning("No se pudo construir chart de m?tricas: %s", exc)

        for result in tool_results:
            if result.name == "query_timeseries" and result.ok:
                payload = result.payload
                candles = payload.get("candles", [])
                if candles:
                    chart = ChatChart(
                        symbol=str(payload.get("symbol") or symbol),
                        interval=str(payload.get("interval") or interval),
                        candles=candles,
                        source=str(payload.get("source") or ""),
                        data_source=str(payload.get("data_source") or "no_data_available"),
                        is_live=bool(payload.get("is_live", True)),
                        cutoff_time=str(payload.get("cutoff_time") or ""),
                    )
                break

        sources = self._merge_sources(rag_hits, tool_results)
        if source is None and sources:
            source = sources[0].source
        answer_text = _clean_answer_text(answer_text)
        answer_text = self._ensure_disclaimer(answer_text)
        provenance = self._normalize_provenance(provenance, tool_results)

        return ChatResponse(
            answer=answer_text,
            sql=sql if request.include_sql else None,
            provenance=provenance,  # type: ignore[arg-type]
            asset=asset,
            source=source,
            sources=sources,
            cutoff_time=cutoff_time,
            disclaimer=DISCLAIMER,
            tool_calls=tool_records,
            rag_used=bool(rag_hits),
            intent=decision.intent,
            router_score=decision.score,
            chart=chart,
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
        )

    # ------------------------------------------------------------------
    # Groq tool loop
    # ------------------------------------------------------------------

    async def _run_groq(
        self,
        message: str,
        symbol: str,
        interval: str,
        rag_hits: list[RagHit],
    ) -> dict[str, Any]:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self._system_prompt(rag_hits)},
            {
                "role": "user",
                "content": (
                    f"Consulta: {message}\n"
                    f"Activo sugerido: {symbol}\n"
                    f"Intervalo sugerido: {interval}\n"
                    "Si la pregunta requiere datos, invoca una herramienta antes de responder."
                ),
            },
        ]

        tool_results: list[ToolResult] = []
        tool_records: list[ToolCallRecord] = []

        for _ in range(3):
            payload = await self._post_groq(messages, tools=TOOL_SPECS)
            choices = payload.get("choices") or []
            response_message = (choices[0].get("message") if choices else {}) or {}
            raw_calls = response_message.get("tool_calls") or []

            if raw_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": response_message.get("content") or "",
                        "tool_calls": raw_calls,
                    }
                )
                for raw_call in raw_calls:
                    function = raw_call.get("function") or {}
                    name = str(function.get("name") or "")
                    arguments = function.get("arguments") or {}
                    if isinstance(arguments, str):
                        try:
                            arguments = json.loads(arguments)
                        except json.JSONDecodeError:
                            arguments = {}
                    if not isinstance(arguments, dict):
                        arguments = {}
                    result = self.tools.execute(name, arguments)
                    tool_results.append(result)
                    tool_records.append(
                        ToolCallRecord(
                            name=name,
                            arguments=arguments,
                            result_summary=result.summary if result.ok else (result.error or "error"),
                        )
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": raw_call.get("id"),
                            "name": name,
                            "content": json.dumps(
                                result.payload if result.ok else {"error": result.error},
                                ensure_ascii=False,
                                default=str,
                            ),
                        }
                    )
                continue

            content = str(response_message.get("content") or "").strip()
            refused = bool(
                re.search(
                    r"(no cuento con informaci[oó]n|no tengo informaci[oó]n|"
                    r"i do not have information|i don't have information)",
                    content,
                    re.IGNORECASE,
                )
            )
            if not tool_results and ANALYTICS_HINTS.search(message) and (not content or refused):
                return self._fallback(message, symbol, interval)

            parsed = _parse_model_json(content)
            if parsed is not None:
                model_sql = parsed.get("sql")
                if isinstance(model_sql, str) and model_sql.strip():
                    guard = validate_sql(model_sql)
                    model_sql = guard.sql if guard.ok else None
                else:
                    model_sql = None
                return {
                    "answer": str(parsed.get("answer") or content),
                    "sql": model_sql or next(
                        (result.sql for result in tool_results if result.sql),
                        None,
                    ),
                    "provenance": parsed.get("provenance"),
                    "tool_results": tool_results,
                    "tool_records": tool_records,
                }
            # Groq sometimes returns only a heading after a tool call, an
            # empty completion, or the domain guardrail for a legitimate data
            # question. Never return a data answer without the actual numbers:
            # fall back to the deterministic tool summary.
            refused = bool(
                re.search(
                    r"(no cuento con informaci[oó]n|no tengo informaci[oó]n|"
                    r"i do not have information|i don't have information)",
                    content,
                    re.IGNORECASE,
                )
            )
            if (tool_results and (not content or not re.search(r"\d", content))) or (
                not tool_results and ANALYTICS_HINTS.search(message) and (not content or refused)
            ):
                fallback = self._fallback(message, symbol, interval)
                fallback["tool_results"] = tool_results
                fallback["tool_records"] = tool_records
                return fallback

            return {
                "answer": content,
                "sql": next((result.sql for result in tool_results if result.sql), None),
                "provenance": next(
                    (result.provenance for result in tool_results),
                    "RAG",
                ),
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        fallback = self._fallback(message, symbol, interval)
        fallback["tool_results"] = tool_results
        fallback["tool_records"] = tool_records
        return fallback

    async def _post_groq(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if not self.settings.groq_api_key:
            raise GroqUnavailable("GROQ_API_KEY no configurada")

        url = f"{self.settings.groq_base_url.rstrip('/')}/chat/completions"
        payload: dict[str, Any] = {
            "model": self.settings.groq_model,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 512,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        headers = {
            "Authorization": f"Bearer {self.settings.groq_api_key}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self.settings.groq_timeout_seconds) as client:
            response = await client.post(url, headers=headers, json=payload)
            if response.status_code >= 400:
                raise GroqUnavailable(
                    f"Groq respondió {response.status_code}: {response.text[:300]}"
                )
            data = response.json()
            if not isinstance(data, dict):
                raise GroqUnavailable("respuesta de Groq inválida")
            return data

    # ------------------------------------------------------------------
    # Ollama tool loop
    # ------------------------------------------------------------------

    async def _run_ollama(
        self,
        message: str,
        symbol: str,
        interval: str,
        rag_hits: list[RagHit],
    ) -> dict[str, Any]:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self._system_prompt(rag_hits)},
            {
                "role": "user",
                "content": (
                    f"Consulta: {message}\n"
                    f"Activo sugerido: {symbol}\n"
                    f"Intervalo sugerido: {interval}\n"
                    "Si la pregunta requiere datos, invoca una herramienta antes de responder."
                ),
            },
        ]

        tool_results: list[ToolResult] = []
        tool_records: list[ToolCallRecord] = []

        for _ in range(3):
            payload = await self._post_ollama(messages, tools=TOOL_SPECS)
            response_message = payload.get("message") or {}
            raw_calls = response_message.get("tool_calls") or []

            if raw_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": response_message.get("content") or "",
                        "tool_calls": raw_calls,
                    }
                )
                for raw_call in raw_calls:
                    function = raw_call.get("function") or {}
                    name = str(function.get("name") or "")
                    arguments = function.get("arguments") or {}
                    if isinstance(arguments, str):
                        try:
                            arguments = json.loads(arguments)
                        except json.JSONDecodeError:
                            arguments = {}
                    if not isinstance(arguments, dict):
                        arguments = {}
                    result = self.tools.execute(name, arguments)
                    tool_results.append(result)
                    tool_records.append(
                        ToolCallRecord(
                            name=name,
                            arguments=arguments,
                            result_summary=result.summary if result.ok else (result.error or "error"),
                        )
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "name": name,
                            "content": json.dumps(
                                result.payload if result.ok else {"error": result.error},
                                ensure_ascii=False,
                                default=str,
                            ),
                        }
                    )
                continue

            content = str(response_message.get("content") or "").strip()
            parsed = _parse_model_json(content)
            if parsed is not None:
                model_sql = parsed.get("sql")
                if isinstance(model_sql, str) and model_sql.strip():
                    guard = validate_sql(model_sql)
                    model_sql = guard.sql if guard.ok else None
                else:
                    model_sql = None
                return {
                    "answer": str(parsed.get("answer") or content),
                    "sql": model_sql or next(
                        (result.sql for result in tool_results if result.sql),
                        None,
                    ),
                    "provenance": parsed.get("provenance"),
                    "tool_results": tool_results,
                    "tool_records": tool_records,
                }
            return {
                "answer": content,
                "sql": next((result.sql for result in tool_results if result.sql), None),
                "provenance": next(
                    (result.provenance for result in tool_results),
                    "RAG",
                ),
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        return {
            "answer": self._fallback(message, symbol, interval)["answer"],
            "sql": next((result.sql for result in tool_results if result.sql), None),
            "provenance": next((result.provenance for result in tool_results), "RAG"),
            "tool_results": tool_results,
            "tool_records": tool_records,
        }

    async def _post_ollama(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/chat"
        payload: dict[str, Any] = {
            "model": self.settings.ollama_model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 320},
        }
        if tools:
            payload["tools"] = tools

        async with httpx.AsyncClient(timeout=self.settings.ollama_timeout_seconds) as client:
            response = await client.post(url, json=payload)
            if response.status_code >= 400 and tools:
                # Older Ollama builds or models without tool support: retry
                # without tools and let the system prompt enforce fallback.
                payload.pop("tools", None)
                response = await client.post(url, json=payload)
            if response.status_code >= 400:
                raise OllamaUnavailable(
                    f"Ollama respondió {response.status_code}: {response.text[:200]}"
                )
            data = response.json()
            if not isinstance(data, dict):
                raise OllamaUnavailable("respuesta de Ollama inválida")
            return data

    # ------------------------------------------------------------------
    # Deterministic fallback (works without Ollama)
    # ------------------------------------------------------------------

    def _fallback(self, message: str, symbol: str, interval: str) -> dict[str, Any]:
        lower = message.lower()
        tool_results: list[ToolResult] = []
        tool_records: list[ToolCallRecord] = []

        def run_tool(name: str, args: dict[str, Any]) -> ToolResult:
            result = self.tools.execute(name, args)
            tool_results.append(result)
            tool_records.append(
                ToolCallRecord(
                    name=name,
                    arguments=args,
                    result_summary=result.summary if result.ok else (result.error or "error"),
                )
            )
            return result

        is_en = _looks_english(message)
        symbols_in_message = [
            candidate
            for candidate in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")
            if candidate in message.upper() or candidate.replace("USDT", "") in message.upper()
        ]

        if len(symbols_in_message) >= 2 and re.search(r"\b(compare|comparar|comparaci[oó]n|comparison|vs|versus)\b", lower):
            window = _detect_window(message)
            comparisons = [run_tool("query_metrics", {"symbol": item, "window": window}) for item in symbols_in_message[:2]]
            if all(item.ok for item in comparisons):
                if is_en:
                    lines = [
                        f"- {item.payload['symbol']}: price {_money(item.payload['current_price'])}, "
                        f"return {item.payload['return_24h']:+.2f}%, volume {_money(item.payload['volume_24h'])}"
                        for item in comparisons
                    ]
                    answer = f"Comparison ({window} window):\n" + "\n".join(lines)
                else:
                    lines = [
                        f"- {item.payload['symbol']}: precio {_money(item.payload['current_price'])}, "
                        f"retorno {item.payload['return_24h']:+.2f}%, volumen {_money(item.payload['volume_24h'])}"
                        for item in comparisons
                    ]
                    answer = f"Comparación (ventana {window}):\n" + "\n".join(lines)
            else:
                answer = "Could not compare both assets." if is_en else "No pude comparar ambos activos."
            return {
                "answer": answer,
                "sql": comparisons[-1].sql if comparisons else None,
                "provenance": comparisons[-1].provenance if comparisons else "SPEED_LAYER",
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        if re.search(r"\b(pipeline|dlq|lag|salud|estado|status|health|consumer)\b", lower):
            status = self.query_service.get_market_pipeline_status()
            tool_results.append(
                ToolResult(
                    name="query_pipeline_status",
                    arguments={},
                    payload=status,
                    summary=(
                        f"Kafka lag={status['kafka']['consumer_lag']} · "
                        f"Spark={status['spark']['status']} · DLQ={status['dlq']['rejected_events']}"
                    ),
                    source="serving.pipeline_status",
                    cutoff_time=str(status.get("timestamp", "")),
                    provenance="SERVING_LAYER",
                )
            )
            tool_records.append(
                ToolCallRecord(
                    name="query_pipeline_status",
                    arguments={},
                    result_summary=tool_results[-1].summary,
                )
            )
            if is_en:
                answer = (
                    "Lambda pipeline status:\n"
                    f"- Kafka: {status['kafka']['status']} · lag {status['kafka']['consumer_lag']} msgs · "
                    f"{status['kafka']['ingestion_rate']}\n"
                    f"- Spark: {status['spark']['status']} · window {status['spark']['window']} · "
                    f"watermark {status['spark']['watermark']}\n"
                    f"- Airflow: {status['airflow']['dag_id']} {status['airflow']['status']} · "
                    f"last run {status['airflow']['last_run']}\n"
                    f"- MinIO: {status['minio']['status']} · {status['minio']['path']}\n"
                    f"- DLQ: {status['dlq']['rejected_events']} rejected events\n"
                    f"- Freshness: {status['freshness_seconds']}s"
                )
            else:
                answer = (
                    "Estado del pipeline Lambda:\n"
                    f"- Kafka: {status['kafka']['status']} · lag {status['kafka']['consumer_lag']} msgs · "
                    f"{status['kafka']['ingestion_rate']}\n"
                    f"- Spark: {status['spark']['status']} · ventana {status['spark']['window']} · "
                    f"watermark {status['spark']['watermark']}\n"
                    f"- Airflow: {status['airflow']['dag_id']} {status['airflow']['status']} · "
                    f"último run {status['airflow']['last_run']}\n"
                    f"- MinIO: {status['minio']['status']} · {status['minio']['path']}\n"
                    f"- DLQ: {status['dlq']['rejected_events']} rechazos\n"
                    f"- Frescura: {status['freshness_seconds']}s"
                )
            return {
                "answer": answer,
                "sql": (
                    "SELECT consumer_group, topic, consumer_lag_msgs, micro_batch_ms, "
                    "watermark_s, dlq_rejected_events, measured_at\n"
                    "FROM ops.pipeline_telemetry_metrics\n"
                    "ORDER BY measured_at DESC\n"
                    "LIMIT 1;"
                ),
                "provenance": "SERVING_LAYER",
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        if re.search(r"\b(cat[aá]logo|linaje|lineage|owner|schema|esquema|dataset|gobierno|catalog|governance)\b", lower):
            dataset = "market_metrics" if "metric" in lower or "retorno" in lower else "market_ohlcv"
            result = run_tool("get_catalog_metadata", {"dataset": dataset})
            if result.ok:
                payload = result.payload
                lineage = " → ".join(str(step) for step in payload.get("lineage", []))
                answer = (
                    f"Dataset {payload.get('dataset')}:\n"
                    f"- Owner: {payload.get('owner')}\n"
                    f"- Schema: {payload.get('schema_version')}\n"
                    f"- Retención: {payload.get('retention')}\n"
                    f"- Particiones: {payload.get('partitions')}\n"
                    f"- Linaje: {lineage}"
                )
            else:
                answer = f"No pude consultar el catálogo: {result.error}"
            return {
                "answer": answer,
                "sql": result.sql,
                "provenance": result.provenance,
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        if re.search(r"\b(retorno|volumen|vwap|varianza|volatilidad|m[eé]trica|rendimiento|return|volume|variance|volatility|metric|metrics|return|returns|volume|volumes|performance)\b", lower):
            window = _detect_window(message)
            result = run_tool("query_metrics", {"symbol": symbol, "window": window})
            if result.ok:
                payload = result.payload
                if is_en:
                    answer = (
                        f"{payload['symbol']} · {window} window:\n"
                        f"- Current price: {_money(payload['current_price'])}\n"
                        f"- Return: {payload['return_24h']:+.2f}%\n"
                        f"- Volume: {_money(payload['volume_24h'])}\n"
                        f"- VWAP: {_money(payload['vwap'])}\n"
                        f"- Price variance: {payload['price_variance']:.2f}\n"
                        f"- Source: {payload['source']}\n"
                        f"- Cutoff: {payload['cutoff_time']}"
                    )
                else:
                    answer = (
                        f"{payload['symbol']} · ventana {window}:\n"
                        f"- Precio actual: {_money(payload['current_price'])}\n"
                        f"- Retorno: {payload['return_24h']:+.2f}%\n"
                        f"- Volumen: {_money(payload['volume_24h'])}\n"
                        f"- VWAP: {_money(payload['vwap'])}\n"
                        f"- Varianza de precio: {payload['price_variance']:.2f}\n"
                        f"- Fuente: {payload['source']}\n"
                        f"- Corte: {payload['cutoff_time']}"
                    )
            else:
                answer = f"No pude calcular métricas: {result.error}"
            return {
                "answer": answer,
                "sql": result.sql,
                "provenance": result.provenance,
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        if re.search(r"\b(semana|week|7 d[ií]as|siete d[ií]as|historial|history)\b", lower):
            target_interval = "1d" if re.search(r"(semana|week|diaria|daily|7 d[ií]as|siete d[ií]as)", lower) else interval
            target_limit = 7 if target_interval == "1d" else 672
            result = run_tool(
                "query_timeseries",
                {"symbol": symbol, "interval": target_interval, "limit": target_limit},
            )
            if result.ok:
                candles = result.payload.get("candles", [])
                last = candles[-1] if candles else {}
                high = max((c["high"] for c in candles), default=0)
                low = min((c["low"] for c in candles), default=0)
                if is_en:
                    answer = (
                        f"{result.payload['symbol']} · {target_interval} · {len(candles)} candles (last {target_limit} periods):\n"
                        f"- Last close: {_money(float(last.get('close', 0)))}\n"
                        f"- High: {_money(float(high))}\n"
                        f"- Low: {_money(float(low))}\n"
                        f"- Source: {result.payload.get('source')}\n"
                        f"- Cutoff: {result.payload.get('cutoff_time')}"
                    )
                else:
                    answer = (
                        f"{result.payload['symbol']} · {target_interval} · {len(candles)} velas (últimos {target_limit} períodos):\n"
                        f"- último close: {_money(float(last.get('close', 0)))}\n"
                        f"- Máximo: {_money(float(high))}\n"
                        f"- Mínimo: {_money(float(low))}\n"
                        f"- Fuente: {result.payload.get('source')}\n"
                        f"- Corte: {result.payload.get('cutoff_time')}"
                    )
            else:
                answer = "Could not retrieve history." if is_en else "No pude consultar el historial."
            return {
                "answer": answer,
                "sql": result.sql,
                "provenance": result.provenance,
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        if re.search(r"\b(precio|vela|velas|ohlcv|m[aá]ximo|m[ií]nimo|serie|candle|cierre|apertura|price|prices|high|low|series|close|open)\b", lower):
            limit = 24
            result = run_tool(
                "query_timeseries",
                {"symbol": symbol, "interval": interval, "limit": limit},
            )
            if result.ok:
                candles = result.payload.get("candles", [])
                last = candles[-1] if candles else {}
                high = max((c["high"] for c in candles), default=0)
                low = min((c["low"] for c in candles), default=0)
                if is_en:
                    answer = (
                        f"{result.payload['symbol']} · {result.payload['interval']} · {len(candles)} candles:\n"
                        f"- Last close: {_money(float(last.get('close', 0)))}\n"
                        f"- High: {_money(float(high))}\n"
                        f"- Low: {_money(float(low))}\n"
                        f"- Source: {result.payload.get('source')}\n"
                        f"- Cutoff: {result.payload.get('cutoff_time')}"
                    )
                else:
                    answer = (
                        f"{result.payload['symbol']} · {result.payload['interval']} · {len(candles)} velas:\n"
                        f"- Último close: {_money(float(last.get('close', 0)))}\n"
                        f"- Máximo: {_money(float(high))}\n"
                        f"- Mínimo: {_money(float(low))}\n"
                        f"- Fuente: {result.payload.get('source')}\n"
                        f"- Corte: {result.payload.get('cutoff_time')}"
                    )
            else:
                answer = f"No pude consultar la serie: {result.error}"
            return {
                "answer": answer,
                "sql": result.sql,
                "provenance": result.provenance,
                "tool_results": tool_results,
                "tool_records": tool_records,
            }

        return {
            "answer": (
                "No tengo una herramienta específica para esa pregunta conceptual. "
                "Puedo consultar el catálogo, métricas agregadas o series OHLCV. "
                "Si la pregunta es sobre la plataforma, revisá las fuentes RAG citadas."
            ),
            "sql": None,
            "provenance": "RAG",
            "tool_results": tool_results,
            "tool_records": tool_records,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _system_prompt(self, rag_hits: list[RagHit]) -> str:
        context_lines = [
            f"[{hit.source}] {hit.text[:900]}"
            for hit in rag_hits
        ]
        context = "\n\n".join(context_lines) if context_lines else "(sin contexto RAG recuperado)"
        return f"{SYSTEM_PROMPT}\nContexto RAG:\n{context}"

    def _merge_sources(
        self,
        rag_hits: list[RagHit],
        tool_results: list[ToolResult],
    ) -> list[SourceReference]:
        sources: list[SourceReference] = []
        seen: set[tuple[str, str]] = set()
        for hit in rag_hits:
            source = hit.as_source()
            key = (str(source["kind"]), str(source["source"]))
            if key in seen:
                continue
            seen.add(key)
            sources.append(SourceReference.model_validate(source))
        for result in tool_results:
            if not result.source:
                continue
            key = ("tool", result.source)
            if key in seen:
                continue
            seen.add(key)
            sources.append(
                SourceReference(
                    source=result.source,
                    title=result.name,
                    snippet=result.summary[:280],
                    kind="tool",
                )
            )
        return sources

    def _normalize_provenance(self, value: str, tool_results: list[ToolResult]) -> str:
        if value in {"SPEED_LAYER", "BATCH_LAYER", "SERVING_LAYER", "RAG"}:
            return value
        for result in tool_results:
            if result.provenance in {"SPEED_LAYER", "BATCH_LAYER", "SERVING_LAYER", "RAG"}:
                return result.provenance
        return "RAG"

    def _ensure_disclaimer(self, answer: str) -> str:
        if DISCLAIMER.lower() in answer.lower():
            return answer
        return f"{answer.strip()}\n\n{DISCLAIMER}"

    async def health(self) -> dict[str, Any]:
        groq_status = "KEY_MISSING" if not self.settings.groq_api_key else "DOWN"
        if self.settings.groq_api_key:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(
                        f"{self.settings.groq_base_url.rstrip('/')}/models",
                        headers={"Authorization": f"Bearer {self.settings.groq_api_key}"},
                    )
                    if response.status_code == 200:
                        groq_status = "UP"
            except Exception:  # noqa: BLE001
                pass

        embedding_status = "DOWN"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.settings.ollama_base_url.rstrip('/')}/api/tags")
                if response.status_code == 200:
                    embedding_status = "UP"
                    models = response.json().get("models", [])
                    names = {str(item.get("name", "")) for item in models if isinstance(item, dict)}
                    model = self.settings.ollama_embedding_model
                    if model not in names and f"{model}:latest" not in names:
                        embedding_status = "MODEL_MISSING"
        except Exception:  # noqa: BLE001
            pass

        overall = "UP" if groq_status == "UP" and embedding_status == "UP" else "DEGRADED"
        return {
            "status": overall,
            "provider": "groq",
            "groq": {
                "status": groq_status,
                "model": self.settings.groq_model,
                "base_url": self.settings.groq_base_url,
            },
            "embeddings": {
                "status": embedding_status,
                "model": self.settings.ollama_embedding_model,
                "base_url": self.settings.ollama_base_url,
            },
            "router": self.router.status(),
            "rag": self.rag_index.status(),
        }
