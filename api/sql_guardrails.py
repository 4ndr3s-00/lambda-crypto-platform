"""Read-only SQL guardrails for the Qwen2.5 tool layer (US-29).

The guardrail is enforced twice:
1. AST validation with sqlglot when installed (single SELECT, allowlisted
   tables, mandatory LIMIT <= 100, no destructive nodes/functions).
2. A conservative regex fallback so the API never depends on an optional
   parser being present in the container.

The returned SQL is sanitized and capped; it is never concatenated with
untrusted identifiers by callers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

try:  # pragma: no cover - exercised through both branches in tests
    import sqlglot
    from sqlglot import exp
except Exception:  # noqa: BLE001 - optional dependency
    sqlglot = None  # type: ignore[assignment]
    exp = None  # type: ignore[assignment]

MAX_LIMIT = 100
TIMEOUT_SECONDS = 5.0

DEFAULT_ALLOWED_TABLES = frozenset(
    {
        "serving.assets",
        "serving.ohlcv_reconciled",
        "serving.realtime_metrics",
        "serving.pipeline_status",
        "market_ohlcv",
        "market_metrics",
        "market_speed_events",
        "pipeline_telemetry_metrics",
        "dataset_catalog",
        "kafka_metrics.consumer_offsets",
        "governance.dataset_catalog",
        "ops.pipeline_telemetry_metrics",
    }
)

DESTRUCTIVE_KEYWORDS = (
    "DROP",
    "DELETE",
    "UPDATE",
    "ALTER",
    "INSERT",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "EXEC",
    "EXECUTE",
    "CALL",
    "COPY",
    "VACUUM",
    "ANALYZE",
    "ATTACH",
    "DETACH",
    "PRAGMA",
    "REPLACE",
    "UPSERT",
    "SET",
    "COMMIT",
    "ROLLBACK",
    "BEGIN",
)

DANGEROUS_FUNCTIONS = frozenset(
    {
        "pg_sleep",
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "lo_import",
        "lo_export",
        "dblink",
        "dblink_exec",
        "copy",
        "current_setting",
        "set_config",
    }
)

DDL_NODE_NAMES = (
    "Insert",
    "Update",
    "Delete",
    "Drop",
    "Alter",
    "Create",
    "Merge",
    "Command",
    "Transaction",
    "Commit",
    "Rollback",
    "Grant",
    "Revoke",
    "Copy",
    "Into",
    "Lock",
)


class SQLGuardrailError(ValueError):
    """Raised when a query violates the read-only policy."""


@dataclass(frozen=True)
class GuardrailResult:
    ok: bool
    sql: str
    reason: str | None = None
    tables: tuple[str, ...] = ()
    limit: int | None = None
    timeout_seconds: float = TIMEOUT_SECONDS

    def raise_for_error(self) -> None:
        if not self.ok:
            raise SQLGuardrailError(self.reason or "consulta rechazada por el guardrail SQL")


_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE = re.compile(r"--[^\n]*")
_STRING_LITERAL = re.compile(r"'(?:''|[^'])*'")
_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)", re.IGNORECASE)
_LIMIT = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)
_WORD = re.compile(r"\b[A-Za-z_]+\b")


def _strip_comments(sql: str) -> str:
    sql = _COMMENT_BLOCK.sub(" ", sql)
    return _COMMENT_LINE.sub(" ", sql)


def _without_strings(sql: str) -> str:
    return _STRING_LITERAL.sub("''", sql)


def _normalize_table(name: str) -> str:
    return name.strip().strip('"').strip("`").lower()


def _tables_in(sql: str) -> tuple[str, ...]:
    found = {_normalize_table(match) for match in _TABLE_REF.findall(_strip_comments(sql))}
    return tuple(sorted(found))


def _check_keywords(sql: str) -> str | None:
    scrubbed = _without_strings(_strip_comments(sql)).upper()
    for keyword in DESTRUCTIVE_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", scrubbed):
            return f"palabra clave destructiva bloqueada: {keyword}"
    for function in sorted(DANGEROUS_FUNCTIONS):
        if re.search(rf"\b{re.escape(function)}\s*\(", scrubbed, re.IGNORECASE):
            return f"función peligrosa bloqueada: {function}"
    return None


def _fallback_validate(sql: str, allowed_tables: frozenset[str], max_limit: int) -> GuardrailResult:
    cleaned = _strip_comments(sql).strip().rstrip(";").strip()
    if not cleaned:
        return GuardrailResult(False, sql, "consulta vacía")

    # Only one statement; an optional trailing semicolon was already removed.
    if ";" in _without_strings(cleaned):
        return GuardrailResult(False, sql, "solo se permite una sentencia SELECT")

    if not re.match(r"^(SELECT|WITH)\b", cleaned, re.IGNORECASE):
        return GuardrailResult(False, sql, "solo se permiten consultas SELECT de lectura")

    keyword_error = _check_keywords(cleaned)
    if keyword_error:
        return GuardrailResult(False, sql, keyword_error)

    cte_names = {
        match.group(1).lower()
        for match in re.finditer(r"\b(?:WITH|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", cleaned, re.IGNORECASE)
    }
    tables = tuple(table for table in _tables_in(cleaned) if table not in cte_names)
    if not tables:
        return GuardrailResult(False, sql, "no se pudo determinar la tabla de la consulta")
    unauthorized = [table for table in tables if table not in allowed_tables]
    if unauthorized:
        return GuardrailResult(False, sql, f"tabla no autorizada: {', '.join(unauthorized)}")

    # Only a trailing LIMIT protects the final result set. A LIMIT inside a
    # CTE/subquery does not count.
    outer_limit = re.search(r"\bLIMIT\s+(\d+)\s*$", cleaned, re.IGNORECASE)
    if outer_limit:
        limit = int(outer_limit.group(1))
        if limit < 1 or limit > max_limit:
            return GuardrailResult(False, sql, f"LIMIT debe estar entre 1 y {max_limit}")
        sanitized = cleaned
    else:
        limit = max_limit
        sanitized = f"{cleaned}\nLIMIT {max_limit}"

    return GuardrailResult(
        ok=True,
        sql=sanitized,
        tables=tables,
        limit=limit,
        timeout_seconds=TIMEOUT_SECONDS,
    )


def _ast_validate(sql: str, allowed_tables: frozenset[str], max_limit: int) -> GuardrailResult:
    assert sqlglot is not None and exp is not None
    try:
        statements = sqlglot.parse(sql, read="postgres")
    except Exception as exc:  # noqa: BLE001 - parser errors are client errors
        return GuardrailResult(False, sql, f"SQL inválido: {exc}")

    statements = [statement for statement in statements if statement is not None]
    if len(statements) != 1:
        return GuardrailResult(False, sql, "solo se permite una sentencia SELECT")

    root = statements[0]
    if not isinstance(root, (exp.Select, exp.Union, exp.Subquery)):
        return GuardrailResult(False, sql, "solo se permiten consultas SELECT de lectura")

    for node_name in DDL_NODE_NAMES:
        node_type = getattr(exp, node_name, None)
        if node_type is not None and root.find(node_type) is not None:
            return GuardrailResult(False, sql, f"operación bloqueada: {node_name.upper()}")

    tables = tuple(
        sorted(
            {
                _normalize_table(table.sql(dialect="postgres"))
                for table in root.find_all(exp.Table)
                if table.name.lower()
                not in {cte.alias_or_name.lower() for cte in root.find_all(exp.CTE)}
            }
        )
    )
    if not tables:
        return GuardrailResult(False, sql, "no se pudo determinar la tabla de la consulta")
    unauthorized = [table for table in tables if table not in allowed_tables]
    if unauthorized:
        return GuardrailResult(False, sql, f"tabla no autorizada: {', '.join(unauthorized)}")

    for function in root.find_all(exp.Anonymous):
        name = (function.name or "").lower()
        if name in DANGEROUS_FUNCTIONS:
            return GuardrailResult(False, sql, f"función peligrosa bloqueada: {name}")

    keyword_error = _check_keywords(sql)
    if keyword_error:
        return GuardrailResult(False, sql, keyword_error)

    # Enforce an outer LIMIT. A LIMIT inside a CTE/subquery does not protect
    # the final result set, so we only inspect the root's limit clause.
    limit_node = root.args.get("limit") if hasattr(root, "args") else None
    if limit_node is None:
        sanitized_root = root.limit(max_limit)
        limit = max_limit
    else:
        raw_limit = limit_node.expression
        try:
            limit = int(raw_limit.name) if hasattr(raw_limit, "name") else int(str(raw_limit))
        except (TypeError, ValueError):
            return GuardrailResult(False, sql, "LIMIT debe ser un entero")
        if limit < 1 or limit > max_limit:
            return GuardrailResult(False, sql, f"LIMIT debe estar entre 1 y {max_limit}")
        sanitized_root = root

    sanitized = sanitized_root.sql(dialect="postgres")
    return GuardrailResult(
        ok=True,
        sql=sanitized,
        tables=tables,
        limit=limit,
        timeout_seconds=TIMEOUT_SECONDS,
    )


def validate_sql(
    sql: str,
    *,
    allowed_tables: Iterable[str] = DEFAULT_ALLOWED_TABLES,
    max_limit: int = MAX_LIMIT,
) -> GuardrailResult:
    """Validate and sanitize a read-only query.

    Returns a GuardrailResult instead of raising so API callers can turn the
    reason into a structured 400/422 response. Use `raise_for_error()` when a
    hard failure is more convenient (tests, tools).
    """
    if not isinstance(sql, str) or not sql.strip():
        return GuardrailResult(False, str(sql), "consulta vacía")

    allowed = frozenset(_normalize_table(table) for table in allowed_tables)
    if sqlglot is not None and exp is not None:
        try:
            return _ast_validate(sql, allowed, max_limit)
        except Exception:  # noqa: BLE001 - never let parser quirks bypass validation
            fallback = _fallback_validate(sql, allowed, max_limit)
            if not fallback.ok:
                return fallback
            return fallback
    return _fallback_validate(sql, allowed, max_limit)


def assert_safe_sql(sql: str, *, allowed_tables: Iterable[str] = DEFAULT_ALLOWED_TABLES) -> str:
    result = validate_sql(sql, allowed_tables=allowed_tables)
    result.raise_for_error()
    return result.sql


def is_read_only_sql(sql: str) -> bool:
    return validate_sql(sql).ok
