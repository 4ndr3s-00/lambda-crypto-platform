"use client";

import { useMemo, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { LayerSource, SQLInspectionData } from "@/lib/types";
import { cn, formatUtcStamp } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/* Minimal SQL syntax highlighter (token-stream, no innerHTML)         */
/* ------------------------------------------------------------------ */

const KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "JOIN", "LEFT", "RIGHT",
  "INNER", "OUTER", "FULL", "ON", "AS", "AND", "OR", "NOT", "IN", "BETWEEN", "INTERVAL", "OVER",
  "PARTITION", "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "WITH", "CASE",
  "WHEN", "THEN", "ELSE", "END", "DESC", "ASC", "DISTINCT", "UNION", "ALL", "NULL", "IS", "LIKE",
  "TRUE", "FALSE", "USING", "WINDOW",
]);

const FUNCTIONS = new Set([
  "SUM", "AVG", "MIN", "MAX", "COUNT", "ROUND", "FIRST", "LAST", "FIRST_VALUE", "LAST_VALUE",
  "STDDEV_POP", "STDDEV_SAMP", "NULLIF", "COALESCE", "ABS", "CEIL", "FLOOR", "DATE_TRUNC", "EXTRACT",
  "CURRENT_TIMESTAMP", "NOW", "CAST", "ROW_NUMBER",
]);

const TOKEN_RE = /(--[^\n]*)|('(?:[^']|'')*')|(\d+(?:\.\d+)?)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([^\w\s])|([\s\S])/g;

function highlightSql(sql: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  let match: RegExpExecArray | null;
  let key = 0;
  TOKEN_RE.lastIndex = 0;

  while ((match = TOKEN_RE.exec(sql)) !== null) {
    const [, comment, str, num, word, space, punct, other] = match;
    key += 1;

    if (comment !== undefined) {
      nodes.push(
        <span key={key} className="text-zinc-600 italic">
          {comment}
        </span>,
      );
    } else if (str !== undefined) {
      nodes.push(
        <span key={key} className="text-amber-200/90">
          {str}
        </span>,
      );
    } else if (num !== undefined) {
      nodes.push(
        <span key={key} className="text-rose-300/90">
          {num}
        </span>,
      );
    } else if (word !== undefined) {
      const upper = word.toUpperCase();
      if (KEYWORDS.has(upper)) {
        nodes.push(
          <span key={key} className="font-medium text-sky-300">
            {word}
          </span>,
        );
      } else if (FUNCTIONS.has(upper)) {
        nodes.push(
          <span key={key} className="text-emerald-300">
            {word}
          </span>,
        );
      } else {
        nodes.push(
          <span key={key} className="text-zinc-200">
            {word}
          </span>,
        );
      }
    } else if (space !== undefined) {
      nodes.push(space);
    } else if (punct !== undefined || other !== undefined) {
      nodes.push(
        <span key={key} className="text-zinc-500">
          {punct ?? other}
        </span>,
      );
    }
  }

  return nodes;
}

/* ------------------------------------------------------------------ */
/* Layer metadata                                                      */
/* ------------------------------------------------------------------ */

const LAYER_META: Record<LayerSource, { label: string; tone: string }> = {
  SPEED_LAYER: { label: "SPEED_LAYER · Kafka", tone: "text-emerald-400" },
  BATCH_LAYER: { label: "BATCH_LAYER · MinIO", tone: "text-sky-400" },
  RECONCILED: { label: "RECONCILED · Speed + Batch", tone: "text-zinc-200" },
};

interface SQLInspectorProps {
  data: SQLInspectionData;
}

/** Collapsible proof-of-work panel attached to every analytical answer. */
export function SQLInspector({ data }: SQLInspectorProps) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const layer = LAYER_META[data.layerSource];
  const highlighted = useMemo(() => highlightSql(data.generatedSql), [data.generatedSql]);

  const copy = async () => {
    try {
      if (typeof navigator !== "undefined" && navigator.clipboard) {
        await navigator.clipboard.writeText(data.generatedSql);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1500);
      }
    } catch {
      // clipboard unavailable (insecure context) — fail silent, keep the query visible.
    }
  };

  const watermark =
    data.watermarkDelaySec > 0
      ? `${data.watermarkDelaySec}s de tolerancia (event-time)`
      : "sin watermark (snapshot batch congelado)";

  return (
    <div className="overflow-hidden rounded-xl border border-white/[0.08] bg-white/[0.015]">
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open}
        className="flex w-full flex-col items-start gap-2 px-3.5 py-2.5 text-left transition-colors hover:bg-white/[0.03] sm:flex-row sm:items-center sm:justify-between sm:gap-3"
      >
        <div className="flex w-full min-w-0 items-center gap-2">
          <motion.svg
            viewBox="0 0 12 12"
            className="h-2.5 w-2.5 shrink-0 text-zinc-500"
            animate={{ rotate: open ? 0 : -90 }}
            transition={{ type: "spring", stiffness: 400, damping: 30 }}
          >
            <path d="M1 3L6 8L11 3" stroke="currentColor" strokeWidth="1.6" fill="none" strokeLinecap="round" />
          </motion.svg>
          <span className="truncate font-mono text-[11px] uppercase tracking-wider text-zinc-400">
            <span className="sm:hidden">SQL GENERADO</span>
            <span className="hidden sm:inline">SQL GENERADO & TELEMETRÍA</span>
          </span>
          <span className="hidden shrink-0 font-mono text-[10px] text-zinc-600 sm:inline">
            ({data.executionTimeMs}ms · {data.engine})
          </span>
        </div>
        <span
          className={cn(
            "shrink-0 rounded-full border border-white/10 px-2 py-0.5 font-mono text-[9px] sm:text-[10px]",
            layer.tone,
          )}
        >
          [{layer.label}]
        </span>
      </button>

      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ type: "spring", stiffness: 300, damping: 32 }}
            className="overflow-hidden"
          >
            <div className="border-t border-white/[0.06] p-3.5">
              {/* Code block */}
              <div className="relative overflow-hidden rounded-lg border border-white/[0.06] bg-black/60">
                <div className="flex items-center justify-between border-b border-white/[0.06] px-3 py-1.5">
                  <span className="font-mono text-[10px] uppercase tracking-wider text-zinc-600">query.sql</span>
                  <button
                    type="button"
                    onClick={copy}
                    className="rounded border border-white/10 px-2 py-0.5 font-mono text-[10px] text-zinc-400 transition-colors hover:border-white/25 hover:text-zinc-100"
                  >
                    {copied ? "COPIADO" : "COPIAR"}
                  </button>
                </div>
                <pre className="max-h-[360px] overflow-auto p-3 font-mono text-[11px] leading-relaxed">
                  <code>{highlighted}</code>
                </pre>
              </div>

              {/* Provenance grid */}
              <div className="mt-3 grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-white/[0.06] bg-white/[0.04] sm:grid-cols-2">
                <Provenance label="Ingesta" value="Binance WebSocket · Stream: market.events" />
                <Provenance label="Dataset" value={data.sourceDataset} mono />
                <Provenance label="Capa" value={layer.label} />
                <Provenance label="Watermark / Tolerancia" value={watermark} />
                <Provenance
                  label="Reconciliación"
                  value={
                    data.idempotencyVerified
                      ? "Event-time deduplication / Idempotencia garantizada"
                      : "Sin verificar"
                  }
                />
                <Provenance label="Frescura" value={data.dataFreshness} />
                <Provenance label="Engine" value={`${data.engine} · ${data.executionTimeMs}ms`} mono />
                <Provenance label="Fecha de corte" value={formatUtcStamp(new Date(data.cutoffDate))} mono />
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function Provenance({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex flex-col gap-0.5 bg-zinc-950/60 px-3 py-2">
      <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{label}</span>
      <span className={cn("truncate text-[11px] text-zinc-300", mono && "font-mono")} title={value}>
        {value}
      </span>
    </div>
  );
}
