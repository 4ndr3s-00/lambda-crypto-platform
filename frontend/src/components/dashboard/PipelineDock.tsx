"use client";

import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { PipelineStatusResponse } from "@/lib/types";
import { cn } from "@/lib/utils";

interface PipelineDockProps {
  status: PipelineStatusResponse | null;
  lagSec: number;
}

interface DockRow {
  label: string;
  value: string;
}

/**
 * US-24 telemetry dock. Collapsed by default so the chart keeps the viewport;
 * every service reads as a static status line, never a blinking party light.
 */
export function PipelineDock({ status, lagSec }: PipelineDockProps) {
  const [open, setOpen] = useState(false);

  const isReal = status?.dataSource === "postgres_reconciled";

  const rows: DockRow[] = [
    {
      label: "Kafka Ingestion",
      value: status ? `Topics: market.events · Lag: ${status.kafka.consumerLag}` : "sin datos reales",
    },
    {
      label: "Spark Streaming",
      value: status ? status.spark.status : "sin datos reales",
    },
    {
      label: "Airflow Batch",
      value: status ? `DAG batch_recompute ${status.airflow.status}` : "sin datos reales",
    },
    {
      label: "MinIO Data Lake",
      value: status ? status.minio.status : "sin datos reales",
    },
    {
      label: "DLQ Isolation",
      value: status ? `${status.dlq.rejectedEvents} ERRORES / RECHAZOS` : "sin datos reales",
    },
  ];

  return (
    <div className="shrink-0 px-4 pb-4 sm:px-6">
      <div className="overflow-hidden rounded-xl border border-white/[0.08] bg-white/[0.012]">
        <button
          type="button"
          onClick={() => setOpen((prev) => !prev)}
          aria-expanded={open}
          className="flex w-full items-center justify-between gap-3 px-3.5 py-2.5 text-left transition-colors hover:bg-white/[0.03]"
        >
          <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-wider text-zinc-400">
            <span>Pipeline Health</span>
            {isReal ? (
              <span className="text-emerald-400">● REAL</span>
            ) : (
              <span className="text-amber-400">● SIN DATOS REALES</span>
            )}
          </div>
          <div className="flex shrink-0 items-center gap-3 font-mono text-[10px] text-zinc-600">
            <span>Frescura {lagSec.toFixed(1)}s</span>
            <motion.svg
              viewBox="0 0 12 12"
              className="h-2.5 w-2.5 text-zinc-500"
              animate={{ rotate: open ? 0 : -90 }}
              transition={{ type: "spring", stiffness: 400, damping: 30 }}
            >
              <path d="M1 3L6 8L11 3" stroke="currentColor" strokeWidth="1.6" fill="none" strokeLinecap="round" />
            </motion.svg>
          </div>
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
              <div className="grid grid-cols-1 gap-px bg-white/[0.04] sm:grid-cols-2 lg:grid-cols-5">
                {rows.map((row) => (
                  <div key={row.label} className="flex flex-col gap-0.5 bg-zinc-950/60 px-3 py-2.5">
                    <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{row.label}</span>
                    <span className={cn("font-mono text-[10px] text-zinc-300")}>
                      <span className={isReal ? "text-emerald-400" : "text-amber-400"}>●</span> {row.value}
                    </span>
                  </div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
