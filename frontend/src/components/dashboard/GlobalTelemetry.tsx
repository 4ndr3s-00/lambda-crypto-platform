"use client";

import { useEffect, useState } from "react";
import type { GlobalTelemetryData } from "@/lib/types";
import { createTelemetry, fundingCountdown } from "@/lib/simulatedMetrics";
import { cn } from "@/lib/utils";

function latencyTone(value: number): string {
  if (value < 50) return "text-emerald-400";
  if (value <= 150) return "text-amber-400";
  return "text-rose-400";
}

export function GlobalTelemetry() {
  const [data, setData] = useState<GlobalTelemetryData>(() => createTelemetry());

  useEffect(() => {
    const tick = window.setInterval(() => {
      setData({
        ...createTelemetry(Date.now()),
        fundingCountdown: fundingCountdown(),
      });
    }, 1000);
    return () => window.clearInterval(tick);
  }, []);

  const fundingUp = data.fundingRate >= 0;
  return (
    <div className="flex flex-wrap items-center justify-center gap-x-5 gap-y-2 font-mono text-[10px] text-zinc-500">
      <span
        className="inline-flex items-center gap-1 rounded-full border border-amber-500/25 px-2 py-0.5 text-amber-400"
        title="Todas las métricas de esta barra son simuladas; no provienen del pipeline Batch/Speed real."
      >
        SIMULADO
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className={cn("inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-current", latencyTone(data.latencyMs))} />
        <span className={latencyTone(data.latencyMs)}>{data.latencyMs}ms</span>
        <span className="text-zinc-700">WS</span>
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="text-zinc-700">THROUGHPUT</span>
        <span className="text-zinc-300">{data.throughput.toLocaleString("en-US")} msg/s</span>
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="text-zinc-700">FUNDING</span>
        <span className={fundingUp ? "text-emerald-400" : "text-rose-400"}>
          {fundingUp ? "+" : ""}
          {data.fundingRate.toFixed(4)}%
        </span>
        <span className="text-zinc-600">{data.fundingCountdown}</span>
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="text-zinc-700">GAS</span>
        <span className="text-zinc-300">{data.gasGwei} Gwei</span>
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="text-zinc-700">F&amp;G</span>
        <span className={data.fearGreed >= 60 ? "text-emerald-400" : data.fearGreed >= 40 ? "text-amber-400" : "text-rose-400"}>
          {data.fearGreed} · {data.fearGreedLabel}
        </span>
      </span>
    </div>
  );
}
