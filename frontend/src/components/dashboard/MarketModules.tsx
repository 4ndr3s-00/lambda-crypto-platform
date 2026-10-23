"use client";

import type { ArbitrageQuote, LiquidationEvent, OnChainSnapshot } from "@/lib/types";
import { cn, formatCompact, formatNumber, formatUsd, toBogota } from "@/lib/utils";
import { Panel } from "./Panel";

export function ArbitrageMatrix({ quotes }: { quotes: ArbitrageQuote[] }) {
  return (
    <Panel title="Cross-Exchange Arbitrage" hint="SIMULADO · TAKER FEE 0.06%">
      <div className="grid grid-cols-[1fr_1fr_0.8fr_0.8fr] gap-1 border-b border-white/[0.06] pb-1 font-mono text-[8px] uppercase tracking-wider text-zinc-600">
        <span>Exchange</span>
        <span className="text-right">Price</span>
        <span className="text-right">Gross</span>
        <span className="text-right">Net</span>
      </div>
      <div className="mt-1 space-y-[2px]">
        {quotes.map((quote) => (
          <div
            key={quote.exchange}
            className={cn(
              "grid grid-cols-[1fr_1fr_0.8fr_0.8fr] gap-1 rounded-sm px-1 py-[3px] font-mono text-[9px] tabular-nums",
              quote.profitable && "bg-emerald-500/[0.07]",
            )}
          >
            <span className="text-zinc-300">{quote.exchange}</span>
            <span className="text-right text-zinc-300">{formatUsd(quote.price)}</span>
            <span className="text-right text-zinc-500">{quote.grossSpreadPct.toFixed(3)}%</span>
            <span className={cn("text-right", quote.profitable ? "text-emerald-400" : "text-zinc-500")}>
              {quote.netSpreadPct.toFixed(3)}%
            </span>
          </div>
        ))}
      </div>
      <p className="mt-2 font-mono text-[8px] leading-relaxed text-zinc-600">
        Net = gross spread − 0.06% taker fee. Solo las filas verdes son ejecutables.
      </p>
    </Panel>
  );
}

export function LiquidationRadar({ events }: { events: LiquidationEvent[] }) {
  return (
    <Panel title="Liquidation Radar" hint="SIMULADO · FORCED LIQUIDATIONS">
      <div className="grid grid-cols-[1fr_0.7fr_0.8fr_0.7fr] gap-1 border-b border-white/[0.06] pb-1 font-mono text-[8px] uppercase tracking-wider text-zinc-600">
        <span>Hora</span>
        <span className="text-right">USD</span>
        <span className="text-right">Side</span>
        <span className="text-right">Price</span>
      </div>
      <div className="mt-1 max-h-[220px] space-y-[2px] overflow-y-auto">
        {events.map((event) => (
          <div
            key={event.id}
            className="grid grid-cols-[1fr_0.7fr_0.8fr_0.7fr] gap-1 rounded-sm px-1 py-[3px] font-mono text-[9px] tabular-nums"
          >
            <span className="text-zinc-600">{toBogota(new Date(event.timestamp)).toISOString().slice(11, 19)}</span>
            <span className="text-right text-zinc-300">${formatCompact(event.amount)}</span>
            <span className={cn("text-right font-medium", event.direction === "LONG" ? "text-emerald-400" : "text-rose-400")}>
              {event.direction}
            </span>
            <span className="text-right text-zinc-400">{formatNumber(event.price, 2)}</span>
          </div>
        ))}
      </div>
    </Panel>
  );
}

export function OnChainMacro({ snapshot }: { snapshot: OnChainSnapshot }) {
  const maxAbs = Math.max(...snapshot.netflow.map((point) => Math.abs(point.value)), 1);
  return (
    <Panel title="On-Chain & Macro · Batch Layer" hint="SIMULADO · MINIO / SPARK">
      <div className="grid gap-4 md:grid-cols-[1.5fr_1fr]">
        <div>
          <div className="mb-1 flex items-center justify-between font-mono text-[8px] uppercase tracking-wider text-zinc-600">
            <span>Exchange Netflow</span>
            <span>
              <span className="text-emerald-400">outflow</span> / <span className="text-rose-400">deposit</span>
            </span>
          </div>
          <div className="flex h-24 items-center gap-[3px]">
            {snapshot.netflow.map((point) => {
              const height = Math.max(4, (Math.abs(point.value) / maxAbs) * 44);
              const outflow = point.value < 0;
              return (
                <div key={point.label} className="flex flex-1 flex-col items-center justify-center gap-1">
                  <div className="flex h-11 w-full items-end justify-center">
                    {outflow && (
                      <span className="w-full rounded-sm bg-emerald-500/50" style={{ height: `${height}px` }} />
                    )}
                  </div>
                  <div className="flex h-11 w-full items-start justify-center">
                    {!outflow && (
                      <span className="w-full rounded-sm bg-rose-500/50" style={{ height: `${height}px` }} />
                    )}
                  </div>
                  <span className="font-mono text-[7px] text-zinc-700">{point.label}</span>
                </div>
              );
            })}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <Metric label="Open Interest" value={`$${formatCompact(snapshot.openInterest)}`} />
          <Metric label="Long / Short" value={snapshot.longShortRatio.toFixed(2)} />
          <Metric label="MVRV" value={snapshot.mvrv.toFixed(2)} />
          <Metric label="Active Addr." value={formatCompact(snapshot.activeAddresses)} />
        </div>
      </div>
    </Panel>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] px-2 py-1.5">
      <span className="block font-mono text-[8px] uppercase tracking-wider text-zinc-600">{label}</span>
      <span className="mt-0.5 block font-mono text-[11px] text-zinc-200">{value}</span>
    </div>
  );
}
