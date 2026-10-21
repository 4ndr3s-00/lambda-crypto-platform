"use client";

import type { OrderBookSnapshot } from "@/lib/types";
import { formatCompact, formatNumber, formatUsd } from "@/lib/utils";
import { Panel } from "./Panel";

interface OrderBookPanelProps {
  snapshot: OrderBookSnapshot;
}

export function OrderBookPanel({ snapshot }: OrderBookPanelProps) {
  const maxTotal = Math.max(
    snapshot.bids[snapshot.bids.length - 1]?.total ?? 1,
    snapshot.asks[snapshot.asks.length - 1]?.total ?? 1,
  );
  const marker = Math.max(0, Math.min(100, (snapshot.obi + 100) / 2));

  return (
    <Panel title="Order Book L2 · OBI" hint="TOP 20">
      <div className="mb-2.5">
        <div className="mb-1 flex items-center justify-between font-mono text-[8px] uppercase tracking-wider text-zinc-600">
          <span className="text-rose-400">-100 Sell</span>
          <span className="text-zinc-400">
            OBI{" "}
            <span className={snapshot.obi >= 0 ? "text-emerald-400" : "text-rose-400"}>
              {snapshot.obi >= 0 ? "+" : ""}
              {snapshot.obi.toFixed(1)}%
            </span>
          </span>
          <span className="text-emerald-400">Buy +100</span>
        </div>
        <div className="relative h-2 overflow-hidden rounded-full bg-zinc-900">
          <div className="absolute inset-y-0 left-0 w-1/2 bg-gradient-to-r from-rose-500/60 to-rose-500/5" />
          <div className="absolute inset-y-0 right-0 w-1/2 bg-gradient-to-l from-emerald-500/60 to-emerald-500/5" />
          <span
            className="absolute top-1/2 h-3.5 w-0.5 -translate-x-1/2 -translate-y-1/2 rounded-full bg-white shadow-[0_0_8px_rgba(255,255,255,0.7)]"
            style={{ left: `${marker}%` }}
          />
        </div>
      </div>

      <div className="mb-1.5 flex items-center justify-between font-mono text-[9px] text-zinc-600">
        <span>Spread</span>
        <span className="text-zinc-300">
          {formatUsd(snapshot.spread)} · {snapshot.spreadPct.toFixed(4)}%
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 font-mono text-[9px]">
        <div>
          <div className="mb-1 flex justify-between text-[8px] uppercase tracking-wider text-zinc-600">
            <span>Bid</span>
            <span>Size</span>
          </div>
          <div className="space-y-[2px]">
            {snapshot.bids.map((level) => (
              <div key={`bid-${level.price}`} className="relative flex justify-between overflow-hidden rounded-sm px-1 py-[2px]">
                <span
                  className="absolute inset-y-0 right-0 bg-emerald-500/10"
                  style={{ width: `${Math.min(100, (level.total / maxTotal) * 100)}%` }}
                />
                <span className="relative text-emerald-400 tabular-nums">{formatNumber(level.price, 2)}</span>
                <span className="relative text-zinc-400 tabular-nums">{formatCompact(level.size)}</span>
              </div>
            ))}
          </div>
        </div>
        <div>
          <div className="mb-1 flex justify-between text-[8px] uppercase tracking-wider text-zinc-600">
            <span>Ask</span>
            <span>Size</span>
          </div>
          <div className="space-y-[2px]">
            {snapshot.asks.map((level) => (
              <div key={`ask-${level.price}`} className="relative flex justify-between overflow-hidden rounded-sm px-1 py-[2px]">
                <span
                  className="absolute inset-y-0 left-0 bg-rose-500/10"
                  style={{ width: `${Math.min(100, (level.total / maxTotal) * 100)}%` }}
                />
                <span className="relative text-rose-400 tabular-nums">{formatNumber(level.price, 2)}</span>
                <span className="relative text-zinc-400 tabular-nums">{formatCompact(level.size)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Panel>
  );
}
