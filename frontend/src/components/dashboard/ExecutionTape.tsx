"use client";

import { useMemo, useState } from "react";
import type { ExecutionTrade } from "@/lib/types";
import { cn, formatCompact, formatNumber, toBogota } from "@/lib/utils";
import { Panel } from "./Panel";

interface ExecutionTapeProps {
  trades: ExecutionTrade[];
  source: string;
}

function tradeClock(iso: string): string {
  const date = toBogota(new Date(iso));
  const base = date.toISOString().slice(11, 19);
  return `${base}.${String(date.getUTCMilliseconds()).padStart(3, "0")}`;
}

export function ExecutionTape({ trades, source }: ExecutionTapeProps) {
  const [filter, setFilter] = useState<"all" | "whales">("all");

  const visible = useMemo(() => {
    const filtered = filter === "whales" ? trades.filter((trade) => trade.whale) : trades;
    return filtered.slice(0, 34);
  }, [trades, filter]);

  return (
    <Panel title="Execution Tape" hint={`${visible.length} ROWS · ${source}`}>
      <div className="mb-2 flex items-center gap-1">
        <button
          type="button"
          onClick={() => setFilter("all")}
          className={cn(
            "rounded-full border px-2 py-1 font-mono text-[9px] transition-colors",
            filter === "all"
              ? "border-white/20 bg-white/[0.08] text-zinc-100"
              : "border-white/10 text-zinc-500 hover:text-zinc-200",
          )}
        >
          Todos los trades
        </button>
        <button
          type="button"
          onClick={() => setFilter("whales")}
          className={cn(
            "rounded-full border px-2 py-1 font-mono text-[9px] transition-colors",
            filter === "whales"
              ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
              : "border-white/10 text-zinc-500 hover:text-zinc-200",
          )}
        >
          Solo ballenas &gt; $100k
        </button>
      </div>

      <div className="grid grid-cols-[1fr_0.9fr_0.7fr_0.9fr] gap-1 border-b border-white/[0.06] pb-1 font-mono text-[8px] uppercase tracking-wider text-zinc-600">
        <span>Hora</span>
        <span className="text-right">Precio</span>
        <span className="text-right">Cant.</span>
        <span className="text-right">USD</span>
      </div>
      <div className="max-h-[290px] overflow-y-auto">
        {visible.map((trade) => (
          <div
            key={trade.id}
            className="grid grid-cols-[1fr_0.9fr_0.7fr_0.9fr] gap-1 border-b border-white/[0.03] py-[3px] font-mono text-[9px] tabular-nums"
          >
            <span className="text-zinc-600">{tradeClock(trade.timestamp)}</span>
            <span className={cn("text-right", trade.side === "buy" ? "text-emerald-400" : "text-rose-400")}>
              {formatNumber(trade.price, 2)}
            </span>
            <span className="text-right text-zinc-400">{formatNumber(trade.size, trade.size < 1 ? 6 : 2)}</span>
            <span className={cn("text-right", trade.whale ? "font-medium text-zinc-100" : "text-zinc-400")}>
              ${formatCompact(trade.notional)}
            </span>
          </div>
        ))}
        {visible.length === 0 && (
          <div className="py-8 text-center font-mono text-[10px] text-zinc-600">
            Sin trades reales en esta ventana.
          </div>
        )}
      </div>
    </Panel>
  );
}
