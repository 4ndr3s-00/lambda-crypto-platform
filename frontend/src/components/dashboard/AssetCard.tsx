"use client";

import { memo } from "react";
import { motion } from "framer-motion";
import type { AssetCardData } from "@/lib/types";
import { cn, formatCompact, formatPercent, formatUsd } from "@/lib/utils";

interface AssetCardProps {
  asset: AssetCardData;
  onSelect: (symbol: AssetCardData["symbol"]) => void;
}

function Sparkline({ values, positive }: { values: number[]; positive: boolean }) {
  if (values.length < 2) {
    return <div className="h-10 w-full rounded bg-white/[0.02]" />;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values
    .map((value, index) => {
      const x = (index / Math.max(values.length - 1, 1)) * 120;
      const y = 34 - ((value - min) / span) * 30;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");

  return (
    <svg viewBox="0 0 120 38" className="h-10 w-full" preserveAspectRatio="none" aria-hidden="true">
      <polyline
        points={points}
        fill="none"
        stroke={positive ? "#10B981" : "#EF4444"}
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}

export const AssetCard = memo(function AssetCard({ asset, onSelect }: AssetCardProps) {
  const up = asset.change24h >= 0;
  const hasPrice = asset.isLive && asset.price > 0;

  return (
    <motion.button
      type="button"
      onClick={() => onSelect(asset.symbol)}
      whileHover={{ y: -2 }}
      whileTap={{ scale: 0.985 }}
      transition={{ type: "spring", stiffness: 260, damping: 26 }}
      className="group relative min-w-0 overflow-hidden rounded-2xl border border-white/10 bg-zinc-950/70 p-5 text-left shadow-xl backdrop-blur-xl transition-colors hover:border-white/25 focus-visible:border-white/30"
      aria-label={`Abrir workspace de ${asset.base} / ${asset.quote}`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] font-mono text-[11px] font-semibold text-zinc-200">
            {asset.base.slice(0, 1)}
          </span>
          <div className="min-w-0">
            <span className="block truncate font-mono text-[11px] font-medium tracking-wide text-zinc-200">
              {asset.base} / {asset.quote}
            </span>
            <span className="block truncate font-mono text-[9px] uppercase tracking-wider text-zinc-600">
              {asset.exchange} · {asset.source}
            </span>
          </div>
        </div>
        <span
          className={cn(
            "shrink-0 rounded-full border px-2 py-0.5 font-mono text-[9px] tabular-nums",
            !hasPrice
              ? "border-white/10 text-zinc-600"
              : up
                ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-400"
                : "border-rose-500/25 bg-rose-500/10 text-rose-400",
          )}
        >
          {hasPrice ? formatPercent(asset.change24h) : "—"}
        </span>
      </div>

      <div className="mt-5">
        <motion.span
          key={hasPrice ? asset.price : "unavailable"}
          initial={{ opacity: 0.55, y: -1 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.18 }}
          className={cn(
            "block font-mono text-[26px] font-medium tracking-[-0.04em] tabular-nums",
            hasPrice ? "text-zinc-100" : "text-zinc-600",
          )}
        >
          {hasPrice ? formatUsd(asset.price) : "—"}
        </motion.span>
        <div className="mt-1">
          <Sparkline values={asset.sparkline} positive={up} />
        </div>
      </div>

      <div className="mt-4 grid grid-cols-3 gap-2 border-t border-white/[0.06] pt-3">
        <div>
          <span className="block font-mono text-[8px] uppercase tracking-wider text-zinc-600">Vol 24h</span>
          <span className="mt-0.5 block truncate font-mono text-[10px] text-zinc-300">
            {hasPrice ? `$${formatCompact(asset.volume24h)}` : "—"}
          </span>
        </div>
        <div>
          <span className="block font-mono text-[8px] uppercase tracking-wider text-zinc-600">Exchange</span>
          <span className="mt-0.5 block truncate font-mono text-[10px] text-zinc-300">{asset.exchange}</span>
        </div>
        <div>
          <span className="block font-mono text-[8px] uppercase tracking-wider text-zinc-600">Estado</span>
          <span
            className={cn(
              "mt-0.5 block font-mono text-[10px]",
              asset.isLive ? "text-emerald-400" : "text-rose-400",
            )}
          >
            {asset.isLive ? "LIVE" : "SIN DATOS"}
          </span>
        </div>
      </div>
    </motion.button>
  );
});
