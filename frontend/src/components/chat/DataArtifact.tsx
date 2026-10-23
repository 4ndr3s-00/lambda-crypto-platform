"use client";

import { useEffect, useId, useMemo, useState, type PointerEvent as ReactPointerEvent } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { DataArtifact as ArtifactModel, MarketInterval, OHLCVCandle } from "@/lib/types";
import { MARKET_INTERVALS, isMarketInterval, regenerateArtifact } from "@/lib/mockData";
import { clamp, cn, formatCompact, formatNumber, formatPercent, formatUsd, toBogota } from "@/lib/utils";

const BULL = "#10B981";
const BEAR = "#F43F5E";

/** Shared chart geometry — a single source of truth for every series type. */
const PLOT = {
  w: 900,
  h: 320,
  left: 12,
  right: 68,
  top: 18,
  priceBottom: 236,
  volTop: 254,
  volBottom: 288,
} as const;

const PLOT_WIDTH = PLOT.w - PLOT.left - PLOT.right;
const PRICE_HEIGHT = PLOT.priceBottom - PLOT.top;
const VOL_HEIGHT = PLOT.volBottom - PLOT.volTop;

/** Tooltip geometry — the card hugs the cursor and flips before it clips an edge. */
const TIP_WIDTH = 176;
const TIP_HEIGHT = 132;
const TIP_GAP = 16;

interface CursorPoint {
  x: number;
  y: number;
  width: number;
  height: number;
}

interface Bounds {
  min: number;
  max: number;
}

function computeBounds(candles: OHLCVCandle[]): Bounds {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (const candle of candles) {
    if (candle.low < min) min = candle.low;
    if (candle.high > max) max = candle.high;
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 1 };
  const pad = (max - min) * 0.06 || max * 0.02 || 1;
  return { min: min - pad, max: max + pad };
}

function priceToY(price: number, bounds: Bounds): number {
  const span = bounds.max - bounds.min || 1;
  return PLOT.top + ((bounds.max - price) / span) * PRICE_HEIGHT;
}

function volumeToHeight(volume: number, maxVolume: number): number {
  if (maxVolume <= 0) return 0;
  return Math.max(1, (volume / maxVolume) * VOL_HEIGHT);
}

function candleX(index: number, count: number): number {
  const step = PLOT_WIDTH / Math.max(count, 1);
  return PLOT.left + step * index + step / 2;
}

function timeLabel(iso: string, interval: string): string {
  const raw = new Date(iso);
  if (interval === "1d") return raw.toISOString().slice(5, 10);
  const date = toBogota(raw);
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const mm = String(date.getUTCMinutes()).padStart(2, "0");
  return `${hh}:${mm}`;
}

function stampLabel(iso: string, interval: string): string {
  if (interval === "1d") return new Date(iso).toISOString().slice(0, 10) + " UTC";
  return toBogota(new Date(iso)).toISOString().replace("T", " ").slice(0, 16) + " COT";
}

/* ------------------------------------------------------------------ */
/* Candlestick series                                                  */
/* ------------------------------------------------------------------ */

interface ChartProps {
  candles: OHLCVCandle[];
  interval: string;
  hoveredIndex: number | null;
  onHover: (index: number | null, cursor: CursorPoint | null) => void;
}

function CandlestickChart({ candles, interval, onHover, hoveredIndex }: ChartProps) {
  const { bounds, maxVolume, gridLines } = useMemo(() => {
    const b = computeBounds(candles);
    let maxVol = 0;
    for (const candle of candles) maxVol = Math.max(maxVol, candle.volume);
    const lines = Array.from({ length: 5 }, (_, i) => {
      const price = b.max - ((b.max - b.min) / 4) * i;
      return { price, y: priceToY(price, b) };
    });
    return { bounds: b, maxVolume: maxVol, gridLines: lines };
  }, [candles]);

  const step = PLOT_WIDTH / Math.max(candles.length, 1);
  const bodyWidth = Math.max(2, step * 0.56);

  const handleMove = (event: ReactPointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const vbX = (x / Math.max(rect.width, 1)) * PLOT.w;
    const index = Math.round((vbX - PLOT.left - step / 2) / step);
    onHover(clamp(index, 0, candles.length - 1), { x, y, width: rect.width, height: rect.height });
  };

  return (
    <svg
      viewBox={`0 0 ${PLOT.w} ${PLOT.h}`}
      className="h-auto w-full touch-none"
      preserveAspectRatio="xMidYMid meet"
      onPointerMove={handleMove}
      onPointerLeave={() => onHover(null, null)}
      role="img"
      aria-label="Gráfica de velas OHLCV"
    >
      {gridLines.map((line) => (
        <g key={line.price}>
          <line x1={PLOT.left} y1={line.y} x2={PLOT.w - PLOT.right} y2={line.y} stroke="rgba(255,255,255,0.05)" strokeWidth="1" />
          <text x={PLOT.w - PLOT.right + 8} y={line.y + 3} fill="rgba(255,255,255,0.35)" fontSize="9" fontFamily="monospace">
            {formatNumber(line.price, 0)}
          </text>
        </g>
      ))}

      {/* Volume band */}
      {candles.map((candle, index) => {
        const bullish = candle.close >= candle.open;
        return (
          <rect
            key={`vol-${candle.timestamp}`}
            x={candleX(index, candles.length) - bodyWidth / 2}
            y={PLOT.volBottom - volumeToHeight(candle.volume, maxVolume)}
            width={bodyWidth}
            height={volumeToHeight(candle.volume, maxVolume)}
            fill={bullish ? BULL : BEAR}
            opacity={0.18}
          />
        );
      })}

      {/* Candles */}
      {candles.map((candle, index) => {
        const x = candleX(index, candles.length);
        const bullish = candle.close >= candle.open;
        const color = bullish ? BULL : BEAR;
        const yOpen = priceToY(candle.open, bounds);
        const yClose = priceToY(candle.close, bounds);
        const yHigh = priceToY(candle.high, bounds);
        const yLow = priceToY(candle.low, bounds);
        const bodyTop = Math.min(yOpen, yClose);
        const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));

        return (
          <g key={candle.timestamp}>
            <line x1={x} y1={yHigh} x2={x} y2={yLow} stroke={color} strokeWidth="1" opacity="0.85" />
            <rect x={x - bodyWidth / 2} y={bodyTop} width={bodyWidth} height={bodyHeight} fill={color} opacity="0.9" rx="0.5" />
          </g>
        );
      })}

      {hoveredIndex !== null && candles[hoveredIndex] && (
        <Crosshair candle={candles[hoveredIndex]} index={hoveredIndex} count={candles.length} bounds={bounds} />
      )}

      {candles.length > 0 && (
        <g>
          <text x={PLOT.left} y={PLOT.h - 8} fill="rgba(255,255,255,0.3)" fontSize="9" fontFamily="monospace">
            {timeLabel(candles[0]!.timestamp, interval)}
          </text>
          <text x={PLOT.w - PLOT.right} y={PLOT.h - 8} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize="9" fontFamily="monospace">
            {timeLabel(candles[candles.length - 1]!.timestamp, interval)}
          </text>
        </g>
      )}
    </svg>
  );
}

interface CrosshairProps {
  candle: OHLCVCandle;
  index: number;
  count: number;
  bounds: Bounds;
}

function Crosshair({ candle, index, count, bounds }: CrosshairProps) {
  const x = candleX(index, count);
  const y = priceToY(candle.close, bounds);
  return (
    <g pointerEvents="none">
      <line x1={x} y1={PLOT.top} x2={x} y2={PLOT.volBottom} stroke="rgba(255,255,255,0.25)" strokeWidth="1" strokeDasharray="2 3" />
      <line x1={PLOT.left} y1={y} x2={PLOT.w - PLOT.right} y2={y} stroke="rgba(255,255,255,0.18)" strokeWidth="1" strokeDasharray="2 3" />
      <circle cx={x} cy={y} r="2.4" fill="#FFFFFF" />
    </g>
  );
}

/* ------------------------------------------------------------------ */
/* Area series (comparison / reconciliation views)                     */
/* ------------------------------------------------------------------ */

function AreaChart({ candles, interval, onHover, hoveredIndex }: ChartProps) {
  const gradientId = useId();
  const { bounds, path, areaPath, gridLines, points } = useMemo(() => {
    const b = computeBounds(candles);
    const pts = candles.map((candle, index) => ({
      x: candleX(index, candles.length),
      y: priceToY(candle.close, b),
    }));
    const line = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(" ");
    const area =
      pts.length > 0
        ? `${line} L${pts[pts.length - 1]!.x.toFixed(2)},${PLOT.priceBottom} L${pts[0]!.x.toFixed(2)},${PLOT.priceBottom} Z`
        : "";
    const lines = Array.from({ length: 5 }, (_, i) => {
      const price = b.max - ((b.max - b.min) / 4) * i;
      return { price, y: priceToY(price, b) };
    });
    return { bounds: b, path: line, areaPath: area, gridLines: lines, points: pts };
  }, [candles]);

  const basePrice = candles[0]?.open ?? 0;
  const lastPrice = candles[candles.length - 1]?.close ?? basePrice;
  const accent = lastPrice >= basePrice ? BULL : BEAR;

  const handleMove = (event: ReactPointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const vbX = (x / Math.max(rect.width, 1)) * PLOT.w;
    const step = PLOT_WIDTH / Math.max(candles.length, 1);
    const index = Math.round((vbX - PLOT.left - step / 2) / step);
    onHover(clamp(index, 0, candles.length - 1), { x, y, width: rect.width, height: rect.height });
  };

  return (
    <svg
      viewBox={`0 0 ${PLOT.w} ${PLOT.h}`}
      className="h-auto w-full touch-none"
      preserveAspectRatio="xMidYMid meet"
      onPointerMove={handleMove}
      onPointerLeave={() => onHover(null, null)}
      role="img"
      aria-label="Gráfica de área"
    >
      <defs>
        <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={accent} stopOpacity="0.35" />
          <stop offset="100%" stopColor={accent} stopOpacity="0" />
        </linearGradient>
      </defs>

      {gridLines.map((line) => (
        <g key={line.price}>
          <line x1={PLOT.left} y1={line.y} x2={PLOT.w - PLOT.right} y2={line.y} stroke="rgba(255,255,255,0.05)" strokeWidth="1" />
          <text x={PLOT.w - PLOT.right + 8} y={line.y + 3} fill="rgba(255,255,255,0.35)" fontSize="9" fontFamily="monospace">
            {formatNumber(line.price, 0)}
          </text>
        </g>
      ))}

      <path d={areaPath} fill={`url(#${gradientId})`} />
      <path d={path} fill="none" stroke={accent} strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round" />

      {hoveredIndex !== null && points[hoveredIndex] && (
        <g pointerEvents="none">
          <line
            x1={points[hoveredIndex]!.x}
            y1={PLOT.top}
            x2={points[hoveredIndex]!.x}
            y2={PLOT.priceBottom}
            stroke="rgba(255,255,255,0.22)"
            strokeWidth="1"
            strokeDasharray="2 3"
          />
          <circle cx={points[hoveredIndex]!.x} cy={points[hoveredIndex]!.y} r="2.6" fill="#FFFFFF" />
        </g>
      )}

      {candles.length > 0 && (
        <g>
          <text x={PLOT.left} y={PLOT.h - 8} fill="rgba(255,255,255,0.3)" fontSize="9" fontFamily="monospace">
            {timeLabel(candles[0]!.timestamp, interval)}
          </text>
          <text x={PLOT.w - PLOT.right} y={PLOT.h - 8} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize="9" fontFamily="monospace">
            {timeLabel(candles[candles.length - 1]!.timestamp, interval)}
          </text>
        </g>
      )}
      <g>
        <text x={PLOT.left} y={PLOT.h - 20} fill="rgba(255,255,255,0.22)" fontSize="8" fontFamily="monospace">
          {bounds.min.toFixed(2)} — {bounds.max.toFixed(2)}
        </text>
      </g>
    </svg>
  );
}

/* ------------------------------------------------------------------ */
/* Data table                                                          */
/* ------------------------------------------------------------------ */

function ArtifactTable({ candles, interval }: { candles: OHLCVCandle[]; interval: string }) {
  return (
    <div className="max-h-[320px] overflow-auto rounded-lg border border-white/[0.06]">
      <table className="w-full min-w-[560px] border-collapse font-mono text-[11px] tabular-nums">
        <thead className="sticky top-0 z-10 bg-zinc-950/95 backdrop-blur">
          <tr className="text-left text-[10px] uppercase tracking-wider text-zinc-500">
            <th className="px-3 py-2 font-medium">Timestamp</th>
            <th className="px-3 py-2 text-right font-medium">Open</th>
            <th className="px-3 py-2 text-right font-medium">High</th>
            <th className="px-3 py-2 text-right font-medium">Low</th>
            <th className="px-3 py-2 text-right font-medium">Close</th>
            <th className="px-3 py-2 text-right font-medium">Volume</th>
          </tr>
        </thead>
        <tbody>
          {candles.map((candle) => {
            const bullish = candle.close >= candle.open;
            return (
              <tr key={candle.timestamp} className="border-t border-white/[0.04] hover:bg-white/[0.03]">
                <td className="whitespace-nowrap px-3 py-1.5 text-zinc-500">
                  {stampLabel(candle.timestamp, interval)}
                </td>
                <td className="whitespace-nowrap px-3 py-1.5 text-right text-zinc-300">{formatNumber(candle.open, 2)}</td>
                <td className="whitespace-nowrap px-3 py-1.5 text-right text-zinc-300">{formatNumber(candle.high, 2)}</td>
                <td className="whitespace-nowrap px-3 py-1.5 text-right text-zinc-300">{formatNumber(candle.low, 2)}</td>
                <td className={cn("whitespace-nowrap px-3 py-1.5 text-right font-medium", bullish ? "text-emerald-400" : "text-rose-400")}>
                  {formatNumber(candle.close, 2)}
                </td>
                <td className="whitespace-nowrap px-3 py-1.5 text-right text-zinc-500">{formatCompact(candle.volume)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Public component                                                    */
/* ------------------------------------------------------------------ */

interface DataArtifactProps {
  artifact: ArtifactModel;
}

export function DataArtifact({ artifact }: DataArtifactProps) {
  const isMarket = artifact.type !== "METRIC_TABLE";
  const initialInterval: MarketInterval = isMarketInterval(artifact.interval) ? artifact.interval : "15m";
  const [activeInterval, setActiveInterval] = useState<MarketInterval>(initialInterval);
  const [tab, setTab] = useState<"chart" | "table">(artifact.type === "METRIC_TABLE" ? "table" : "chart");
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [cursor, setCursor] = useState<CursorPoint | null>(null);

  useEffect(() => {
    setActiveInterval(isMarketInterval(artifact.interval) ? artifact.interval : "15m");
    setHoveredIndex(null);
    setCursor(null);
  }, [artifact.interval, artifact.symbol]);

  const [view, setView] = useState<ArtifactModel>(artifact);

  useEffect(() => {
    if (!isMarket) {
      setView(artifact);
      return;
    }
    let cancelled = false;
    regenerateArtifact(artifact, activeInterval).then((next) => {
      if (!cancelled) setView(next);
    });
    return () => {
      cancelled = true;
    };
  }, [artifact, activeInterval, isMarket]);

  const hovered = hoveredIndex !== null ? view.data[hoveredIndex] : undefined;

  const handleHover = (index: number | null, point: CursorPoint | null) => {
    setHoveredIndex(index);
    setCursor(point);
  };

  const exportCsv = () => {
    const header = "timestamp,open,high,low,close,volume";
    const rows = view.data.map(
      (candle) =>
        `${candle.timestamp},${candle.open},${candle.high},${candle.low},${candle.close},${candle.volume}`,
    );
    const blob = new Blob([[header, ...rows].join("\n")], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${view.symbol.replace(/[^\w.-]/g, "_")}_${view.interval}.csv`;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(url);
  };

  /** The card tracks the cursor, flips at the right edge, and never leaves the plot. */
  const tooltipPosition = useMemo(() => {
    if (!cursor) return null;
    let left = cursor.x + TIP_GAP;
    if (left + TIP_WIDTH > cursor.width - 6) {
      left = cursor.x - TIP_GAP - TIP_WIDTH;
    }
    left = clamp(left, 6, Math.max(6, cursor.width - TIP_WIDTH - 6));
    const top = clamp(cursor.y - TIP_HEIGHT / 2, 6, Math.max(6, cursor.height - TIP_HEIGHT - 6));
    return { left, top };
  }, [cursor]);

  return (
    <div className="rounded-xl border border-white/[0.08] bg-white/[0.015] p-3.5">
      {/* Header */}
      <div className="mb-3 flex flex-col gap-2.5 sm:flex-row sm:items-center sm:justify-between sm:gap-3">
        <div className="flex items-center gap-2">
          <span className="font-mono text-xs font-medium tracking-wide text-zinc-100">{view.symbol}</span>
          <span className="rounded-full border border-white/10 px-2 py-0.5 font-mono text-[10px] text-zinc-500">
            {view.interval}
          </span>
          <span className="rounded-full border border-white/10 px-2 py-0.5 font-mono text-[10px] text-zinc-500">
            {view.type}
          </span>
          {view.isLive === false && (
            <span
              className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-mono text-[10px] text-amber-400"
              title={view.error ?? "Binance no disponible"}
            >
              sin datos reales
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-1.5 sm:justify-end">
          <div className="flex items-center rounded-full border border-white/10 bg-white/[0.02] p-0.5">
            {(["chart", "table"] as const).map((option) => (
              <button
                key={option}
                type="button"
                onClick={() => setTab(option)}
                className={cn(
                  "relative rounded-full px-2 py-1 font-mono text-[9px] uppercase tracking-wide transition-colors sm:px-2.5 sm:text-[10px]",
                  tab === option ? "text-zinc-100" : "text-zinc-500 hover:text-zinc-300",
                )}
              >
                {tab === option && (
                  <motion.span
                    layoutId={`artifact-tab-${view.symbol}-${view.interval}`}
                    className="absolute inset-0 rounded-full bg-white/[0.08]"
                    transition={{ type: "spring", stiffness: 450, damping: 34 }}
                  />
                )}
                <span className="relative z-10">{option === "chart" ? "Gráfica" : "Tabla de Datos"}</span>
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={exportCsv}
            className="rounded-full border border-white/10 px-2 py-1 font-mono text-[9px] uppercase tracking-wide text-zinc-400 transition-colors hover:border-white/25 hover:text-zinc-100 sm:px-2.5 sm:text-[10px]"
          >
            Exportar CSV
          </button>
        </div>
      </div>

      {isMarket && (
        <div className="mb-2.5 flex flex-wrap items-center gap-1">
          {MARKET_INTERVALS.map((interval) => (
            <button
              key={interval}
              type="button"
              onClick={() => setActiveInterval(interval)}
              className={cn(
                "relative rounded-full px-2.5 py-1 font-mono text-[10px] tabular-nums transition-colors",
                activeInterval === interval ? "text-white" : "text-zinc-600 hover:text-zinc-300",
              )}
            >
              {activeInterval === interval && (
                <motion.span
                  layoutId={`timeframe-${artifact.symbol}`}
                  className="absolute inset-0 rounded-full bg-white/[0.08]"
                  transition={{ type: "spring", stiffness: 450, damping: 34 }}
                />
              )}
              <span className="relative z-10">{interval}</span>
            </button>
          ))}
        </div>
      )}

      {/* Body */}
      <AnimatePresence mode="wait" initial={false}>
        {tab === "chart" ? (
          <motion.div
            key="chart"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            transition={{ type: "spring", stiffness: 300, damping: 30 }}
            className="relative"
          >
            {view.type === "CANDLESTICK" ? (
              <CandlestickChart candles={view.data} interval={view.interval} hoveredIndex={hoveredIndex} onHover={handleHover} />
            ) : (
              <AreaChart candles={view.data} interval={view.interval} hoveredIndex={hoveredIndex} onHover={handleHover} />
            )}

            <AnimatePresence>
              {hovered && tooltipPosition && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.1 }}
                  style={{ left: tooltipPosition.left, top: tooltipPosition.top, width: TIP_WIDTH }}
                  className="pointer-events-none absolute z-20 rounded-lg border border-white/10 bg-zinc-950 px-3 py-2 font-mono text-[10px] tabular-nums shadow-[0_10px_40px_-10px_rgba(0,0,0,0.9)]"
                >
                  <div className="mb-1 text-zinc-500">{stampLabel(hovered.timestamp, view.interval)}</div>
                  <div className="grid grid-cols-2 gap-x-3 gap-y-0.5">
                    <span className="text-zinc-500">O</span>
                    <span className="text-right text-zinc-200">{formatNumber(hovered.open, 2)}</span>
                    <span className="text-zinc-500">H</span>
                    <span className="text-right text-zinc-200">{formatNumber(hovered.high, 2)}</span>
                    <span className="text-zinc-500">L</span>
                    <span className="text-right text-zinc-200">{formatNumber(hovered.low, 2)}</span>
                    <span className="text-zinc-500">C</span>
                    <span
                      className={cn(
                        "text-right font-medium",
                        hovered.close >= hovered.open ? "text-emerald-400" : "text-rose-400",
                      )}
                    >
                      {formatNumber(hovered.close, 2)}
                    </span>
                    <span className="text-zinc-500">V</span>
                    <span className="text-right text-zinc-300">{formatCompact(hovered.volume)}</span>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        ) : (
          <motion.div
            key="table"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            transition={{ type: "spring", stiffness: 300, damping: 30 }}
          >
            <ArtifactTable candles={view.data} interval={view.interval} />
          </motion.div>
        )}
      </AnimatePresence>

      {/* Summary strip */}
      <div className="mt-3 grid grid-cols-3 gap-2 border-t border-white/[0.06] pt-3">
        <Metric label="Volumen total" value={formatCompact(view.summary.totalVolume)} />
        <Metric
          label="Δ Precio"
          value={formatPercent(view.summary.priceChangePercent)}
          tone={view.summary.priceChangePercent >= 0 ? "up" : "down"}
        />
        <Metric label="VWAP" value={formatUsd(view.summary.vwap)} />
      </div>

      {view.peers && view.peers.length > 0 && (
        <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-white/[0.06] pt-2 font-mono text-[10px]">
          {view.peers.map((peer) => (
            <span key={peer.symbol} className="text-zinc-500">
              {peer.symbol}{" "}
              <span className={peer.summary.priceChangePercent >= 0 ? "text-emerald-400" : "text-rose-400"}>
                {formatPercent(peer.summary.priceChangePercent)}
              </span>{" "}
              <span className="text-zinc-600">vol {formatCompact(peer.summary.totalVolume)}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function Metric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "up" | "down" }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{label}</span>
      <span
        className={cn(
          "font-mono text-sm tabular-nums",
          tone === "up" && "text-emerald-400",
          tone === "down" && "text-rose-400",
          tone === "neutral" && "text-zinc-200",
        )}
      >
        {value}
      </span>
    </div>
  );
}
