"use client";

import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from "react";
import type { DashboardInterval, OHLCVCandle } from "@/lib/types";
import { clamp, cn, formatCompact, formatNumber } from "@/lib/utils";

const BULL = "#10B981";
const BEAR = "#F43F5E";

const TIP_WIDTH = 192;
const TIP_HEIGHT = 158;
const TIP_GAP = 14;

interface Size {
  w: number;
  h: number;
}

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

interface Plot {
  left: number;
  right: number;
  top: number;
  bottom: number;
  priceBottom: number;
  volTop: number;
  volBottom: number;
  width: number;
  priceHeight: number;
  volHeight: number;
}

interface StreamingChartProps {
  candles: OHLCVCandle[];
  interval: DashboardInterval;
  live?: boolean;
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

function priceDigits(price: number): number {
  return price >= 1000 ? 0 : 2;
}

function axisLabel(iso: string, interval: DashboardInterval): string {
  const date = new Date(iso);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const mm = String(date.getUTCMinutes()).padStart(2, "0");
  if (interval === "1m" || interval === "5m") return `${hh}:${mm}`;
  if (interval === "1d") return `${month}-${day}`;
  return `${month}-${day} ${hh}:${mm}`;
}

/**
 * Full-bleed OHLCV + volume SVG. Zero dependencies, measured to the real
 * container box so text and hairlines stay 1:1 instead of being stretched.
 */
export function StreamingChart({ candles, interval, live = true }: StreamingChartProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState<Size>({ w: 0, h: 0 });
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [cursor, setCursor] = useState<CursorPoint | null>(null);
  const autoScrolled = useRef(false);
  const minCandleWidth = interval === "1d" ? 20 : 7;
  const contentWidth = Math.max(size.w, candles.length * minCandleWidth);

  useEffect(() => {
    const node = wrapRef.current;
    if (!node || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      setSize({ w: Math.max(1, Math.floor(rect.width)), h: Math.max(1, Math.floor(rect.height)) });
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const plot: Plot = useMemo(() => {
    const left = 12;
    const right = size.w >= 700 ? 76 : 58;
    const top = 18;
    const bottom = Math.max(top + 150, size.h - 24);
    const priceBottom = Math.max(top + 80, bottom - 82);
    const volTop = priceBottom + 18;
    const volBottom = Math.max(volTop + 20, bottom - 8);
    return {
      left,
      right,
      top,
      bottom,
      priceBottom,
      volTop,
      volBottom,
      width: Math.max(1, contentWidth - left - right),
      priceHeight: Math.max(1, priceBottom - top),
      volHeight: Math.max(1, volBottom - volTop),
    };
  }, [size, contentWidth]);

  useEffect(() => {
    autoScrolled.current = false;
  }, [interval]);

  useEffect(() => {
    const node = wrapRef.current;
    if (!node || autoScrolled.current || candles.length === 0) return;
    node.scrollLeft = node.scrollWidth;
    autoScrolled.current = true;
  }, [candles.length, interval]);

  const { bounds, maxVolume, grid } = useMemo(() => {
    const b = computeBounds(candles);
    let maxVol = 0;
    for (const candle of candles) maxVol = Math.max(maxVol, candle.volume);
    const span = b.max - b.min || 1;
    const lines = Array.from({ length: 5 }, (_, index) => {
      const price = b.max - (span / 4) * index;
      return {
        price,
        y: plot.top + ((b.max - price) / span) * plot.priceHeight,
      };
    });
    return { bounds: b, maxVolume: maxVol, grid: lines };
  }, [candles, plot]);

  const step = plot.width / Math.max(candles.length, 1);
  const bodyWidth = Math.max(1, step * 0.58);

  const priceToY = (price: number): number => {
    const span = bounds.max - bounds.min || 1;
    return plot.top + ((bounds.max - price) / span) * plot.priceHeight;
  };

  const candleX = (index: number): number => plot.left + step * index + step / 2;

  const handleMove = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (candles.length === 0) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const wrapRect = wrapRef.current?.getBoundingClientRect();
    const contentX = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const index = Math.round((contentX - plot.left - step / 2) / step);
    setHoveredIndex(clamp(index, 0, candles.length - 1));
    setCursor({
      x: wrapRect ? event.clientX - wrapRect.left : contentX,
      y,
      width: wrapRect?.width ?? rect.width,
      height: rect.height,
    });
  };

  const handleLeave = () => {
    setHoveredIndex(null);
    setCursor(null);
  };

  const hovered = hoveredIndex !== null ? candles[hoveredIndex] : undefined;
  const last = candles[candles.length - 1];

  const tooltipPosition = useMemo(() => {
    if (!cursor) return null;
    let left = cursor.x + TIP_GAP;
    if (left + TIP_WIDTH > cursor.width - 6) left = cursor.x - TIP_GAP - TIP_WIDTH;
    left = clamp(left, 6, Math.max(6, cursor.width - TIP_WIDTH - 6));
    const top = clamp(cursor.y - TIP_HEIGHT / 2, 6, Math.max(6, cursor.height - TIP_HEIGHT - 6));
    return { left, top };
  }, [cursor]);

  return (
    <div ref={wrapRef} className="relative h-full w-full overflow-x-scroll overscroll-x-contain pb-2 [scrollbar-width:thin] [&::-webkit-scrollbar]:h-2 [&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-white/20 [&::-webkit-scrollbar-track]:bg-transparent">
      {size.w > 0 && size.h > 0 && (
        <svg
          width={contentWidth}
          height={size.h}
          viewBox={`0 0 ${contentWidth} ${size.h}`}
          className="block h-full touch-none"
          onPointerMove={handleMove}
          onPointerLeave={handleLeave}
          role="img"
          aria-label="Gráfica de velas OHLCV y volumen en streaming"
        >
          {/* Price grid */}
          {grid.map((line) => (
            <g key={line.price}>
              <line
                x1={plot.left}
                y1={line.y}
                x2={size.w - plot.right}
                y2={line.y}
                stroke="rgba(255,255,255,0.05)"
                strokeWidth="1"
              />
              <text
                x={size.w - plot.right + 8}
                y={line.y + 3}
                fill="rgba(255,255,255,0.34)"
                fontSize="9"
                fontFamily="monospace"
              >
                {formatNumber(line.price, priceDigits(line.price))}
              </text>
            </g>
          ))}

          {/* Volume band */}
          <text x={plot.left} y={plot.volTop - 5} fill="rgba(255,255,255,0.26)" fontSize="8" fontFamily="monospace">
            VOL
          </text>
          {candles.map((candle, index) => {
            const bullish = candle.close >= candle.open;
            const height = maxVolume > 0 ? Math.max(1, (candle.volume / maxVolume) * plot.volHeight) : 0;
            return (
              <rect
                key={`vol-${candle.timestamp}-${index}`}
                x={candleX(index) - bodyWidth / 2}
                y={plot.volBottom - height}
                width={bodyWidth}
                height={height}
                fill={bullish ? BULL : BEAR}
                opacity={0.22}
              />
            );
          })}

          {/* Candles */}
          {candles.map((candle, index) => {
            const x = candleX(index);
            const bullish = candle.close >= candle.open;
            const color = bullish ? BULL : BEAR;
            const yOpen = priceToY(candle.open);
            const yClose = priceToY(candle.close);
            const yHigh = priceToY(candle.high);
            const yLow = priceToY(candle.low);
            const bodyTop = Math.min(yOpen, yClose);
            const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));

            return (
              <g key={`candle-${candle.timestamp}-${index}`}>
                <line x1={x} y1={yHigh} x2={x} y2={yLow} stroke={color} strokeWidth="1" opacity="0.85" />
                <rect
                  x={x - bodyWidth / 2}
                  y={bodyTop}
                  width={bodyWidth}
                  height={bodyHeight}
                  fill={color}
                  opacity="0.92"
                  rx="0.4"
                />
              </g>
            );
          })}

          {/* Live last-trade marker */}
          {live && last && (
            <g pointerEvents="none">
              <line
                x1={plot.left}
                y1={priceToY(last.close)}
                x2={size.w - plot.right}
                y2={priceToY(last.close)}
                stroke="rgba(255,255,255,0.16)"
                strokeWidth="1"
                strokeDasharray="2 4"
              />
              <circle cx={candleX(candles.length - 1)} cy={priceToY(last.close)} r="2.4" fill="#FFFFFF" />
            </g>
          )}

          {/* Crosshair */}
          {hovered && hoveredIndex !== null && (
            <g pointerEvents="none">
              <line
                x1={candleX(hoveredIndex)}
                y1={plot.top}
                x2={candleX(hoveredIndex)}
                y2={plot.volBottom}
                stroke="rgba(255,255,255,0.24)"
                strokeWidth="1"
                strokeDasharray="2 3"
              />
              <line
                x1={plot.left}
                y1={priceToY(hovered.close)}
                x2={size.w - plot.right}
                y2={priceToY(hovered.close)}
                stroke="rgba(255,255,255,0.18)"
                strokeWidth="1"
                strokeDasharray="2 3"
              />
              <circle cx={candleX(hoveredIndex)} cy={priceToY(hovered.close)} r="2.4" fill="#FFFFFF" />
            </g>
          )}

          {/* Time axis */}
          {candles.length > 0 && (
            <g>
              <text x={plot.left} y={size.h - 8} fill="rgba(255,255,255,0.3)" fontSize="9" fontFamily="monospace">
                {axisLabel(candles[0]!.timestamp, interval)}
              </text>
              <text
                x={size.w - plot.right}
                y={size.h - 8}
                textAnchor="end"
                fill="rgba(255,255,255,0.3)"
                fontSize="9"
                fontFamily="monospace"
              >
                {axisLabel(candles[candles.length - 1]!.timestamp, interval)}
              </text>
            </g>
          )}
        </svg>
      )}

      {candles.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="font-mono text-[11px] text-zinc-600">Esperando feed de mercado…</span>
        </div>
      )}

      {hovered && tooltipPosition && (
        <div
          style={{ left: tooltipPosition.left, top: tooltipPosition.top, width: TIP_WIDTH }}
          className="pointer-events-none absolute z-20 rounded-lg border border-white/10 bg-zinc-950 px-3 py-2 font-mono text-[10px] tabular-nums shadow-[0_10px_40px_-10px_rgba(0,0,0,0.9)]"
        >
          <div className="mb-1 text-zinc-500">
            {interval === "1d" ? hovered.timestamp.slice(0, 10) + " UTC" : hovered.timestamp.replace("T", " ").slice(0, 19) + " UTC"}
          </div>
          <div className="grid grid-cols-2 gap-x-3 gap-y-0.5">
            <span className="text-zinc-500">O</span>
            <span className="text-right text-zinc-200">{formatNumber(hovered.open, priceDigits(hovered.open))}</span>
            <span className="text-zinc-500">H</span>
            <span className="text-right text-zinc-200">{formatNumber(hovered.high, priceDigits(hovered.high))}</span>
            <span className="text-zinc-500">L</span>
            <span className="text-right text-zinc-200">{formatNumber(hovered.low, priceDigits(hovered.low))}</span>
            <span className="text-zinc-500">C</span>
            <span
              className={cn(
                "text-right font-medium",
                hovered.close >= hovered.open ? "text-emerald-400" : "text-rose-400",
              )}
            >
              {formatNumber(hovered.close, priceDigits(hovered.close))}
            </span>
            <span className="text-zinc-500">V</span>
            <span className="text-right text-zinc-300">{formatCompact(hovered.volume)}</span>
          </div>
        </div>
      )}
    </div>
  );
}
