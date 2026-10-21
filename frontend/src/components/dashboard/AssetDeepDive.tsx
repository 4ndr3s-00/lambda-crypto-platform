"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import type {
  ArbitrageQuote,
  DashboardInterval,
  DashboardSymbol,
  ExecutionTrade,
  LiquidationEvent,
  OHLCVCandle,
  OnChainSnapshot,
  OrderBookSnapshot,
} from "@/lib/types";
import { DASHBOARD_INTERVALS, mockApi } from "@/lib/mockData";
import {
  ASSET_PROFILES,
  createArbitrage,
  createLiquidations,
  createOnChain,
  createOrderBook,
  createTrades,
} from "@/lib/streamMock";
import { cn, formatCompact, formatPercent, formatUsd } from "@/lib/utils";
import { StreamingChart } from "./StreamingChart";
import { OrderBookPanel } from "./OrderBookPanel";
import { ExecutionTape } from "./ExecutionTape";
import { ArbitrageMatrix, LiquidationRadar, OnChainMacro } from "./MarketModules";

interface AssetDeepDiveProps {
  symbol: DashboardSymbol;
  interval: DashboardInterval;
  onIntervalChange: (interval: DashboardInterval) => void;
  onBack: () => void;
}

function symbolSeed(symbol: string): number {
  return symbol.split("").reduce((acc, char) => acc * 31 + char.charCodeAt(0), 7);
}

export function AssetDeepDive({ symbol, interval, onIntervalChange, onBack }: AssetDeepDiveProps) {
  const seed = useMemo(() => symbolSeed(symbol), [symbol]);
  const profile = ASSET_PROFILES[symbol];
  const [candles, setCandles] = useState<OHLCVCandle[]>([]);
  const [loading, setLoading] = useState(true);
  const [price, setPrice] = useState(profile.basePrice);
  const [orderBook, setOrderBook] = useState<OrderBookSnapshot>(() =>
    createOrderBook(symbol, profile.basePrice, seed),
  );
  const [trades, setTrades] = useState<ExecutionTrade[]>(() =>
    createTrades(symbol, profile.basePrice, seed, 48),
  );
  const [arbitrage, setArbitrage] = useState<ArbitrageQuote[]>(() =>
    createArbitrage(profile.basePrice, seed),
  );
  const [liquidations, setLiquidations] = useState<LiquidationEvent[]>(() =>
    createLiquidations(symbol, profile.basePrice, seed),
  );
  const [onChain, setOnChain] = useState<OnChainSnapshot>(() => createOnChain(seed));
  const priceRef = useRef(price);
  priceRef.current = price;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setCandles([]);

    void mockApi
      .getOHLCV({ symbol, interval, limit: 96 })
      .then((series) => {
        if (cancelled) return;
        setCandles(series.candles);
        const last = series.candles[series.candles.length - 1];
        if (last) {
          setPrice(last.close);
          priceRef.current = last.close;
        }
        setLoading(false);
      })
      .catch(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [symbol, interval]);

  useEffect(() => {
    let frame = 0;
    const id = window.setInterval(() => {
      const now = Date.now();
      const drift = (Math.random() - 0.5) * profile.volatility * 0.7;
      const nextPrice = Math.max(priceRef.current * (1 + drift), 0.01);
      priceRef.current = nextPrice;
      setPrice(nextPrice);

      if (frame % 2 === 0) {
        setOrderBook(createOrderBook(symbol, nextPrice, now));
        setTrades((previous) => [...createTrades(symbol, nextPrice, now, 2), ...previous].slice(0, 80));
      }
      if (frame % 8 === 0) setArbitrage(createArbitrage(nextPrice, now));
      if (frame % 12 === 0) {
        setLiquidations((previous) => [
          ...createLiquidations(symbol, nextPrice, now).slice(0, 2),
          ...previous,
        ].slice(0, 12));
      }
      if (frame % 20 === 0) setOnChain(createOnChain(now));
      frame += 1;
    }, 250);

    return () => window.clearInterval(id);
  }, [profile.volatility, symbol]);

  useEffect(() => {
    if (candles.length === 0) return;
    const id = window.setInterval(() => {
      setCandles((previous) => {
        if (previous.length === 0) return previous;
        const last = previous[previous.length - 1]!;
        const close = priceRef.current;
        return [
          ...previous.slice(0, -1),
          {
            ...last,
            close,
            high: Math.max(last.high, close),
            low: Math.min(last.low, close),
            volume: last.volume * (1 + Math.random() * 0.02),
          },
        ];
      });
    }, 1000);
    return () => window.clearInterval(id);
  }, [candles.length]);

  const first = candles[0];
  const last = candles[candles.length - 1];
  const returnPct = first && last && first.open > 0 ? ((last.close - first.open) / first.open) * 100 : 0;
  const volume = candles.reduce((acc, candle) => acc + candle.volume * candle.close, 0);
  const up = returnPct >= 0;

  return (
    <motion.section
      initial={{ opacity: 0, scale: 0.985 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ type: "spring", stiffness: 240, damping: 28 }}
      className="min-w-0 rounded-2xl border border-white/10 bg-zinc-950/40 p-3 sm:p-4"
    >
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <button
          type="button"
          onClick={onBack}
          className="rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5 font-mono text-[10px] text-zinc-300 transition-colors hover:border-white/25 hover:text-white active:scale-[0.98]"
        >
          ← Volver a todas las monedas
        </button>

        <div className="flex items-center gap-2">
          <span className="font-mono text-[11px] font-medium tracking-wide text-zinc-200">
            {symbol} · WORKSPACE ANALÍTICO EN VIVO
          </span>
          <span className={cn("rounded-full border px-2 py-0.5 font-mono text-[9px]", up ? "border-emerald-500/25 text-emerald-400" : "border-rose-500/25 text-rose-400")}>
            {formatPercent(returnPct)}
          </span>
        </div>

        <div className="flex items-center gap-0.5 rounded-full border border-white/10 bg-white/[0.02] p-0.5">
          {DASHBOARD_INTERVALS.map((option) => (
            <button
              key={option}
              type="button"
              onClick={() => onIntervalChange(option)}
              className={cn(
                "relative rounded-full px-2.5 py-1 font-mono text-[9px] transition-colors",
                interval === option ? "bg-white text-black" : "text-zinc-500 hover:text-zinc-200",
              )}
            >
              {option}
            </button>
          ))}
        </div>
      </div>

      <div className="mb-3 grid grid-cols-2 gap-2 lg:grid-cols-4">
        <DiveMetric label="Precio" value={formatUsd(price)} />
        <DiveMetric label="Retorno ventana" value={formatPercent(returnPct)} tone={up ? "up" : "down"} />
        <DiveMetric label="Volumen" value={`$${formatCompact(volume)}`} />
        <DiveMetric label="OBI" value={`${orderBook.obi >= 0 ? "+" : ""}${orderBook.obi.toFixed(1)}%`} tone={orderBook.obi >= 0 ? "up" : "down"} />
      </div>

      <div className="mb-3 flex h-[420px] min-h-[390px] flex-col rounded-xl border border-white/[0.08] bg-white/[0.012] p-3">
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2 px-1">
          <div className="flex items-center gap-2 font-mono text-[10px] uppercase tracking-wider text-zinc-500">
            <span className="text-zinc-200">{symbol}</span>
            <span className="rounded-full border border-white/10 px-2 py-0.5">{interval}</span>
            <span className="hidden sm:inline">OHLCV · live</span>
          </div>
          <span className="font-mono text-[9px] text-zinc-600">
            {loading ? "Cargando feed…" : `${candles.length} velas · tick 1s`}
          </span>
        </div>
        <div className="min-h-0 flex-1">
          <StreamingChart candles={candles} interval={interval} live />
        </div>
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <OrderBookPanel snapshot={orderBook} />
        <ExecutionTape trades={trades} />
        <ArbitrageMatrix quotes={arbitrage} />
        <LiquidationRadar events={liquidations} />
        <div className="lg:col-span-2">
          <OnChainMacro snapshot={onChain} />
        </div>
      </div>
    </motion.section>
  );
}

function DiveMetric({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value: string;
  tone?: "neutral" | "up" | "down";
}) {
  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.02] px-2.5 py-2">
      <span className="block font-mono text-[8px] uppercase tracking-wider text-zinc-600">{label}</span>
      <span
        className={cn(
          "mt-0.5 block font-mono text-[13px] tabular-nums",
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
