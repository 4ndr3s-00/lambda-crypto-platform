"use client";

import { useEffect, useMemo, useState } from "react";
import { motion } from "framer-motion";
import type {
  ArbitrageQuote,
  DashboardInterval,
  DashboardSymbol,
  ExecutionTrade,
  LiquidationEvent,
  OHLCVCandle,
  OHLCVResponse,
  OnChainSnapshot,
  OrderBookSnapshot,
} from "@/lib/types";
import { DASHBOARD_INTERVALS, mockApi } from "@/lib/mockData";
import { createArbitrage, createLiquidations, createOnChain } from "@/lib/streamMock";
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
  const [series, setSeries] = useState<OHLCVResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [seriesError, setSeriesError] = useState<string | null>(null);
  const [orderBook, setOrderBook] = useState<OrderBookSnapshot | null>(null);
  const [orderBookError, setOrderBookError] = useState<string | null>(null);
  const [trades, setTrades] = useState<ExecutionTrade[]>([]);
  const [tradesSource, setTradesSource] = useState<string>("—");
  const [arbitrage, setArbitrage] = useState<ArbitrageQuote[]>(() =>
    createArbitrage(1, seed),
  );
  const [liquidations, setLiquidations] = useState<LiquidationEvent[]>(() =>
    createLiquidations(symbol, 1, seed),
  );
  const [onChain, setOnChain] = useState<OnChainSnapshot>(() => createOnChain(seed));

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setSeriesError(null);

    const load = async () => {
      try {
        const next = await mockApi.getOHLCV({ symbol, interval, limit: interval === "1d" ? 100 : 96 });
        if (cancelled) return;
        setSeries(next);
        setSeriesError(next.isLive ? null : next.error ?? "datos simulados");
        setLoading(false);
      } catch (error) {
        if (cancelled) return;
        setSeriesError(error instanceof Error ? error.message : "sin datos");
        setLoading(false);
      }
    };

    void load();
    const id = window.setInterval(load, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [symbol, interval]);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [book, tape] = await Promise.all([
          mockApi.getOrderBook(symbol, 20),
          mockApi.getTrades(symbol, 60),
        ]);
        if (cancelled) return;
        setOrderBook(book);
        setOrderBookError(null);
        setTrades(tape.trades);
        setTradesSource(tape.source);
      } catch (error) {
        if (cancelled) return;
        setOrderBookError(error instanceof Error ? error.message : "sin datos");
      }
    };

    void load();
    const id = window.setInterval(load, 3000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [symbol]);

  useEffect(() => {
    const id = window.setInterval(() => {
      const price = series?.candles[series.candles.length - 1]?.close ?? 1;
      setArbitrage(createArbitrage(price, Date.now()));
      setLiquidations((previous) => [
        ...createLiquidations(symbol, price, Date.now()).slice(0, 1),
        ...previous,
      ].slice(0, 12));
      setOnChain(createOnChain(Date.now()));
    }, 6000);
    return () => window.clearInterval(id);
  }, [series, symbol]);

  const candles: OHLCVCandle[] = series?.candles ?? [];
  const first = candles[0];
  const last = candles[candles.length - 1];
  const returnPct = first && last && first.open > 0 ? ((last.close - first.open) / first.open) * 100 : 0;
  const volume = candles.reduce((acc, candle) => acc + candle.volume * candle.close, 0);
  const price = last?.close ?? 0;
  const up = returnPct >= 0;
  const isLive = Boolean(series?.isLive);
  const source = series?.source ?? "—";

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
          <span
            className={cn(
              "rounded-full border px-2 py-0.5 font-mono text-[9px]",
              isLive
                ? "border-emerald-500/25 text-emerald-400"
                : "border-rose-500/25 text-rose-400",
            )}
          >
            {isLive ? "LIVE" : "SIMULADO"}
          </span>
          {candles.length > 0 && (
            <span className={cn("font-mono text-[9px]", up ? "text-emerald-400" : "text-rose-400")}>
              {formatPercent(returnPct)}
            </span>
          )}
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
        <DiveMetric label="Precio" value={price > 0 ? formatUsd(price) : "—"} />
        <DiveMetric label="Retorno ventana" value={candles.length > 0 ? formatPercent(returnPct) : "—"} tone={up ? "up" : "down"} />
        <DiveMetric label="Volumen" value={volume > 0 ? `$${formatCompact(volume)}` : "—"} />
        <DiveMetric label="OBI real" value={orderBook ? `${orderBook.obi >= 0 ? "+" : ""}${orderBook.obi.toFixed(1)}%` : "—"} tone={orderBook && orderBook.obi >= 0 ? "up" : "down"} />
      </div>

      <div className="mb-3 flex h-[420px] min-h-[390px] flex-col rounded-xl border border-white/[0.08] bg-white/[0.012] p-3">
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2 px-1">
          <div className="flex items-center gap-2 font-mono text-[10px] uppercase tracking-wider text-zinc-500">
            <span className="text-zinc-200">{symbol}</span>
            <span className="rounded-full border border-white/10 px-2 py-0.5">{interval}</span>
            <span className={isLive ? "text-emerald-400" : "text-rose-400"}>{source}</span>
          </div>
          <span className="font-mono text-[9px] text-zinc-600">
            {loading ? "Cargando OHLCV…" : `${candles.length} velas · polling 5s`}
          </span>
        </div>
        {seriesError && !isLive && (
          <div className="mb-2 rounded border border-rose-500/20 bg-rose-500/[0.06] px-2 py-1 font-mono text-[9px] text-rose-300">
            Binance no respondió: {seriesError}. Se muestra el fallback simulado marcado.
          </div>
        )}
        <div className="min-h-0 flex-1">
          <StreamingChart candles={candles} interval={interval} live />
        </div>
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        {orderBook ? (
          <OrderBookPanel snapshot={orderBook} />
        ) : (
          <div className="rounded-xl border border-white/10 bg-zinc-950/70 p-4 font-mono text-[10px] text-zinc-500">
            Order Book: {orderBookError ?? "consultando Binance…"}
          </div>
        )}
        <ExecutionTape trades={trades} source={tradesSource} />
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
