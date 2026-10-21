"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import type {
  AssetCardData,
  DashboardInterval,
  DashboardSymbol,
  PipelineStatusResponse,
} from "@/lib/types";
import { mockApi } from "@/lib/mockData";
import { createCardDeck, tickCardDeck } from "@/lib/streamMock";
import { AssetCard } from "./AssetCard";
import { AssetDeepDive } from "./AssetDeepDive";
import { GlobalTelemetry } from "./GlobalTelemetry";
import { PipelineDock } from "./PipelineDock";

interface DashboardViewProps {
  interval: DashboardInterval;
  onIntervalChange: (interval: DashboardInterval) => void;
  onBack: () => void;
}

export function DashboardView({ interval, onIntervalChange, onBack }: DashboardViewProps) {
  const [cards, setCards] = useState<AssetCardData[]>(() => createCardDeck());
  const [selectedAsset, setSelectedAsset] = useState<DashboardSymbol | null>(null);
  const [pipeline, setPipeline] = useState<PipelineStatusResponse | null>(null);
  const [lagSec, setLagSec] = useState(0.8);

  useEffect(() => {
    const id = window.setInterval(() => {
      setCards((previous) => tickCardDeck(previous));
      setLagSec(0.4 + Math.random() * 0.8);
    }, 500);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      void mockApi.getPipelineStatus().then((status) => {
        if (!cancelled) setPipeline(status);
      });
    };
    load();
    const id = window.setInterval(load, 8000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  const selectAsset = (symbol: DashboardSymbol) => {
    setSelectedAsset(symbol);
  };

  return (
    <div className="flex h-screen w-screen flex-col overflow-hidden bg-black text-zinc-100">
      <header className="shrink-0 px-4 pt-4 sm:px-6">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2.5">
            <button
              type="button"
              onClick={onBack}
              className="whitespace-nowrap rounded-full border border-white/10 px-3 py-1.5 font-mono text-[10px] tracking-wide text-zinc-300 transition-colors hover:border-white/25 hover:text-white active:scale-[0.98]"
            >
              ← ✦ Consola AI
            </button>
            <span className="font-mono text-[11px] font-medium tracking-[0.16em] text-zinc-200">
              IQ ANALYZER
              <span className="ml-2 font-normal text-zinc-600">/ CRYPTO STREAMING SUITE</span>
            </span>
          </div>
          <span className="font-mono text-[9px] uppercase tracking-[0.12em] text-zinc-600">
            {selectedAsset ? "DEEP DIVE" : "MULTI-ASSET OVERVIEW"}
          </span>
        </div>
      </header>

      <div className="sticky top-0 z-20 mt-3 border-y border-white/[0.04] bg-black/80 px-4 py-2 backdrop-blur-xl sm:px-6">
        <GlobalTelemetry />
      </div>

      <main className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-4 sm:px-6">
        {selectedAsset ? (
          <AssetDeepDive
            key={selectedAsset}
            symbol={selectedAsset}
            interval={interval}
            onIntervalChange={onIntervalChange}
            onBack={() => setSelectedAsset(null)}
          />
        ) : (
          <motion.div
            key="deck"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ type: "spring", stiffness: 260, damping: 28 }}
            className="mx-auto w-full max-w-[1500px]"
          >
            <div className="mb-4 flex flex-wrap items-end justify-between gap-2">
              <div>
                <h2 className="font-mono text-[12px] uppercase tracking-[0.16em] text-zinc-300">
                  Vista general de monedas
                </h2>
                <p className="mt-1 font-mono text-[9px] text-zinc-600">
                  Click en una card para abrir el workspace analítico en vivo.
                </p>
              </div>
              <span className="font-mono text-[9px] text-zinc-600">4 ACTIVOS · BINANCE / COINGECKO</span>
            </div>

            <motion.div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
              {cards.map((asset) => (
                <AssetCard key={asset.symbol} asset={asset} onSelect={selectAsset} />
              ))}
            </motion.div>
          </motion.div>
        )}
      </main>

      <PipelineDock status={pipeline} lagSec={lagSec} />
    </div>
  );
}
