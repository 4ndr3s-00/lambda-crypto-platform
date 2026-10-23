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
  const [cards, setCards] = useState<AssetCardData[]>([]);
  const [overviewSource, setOverviewSource] = useState("—");
  const [overviewError, setOverviewError] = useState<string | null>(null);
  const [selectedAsset, setSelectedAsset] = useState<DashboardSymbol | null>(null);
  const [pipeline, setPipeline] = useState<PipelineStatusResponse | null>(null);
  const [lagSec, setLagSec] = useState(0.8);
  const hasLiveCards = cards.some((card) => card.isLive);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const overview = await mockApi.getOverview();
        if (cancelled) return;
        setCards(overview.assets);
        setOverviewSource(overview.source);
        setOverviewError(overview.isLive ? null : overview.errors[0] ?? "Exchange no disponible");
      } catch (error) {
        if (cancelled) return;
        setOverviewError(error instanceof Error ? error.message : "Exchange no disponible");
      }
    };
    void load();
    const id = window.setInterval(load, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const load = () => {
      void mockApi.getPipelineStatus().then((status) => {
        if (!cancelled) {
          setPipeline(status);
          setLagSec(status.freshnessSeconds);
        }
      });
    };
    load();
    const id = window.setInterval(load, 8000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

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
              ← Consola AI
            </button>
            <span className="font-mono text-[11px] font-medium tracking-[0.16em] text-zinc-200">
              IQ ANALYZER
              <span className="ml-2 font-normal text-zinc-600">/ CRYPTO STREAMING SUITE</span>
            </span>
          </div>
          <div className="flex items-center gap-2 font-mono text-[9px] uppercase tracking-[0.12em] text-zinc-600">
            <span className={cards.length === 0 ? "text-amber-400" : hasLiveCards ? "text-emerald-400" : "text-rose-400"}>
              {cards.length === 0 ? "CARGANDO" : hasLiveCards ? "LIVE" : "SIN DATOS"}
            </span>
            <span>{overviewSource}</span>
            <span>{selectedAsset ? "DEEP DIVE" : "MULTI-ASSET OVERVIEW"}</span>
          </div>
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
            className="mx-auto w-full max-w-[1200px]"
          >
            <div className="mb-4 flex flex-wrap items-end justify-between gap-2">
              <div>
                <h2 className="font-mono text-[12px] uppercase tracking-[0.16em] text-zinc-300">
                  Vista general de monedas
                </h2>
                <p className="mt-1 font-mono text-[9px] text-zinc-600">
                  Precios reales desde {overviewSource}. Click en una card para abrir el workspace.
                </p>
              </div>
              <span className="font-mono text-[9px] text-zinc-600">
                {cards.length > 0 ? `${cards.length} ACTIVOS` : "CARGANDO"}
              </span>
            </div>

            {overviewError && (
              <div className="mb-4 rounded-xl border border-rose-500/20 bg-rose-500/[0.06] px-3 py-2 font-mono text-[10px] text-rose-300">
                No se pudo consultar Binance: {overviewError}. No se muestran precios inventados.
              </div>
            )}

            <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
              {cards.map((asset) => (
                <AssetCard key={asset.symbol} asset={asset} onSelect={setSelectedAsset} />
              ))}
              {cards.length === 0 && (
                <div className="col-span-full rounded-xl border border-white/10 bg-white/[0.02] p-8 text-center font-mono text-[10px] text-zinc-600">
                  Cargando precios reales de Binance…
                </div>
              )}
            </div>
          </motion.div>
        )}
      </main>

      <PipelineDock status={pipeline} lagSec={lagSec} />
    </div>
  );
}
