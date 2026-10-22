import type { ArbitrageQuote, GlobalTelemetryData, LiquidationEvent, OnChainSnapshot } from "./types";

/** Deterministic PRNG so a given seed always reproduces the same simulated frame. */
function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const EXCHANGES = ["Binance", "Coinbase", "Kraken", "OKX", "Bybit"];

export function createArbitrage(referencePrice: number, seed: number): ArbitrageQuote[] {
  const rand = mulberry32(Math.floor(seed));
  const base = referencePrice > 0 ? referencePrice : 100;
  return EXCHANGES.map((exchange) => {
    const spreadPct = (rand() - 0.3) * 0.18;
    const price = base * (1 + spreadPct / 100);
    const grossSpreadPct = Math.abs(spreadPct);
    const netSpreadPct = grossSpreadPct - 0.06;
    return {
      exchange,
      price,
      grossSpreadPct,
      netSpreadPct,
      profitable: netSpreadPct > 0,
    };
  });
}

export function createLiquidations(symbol: string, referencePrice: number, seed: number): LiquidationEvent[] {
  const rand = mulberry32(Math.floor(seed));
  const base = referencePrice > 0 ? referencePrice : 100;
  const count = 1 + Math.floor(rand() * 3);
  return Array.from({ length: count }, (_, index) => ({
    id: `liq_${symbol}_${Math.floor(seed)}_${index}`,
    timestamp: Date.now() - index * 1500,
    amount: 5_000 + rand() * 495_000,
    direction: rand() > 0.5 ? "LONG" : "SHORT",
    price: base * (1 + (rand() - 0.5) * 0.01),
  }));
}

export function createOnChain(seed: number): OnChainSnapshot {
  const rand = mulberry32(Math.floor(seed));
  const netflow = Array.from({ length: 12 }, (_, index) => ({
    label: `${index * 2}h`,
    value: (rand() - 0.5) * 2_000,
  }));
  return {
    netflow,
    openInterest: 1_200_000_000 + rand() * 400_000_000,
    longShortRatio: 0.8 + rand() * 0.6,
    mvrv: 1.1 + rand() * 1.2,
    activeAddresses: 450_000 + rand() * 120_000,
  };
}

export function createTelemetry(seed: number = Date.now()): GlobalTelemetryData {
  const rand = mulberry32(Math.floor(seed));
  const fearGreed = Math.floor(20 + rand() * 60);
  return {
    latencyMs: Math.floor(18 + rand() * 90),
    throughput: Math.floor(1200 + rand() * 3800),
    fundingRate: (rand() - 0.5) * 0.08,
    fundingCountdown: fundingCountdown(),
    gasGwei: Math.floor(8 + rand() * 40),
    fearGreed,
    fearGreedLabel: fearGreed >= 60 ? "Greed" : fearGreed >= 40 ? "Neutral" : "Fear",
  };
}

/** Minutes/seconds left until the next perpetual-futures funding event (every 8h, UTC). */
export function fundingCountdown(): string {
  const now = new Date();
  const msIntoDay = now.getUTCHours() * 3_600_000 + now.getUTCMinutes() * 60_000 + now.getUTCSeconds() * 1000;
  const period = 8 * 3_600_000;
  const remaining = period - (msIntoDay % period);
  const minutes = Math.floor(remaining / 60_000);
  const seconds = Math.floor((remaining % 60_000) / 1000);
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}
