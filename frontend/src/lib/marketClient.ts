import type {
  AssetCardData,
  ChatMessage,
  DashboardInterval,
  DashboardSymbol,
  DataArtifact,
  ExecutionTapeResponse,
  ExecutionTrade,
  MarketInterval,
  MarketOverviewResponse,
  OHLCVCandle,
  OHLCVResponse,
  OrderBookSnapshot,
  PipelineStatusResponse,
} from "./types";

export interface QuickPrompt {
  label: string;
  query: string;
}

export const QUICK_PROMPTS: QuickPrompt[] = [
  { label: "¿Cuál fue el retorno de BTC en 24h?", query: "¿Cuál fue el retorno de BTC en las últimas 24 horas?" },
  { label: "Compara BTC vs ETH esta semana", query: "Compara el rendimiento de BTC y ETH en la última semana" },
  { label: "Volumen de ETH por hora", query: "Muéstrame el volumen de ETH por hora en las últimas 24 horas" },
  { label: "Linaje del dataset de velas", query: "¿Cuál es el linaje del dataset de velas OHLCV?" },
];

export const MARKET_INTERVALS: MarketInterval[] = ["5m", "15m", "1h", "4h"];
export const DASHBOARD_INTERVALS: DashboardInterval[] = ["1m", "5m", "1h", "1d"];

export function isMarketInterval(value: string | undefined): value is MarketInterval {
  return !!value && (MARKET_INTERVALS as string[]).includes(value);
}

const BINANCE_REST = "https://api.binance.com/api/v3";
const EXCHANGE = "Binance";

const DASHBOARD_ASSETS: Array<{ symbol: DashboardSymbol; base: string; quote: string; pair: string }> = [
  { symbol: "BTC", base: "BTC", quote: "USDT", pair: "BTCUSDT" },
  { symbol: "ETH", base: "ETH", quote: "USDT", pair: "ETHUSDT" },
  { symbol: "SOL", base: "SOL", quote: "USDT", pair: "SOLUSDT" },
  { symbol: "BNB", base: "BNB", quote: "USDT", pair: "BNBUSDT" },
];

function pairFor(symbol: string): string {
  return DASHBOARD_ASSETS.find((asset) => asset.symbol === symbol)?.pair ?? `${symbol}USDT`;
}

function summarize(candles: OHLCVCandle[]): { totalVolume: number; priceChangePercent: number; vwap: number } {
  const totalVolume = candles.reduce((acc, c) => acc + c.volume, 0);
  const first = candles[0];
  const last = candles[candles.length - 1];
  const priceChangePercent = first && last && first.open > 0 ? ((last.close - first.open) / first.open) * 100 : 0;
  const vwapNumerator = candles.reduce((acc, c) => acc + c.close * c.volume, 0);
  const vwap = totalVolume > 0 ? vwapNumerator / totalVolume : last?.close ?? 0;
  return { totalVolume, priceChangePercent, vwap };
}

/** Refetches a chat data-artifact's series at a different timeframe from Binance. */
export async function regenerateArtifact(artifact: DataArtifact, interval: MarketInterval): Promise<DataArtifact> {
  const payload = await fetchOHLCV({ symbol: artifact.symbol, interval, limit: 48 });
  return {
    ...artifact,
    interval,
    data: payload.candles,
    summary: summarize(payload.candles),
    isLive: payload.isLive,
    error: payload.error,
  };
}

async function fetchJson<T>(url: string, timeoutMs = 4000): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) throw new Error(`Binance respondió ${response.status}`);
    return (await response.json()) as T;
  } finally {
    window.clearTimeout(timer);
  }
}

interface BinanceTicker24h {
  symbol: string;
  lastPrice: string;
  priceChangePercent: string;
  quoteVolume: string;
}

async function fetchOverview(): Promise<MarketOverviewResponse> {
  const errors: string[] = [];
  try {
    const symbols = DASHBOARD_ASSETS.map((asset) => `"${asset.pair}"`).join(",");
    const tickers = await fetchJson<BinanceTicker24h[]>(
      `${BINANCE_REST}/ticker/24hr?symbols=[${symbols}]`,
    );

    const assets: AssetCardData[] = await Promise.all(
      DASHBOARD_ASSETS.map(async (asset) => {
        const ticker = tickers.find((t) => t.symbol === asset.pair);
        if (!ticker) {
          return unavailableAsset(asset, `Binance no devolvió datos para ${asset.pair}`);
        }
        let sparkline: number[] = [];
        try {
          const klines = await fetchJson<unknown[][]>(
            `${BINANCE_REST}/klines?symbol=${asset.pair}&interval=15m&limit=24`,
          );
          sparkline = klines.map((row) => Number(row[4]));
        } catch {
          sparkline = [];
        }
        return {
          symbol: asset.symbol,
          base: asset.base,
          quote: asset.quote,
          exchange: EXCHANGE,
          source: EXCHANGE,
          price: Number(ticker.lastPrice),
          change24h: Number(ticker.priceChangePercent),
          volume24h: Number(ticker.quoteVolume),
          sparkline: sparkline.length > 1 ? sparkline : [Number(ticker.lastPrice)],
          isLive: true,
        };
      }),
    );

    return { assets, source: EXCHANGE, isLive: true, errors };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Binance no disponible";
    errors.push(message);
    return {
      assets: DASHBOARD_ASSETS.map((asset) => unavailableAsset(asset, message)),
      source: "unavailable",
      isLive: false,
      errors,
    };
  }
}

/** Explicit "no real data" state — never a fabricated price. */
function unavailableAsset(
  asset: { symbol: DashboardSymbol; base: string; quote: string },
  error: string,
): AssetCardData {
  return {
    symbol: asset.symbol,
    base: asset.base,
    quote: asset.quote,
    exchange: EXCHANGE,
    source: "unavailable",
    price: 0,
    change24h: 0,
    volume24h: 0,
    sparkline: [],
    isLive: false,
    error,
  };
}

async function fetchOHLCV(params: { symbol: string; interval: DashboardInterval | MarketInterval; limit: number }): Promise<OHLCVResponse> {
  const pair = pairFor(params.symbol);
  try {
    const klines = await fetchJson<unknown[][]>(
      `${BINANCE_REST}/klines?symbol=${pair}&interval=${params.interval}&limit=${params.limit}`,
    );
    const candles: OHLCVCandle[] = klines.map((row) => ({
      timestamp: new Date(Number(row[0])).toISOString(),
      open: Number(row[1]),
      high: Number(row[2]),
      low: Number(row[3]),
      close: Number(row[4]),
      volume: Number(row[5]),
    }));
    return { symbol: params.symbol, interval: params.interval, candles, isLive: true, source: EXCHANGE };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Binance no disponible";
    return {
      symbol: params.symbol,
      interval: params.interval,
      candles: [],
      isLive: false,
      source: "unavailable",
      error: message,
    };
  }
}

async function fetchOrderBook(symbol: string, depth: number): Promise<OrderBookSnapshot> {
  const pair = pairFor(symbol);
  const toLevels = (rows: string[][]): Array<{ price: number; size: number; total: number }> => {
    let total = 0;
    return rows.map(([price, size]) => {
      total += Number(size);
      return { price: Number(price), size: Number(size), total };
    });
  };

  try {
    const book = await fetchJson<{ bids: string[][]; asks: string[][] }>(
      `${BINANCE_REST}/depth?symbol=${pair}&limit=${depth}`,
    );
    const bids = toLevels(book.bids);
    const asks = toLevels(book.asks);
    const bidVolume = bids.reduce((acc, level) => acc + level.size, 0);
    const askVolume = asks.reduce((acc, level) => acc + level.size, 0);
    const obi = bidVolume + askVolume > 0 ? ((bidVolume - askVolume) / (bidVolume + askVolume)) * 100 : 0;
    const bestBid = bids[0]?.price ?? 0;
    const bestAsk = asks[0]?.price ?? 0;
    const spread = Math.max(0, bestAsk - bestBid);
    const spreadPct = bestBid > 0 ? (spread / bestBid) * 100 : 0;
    return { bids, asks, obi, spread, spreadPct, source: EXCHANGE };
  } catch {
    return { bids: [], asks: [], obi: 0, spread: 0, spreadPct: 0, source: "unavailable" };
  }
}

async function fetchTrades(symbol: string, limit: number): Promise<ExecutionTapeResponse> {
  const pair = pairFor(symbol);
  try {
    const rows = await fetchJson<Array<{ id: number; price: string; qty: string; isBuyerMaker: boolean; time: number }>>(
      `${BINANCE_REST}/trades?symbol=${pair}&limit=${limit}`,
    );
    const trades: ExecutionTrade[] = rows.map((row) => {
      const price = Number(row.price);
      const size = Number(row.qty);
      const notional = price * size;
      return {
        id: String(row.id),
        timestamp: new Date(row.time).toISOString(),
        price,
        size,
        notional,
        side: row.isBuyerMaker ? "sell" : "buy",
        whale: notional >= 100_000,
      };
    });
    return { trades: trades.reverse(), source: EXCHANGE };
  } catch {
    return { trades: [], source: "Sin datos" };
  }
}

interface BackendPipelineStatus {
  kafka: { consumer_lag: number };
  spark: { status: string };
  airflow: { status: string };
  minio: { status: string };
  dlq: { rejected_events: number };
  data_source: string;
  freshness_seconds: number;
}

/** Real telemetry from the serving API (same origin). No fabricated numbers on failure. */
async function fetchPipelineStatus(): Promise<PipelineStatusResponse> {
  try {
    const payload = await fetchJson<BackendPipelineStatus>("/api/market/pipeline");
    return {
      kafka: { consumerLag: payload.kafka.consumer_lag },
      spark: { status: payload.spark.status },
      airflow: { status: payload.airflow.status },
      minio: { status: payload.minio.status },
      dlq: { rejectedEvents: payload.dlq.rejected_events },
      dataSource: payload.data_source,
      freshnessSeconds: payload.freshness_seconds,
    };
  } catch {
    return {
      kafka: { consumerLag: 0 },
      spark: { status: "UNKNOWN" },
      airflow: { status: "UNKNOWN" },
      minio: { status: "UNKNOWN" },
      dlq: { rejectedEvents: 0 },
      dataSource: "no_data_available",
      freshnessSeconds: 0,
    };
  }
}

interface BackendChatResponse {
  answer: string;
  sql: string | null;
  provenance: string;
  asset: string | null;
  source: string | null;
  sources: Array<{ source: string; title?: string | null; snippet?: string | null }>;
  cutoff_time: string;
  disclaimer: string;
  latency_ms: number;
}

/** Backend's Provenance is a superset of the chart's LayerSource; map the rest onto RECONCILED. */
function toLayerSource(provenance: string): "SPEED_LAYER" | "BATCH_LAYER" | "RECONCILED" {
  if (provenance === "SPEED_LAYER" || provenance === "BATCH_LAYER") return provenance;
  return "RECONCILED";
}

/** Calls the real serving API (Qwen2.5 + RAG + SQL tools). Fails loudly — never a canned reply. */
async function postChat({ query }: { query: string }): Promise<{ message: ChatMessage }> {
  const response = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message: query }),
  });
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`el asistente no respondió (${response.status}): ${detail}`);
  }
  const payload = (await response.json()) as BackendChatResponse;
  const now = new Date().toISOString();

  const message: ChatMessage = {
    id: `msg_assistant_${Date.now()}`,
    role: "assistant",
    timestamp: now,
    content: payload.answer,
    sqlInspection: payload.sql
      ? {
          layerSource: toLayerSource(payload.provenance),
          generatedSql: payload.sql,
          sourceDataset: payload.source ?? payload.asset ?? "serving layer",
          engine: "Qwen2.5 + Serving API",
          executionTimeMs: Math.round(payload.latency_ms),
          cutoffDate: payload.cutoff_time,
        }
      : undefined,
    meta: {
      sources: payload.sources.map((source) => source.title ?? source.source),
      disclaimer: payload.disclaimer,
      cutoffDate: payload.cutoff_time,
    },
  };
  return { message };
}

export const marketApi = {
  getOverview: fetchOverview,
  getOHLCV: fetchOHLCV,
  getOrderBook: fetchOrderBook,
  getTrades: fetchTrades,
  getPipelineStatus: fetchPipelineStatus,
  postChat,
};
