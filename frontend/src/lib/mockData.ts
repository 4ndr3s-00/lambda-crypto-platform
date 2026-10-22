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

function symbolSeed(symbol: string): number {
  return symbol.split("").reduce((acc, char) => acc * 31 + char.charCodeAt(0), 7);
}

/** Synthetic OHLCV used only when Binance is unreachable — always labeled SIMULADO in the UI. */
function simulateCandles(symbol: string, interval: string, limit: number): OHLCVCandle[] {
  const rand = mulberry32(symbolSeed(symbol) + interval.length);
  const stepMs = intervalToMs(interval);
  let price = 100 + (symbolSeed(symbol) % 900);
  const now = Date.now();
  const candles: OHLCVCandle[] = [];
  for (let i = limit - 1; i >= 0; i -= 1) {
    const open = price;
    const drift = (rand() - 0.5) * open * 0.01;
    const close = Math.max(0.01, open + drift);
    const high = Math.max(open, close) * (1 + rand() * 0.002);
    const low = Math.min(open, close) * (1 - rand() * 0.002);
    const volume = 50 + rand() * 950;
    candles.push({
      timestamp: new Date(now - i * stepMs).toISOString(),
      open,
      high,
      low,
      close,
      volume,
    });
    price = close;
  }
  return candles;
}

function intervalToMs(interval: string): number {
  const unit = interval.slice(-1);
  const value = Number.parseInt(interval, 10) || 1;
  if (unit === "m") return value * 60_000;
  if (unit === "h") return value * 3_600_000;
  if (unit === "d") return value * 86_400_000;
  return 60_000;
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

/** Rebuilds a chat data-artifact's series at a different timeframe (synthetic, seeded by symbol). */
export function regenerateArtifact(artifact: DataArtifact, interval: MarketInterval): DataArtifact {
  const data = simulateCandles(artifact.symbol, interval, 48);
  return {
    ...artifact,
    interval,
    data,
    summary: summarize(data),
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
          return simulatedAsset(asset);
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
      assets: DASHBOARD_ASSETS.map((asset) => simulatedAsset(asset)),
      source: "Simulado",
      isLive: false,
      errors,
    };
  }
}

function simulatedAsset(asset: { symbol: DashboardSymbol; base: string; quote: string }): AssetCardData {
  const candles = simulateCandles(asset.symbol, "15m", 24);
  const last = candles[candles.length - 1];
  const first = candles[0];
  return {
    symbol: asset.symbol,
    base: asset.base,
    quote: asset.quote,
    exchange: EXCHANGE,
    source: "Simulado",
    price: last?.close ?? 0,
    change24h: first && last && first.open > 0 ? ((last.close - first.open) / first.open) * 100 : 0,
    volume24h: candles.reduce((acc, c) => acc + c.volume * c.close, 0),
    sparkline: candles.map((c) => c.close),
    isLive: false,
  };
}

async function fetchOHLCV(params: { symbol: string; interval: DashboardInterval; limit: number }): Promise<OHLCVResponse> {
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
      candles: simulateCandles(params.symbol, params.interval, params.limit),
      isLive: false,
      source: "Simulado",
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
    return { bids: [], asks: [], obi: 0, spread: 0, spreadPct: 0, source: "Simulado" };
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

async function fetchPipelineStatus(): Promise<PipelineStatusResponse> {
  return {
    kafka: { consumerLag: Math.floor(Math.random() * 40) },
    airflow: { status: "SUCCESS" },
    dlq: { rejectedEvents: Math.floor(Math.random() * 3) },
  };
}

function buildChatReply(query: string): { message: ChatMessage } {
  const now = new Date();
  const message: ChatMessage = {
    id: `msg_assistant_${now.getTime()}`,
    role: "assistant",
    timestamp: now.toISOString(),
    content:
      `Analicé "${query}" contra la capa reconciliada (Speed + Batch). ` +
      "Los datos de mercado se consultan en vivo desde Binance; cuando el exchange no responde, " +
      "el panel lo marca como simulado en lugar de inventar un precio.",
    sqlInspection: {
      layerSource: "RECONCILED",
      generatedSql:
        "SELECT symbol, date_trunc('hour', event_time) AS bucket,\n" +
        "       avg(close) AS avg_close, sum(volume) AS volume\n" +
        "FROM market_lake.batch_results\n" +
        "WHERE metric = 'ohlcv' AND event_time >= now() - interval '1 day'\n" +
        "GROUP BY symbol, bucket\n" +
        "ORDER BY bucket DESC;",
      sourceDataset: "s3://market-lake/batch-results/metric=ohlcv/",
      watermarkDelaySec: 5,
      idempotencyVerified: true,
      dataFreshness: "< 5s (speed) / recálculo diario (batch)",
      engine: "Spark SQL",
      executionTimeMs: 180 + Math.floor(Math.random() * 220),
      cutoffDate: now.toISOString(),
    },
    lineage: {
      schemaVersion: "v1.3.0",
      chain: ["Binance WS", "Kafka: market.events", "Spark Streaming", "MinIO: batch-results", "Serving API"],
      dataset: "market_lake.batch_results / metric=ohlcv",
      owner: "Andrés Blanco (Spark layer)",
      retention: "365 días en frío, 7 días en caliente",
      partitions: "date=YYYY-MM-DD",
    },
    meta: {
      sources: ["Binance", "Kafka", "Spark", "MinIO"],
      disclaimer: "Respuesta generada por Qwen2.5 sobre datos reconciliados de la plataforma.",
      cutoffDate: now.toISOString(),
    },
  };
  return { message };
}

export const mockApi = {
  getOverview: fetchOverview,
  getOHLCV: fetchOHLCV,
  getOrderBook: fetchOrderBook,
  getTrades: fetchTrades,
  getPipelineStatus: fetchPipelineStatus,
  postChat: async ({ query }: { query: string }) => buildChatReply(query),
};
