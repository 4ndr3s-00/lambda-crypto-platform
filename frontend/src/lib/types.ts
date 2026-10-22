export type ViewMode = "CHAT_CONSOLE" | "STREAMING_DASHBOARD";

export type DashboardInterval = "1m" | "5m" | "1h" | "1d";
export type DashboardSymbol = "BTC" | "ETH" | "SOL" | "BNB";

export type MarketInterval = "5m" | "15m" | "1h" | "4h";

export interface OHLCVCandle {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface OHLCVResponse {
  symbol: string;
  interval: string;
  candles: OHLCVCandle[];
  isLive: boolean;
  source: string;
  error?: string;
}

export interface ArtifactSummary {
  totalVolume: number;
  priceChangePercent: number;
  vwap: number;
}

export interface ArtifactPeer {
  symbol: string;
  summary: ArtifactSummary;
}

export interface DataArtifact {
  type: "CANDLESTICK" | "AREA" | "METRIC_TABLE";
  symbol: string;
  interval: string;
  data: OHLCVCandle[];
  summary: ArtifactSummary;
  peers?: ArtifactPeer[];
}

export type LayerSource = "SPEED_LAYER" | "BATCH_LAYER" | "RECONCILED";

export interface SQLInspectionData {
  layerSource: LayerSource;
  generatedSql: string;
  sourceDataset: string;
  watermarkDelaySec: number;
  idempotencyVerified: boolean;
  dataFreshness: string;
  engine: string;
  executionTimeMs: number;
  cutoffDate: string;
}

export interface LineageInfo {
  schemaVersion: string;
  chain: string[];
  dataset: string;
  owner: string;
  retention: string;
  partitions: string;
}

export interface ChatMessageMeta {
  sources?: string[];
  disclaimer: string;
  cutoffDate: string;
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  timestamp: string;
  content: string;
  sqlInspection?: SQLInspectionData;
  dataArtifact?: DataArtifact;
  lineage?: LineageInfo;
  meta?: ChatMessageMeta;
}

export interface MarketTicker {
  symbol: string;
  pair: string;
  price: number;
  changePercent: number;
}

export interface AssetCardData {
  symbol: DashboardSymbol;
  base: string;
  quote: string;
  exchange: string;
  source: string;
  price: number;
  change24h: number;
  volume24h: number;
  sparkline: number[];
  isLive: boolean;
}

export interface MarketOverviewResponse {
  assets: AssetCardData[];
  source: string;
  isLive: boolean;
  errors: string[];
}

export interface OrderBookLevel {
  price: number;
  size: number;
  total: number;
}

export interface OrderBookSnapshot {
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  obi: number;
  spread: number;
  spreadPct: number;
  source: string;
}

export interface ExecutionTrade {
  id: string;
  timestamp: string;
  price: number;
  size: number;
  notional: number;
  side: "buy" | "sell";
  whale: boolean;
}

export interface ExecutionTapeResponse {
  trades: ExecutionTrade[];
  source: string;
}

export interface ArbitrageQuote {
  exchange: string;
  price: number;
  grossSpreadPct: number;
  netSpreadPct: number;
  profitable: boolean;
}

export interface LiquidationEvent {
  id: string;
  timestamp: number;
  amount: number;
  direction: "LONG" | "SHORT";
  price: number;
}

export interface NetflowPoint {
  label: string;
  value: number;
}

export interface OnChainSnapshot {
  netflow: NetflowPoint[];
  openInterest: number;
  longShortRatio: number;
  mvrv: number;
  activeAddresses: number;
}

export interface PipelineStatusResponse {
  kafka: { consumerLag: number };
  airflow: { status: string };
  dlq: { rejectedEvents: number };
}

export interface GlobalTelemetryData {
  latencyMs: number;
  throughput: number;
  fundingRate: number;
  fundingCountdown: string;
  gasGwei: number;
  fearGreed: number;
  fearGreedLabel: string;
}
