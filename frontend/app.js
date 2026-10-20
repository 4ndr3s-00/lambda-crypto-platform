"use strict";

/* ------------------------------------------------------------------ */
/* State                                                              */
/* ------------------------------------------------------------------ */

const TIMEFRAMES = ["1m", "5m", "15m", "1h", "1d"];
const QUICK_PROMPTS = [
  "Â¿CuÃ¡l fue el precio mÃ¡ximo de BTC hoy?",
  "Compara el retorno y volumen de BTC vs ETH en 24h",
  "Consultar catÃ¡logo, owner y linaje del dataset OHLCV",
  "Auditar estado del pipeline, lag de Kafka y DLQ",
];

const state = {
  symbol: "BTCUSDT",
  exchange: "binance",
  interval: "15m",
  assets: [],
  candles: [],
  metrics: null,
  pipeline: null,
  chartSize: { w: 0, h: 0 },
  hover: null,
  lastUpdated: null,
  online: false,
  chatBusy: false,
  chartSource: "",
};

const $ = (selector) => document.querySelector(selector);

const els = {
  symbolSelect: $("#symbolSelect"),
  exchangeSelect: $("#exchangeSelect"),
  timeframes: $("#timeframes"),
  kpis: $("#kpis"),
  streamBadge: $("#streamBadge"),
  lastUpdated: $("#lastUpdated"),
  chartSymbol: $("#chartSymbol"),
  chartInterval: $("#chartInterval"),
  chartSource: $("#chartSource"),
  freshness: $("#freshness"),
  chartStatus: $("#chartStatus"),
  chartCanvas: $("#chartCanvas"),
  chartWrap: $("#chartWrap"),
  chartTooltip: $("#chartTooltip"),
  chartEmpty: $("#chartEmpty"),
  pipelineCard: $("#pipelineCard"),
  pipelineToggle: $("#pipelineToggle"),
  pipelineBadge: $("#pipelineBadge"),
  pipelineFreshness: $("#pipelineFreshness"),
  pipelineRows: $("#pipelineRows"),
  chatOpen: $("#chatOpen"),
  chatPanel: $("#chatPanel"),
  chatClose: $("#chatClose"),
  chatMessages: $("#chatMessages"),
  chatQuick: $("#chatQuick"),
  chatForm: $("#chatForm"),
  chatInput: $("#chatInput"),
  chatSend: $("#chatSend"),
  chatStatus: $("#chatStatus"),
  toast: $("#toast"),
};

const ctx = els.chartCanvas.getContext("2d");

/* ------------------------------------------------------------------ */
/* Formatting                                                         */
/* ------------------------------------------------------------------ */

const nf2 = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const nf0 = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

function fmtUsd(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "â€”";
  if (digits === 0) return `$${nf0.format(n)}`;
  return `$${nf2.format(n)}`;
}

function fmtNumber(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "â€”";
  return digits === 0 ? nf0.format(n) : nf2.format(n);
}

function fmtPct(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "â€”";
  return `${n > 0 ? "+" : ""}${n.toFixed(digits)}%`;
}

function fmtCompact(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "â€”";
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return nf0.format(n);
}

function utcClock(value) {
  const date = value instanceof Date ? value : new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return "--:--:--";
  return date.toISOString().slice(11, 19);
}

function utcStamp(value) {
  const date = new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return "â€”";
  return `${date.toISOString().slice(0, 10)} ${date.toISOString().slice(11, 19)} UTC`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function toast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("show");
  window.clearTimeout(toast._timer);
  toast._timer = window.setTimeout(() => els.toast.classList.remove("show"), 3600);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { Accept: "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      detail = payload.detail || detail;
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  return response.json();
}

/* ------------------------------------------------------------------ */
/* Controls                                                           */
/* ------------------------------------------------------------------ */

function populateSymbols() {
  const assets = state.assets.length
    ? state.assets
    : [
        { symbol: "BTCUSDT", base: "BTC", quote: "USDT" },
        { symbol: "ETHUSDT", base: "ETH", quote: "USDT" },
        { symbol: "SOLUSDT", base: "SOL", quote: "USDT" },
      ];
  els.symbolSelect.innerHTML = assets
    .map(
      (asset) =>
        `<option value="${escapeHtml(asset.symbol)}">${escapeHtml(asset.base)} / ${escapeHtml(asset.quote)}</option>`,
    )
    .join("");
  if (!assets.some((asset) => asset.symbol === state.symbol)) {
    state.symbol = assets[0]?.symbol || "BTCUSDT";
  }
  els.symbolSelect.value = state.symbol;
}

function renderTimeframes() {
  els.timeframes.innerHTML = TIMEFRAMES.map(
    (interval) =>
      `<button type="button" class="tf ${interval === state.interval ? "active" : ""}" data-interval="${interval}">${interval}</button>`,
  ).join("");
}

function bindControls() {
  els.symbolSelect.addEventListener("change", () => {
    state.symbol = els.symbolSelect.value;
    state.hover = null;
    void refresh();
  });
  els.exchangeSelect.addEventListener("change", () => {
    state.exchange = els.exchangeSelect.value;
    updateChartSource();
    void refresh();
  });
  els.timeframes.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-interval]");
    if (!button) return;
    state.interval = button.dataset.interval;
    state.hover = null;
    renderTimeframes();
    void refresh();
  });
  els.pipelineToggle.addEventListener("click", () => {
    const expanded = els.pipelineToggle.getAttribute("aria-expanded") === "true";
    els.pipelineToggle.setAttribute("aria-expanded", String(!expanded));
    els.pipelineRows.hidden = expanded;
  });
  els.chatOpen.addEventListener("click", openChat);
  els.chatClose.addEventListener("click", closeChat);
  els.chatForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const message = els.chatInput.value.trim();
    if (!message || state.chatBusy) return;
    els.chatInput.value = "";
    els.chatInput.style.height = "auto";
    void sendChat(message);
  });
  els.chatInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      els.chatForm.requestSubmit();
    }
  });
  els.chatInput.addEventListener("input", () => {
    els.chatInput.style.height = "auto";
    els.chatInput.style.height = `${Math.min(els.chatInput.scrollHeight, 120)}px`;
  });
}

function updateChartSource() {
  els.chartSource.textContent = `OHLCV Â· ${state.exchange === "binance" ? "Binance WebSocket" : "CoinGecko"}`;
}

/* ------------------------------------------------------------------ */
/* Market data                                                        */
/* ------------------------------------------------------------------ */

async function loadAssets() {
  try {
    const payload = await api("/api/market/assets");
    state.assets = payload.assets || [];
  } catch {
    state.assets = [];
  }
  populateSymbols();
}

async function loadMarket() {
  const query = new URLSearchParams({
    symbol: state.symbol,
    interval: state.interval,
    limit: "96",
  });
  els.chartStatus.textContent = "Actualizandoâ€¦";

  const [ohlcv, metrics] = await Promise.all([
    api(`/api/market/ohlcv?${query.toString()}`),
    api(`/api/market/metrics?symbol=${encodeURIComponent(state.symbol)}&window=24h`),
  ]);

  state.candles = ohlcv.candles || [];
  state.metrics = metrics;
  state.chartSource = ohlcv.source || "";
  state.lastUpdated = new Date();

  if (state.hover && state.hover.index >= state.candles.length) state.hover = null;
  updateMarketUi();
}

function updateMarketUi() {
  const last = state.candles[state.candles.length - 1];
  const metrics = state.metrics || {};
  const current = last ? last.close : metrics.current_price;
  const return24 = Number(metrics.return_24h || 0);
  const volume = Number(metrics.volume_24h || 0);
  const vwap = Number(metrics.vwap || 0);
  const freshness = Number(metrics.freshness_seconds ?? 0);

  els.chartSymbol.textContent = state.symbol;
  els.chartInterval.textContent = state.interval;
  updateChartSource();
  els.chartStatus.textContent = `${state.candles.length} velas Â· polling 5s`;
  els.freshness.textContent = `Frescura ${freshness.toFixed(1)}s`;
  els.freshness.className = freshness > 60 ? "badge-bad" : "muted";
  els.chartEmpty.classList.toggle("hidden", state.candles.length > 0);
  els.lastUpdated.textContent = `Ãšltima actualizaciÃ³n: ${utcClock(state.lastUpdated)} UTC`;
  els.streamBadge.textContent = "â— STREAMING ACTIVO";
  els.streamBadge.className = "badge badge-ok";

  els.kpis.innerHTML = `
    <article class="kpi">
      <span class="kpi-label">Precio actual</span>
      <span class="kpi-value">${fmtUsd(current)}</span>
      <span class="kpi-hint">${last ? `Ãšltimo close ${fmtUsd(last.close)}` : "Sin datos"}</span>
    </article>
    <article class="kpi">
      <span class="kpi-label">Retorno 24h</span>
      <span class="kpi-value ${return24 >= 0 ? "up" : "down"}">${fmtPct(return24)}</span>
      <span class="kpi-hint">Ventana mÃ³vil 1h</span>
    </article>
    <article class="kpi">
      <span class="kpi-label">Volumen 24h</span>
      <span class="kpi-value">$${fmtCompact(volume)}</span>
      <span class="kpi-hint">USD nocional</span>
    </article>
    <article class="kpi">
      <span class="kpi-label">VWAP</span>
      <span class="kpi-value">${fmtUsd(vwap)}</span>
      <span class="kpi-hint">Ponderado por volumen</span>
    </article>
  `;

  drawChart();
}

async function loadPipeline() {
  try {
    state.pipeline = await api("/api/market/pipeline");
    renderPipeline();
  } catch {
    els.pipelineBadge.textContent = "â— SIN TELEMETRÃA";
    els.pipelineBadge.className = "badge badge-bad";
  }
}

function renderPipeline() {
  const p = state.pipeline;
  if (!p) return;
  const lag = Number(p.kafka?.consumer_lag ?? 0);
  const freshness = Number(p.freshness_seconds ?? p.kafka?.freshness_seconds ?? 0);
  const alert = lag > 60 || freshness > 60;

  els.pipelineBadge.textContent = alert ? "â— ALERTA" : "â— OPERATIVO";
  els.pipelineBadge.className = `badge ${alert ? "badge-bad" : "badge-ok"}`;
  els.pipelineFreshness.textContent = `Frescura ${freshness.toFixed(1)}s`;
  els.pipelineFreshness.className = alert ? "badge-bad" : "muted";

  const rows = [
    {
      label: "Kafka Ingestion",
      value: `ACTIVO (Topics: ${(p.kafka?.topics || ["market.events"]).join(", ")} Â· Lag: ${lag})`,
      alert: lag > 60,
    },
    {
      label: "Spark Streaming",
      value: `PROCESANDO (Ventana ${p.spark?.window || "2s"} Â· Watermark ${p.spark?.watermark || "5s"})`,
      alert: p.spark?.status !== "PROCESSING",
    },
    {
      label: "Airflow Batch",
      value: `DAG ${p.airflow?.dag_id || "batch_recompute"} ${p.airflow?.status || "SUCCESS"}`,
      alert: p.airflow?.status !== "SUCCESS",
    },
    {
      label: "MinIO Data Lake",
      value: `PARQUET ${p.minio?.status || "SYNCED"} (${p.minio?.path || "s3://market-lake/"})`,
      alert: p.minio?.status !== "SYNCED",
    },
    {
      label: "DLQ Isolation",
      value: `${p.dlq?.rejected_events ?? 0} ERRORES / RECHAZOS`,
      alert: Number(p.dlq?.rejected_events ?? 0) > 0,
    },
  ];

  els.pipelineRows.innerHTML = rows
    .map(
      (row) => `
        <div class="pipeline-row ${row.alert ? "alert" : ""}">
          <span class="pr-label">${escapeHtml(row.label)}</span>
          <span class="pr-value"><span class="dot">â—</span> ${escapeHtml(row.value)}</span>
        </div>
      `,
    )
    .join("");
}

async function refresh() {
  try {
    await Promise.all([loadMarket(), loadPipeline()]);
    state.online = true;
    els.streamBadge.textContent = "â— STREAMING ACTIVO";
    els.streamBadge.className = "badge badge-ok";
  } catch (error) {
    state.online = false;
    els.streamBadge.textContent = "â— SIN CONEXIÃ“N";
    els.streamBadge.className = "badge badge-bad";
    els.chartStatus.textContent = "Error de feed";
    toast(`No se pudo actualizar el mercado: ${error.message}`);
  }
}

/* ------------------------------------------------------------------ */
/* Chart                                                              */
/* ------------------------------------------------------------------ */

function resizeChart() {
  const rect = els.chartWrap.getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const width = Math.max(1, Math.floor(rect.width));
  const height = Math.max(1, Math.floor(rect.height));
  els.chartCanvas.width = Math.floor(width * dpr);
  els.chartCanvas.height = Math.floor(height * dpr);
  els.chartCanvas.style.width = `${width}px`;
  els.chartCanvas.style.height = `${height}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  state.chartSize = { w: width, h: height };
  drawChart();
}

function candleBounds(candles) {
  let min = Infinity;
  let max = -Infinity;
  for (const candle of candles) {
    min = Math.min(min, candle.low);
    max = Math.max(max, candle.high);
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 1 };
  const pad = (max - min) * 0.06 || max * 0.02 || 1;
  return { min: min - pad, max: max + pad };
}

function drawChart() {
  const { w, h } = state.chartSize;
  if (!w || !h) return;
  ctx.clearRect(0, 0, w, h);
  const candles = state.candles;
  if (!candles.length) return;

  const left = 12;
  const right = w >= 700 ? 76 : 58;
  const top = 18;
  const bottom = h - 24;
  const priceBottom = Math.max(top + 70, bottom - 76);
  const volTop = priceBottom + 16;
  const volBottom = Math.max(volTop + 18, bottom - 8);
  const plotWidth = Math.max(1, w - left - right);
  const priceHeight = Math.max(1, priceBottom - top);
  const volHeight = Math.max(1, volBottom - volTop);
  const bounds = candleBounds(candles);
  const span = bounds.max - bounds.min || 1;
  const step = plotWidth / candles.length;
  const bodyWidth = Math.max(1, step * 0.58);
  const maxVolume = Math.max(...candles.map((candle) => candle.volume), 1);

  const xFor = (index) => left + step * index + step / 2;
  const yFor = (price) => top + ((bounds.max - price) / span) * priceHeight;

  ctx.font = "9px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
  ctx.textBaseline = "middle";

  // Grid + price labels
  ctx.strokeStyle = "rgba(255,255,255,0.055)";
  ctx.fillStyle = "rgba(255,255,255,0.34)";
  ctx.lineWidth = 1;
  for (let index = 0; index < 5; index += 1) {
    const price = bounds.max - (span / 4) * index;
    const y = yFor(price);
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(w - right, y);
    ctx.stroke();
    ctx.fillText(fmtNumber(price, price >= 1000 ? 0 : 2), w - right + 8, y);
  }

  // Volume
  ctx.fillStyle = "rgba(255,255,255,0.26)";
  ctx.fillText("VOL", left, volTop - 5);
  candles.forEach((candle, index) => {
    const height = Math.max(1, (candle.volume / maxVolume) * volHeight);
    ctx.fillStyle = candle.close >= candle.open ? "rgba(16,185,129,0.24)" : "rgba(244,63,94,0.24)";
    ctx.fillRect(xFor(index) - bodyWidth / 2, volBottom - height, bodyWidth, height);
  });

  // Candles
  candles.forEach((candle, index) => {
    const x = xFor(index);
    const bullish = candle.close >= candle.open;
    const color = bullish ? "#10b981" : "#f43f5e";
    const yOpen = yFor(candle.open);
    const yClose = yFor(candle.close);
    const bodyTop = Math.min(yOpen, yClose);
    const bodyHeight = Math.max(1, Math.abs(yClose - yOpen));

    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, yFor(candle.high));
    ctx.lineTo(x, yFor(candle.low));
    ctx.stroke();

    ctx.fillStyle = color;
    ctx.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
  });

  // Last price
  const last = candles[candles.length - 1];
  const lastY = yFor(last.close);
  ctx.strokeStyle = "rgba(255,255,255,0.18)";
  ctx.setLineDash([2, 4]);
  ctx.beginPath();
  ctx.moveTo(left, lastY);
  ctx.lineTo(w - right, lastY);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = "#ffffff";
  ctx.beginPath();
  ctx.arc(xFor(candles.length - 1), lastY, 2.4, 0, Math.PI * 2);
  ctx.fill();

  // Crosshair
  if (state.hover && candles[state.hover.index]) {
    const hovered = candles[state.hover.index];
    const x = xFor(state.hover.index);
    const y = yFor(hovered.close);
    ctx.strokeStyle = "rgba(255,255,255,0.24)";
    ctx.setLineDash([2, 3]);
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, volBottom);
    ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(w - right, y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "#ffffff";
    ctx.beginPath();
    ctx.arc(x, y, 2.4, 0, Math.PI * 2);
    ctx.fill();
  }

  // Time axis
  ctx.fillStyle = "rgba(255,255,255,0.3)";
  ctx.textBaseline = "alphabetic";
  ctx.fillText(axisLabel(candles[0].timestamp), left, h - 8);
  ctx.textAlign = "right";
  ctx.fillText(axisLabel(candles[candles.length - 1].timestamp), w - right, h - 8);
  ctx.textAlign = "left";
}

function axisLabel(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "";
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const mm = String(date.getUTCMinutes()).padStart(2, "0");
  return state.interval === "1d" ? `${month}-${day}` : `${month}-${day} ${hh}:${mm}`;
}

function handleChartMove(event) {
  if (!state.candles.length) return;
  const rect = els.chartCanvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const left = 12;
  const right = rect.width >= 700 ? 76 : 58;
  const plotWidth = Math.max(1, rect.width - left - right);
  const step = plotWidth / state.candles.length;
  const index = Math.max(0, Math.min(state.candles.length - 1, Math.round((x - left - step / 2) / step)));
  state.hover = { index, x, y };
  drawChart();
  renderTooltip();
}

function renderTooltip() {
  if (!state.hover || !state.candles[state.hover.index]) {
    els.chartTooltip.classList.add("hidden");
    return;
  }
  const candle = state.candles[state.hover.index];
  const up = candle.close >= candle.open;
  const width = 196;
  const height = 150;
  const gap = 14;
  const rect = els.chartCanvas.getBoundingClientRect();
  let left = state.hover.x + gap;
  if (left + width > rect.width - 6) left = state.hover.x - gap - width;
  left = Math.max(6, Math.min(left, rect.width - width - 6));
  const top = Math.max(6, Math.min(state.hover.y - height / 2, rect.height - height - 6));

  els.chartTooltip.style.left = `${left}px`;
  els.chartTooltip.style.top = `${top}px`;
  els.chartTooltip.innerHTML = `
    <div class="tt-time">${utcStamp(candle.timestamp)}</div>
    <div class="tt-grid">
      <span>O</span><span>${fmtNumber(candle.open, 2)}</span>
      <span>H</span><span>${fmtNumber(candle.high, 2)}</span>
      <span>L</span><span>${fmtNumber(candle.low, 2)}</span>
      <span>C</span><span class="${up ? "up" : "down"}">${fmtNumber(candle.close, 2)}</span>
      <span>V</span><span>${fmtCompact(candle.volume)}</span>
    </div>
  `;
  els.chartTooltip.classList.remove("hidden");
}

function clearChartHover() {
  state.hover = null;
  els.chartTooltip.classList.add("hidden");
  drawChart();
}

/* ------------------------------------------------------------------ */
/* Chat                                                               */
/* ------------------------------------------------------------------ */

function renderMarkdown(text) {
  let html = escapeHtml(text);
  html = html.replace(/```([\s\S]*?)```/g, (_, code) => `<pre>${code.trim()}</pre>`);
  html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/^###\s+(.*)$/gm, "<strong>$1</strong>");
  html = html.replace(/^##\s+(.*)$/gm, "<strong>$1</strong>");
  html = html.replace(/^#\s+(.*)$/gm, "<strong>$1</strong>");
  html = html.replace(/(^|\n)[-*]\s+(.*?)(?=\n|$)/g, "$1â€¢ $2");
  html = html.replace(/\n/g, "<br>");
  return html;
}

function appendMessage(role, content) {
  const wrapper = document.createElement("div");
  wrapper.className = `msg ${role}`;
  const roleLabel = document.createElement("span");
  roleLabel.className = "msg-role";
  roleLabel.textContent = role === "user" ? "TÃº" : "IQ Analyzer";
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  if (role === "user") {
    bubble.textContent = content;
  } else {
    bubble.innerHTML = renderMarkdown(content);
  }
  wrapper.append(roleLabel, bubble);
  els.chatMessages.append(wrapper);
  scrollChat();
  return bubble;
}

function appendTyping() {
  const wrapper = document.createElement("div");
  wrapper.className = "msg assistant";
  wrapper.innerHTML = `
    <span class="msg-role">IQ Analyzer</span>
    <div class="bubble">
      <span class="typing">Qwen2.5 sintetizando <i></i><i></i><i></i></span>
    </div>
  `;
  els.chatMessages.append(wrapper);
  scrollChat();
  return wrapper;
}

function scrollChat() {
  els.chatMessages.scrollTop = els.chatMessages.scrollHeight;
}

function renderAssistantMeta(bubble, meta) {
  if (!meta) return;
  const answerText = bubble.textContent || "";
  const chips = [];
  if (meta.asset) chips.push(`<span class="badge-chip green">${escapeHtml(meta.asset)}</span>`);
  if (meta.provenance) chips.push(`<span class="badge-chip">${escapeHtml(meta.provenance)}</span>`);
  if (meta.source) chips.push(`<span class="badge-chip">${escapeHtml(meta.source)}</span>`);
  if (meta.cutoff_time) chips.push(`<span class="badge-chip amber">corte ${escapeHtml(utcStamp(meta.cutoff_time))}</span>`);
  for (const source of meta.sources || []) {
    const label = source.title || source.source;
    const title = source.snippet ? ` title="${escapeHtml(source.snippet)}"` : "";
    chips.push(`<span class="badge-chip"${title}>${escapeHtml(label)}</span>`);
  }
  if (chips.length) {
    const metaRow = document.createElement("div");
    metaRow.className = "answer-meta";
    metaRow.innerHTML = chips.join("");
    bubble.append(metaRow);
  }
  if (meta.sql) {
    const details = document.createElement("details");
    details.className = "sql-details";
    details.innerHTML = `<summary>SQL de trazabilidad</summary><pre>${escapeHtml(meta.sql)}</pre>`;
    bubble.append(details);
  }
  const disclaimer = document.createElement("div");
  disclaimer.className = "disclaimer";
  disclaimer.textContent =
    meta.disclaimer || "InformaciÃ³n exclusivamente referencial. No constituye asesoramiento financiero.";
  if (!answerText.toLowerCase().includes(disclaimer.textContent.toLowerCase())) {
    bubble.append(disclaimer);
  }
}

async function sendChat(message) {
  if (state.chatBusy) return;
  state.chatBusy = true;
  els.chatSend.disabled = true;
  els.chatStatus.textContent = "Consultando Qwen2.5â€¦";
  appendMessage("user", message);
  const typing = appendTyping();

  try {
    const meta = await streamChat(message, typing);
    els.chatStatus.textContent = "Listo";
    return meta;
  } catch {
    try {
      const payload = await api("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message, symbol: state.symbol, interval: state.interval }),
      });
      const bubble = typing.querySelector(".bubble");
      bubble.innerHTML = renderMarkdown(payload.answer);
      renderAssistantMeta(bubble, payload);
      scrollChat();
      els.chatStatus.textContent = "Listo";
    } catch (error) {
      typing.querySelector(".bubble").innerHTML = `<span class="badge-bad">Error: ${escapeHtml(error.message)}</span>`;
      els.chatStatus.textContent = "Error";
      toast(`Chat no disponible: ${error.message}`);
    }
  } finally {
    state.chatBusy = false;
    els.chatSend.disabled = false;
    scrollChat();
  }
}

function streamChat(message, typing) {
  return new Promise((resolve, reject) => {
    const query = new URLSearchParams({
      message,
      symbol: state.symbol,
      interval: state.interval,
    });
    const source = new EventSource(`/api/chat/stream?${query.toString()}`);
    let meta = null;
    let answer = "";
    let settled = false;

    const finish = (value, error) => {
      if (settled) return;
      settled = true;
      source.close();
      if (error) reject(error);
      else resolve(value);
    };

    source.addEventListener("meta", (event) => {
      try {
        meta = JSON.parse(event.data);
      } catch {
        meta = null;
      }
    });

    source.addEventListener("token", (event) => {
      try {
        const payload = JSON.parse(event.data);
        answer += payload.text || "";
        const bubble = typing.querySelector(".bubble");
        bubble.innerHTML = renderMarkdown(answer);
        scrollChat();
      } catch {
        /* ignore malformed frame */
      }
    });

    source.addEventListener("done", (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (!answer && payload.answer) answer = payload.answer;
      } catch {
        /* ignore */
      }
      const bubble = typing.querySelector(".bubble");
      bubble.innerHTML = renderMarkdown(answer);
      renderAssistantMeta(bubble, meta);
      scrollChat();
      finish(meta);
    });

    source.onerror = () => {
      if (!settled) {
        source.close();
        settled = true;
        reject(new Error("SSE no disponible"));
      }
    };

    window.setTimeout(() => {
      if (!settled) finish(null, new Error("timeout del asistente"));
    }, 90000);
  });
}

function openChat() {
  els.chatPanel.classList.add("open");
  els.chatPanel.setAttribute("aria-hidden", "false");
  els.chatOpen.classList.add("hidden");
  window.setTimeout(() => els.chatInput.focus(), 250);
}

function closeChat() {
  els.chatPanel.classList.remove("open");
  els.chatPanel.setAttribute("aria-hidden", "true");
  els.chatOpen.classList.remove("hidden");
}

function renderQuickPrompts() {
  els.chatQuick.innerHTML = QUICK_PROMPTS.map(
    (prompt) => `<button type="button" class="quick-pill">${escapeHtml(prompt)}</button>`,
  ).join("");
  els.chatQuick.addEventListener("click", (event) => {
    const button = event.target.closest(".quick-pill");
    if (!button) return;
    els.chatInput.value = button.textContent;
    els.chatInput.focus();
  });
}

/* ------------------------------------------------------------------ */
/* Boot                                                               */
/* ------------------------------------------------------------------ */

function bindChart() {
  els.chartCanvas.addEventListener("pointermove", handleChartMove);
  els.chartCanvas.addEventListener("pointerleave", clearChartHover);
  if (typeof ResizeObserver !== "undefined") {
    new ResizeObserver(resizeChart).observe(els.chartWrap);
  }
  window.addEventListener("resize", resizeChart);
}

async function boot() {
  bindControls();
  bindChart();
  renderTimeframes();
  renderQuickPrompts();
  updateChartSource();
  await loadAssets();
  resizeChart();
  await refresh();
  window.setInterval(refresh, 5000);
}

boot().catch((error) => {
  toast(`No se pudo inicializar el dashboard: ${error.message}`);
  els.chartStatus.textContent = "Error de inicializaciÃ³n";
});
