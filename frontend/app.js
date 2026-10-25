const PAIRS = ["EUR/USD", "USD/JPY", "EUR/JPY"];
const bars = new Map(PAIRS.map((pair) => [pair, []]));
const liveTicks = new Map();
const predictions = new Map();
let selectedPair = PAIRS[0];
let socket;
let retryDelay = 1000;
let ticksThisMinute = 0;
let chartResizeObserver;

const $ = (query) => document.querySelector(query);
const digits = (pair) => pair.endsWith("JPY") ? 3 : 5;
const formatPrice = (pair, price) => Number.isFinite(Number(price))
  ? Number(price).toFixed(digits(pair))
  : "—";
const pairSlug = (pair) => pair.replace("/", "");
const selectedBars = () => bars.get(selectedPair);
const selectedTick = () => liveTicks.get(selectedPair);

function socketUrl() {
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  return `${scheme}//${location.host}/ws/market`;
}
function connect() {
  socket = new WebSocket(socketUrl());
  socket.addEventListener("open", () => {
    retryDelay = 1000;
    setConnection("connected");
  });
  socket.addEventListener("message", ({ data }) => {
    try {
      onMessage(JSON.parse(data));
    } catch (error) {
      console.error("Invalid forex stream message", error);
    }
  });
  socket.addEventListener("close", () => {
    setConnection("reconnecting");
    window.setTimeout(connect, retryDelay);
    retryDelay = Math.min(retryDelay * 2, 30000);
  });
  socket.addEventListener("error", () => socket.close());
}
function onMessage(message) {
  if (message.type === "snapshot") {
    setConnection(message.status);
    PAIRS.forEach((pair) => {
      bars.set(pair, message.candles?.[pair] || []);
      const tick = message.ticks?.find((item) => item.pair === pair);
      if (tick) {
        liveTicks.set(pair, tick);
        updateLiveBar(pair, tick);
      }
      if (message.predictions?.[pair]) predictions.set(pair, message.predictions[pair]);
    });
    if (message.last_update) $("#last-updated").textContent = new Date(message.last_update).toLocaleTimeString();
    updateInfrastructure(message.status, message);
    render();
  } else if (message.type === "status") {
    setConnection(message.status);
    updateInfrastructure(message.status, {});
  } else if (message.type === "tick" && PAIRS.includes(message.tick?.pair)) {
    liveTicks.set(message.tick.pair, message.tick);
    ticksThisMinute += 1;
    $("#tick-count").textContent = ticksThisMinute;
    $("#last-updated").textContent = new Date(message.tick.timestamp).toLocaleTimeString();
    updateLiveBar(message.tick.pair, message.tick);
    renderPairs();
    if (message.tick.pair === selectedPair) {
      updatePriceAndOhlc();
      drawChart();
    }
  } else if (message.type === "candle" && PAIRS.includes(message.candle?.pair)) {
    appendCandle(message.candle);
    $("#kafka-dot").className = "good";
    $("#pipeline-state").textContent = "SPARK LIVE";
    if (message.candle.pair === selectedPair) {
      renderAnalysis();
      drawChart();
    }
  } else if (message.type === "prediction" && PAIRS.includes(message.prediction?.pair)) {
    predictions.set(message.prediction.pair, message.prediction);
    renderPairs();
    if (message.prediction.pair === selectedPair) {
      renderPrediction();
      drawChart();
    }
  }
}
function setConnection(status) {
  const el = $("#connection");
  el.className = `connection ${status}`;
  const labels = {
    connected: "CONNECTED",
    live: "LIVE",
    missing_credentials: "PRACTICE ACCOUNT REQUIRED",
    pipeline_unavailable: "PIPELINE OFFLINE",
    provider_error: "PROVIDER ERROR",
    reconnecting: "RECONNECTING",
    connecting: "CONNECTING",
  };
  $("#connection-label").textContent = labels[status] || String(status).toUpperCase();
}
function updateInfrastructure(status, snapshot) {
  const providerReady = ["live", "connected"].includes(status);
  $("#provider-dot").className = providerReady ? "good" : status === "provider_error" ? "bad" : "";
  $("#provider-state").textContent = status === "missing_credentials" ? "ADD TOKEN" : providerReady ? "LIVE" : "WAITING";
  const pipelineReady = Boolean(snapshot.kafka);
  $("#kafka-dot").className = pipelineReady ? "good" : "";
  $("#pipeline-state").textContent = snapshot.last_candle
    ? "SPARK LIVE"
    : pipelineReady ? "KAFKA LIVE" : "STARTING";
  const readyModels = [...predictions.values()].filter((item) => item.training_samples >= 30).length;
  $("#model-dot").className = readyModels === PAIRS.length ? "good" : "";
  $("#model-state").textContent = readyModels === 0
    ? "WARMING"
    : `${readyModels}/${PAIRS.length} READY`;
  $("#feed-pill").className = `feed-pill ${providerReady ? "real" : ""}`;
}
function bucketStart(timestamp) {
  const date = new Date(timestamp);
  return Math.floor(date.getTime() / 60000) * 60000;
}
function updateLiveBar(pair, tick) {
  const pairBars = bars.get(pair);
  const start = bucketStart(tick.timestamp);
  const last = pairBars[pairBars.length - 1];
  if (last && new Date(last.timestamp).getTime() === start && last.provisional) {
    if (last.last_tick_timestamp === tick.timestamp) return;
    last.high = Math.max(Number(last.high), tick.price);
    last.low = Math.min(Number(last.low), tick.price);
    last.close = tick.price;
    last.tick_count = Number(last.tick_count || 0) + 1;
    last.last_tick_timestamp = tick.timestamp;
  } else if (last && new Date(last.timestamp).getTime() === start) {
    return;
  } else {
    pairBars.push({
      pair,
      timestamp: new Date(start).toISOString(),
      open: tick.price,
      high: tick.price,
      low: tick.price,
      close: tick.price,
      tick_count: 1,
      last_tick_timestamp: tick.timestamp,
      provisional: true,
    });
  }
  if (pairBars.length > 300) pairBars.splice(0, pairBars.length - 300);
}
function appendCandle(candle) {
  const pairBars = bars.get(candle.pair);
  const timestamp = new Date(candle.timestamp).getTime();
  const existing = pairBars.findIndex((item) => new Date(item.timestamp).getTime() === timestamp);
  const clean = { ...candle, provisional: false };
  if (existing >= 0) pairBars[existing] = clean;
  else pairBars.push(clean);
  pairBars.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  if (pairBars.length > 300) pairBars.splice(0, pairBars.length - 300);
}
function choosePair(pair) {
  if (!PAIRS.includes(pair)) return;
  selectedPair = pair;
  renderPairs();
  render();
}
function priceChange(pair) {
  const history = bars.get(pair);
  if (history.length < 2) return null;
  const first = Number(history[Math.max(0, history.length - 20)].close);
  const last = Number(history.at(-1).close);
  return (last / first - 1) * 100;
}
function renderPairs() {
  $("#pair-grid").innerHTML = PAIRS.map((pair) => {
    const tick = liveTicks.get(pair);
    const prediction = predictions.get(pair);
    const change = priceChange(pair);
    const direction = prediction?.status === "active" ? prediction.direction : "WARMING";
    const dClass = direction === "UP" ? "up" : direction === "DOWN" ? "down" : "neutral";
    return `<button class="pair-card ${pair === selectedPair ? "selected" : ""}" data-pair="${pair}">
      <span class="pair-name">${pair.split("/").join(" / ")} <i>${pair.endsWith("JPY") ? "JPY quote" : "USD quote"}</i></span>
      <strong>${tick ? formatPrice(pair, tick.price) : "—"}</strong>
      <span class="pair-bottom"><span class="${change == null ? "" : change >= 0 ? "positive" : "negative"}">${change == null ? "Waiting for candles" : `${change >= 0 ? "+" : ""}${change.toFixed(3)}% · recent bars`}</span><b class="direction ${dClass}">${direction}</b></span>
    </button>`;
  }).join("");
  $("#pair-grid").querySelectorAll("[data-pair]").forEach((button) => button.addEventListener("click", () => choosePair(button.dataset.pair)));
}
function render() {
  $("#chart-pair").innerHTML = `${selectedPair.replace("/", " / ")} <small class="timeframe">1 MIN</small>`;
  renderPairs();
  updatePriceAndOhlc();
  renderPrediction();
  renderAnalysis();
  drawChart();
  renderSession();
}
function updatePriceAndOhlc() {
  const tick = selectedTick();
  const pairBars = selectedBars();
  const current = pairBars.at(-1);
  $("#current-price").textContent = tick ? formatPrice(selectedPair, tick.price) : "—";
  if (!current) {
    $("#ohlc-line").textContent = "O —   H —   L —   C —";
    return;
  }
  $("#ohlc-line").innerHTML = `O ${formatPrice(selectedPair, current.open)} <i>H ${formatPrice(selectedPair, current.high)}</i> <b>L ${formatPrice(selectedPair, current.low)}</b> C ${formatPrice(selectedPair, current.close)}`;
}
function renderPrediction() {
  const prediction = predictions.get(selectedPair);
  $("#training-count").innerHTML = `${prediction?.training_samples || 0} <small>bars</small>`;
  $("#training-note").textContent = `${selectedPair} · 30 labeled bars to activate`;
  const chip = $("#prediction-chip");
  const arrow = $("#forecast-arrow");
  if (!prediction || prediction.status !== "active") {
    chip.className = "prediction-chip";
    chip.textContent = "MODEL WARMING UP";
    arrow.className = "forecast-arrow";
    arrow.textContent = "—";
    $("#predicted-close").textContent = "—";
    $("#confidence").textContent = "—";
    $("#confidence-meter").style.width = "0%";
    $("#forecast-foot").textContent = `Needs ${Math.max(0, 30 - (prediction?.training_samples || 0))} more labeled candles for ${selectedPair}.`;
    return;
  }
  const up = prediction.direction === "UP";
  chip.className = `prediction-chip ${up ? "up" : "down"}`;
  chip.textContent = `MODEL ${prediction.direction} · ${Math.round(prediction.confidence * 100)}%`;
  arrow.className = `forecast-arrow ${up ? "up" : "down"}`;
  arrow.textContent = up ? "↑" : "↓";
  $("#predicted-close").textContent = formatPrice(selectedPair, prediction.predicted_close);
  $("#confidence").textContent = `${(prediction.confidence * 100).toFixed(1)}%`;
  $("#confidence-meter").style.width = `${Math.min(100, Math.max(0, prediction.confidence * 100))}%`;
  $("#forecast-foot").textContent = `${prediction.direction} estimate for the next 1-minute candle · ${prediction.training_samples} labeled bars trained.`;
}
function renderAnalysis() {
  const pairBars = selectedBars();
  if (!pairBars.length) {
    $("#rsi-value").textContent = "—";
    $("#rsi-label").textContent = "Waiting for bars";
    $("#sma-value").textContent = "—";
    $("#range-value").textContent = "—";
    $("#spread-value").textContent = "—";
    $("#bar-count").textContent = "0 bars";
    return;
  }
  const closes = pairBars.map((bar) => Number(bar.close)).filter(Number.isFinite);
  const latest = pairBars.at(-1);
  const sample = closes.slice(-15);
  const changes = sample.slice(1).map((value, index) => value - sample[index]);
  const gains = changes.filter((value) => value > 0).reduce((sum, value) => sum + value, 0) / Math.max(1, changes.length);
  const losses = changes.filter((value) => value < 0).reduce((sum, value) => sum - value, 0) / Math.max(1, changes.length);
  const rsi = losses === 0 ? gains > 0 ? 100 : 50 : 100 - 100 / (1 + gains / losses);
  const smaValues = closes.slice(-20);
  const sma = smaValues.reduce((sum, value) => sum + value, 0) / smaValues.length;
  const candleRange = Number(latest.high) - Number(latest.low);
  $("#rsi-value").textContent = rsi.toFixed(1);
  $("#rsi-label").textContent = rsi >= 70 ? "Potentially overbought" : rsi <= 30 ? "Potentially oversold" : "Neutral momentum";
  $("#sma-value").textContent = formatPrice(selectedPair, sma);
  $("#range-value").textContent = formatPrice(selectedPair, candleRange);
  $("#spread-value").textContent = formatPrice(selectedPair, Number(latest.average_spread || 0));
  $("#bar-count").textContent = `${closes.length} bars`;
  const model = predictions.get(selectedPair);
  $("#training-count").innerHTML = `${model?.training_samples || 0} <small>bars</small>`;
}
function drawChart() {
  const canvas = $("#candle-chart");
  const ctx = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.round(rect.width * ratio);
  canvas.height = Math.round(rect.height * ratio);
  ctx.scale(ratio, ratio);
  const width = rect.width, height = rect.height;
  ctx.clearRect(0, 0, width, height);
  const all = selectedBars().slice(-90);
  const prediction = predictions.get(selectedPair);
  $("#chart-empty").hidden = all.length > 0;
  if (!all.length) return;
  const prices = all.flatMap((bar) => [Number(bar.high), Number(bar.low)]);
  if (prediction?.status === "active") prices.push(Number(prediction.predicted_close));
  let min = Math.min(...prices), max = Math.max(...prices);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return;
  const pad = Math.max((max - min) * 0.12, max * 0.00012);
  min -= pad; max += pad;
  const left = 12, right = width - 70, top = 15, bottom = height - 22;
  const chartWidth = Math.max(1, right - left), chartHeight = Math.max(1, bottom - top);
  const y = (value) => top + (max - value) / (max - min) * chartHeight;
  const step = chartWidth / all.length;
  const candleWidth = Math.max(2, Math.min(12, step * 0.62));

  ctx.font = '9px "DM Mono", monospace';
  ctx.textAlign = "right";
  ctx.lineWidth = 1;
  for (let line = 0; line <= 4; line++) {
    const value = min + (max - min) * (line / 4);
    const gy = y(value);
    ctx.strokeStyle = "#1b292d";
    ctx.beginPath(); ctx.moveTo(left, gy); ctx.lineTo(right, gy); ctx.stroke();
    ctx.fillStyle = "#73817b";
    ctx.fillText(formatPrice(selectedPair, value), width - 6, gy + 3);
  }
  all.forEach((bar, index) => {
    const x = left + step * index + step / 2;
    const open = Number(bar.open), close = Number(bar.close);
    const high = Number(bar.high), low = Number(bar.low);
    if (![open, close, high, low].every(Number.isFinite)) return;
    const rising = close >= open;
    const color = rising ? "#62d99b" : "#ff806c";
    ctx.strokeStyle = color; ctx.fillStyle = color; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, y(high)); ctx.lineTo(x, y(low)); ctx.stroke();
    const bodyTop = Math.min(y(open), y(close));
    const bodyHeight = Math.max(1, Math.abs(y(open) - y(close)));
    ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
  });
  if (prediction?.status === "active") {
    const py = y(Number(prediction.predicted_close));
    ctx.strokeStyle = prediction.direction === "UP" ? "#66dca2" : "#ff806c";
    ctx.setLineDash([5, 4]);
    ctx.beginPath(); ctx.moveTo(left, py); ctx.lineTo(right + 3, py); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = prediction.direction === "UP" ? "#66dca2" : "#ff806c";
    ctx.textAlign = "left";
    ctx.fillText(`${prediction.direction} ${formatPrice(selectedPair, prediction.predicted_close)}`, right + 7, py - 5);
  }
  const first = new Date(all[0].timestamp);
  const last = new Date(all.at(-1).timestamp);
  ctx.fillStyle = "#73817b"; ctx.textAlign = "left";
  ctx.fillText(first.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }), left, height - 5);
  ctx.textAlign = "right";
  ctx.fillText(last.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }), right, height - 5);
}
function renderSession() {
  const hour = new Date().getUTCHours();
  const session = hour >= 0 && hour < 8 ? "Tokyo" : hour >= 7 && hour < 16 ? "London" : hour >= 12 && hour < 21 ? "New York" : "Closed";
  const open = new Date().getUTCDay() % 6 !== 0;
  $("#market-session").textContent = open ? session : "Weekend";
  $("#session-note").textContent = open ? "UTC session estimate" : "Forex market closed";
}
window.addEventListener("resize", drawChart);
window.setInterval(() => { ticksThisMinute = 0; $("#tick-count").textContent = "0"; }, 60000);
render();
connect();
