const IDS = ["bitcoin", "ethereum", "solana", "ripple", "binancecoin", "dogecoin"];
const history = new Map(IDS.map((id) => [id, []]));
const assets = new Map();
const alerts = [];
const $ = (selector) => document.querySelector(selector);
let selected = "bitcoin";
let socket;
let retryMs = 1000;
let minuteUpdates = 0;
let soundOn = false;
let audioContext;
let toastTimer;

function fmtMoney(value) {
  if (!Number.isFinite(value)) return "—";
  const digits = value >= 1 ? 2 : 5;
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: digits, maximumFractionDigits: digits }).format(value);
}
function compact(value) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 2 }).format(value || 0);
}
function ago(timestamp) {
  const seconds = Math.max(0, (Date.now() - new Date(timestamp).getTime()) / 1000 | 0);
  return seconds < 10 ? "just now" : seconds < 60 ? `${seconds}s ago` : `${Math.floor(seconds / 60)}m ago`;
}
function safe(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}
function connect() {
  socket = new WebSocket(`${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/ws/market`);
  socket.addEventListener("open", () => { retryMs = 1000; status("connecting"); });
  socket.addEventListener("message", ({ data }) => {
    try { onMessage(JSON.parse(data)); } catch (error) { console.error("Invalid market message", error); }
  });
  socket.addEventListener("close", () => {
    status("reconnecting");
    setTimeout(connect, retryMs);
    retryMs = Math.min(retryMs * 2, 30000);
  });
  socket.addEventListener("error", () => socket.close());
}
function onMessage(message) {
  if (message.type === "status") return status(message.status);
  if (message.type === "snapshot") {
    status(message.status);
    feed(message.feed);
    (message.assets || []).forEach(renderAsset);
    if (message.last_update) $("#last-updated").textContent = ago(message.last_update);
  } else if (message.type === "tick" && message.asset) renderAsset(message.asset);
}
function status(value) {
  $("#connection-status").className = `connection ${value}`;
  $("#connection-label").textContent = value === "live" ? "LIVE" : value === "demo" ? "DEMO MODE" : value.toUpperCase();
}
function feed(value) {
  const isLive = value === "coingecko";
  $("#feed-label").textContent = isLive ? "COINGECKO LIVE" : "DEMO FEED";
  $("#feed-pill").className = `feed-pill ${isLive ? "real" : "demo"}`;
}
function renderAsset(asset) {
  if (!history.has(asset.id)) return;
  assets.set(asset.id, asset);
  const points = history.get(asset.id);
  points.push({ price: asset.price, anomaly: asset.anomaly, timestamp: asset.timestamp });
  if (points.length > 120) points.shift();
  if (asset.anomaly) addAlert(asset);
  renderTable();
  renderStats();
  renderSignals();
  if (selected === asset.id) drawChart();
  $("#last-updated").textContent = ago(asset.timestamp);
  $("#stream-health").textContent = ++minuteUpdates;
  if (assets.size === 6) $("#model-status").textContent = "ACTIVE";
}
const MARKS = { bitcoin: "₿", ethereum: "◆", solana: "≋", ripple: "✕", binancecoin: "✦", dogecoin: "Ð" };
const COLOR_CLASS = { bitcoin: "btc", ethereum: "eth", solana: "sol", ripple: "xrp", binancecoin: "bnb", dogecoin: "doge" };
function sparkline(points, positive) {
  const values = points.slice(-20).map((point) => point.price);
  if (!values.length) return "—";
  const min = Math.min(...values), span = Math.max(...values) - min || 1;
  const coords = values.map((v, i) => `${i / Math.max(1, values.length - 1) * 60},${18 - (v - min) / span * 15}`).join(" ");
  return `<svg viewBox="0 0 60 20" class="spark ${positive ? "" : "down"}"><polyline points="${coords}"/></svg>`;
}
function renderTable() {
  $("#empty-assets").hidden = assets.size > 0;
  $("#asset-rows").innerHTML = IDS.filter((id) => assets.has(id)).map((id) => {
    const a = assets.get(id), points = history.get(id);
    const movement = points[0]?.price ? (a.price / points[0].price - 1) * 100 : a.change_24h;
    return `<tr data-id="${id}" class="${selected === id ? "selected" : ""}">
      <td><div class="asset"><i class="coin ${COLOR_CLASS[id]}">${MARKS[id]}</i><span><b>${safe(a.name)}</b><small>${safe(a.symbol)} <i>/</i> USD</small></span></div></td>
      <td class="price">${fmtMoney(a.price)}</td><td class="${a.change_24h >= 0 ? "positive" : "negative"}">${a.change_24h >= 0 ? "+" : ""}${a.change_24h.toFixed(2)}%</td>
      <td>${sparkline(points, movement >= 0)}</td><td class="volume">${compact(a.volume_24h)}</td>
      <td><span class="badge ${a.anomaly ? "alert" : "normal"}"><i></i>${a.anomaly ? "Anomaly" : "Normal"}</span></td></tr>`;
  }).join("");
  $("#asset-rows").querySelectorAll("tr").forEach((row) => row.addEventListener("click", () => choose(row.dataset.id)));
  $("#tracked-count").innerHTML = `${String(assets.size).padStart(2, "0")} <small>assets</small>`;
  $("#asset-total").textContent = String(assets.size).padStart(2, "0");
  $("#asset-footer").textContent = `${assets.size} assets monitored`;
  const select = $("#asset-select");
  const available = IDS.filter((id) => assets.has(id));
  if (select.options.length !== available.length) {
    select.innerHTML = available.map((id) => `<option value="${id}">${assets.get(id).symbol} / USD</option>`).join("");
    if (!available.includes(selected)) selected = available[0];
    select.value = selected;
  }
}
function renderStats() {
  const active = alerts.filter((item) => Date.now() - item.receivedAt < 60000).length;
  $("#anomaly-count").innerHTML = `${active} <small>active</small>`;
  $("#nav-alert-count").textContent = active;
  const values = [...assets.values()].map((item) => item.change_24h);
  const avg = values.length ? values.reduce((x, y) => x + y, 0) / values.length : 0;
  const sentiment = avg > .75 ? "Bullish" : avg < -.75 ? "Bearish" : "Neutral";
  $("#market-sentiment").textContent = sentiment;
  $("#market-sentiment").className = `sentiment ${sentiment.toLowerCase()}`;
}
function choose(id) {
  selected = id;
  if ($("#asset-select").value !== id) $("#asset-select").value = id;
  renderTable();
  drawChart();
}
function drawChart() {
  const canvas = $("#price-chart"), ctx = canvas.getContext("2d"), rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const ratio = devicePixelRatio || 1;
  canvas.width = rect.width * ratio; canvas.height = rect.height * ratio;
  ctx.scale(ratio, ratio);
  const w = rect.width, h = rect.height, points = history.get(selected), asset = assets.get(selected);
  ctx.clearRect(0, 0, w, h);
  $("#selected-token").textContent = `${asset?.symbol || "BTC"} / USD`;
  if (!points.length) return;
  const values = points.map((p) => p.price), min = Math.min(...values), max = Math.max(...values);
  const pad = Math.max((max - min) * .18, max * .0003), low = min - pad, high = max + pad;
  const x = (i) => 16 + i / Math.max(1, points.length - 1) * (w - 32);
  const y = (v) => 10 + (high - v) / (high - low) * (h - 22);
  ctx.strokeStyle = "#1b292d"; ctx.lineWidth = 1;
  for (let i = 0; i < 4; i++) { const gy = 10 + i / 3 * (h - 22); ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(w, gy); ctx.stroke(); }
  const gradient = ctx.createLinearGradient(0, 0, 0, h);
  gradient.addColorStop(0, "rgba(96,221,157,.18)"); gradient.addColorStop(1, "rgba(96,221,157,0)");
  ctx.beginPath(); points.forEach((p, i) => i ? ctx.lineTo(x(i), y(p.price)) : ctx.moveTo(x(i), y(p.price)));
  ctx.lineTo(x(points.length - 1), h); ctx.lineTo(x(0), h); ctx.closePath(); ctx.fillStyle = gradient; ctx.fill();
  ctx.beginPath(); points.forEach((p, i) => i ? ctx.lineTo(x(i), y(p.price)) : ctx.moveTo(x(i), y(p.price)));
  ctx.strokeStyle = "#68dfa3"; ctx.lineWidth = 2; ctx.stroke();
  points.forEach((p, i) => {
    if (!p.anomaly) return;
    ctx.beginPath(); ctx.arc(x(i), y(p.price), 4, 0, Math.PI * 2); ctx.fillStyle = "#ff806c"; ctx.fill();
    ctx.strokeStyle = "#10171a"; ctx.lineWidth = 2; ctx.stroke();
  });
  $("#chart-price").textContent = fmtMoney(asset?.price);
  const change = asset?.change_24h || 0;
  $("#chart-change").textContent = `${change >= 0 ? "▲" : "▼"} ${Math.abs(change).toFixed(2)}% today`;
  $("#chart-change").className = `chart-change ${change >= 0 ? "positive" : "negative"}`;
  $("#chart-range").textContent = points.length < 2 ? "Collecting data…" : `${points.length} recent ticks`;
}
function addAlert(asset) {
  const previous = alerts[0];
  if (previous?.id === asset.id && Date.now() - previous.receivedAt < 5000) return;
  alerts.unshift({ ...asset, receivedAt: Date.now() });
  if (alerts.length > 20) alerts.pop();
  if (soundOn) playSound();
  toast(`${asset.symbol} anomaly detected`);
}
function renderSignals() {
  if (!alerts.length) {
    $("#signal-list").innerHTML = `<div class="all-clear"><i>✓</i><b>All clear</b><span>Unusual activity will appear here.</span></div>`;
    return;
  }
  $("#signal-list").innerHTML = alerts.slice(0, 6).map((a) => `<article class="signal-item"><i>!</i><div><header><b>${safe(a.symbol)} unusual movement</b><time>${ago(a.timestamp)}</time></header><p>Price or volume behavior fell outside the model's normal pattern.</p><footer><span class="badge alert"><i></i>ANOMALY</span><small>Score ${Number(a.anomaly_score || 0).toFixed(3)}</small></footer></div></article>`).join("");
}
function playSound() {
  audioContext ||= new AudioContext();
  const oscillator = audioContext.createOscillator(), gain = audioContext.createGain();
  oscillator.frequency.value = 660; gain.gain.setValueAtTime(.07, audioContext.currentTime);
  gain.gain.exponentialRampToValueAtTime(.001, audioContext.currentTime + .3);
  oscillator.connect(gain); gain.connect(audioContext.destination); oscillator.start(); oscillator.stop(audioContext.currentTime + .3);
}
function toast(text) {
  const el = $("#toast"); el.textContent = text; el.classList.add("show");
  clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove("show"), 2400);
}
$("#asset-select").addEventListener("change", (event) => choose(event.target.value));
$("#view-signals").addEventListener("click", () => $("#signals").scrollIntoView({ behavior: "smooth", block: "center" }));
$("#sound-toggle").addEventListener("click", (event) => {
  soundOn = !soundOn; event.currentTarget.classList.toggle("enabled", soundOn);
  event.currentTarget.setAttribute("aria-label", soundOn ? "Mute sound alerts" : "Enable sound alerts");
  if (soundOn) audioContext ||= new AudioContext();
  toast(soundOn ? "Sound alerts enabled" : "Sound alerts muted");
});
addEventListener("resize", drawChart);
setInterval(() => { minuteUpdates = 0; }, 60000);
setInterval(() => {
  if (assets.size) {
    const latest = [...assets.values()].at(-1);
    $("#last-updated").textContent = ago(latest.timestamp);
    renderStats();
  }
}, 5000);
connect();
