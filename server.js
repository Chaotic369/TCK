"use strict";
// Tick-only server: same buffers, 00:00 UTC (05:30 IST) rollover, backfill and /ticks format as the main app. No scan, no trader, no page hosting.
const http = require("http");
const zlib = require("zlib");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT) || 3000;
const API_TOKEN = (process.env.API_TOKEN || process.env.ACCESS_TOKEN || "").trim();
const WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public?app_id=1089";
const DAY_SECS = 86400, PRE_ROLL_SECS = 900, CHUNK_SECS = 900, BACKFILL_CONCURRENCY = 6;
const FX_CLOSE_SEC = 21 * 3600, FX_OPEN_SUN_SEC = 86400;

const ALL_PAIRS = [
  "1HZ10V", "1HZ15V", "1HZ25V", "1HZ30V", "1HZ50V", "1HZ75V", "1HZ90V", "1HZ100V",
  "frxAUDCAD", "frxAUDCHF", "frxAUDJPY", "frxAUDNZD", "frxAUDUSD",
  "cryBTCUSD", "cryETHUSD",
  "frxEURAUD", "frxEURCAD", "frxEURCHF", "frxEURGBP", "frxEURJPY", "frxEURNZD", "frxEURUSD",
  "frxGBPAUD", "frxGBPCAD", "frxGBPCHF", "frxGBPJPY", "frxGBPNZD", "frxGBPUSD",
  "frxNZDJPY", "frxNZDUSD",
  "frxUSDCAD", "frxUSDCHF", "frxUSDJPY", "frxUSDMXN",
  "frxXAGUSD", "frxXAUUSD", "frxXPTUSD"
];
const PAIR_SET = new Set(ALL_PAIRS);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const nowSec = () => Math.floor(Date.now() / 1000);
const calcDayEpoch = sec => sec - (sec % DAY_SECS);
const isFxPair = p => /^frx/i.test(String(p));

const startedAtIso = new Date().toISOString(), startedAtEpoch = nowSec(), recentEvents = [];
function log(type, reason) {
  const e = { ts: new Date().toISOString(), epoch: nowSec(), type, reason };
  recentEvents.push(e);
  if (recentEvents.length > 100) recentEvents.shift();
  console.log(`[${e.ts}] [${type}] ${reason}`);
}
process.on("uncaughtException", e => log("UNCAUGHT_EXCEPTION", e && e.stack ? e.stack : String(e)));
process.on("unhandledRejection", e => log("UNHANDLED_REJECTION", e && e.stack ? e.stack : String(e)));

// ── buffers: one slot per second, -1 = empty ──
let activeDayEpoch = calcDayEpoch(nowSec());
const dayTicks = {}, preTicks = {}, lastTickTime = {};
const filled = {}, liveN = {}, bfN = {};
let totalLive = 0, totalBf = 0;
for (const p of ALL_PAIRS) {
  dayTicks[p] = new Float64Array(DAY_SECS).fill(-1);
  preTicks[p] = new Float64Array(PRE_ROLL_SECS).fill(-1);
  lastTickTime[p] = null;
  filled[p] = liveN[p] = bfN[p] = 0;
}

function checkRollover() {
  const cur = calcDayEpoch(nowSec());
  if (cur === activeDayEpoch) return;
  const adjacent = cur - activeDayEpoch === DAY_SECS;
  for (const p of ALL_PAIRS) {
    if (adjacent) preTicks[p].set(dayTicks[p].subarray(DAY_SECS - PRE_ROLL_SECS, DAY_SECS));
    else preTicks[p].fill(-1);
    dayTicks[p].fill(-1);
    filled[p] = liveN[p] = bfN[p] = 0;
  }
  totalLive = totalBf = 0;
  log("ROLLOVER", `05:30 IST rollover ${activeDayEpoch} -> ${cur}`);
  activeDayEpoch = cur;
}
setInterval(checkRollover, 1000);

function recordTick(pair, epochSec, price, src) {
  if (!PAIR_SET.has(pair)) return;
  const ts = Math.floor(Number(epochSec)), val = Number(price);
  if (!Number.isFinite(ts) || !Number.isFinite(val)) return;
  checkRollover();
  const idx = ts - activeDayEpoch;
  let wasEmpty;
  if (idx >= 0 && idx < DAY_SECS) {
    wasEmpty = dayTicks[pair][idx] === -1;
    if (wasEmpty) filled[pair]++;
    dayTicks[pair][idx] = val;
  } else if (idx >= -PRE_ROLL_SECS && idx < 0) {
    wasEmpty = preTicks[pair][PRE_ROLL_SECS + idx] === -1;
    preTicks[pair][PRE_ROLL_SECS + idx] = val;
  } else return;
  lastTickTime[pair] = Math.max(lastTickTime[pair] || 0, ts);
  if (src === "L") { liveN[pair]++; totalLive++; } else if (src === "B" && wasEmpty) { bfN[pair]++; totalBf++; }
}

// ── load sampler: CPU % of one core (free tier cap is about 10%) and event-loop lag ──
let cpuPct = 0, lagMs = 0, lagMax = 0, prevCpu = process.cpuUsage(), prevAt = Date.now(), lagPrev = Date.now();
setInterval(() => { const n = Date.now(); lagMax = Math.max(lagMax, n - lagPrev - 1000); lagPrev = n; }, 1000).unref();
setInterval(() => {
  const n = Date.now(), c = process.cpuUsage();
  cpuPct = ((c.user - prevCpu.user) + (c.system - prevCpu.system)) / ((n - prevAt) * 1000) * 100;
  prevCpu = c; prevAt = n; lagMs = lagMax; lagMax = 0;
}, 5000).unref();

// ── FX weekly hours (crypto and 1HZ run 24x7) ──
function fxClosedAt(sec) {
  const d = new Date(sec * 1000), dow = d.getUTCDay();
  const s = d.getUTCHours() * 3600 + d.getUTCMinutes() * 60 + d.getUTCSeconds();
  return dow === 6 || (dow === 5 && s >= FX_CLOSE_SEC) || (dow === 0 && s < FX_OPEN_SUN_SEC);
}
function fxFetchEnd(pair, endSec) {
  if (!isFxPair(pair) || !fxClosedAt(endSec)) return endSec;
  const dayStart = endSec - (endSec % DAY_SECS);
  if (new Date(endSec * 1000).getUTCDay() === 5) return Math.min(endSec, dayStart + FX_CLOSE_SEC);
  return -1;
}

// ── backfill from ticks_history ──
let backfillState = { active: false, pct: 100, doneChunks: 0, totalChunks: 0, currentPair: null, reason: null };
let queuedReason = null, backfillSeq = 0;
const preBackfillDay = {};

function fetchChunk(pair, t0, t1) {
  return new Promise(resolve => {
    const map = {};
    let settled = false, ws = null;
    const done = (ok, closedMarket) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (ws) { try { ws.terminate(); } catch {} }
      resolve({ ok: !!ok, closedMarket: !!closedMarket, map });
    };
    const timer = setTimeout(() => done(false, false), 25000);
    try {
      ws = new WebSocket(WS_URL);
      ws.on("open", () => ws.send(JSON.stringify({ ticks_history: pair, start: t0, end: t1, count: 5000, style: "ticks" })));
      ws.on("message", raw => {
        try {
          const d = JSON.parse(raw.toString());
          if (d.history && Array.isArray(d.history.times)) {
            for (let i = 0; i < d.history.times.length; i++) map[+d.history.times[i]] = +d.history.prices[i];
            done(true, false);
          } else if (d.error) {
            const code = d.error.code || "";
            done(code === "MarketIsClosed" || code === "InvalidSymbol", code === "MarketIsClosed" || code === "InvalidSymbol");
          }
        } catch { done(false, false); }
      });
      ws.on("error", () => done(false, false));
      ws.on("close", () => done(false, false));
    } catch { done(false, false); }
  });
}

function findPairGaps(pair, dayEpoch, endSec, forceFromSec) {
  const gapMin = /^(cry|1HZ)/i.test(pair) ? 120 : 900;
  const gaps = [];
  let preMiss = 0;
  for (let i = 0; i < PRE_ROLL_SECS; i++) if (preTicks[pair][i] === -1) preMiss++;
  if (preMiss > 0 && preBackfillDay[pair] !== dayEpoch) gaps.push([dayEpoch - PRE_ROLL_SECS, dayEpoch - 1]);
  const maxIdx = Math.min(DAY_SECS - 1, Math.max(0, endSec - dayEpoch));
  const prices = dayTicks[pair];
  let gapStart = -1;
  for (let i = 0; i <= maxIdx; i++) {
    const ts = dayEpoch + i;
    if (prices[i] === -1) {
      if (gapStart === -1) gapStart = ts;
    } else if (gapStart !== -1) {
      const gapEnd = ts - 1;
      if (gapEnd - gapStart + 1 >= gapMin || (forceFromSec != null && gapEnd >= forceFromSec)) gaps.push([gapStart, gapEnd]);
      gapStart = -1;
    }
  }
  if (gapStart !== -1) gaps.push([gapStart, dayEpoch + maxIdx]);
  return gaps;
}

async function runBackfill(reason, forceFromSec) {
  if (backfillState.active) { queuedReason = reason; return; }
  checkRollover();
  const targetDay = activeDayEpoch, endSec = nowSec(), queue = [];
  for (const pair of ALL_PAIRS) {
    const pairEnd = fxFetchEnd(pair, endSec);
    if (pairEnd < 0) continue;
    for (const [a, b] of findPairGaps(pair, targetDay, pairEnd, forceFromSec)) {
      for (let s = a; s <= b; s += CHUNK_SECS + 1) queue.push({ pair, s, e: Math.min(s + CHUNK_SECS, b) });
    }
  }
  if (!queue.length) { log("BACKFILL_COMPLETE", `complete (${reason}) - 0 missing chunks`); return; }
  backfillState = { active: true, pct: 0, doneChunks: 0, totalChunks: queue.length, currentPair: queue[0].pair, reason };
  log("BACKFILL_START", `start (${reason}): ${queue.length} chunks`);
  const closed = {};
  let cursor = 0;
  const slot = async n => {
    if (n) await sleep(n * 250);
    while (activeDayEpoch === targetDay) {
      const t = queue[cursor++];
      if (!t) return;
      backfillState.currentPair = t.pair;
      if (!closed[t.pair]) {
        let res = { ok: false, closedMarket: false, map: {} };
        for (let tr = 0; tr <= 3; tr++) {
          if (tr) await sleep(1500 * tr);
          res = await fetchChunk(t.pair, t.s, t.e);
          if (res.ok) break;
        }
        if (res.closedMarket) closed[t.pair] = true;
        for (const ts in res.map) recordTick(t.pair, +ts, res.map[ts], "B");
        if (res.ok && t.e < targetDay) preBackfillDay[t.pair] = targetDay;
        await sleep(300);
      }
      backfillState.doneChunks++;
      backfillState.pct = Math.min(100, Math.round(backfillState.doneChunks / backfillState.totalChunks * 100));
    }
  };
  await Promise.all(Array.from({ length: BACKFILL_CONCURRENCY }, (_, i) => slot(i)));
  backfillState = { active: false, pct: 100, doneChunks: backfillState.doneChunks, totalChunks: queue.length, currentPair: null, reason: null };
  backfillSeq++;
  log("BACKFILL_COMPLETE", `complete (${reason}) - ${backfillState.doneChunks}/${queue.length} chunks`);
  if (queuedReason) { const next = queuedReason; queuedReason = null; runBackfill(next, null); }
}

// ── live Deriv socket ──
let liveWs = null, wsConnected = false, attempts = 0, reconnectTimer = null, pingTimer = null, resubTimer = null, downAt = null, shuttingDown = false;
const subscribed = new Set();
const feedState = () => (!wsConnected ? "RECONNECTING" : backfillState.active ? "BACKFILLING" : "LIVE");
function clearTimers() { clearInterval(pingTimer); clearInterval(resubTimer); pingTimer = resubTimer = null; }

async function subscribeAll(ws) {
  let id = 1;
  for (const pair of ALL_PAIRS) {
    if (ws !== liveWs || ws.readyState !== WebSocket.OPEN) return;
    if (subscribed.has(pair) || (isFxPair(pair) && fxClosedAt(nowSec()))) continue;
    try { ws.send(JSON.stringify({ ticks: pair, subscribe: 1, req_id: id++ })); } catch {}
    await sleep(45);
  }
}

function scheduleReconnect(reason) {
  if (shuttingDown || reconnectTimer) return;
  attempts++;
  const delay = Math.min(30000, 1000 * Math.pow(2, Math.min(attempts - 1, 5)));
  log("RECONNECTING", `in ${delay}ms (attempt ${attempts}): ${reason}`);
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectLiveWs(true); }, delay);
}

function connectLiveWs(isReconnect) {
  if (shuttingDown) return;
  clearTimers();
  subscribed.clear();
  let ws;
  try { ws = new WebSocket(WS_URL); } catch (err) { wsConnected = false; scheduleReconnect(err.message); return; }
  liveWs = ws;
  ws.on("open", () => {
    if (ws !== liveWs) return;
    wsConnected = true;
    const missedFrom = downAt;
    downAt = null;
    attempts = 0;
    log("DERIV_OPEN", `connected; subscribing to ${ALL_PAIRS.length} pairs`);
    subscribeAll(ws);
    pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) { try { ws.ping(); ws.send(JSON.stringify({ ping: 1 })); } catch {} }
    }, 25000);
    let fxWasClosed = fxClosedAt(nowSec()), ticks = 0;
    resubTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ticks++;
      const fxClosed = fxClosedAt(nowSec()), fxJustOpened = fxWasClosed && !fxClosed;
      fxWasClosed = fxClosed;
      if (fxClosed) for (const p of ALL_PAIRS) if (isFxPair(p)) subscribed.delete(p);
      if (fxJustOpened || (ticks % 10 === 0 && subscribed.size < ALL_PAIRS.length)) subscribeAll(ws);
    }, 30000);
    if (isReconnect) runBackfill("reconnect-recovery", missedFrom || (nowSec() - 300));
  });
  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.msg_type === "tick" && msg.tick) {
        subscribed.add(msg.tick.symbol);
        recordTick(msg.tick.symbol, msg.tick.epoch, msg.tick.quote, "L");
      } else if (msg.error && msg.echo_req && msg.echo_req.ticks && msg.error.code === "RateLimit") {
        const sym = msg.echo_req.ticks;
        setTimeout(() => {
          if (ws === liveWs && ws.readyState === WebSocket.OPEN) { try { ws.send(JSON.stringify({ ticks: sym, subscribe: 1 })); } catch {} }
        }, 3000);
      }
    } catch {}
  });
  ws.on("error", err => log("DERIV_ERROR", err && err.message ? err.message : String(err)));
  ws.on("close", (code, reasonBuf) => {
    if (ws !== liveWs) return;
    wsConnected = false;
    clearTimers();
    if (!downAt) downAt = nowSec();
    log("DERIV_CLOSED", `code ${code}${reasonBuf && reasonBuf.length ? " (" + reasonBuf.toString() + ")" : ""}`);
    scheduleReconnect(`closed with code ${code}`);
  });
}

// ── HTTP ──
function authorized(req, u) {
  if (!API_TOKEN) return true;
  const auth = req.headers["authorization"] || "";
  if (u.searchParams.get("token") === API_TOKEN || req.headers["x-api-token"] === API_TOKEN || auth === API_TOKEN || auth === `Bearer ${API_TOKEN}`) return true;
  try { return !!req.headers["referer"] && new URL(req.headers["referer"]).searchParams.get("token") === API_TOKEN; } catch { return false; }
}
function sendJson(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

const PAGE = String.raw`<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Tick server</title>
<style>
body{background:#0b0f1a;color:#dfe6ff;font:12px/1.4 ui-monospace,Menlo,Consolas,monospace;margin:0;padding:12px}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:8px;margin-bottom:10px}
.c{background:#121a2e;border:1px solid #243056;border-radius:6px;padding:8px;margin-bottom:0}
.c i{font-style:normal;color:#8d9ac8;font-size:10px;letter-spacing:.08em}.c b{display:block;font-size:17px;margin:2px 0}
.bar{height:8px;background:#1b2542;border-radius:4px;overflow:hidden;margin:4px 0}.bar div{height:100%}
td .bar{height:4px;margin:2px 0 0}table{border-collapse:collapse;width:100%}
td,th{padding:3px 6px;text-align:right;border-bottom:1px solid #1b2542}th{color:#8d9ac8}td:first-child,th:first-child{text-align:left}
.ok{color:#4ade80}.warn{color:#facc15}.bad{color:#f87171}.dim{color:#7d8ab5}
</style>
<body><div class=g id=cards></div>
<div class=c style="margin-bottom:10px"><i>LIVE TICKS / SEC</i><canvas id=sp width=600 height=60 style="width:100%;height:60px;display:block"></canvas></div>
<div class=c style="margin-bottom:10px;overflow-x:auto"><table id=tb></table></div>
<div class=c id=ev></div>
<script>
const t = new URLSearchParams(location.search).get('token') || '';
let prev = null, lr = 0, br = 0, hist = [];
const f = n => Number(n).toLocaleString();
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;');
const bar = (p, c) => '<div class=bar><div style="width:' + Math.min(100, Math.max(0, p)) + '%;background:' + (c || '#2dd4bf') + '"></div></div>';
const card = (k, v, x) => '<div class=c><i>' + k + '</i><b>' + v + '</b>' + (x || '') + '</div>';
const dur = s => { s = Math.max(0, Math.round(s)); return Math.floor(s / 3600) + 'h ' + Math.floor(s % 3600 / 60) + 'm ' + (s % 60) + 's'; };
async function go() {
  try {
    const r = await fetch('/status?token=' + encodeURIComponent(t), { cache: 'no-store' });
    if (r.status === 401) { cards.innerHTML = card('AUTH', '401', '<div class=dim>open as /?token=YOUR_TOKEN</div>'); return; }
    const j = await r.json(), now = Date.now() / 1000, b = j.backfill, left = j.activeDayEpoch + 86400 - now;
    if (prev && now > prev.t) {
      const dt = now - prev.t, inst = Math.max(0, (b.doneChunks - prev.chunks) / dt);
      lr = Math.max(0, (j.totalLive - prev.live) / dt);
      br = br ? br * 0.7 + inst * 0.3 : inst;
      hist.push(lr); if (hist.length > 150) hist.shift();
    }
    prev = { t: now, live: j.totalLive, chunks: b.doneChunks };
    const st = j.feedState, sc = st === 'LIVE' ? 'ok' : st === 'BACKFILLING' ? 'warn' : 'bad';
    const eta = b.active && br > 0 ? dur((b.totalChunks - b.doneChunks) / br) : '-';
    cards.innerHTML =
      card('FEED', '<span class=' + sc + '>' + st + '</span>', '<div class=dim>WS ' + (j.wsConnected ? 'up' : 'DOWN') + ' | subs ' + j.subscribedCount + '/' + j.totalPairs + '</div>') +
      card('UPTIME', dur(j.uptime), '<div class=dim>backfill runs done ' + j.backfillSeq + '</div>') +
      card('MEMORY', j.rssMB + ' MB', bar(j.rssMB / 5.12, j.rssMB > 400 ? '#f87171' : '#2dd4bf') + '<div class=dim>of 512 MB</div>') +
      card('CPU', j.cpuPct + '%', bar(j.cpuPct * 10, j.cpuPct > 8 ? '#f87171' : '#2dd4bf') + '<div class=dim>cap ~10% | loop lag ' + j.lagMs + 'ms</div>') +
      card('LIVE TICKS', f(j.totalLive), '<div class=dim>' + lr.toFixed(1) + ' / sec (today)</div>') +
      card('BACKFILLED TICKS', f(j.totalBf), '<div class=dim>' + f(b.doneChunks) + ' / ' + f(b.totalChunks) + ' chunks</div>') +
      card('BACKFILL', b.active ? b.pct + '%' : 'idle', bar(b.pct, '#facc15') + '<div class=dim>' + br.toFixed(1) + ' chunks/s | ETA ' + eta + (b.active ? ' | ' + b.currentPair : '') + '</div>') +
      card('DAY (05:30 IST)', Math.floor(j.elapsed / 864) + '%', bar(j.elapsed / 864) + '<div class=dim>reset in ' + dur(left) + '</div>');
    const cx = sp.getContext('2d'), w = sp.width, h = sp.height, mx = Math.max(1, ...hist);
    cx.clearRect(0, 0, w, h); cx.strokeStyle = '#2dd4bf'; cx.beginPath();
    hist.forEach((v, i) => { const px = i / 149 * w, py = h - 2 - v / mx * (h - 6); if (i) cx.lineTo(px, py); else cx.moveTo(px, py); });
    cx.stroke();
    let rows = '', tl = 0, tb2 = 0, tf = 0;
    for (const p in j.lastTickTime) {
      const a = j.lastTickTime[p], age = a ? now - a : null, cov = j.elapsed ? j.filled[p] / j.elapsed * 100 : 0;
      const ac = age === null ? 'dim' : age < 10 ? 'ok' : age < 60 ? 'warn' : 'bad';
      tl += j.liveN[p]; tb2 += j.bfN[p]; tf += j.filled[p];
      rows += '<tr><td>' + p + '</td><td>' + f(j.liveN[p]) + '</td><td>' + f(j.bfN[p]) + '</td><td>' + f(j.filled[p]) + '</td><td>' + cov.toFixed(1) + '%' + bar(cov) + '</td><td class=' + ac + '>' + (age === null ? '-' : Math.round(age) + 's') + '</td></tr>';
    }
    tb.innerHTML = '<tr><th>PAIR</th><th>LIVE</th><th>BACKFILLED</th><th>SECONDS FILLED</th><th>COVERAGE</th><th>LAST TICK</th></tr>' + rows +
      '<tr><th>TOTAL</th><th>' + f(tl) + '</th><th>' + f(tb2) + '</th><th>' + f(tf) + '</th><th></th><th></th></tr>';
    ev.innerHTML = '<i>EVENTS</i>' + j.recentEvents.slice(-10).reverse().map(e => '<div><span class=dim>' + e.ts.slice(11, 19) + '</span> ' + esc(e.type) + ' ' + esc(e.reason) + '</div>').join('');
  } catch (e) { cards.innerHTML = card('ERROR', esc(e.message)); }
}
go(); setInterval(go, 2000);
</script>`;

const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host || "localhost"}`), pathname = u.pathname;
  res.setHeader("Access-Control-Allow-Origin", process.env.CORS_ORIGIN || "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Token");
  res.setHeader("Access-Control-Expose-Headers", "X-Day-Epoch, X-From-Index, X-To-Index, X-Pre-Length");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }
  if (req.method !== "GET") { res.writeHead(404); res.end("Not found"); return; }

  if (pathname === "/" || pathname === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    res.end(PAGE);
    return;
  }
  if (pathname === "/ping") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" });
    res.end("ok");
    return;
  }
  if (pathname === "/status") {
    if (!authorized(req, u)) { sendJson(res, 401, { error: "Unauthorized" }); return; }
    checkRollover();
    sendJson(res, 200, {
      uptime: Math.floor(process.uptime()), lastRestart: startedAtIso, lastRestartEpoch: startedAtEpoch,
      activeDayEpoch, feedState: feedState(), wsConnected, subscribedCount: subscribed.size, totalPairs: ALL_PAIRS.length,
      backfill: backfillState, backfillSeq, serverScan: false, rssMB: Math.round(process.memoryUsage().rss / 1048576),
      lastTickTime, recentEvents, filled, liveN, bfN, totalLive, totalBf,
      elapsed: Math.min(DAY_SECS, nowSec() - activeDayEpoch + 1), cpuPct: Math.round(cpuPct * 10) / 10, lagMs
    });
    return;
  }
  if (pathname === "/scan") { sendJson(res, 200, { ok: false, reason: "server scan is disabled" }); return; }
  if (pathname === "/ticks") {
    if (!authorized(req, u)) { sendJson(res, 401, { error: "Unauthorized" }); return; }
    const pair = u.searchParams.get("pair") || "";
    if (!PAIR_SET.has(pair)) { sendJson(res, 400, { error: "Invalid pair" }); return; }
    checkRollover();
    const nowIdx = Math.max(0, Math.min(DAY_SECS - 1, nowSec() - activeDayEpoch));
    let from = parseInt(u.searchParams.get("from") || "0", 10);
    if (!Number.isFinite(from) || from < 0) from = 0;
    const clientDay = u.searchParams.get("day");
    if (clientDay && Number(clientDay) !== activeDayEpoch) from = 0;
    if (from > nowIdx) from = nowIdx;
    const preLen = from === 0 && u.searchParams.get("pre") === "1" ? PRE_ROLL_SECS : 0;
    const sliceLen = Math.max(0, nowIdx - from + 1);
    const out = new Float64Array(preLen + sliceLen);
    if (preLen) out.set(preTicks[pair], 0);
    if (sliceLen > 0) out.set(dayTicks[pair].subarray(from, nowIdx + 1), preLen);
    zlib.gzip(Buffer.from(out.buffer, out.byteOffset, out.byteLength), { level: 1 }, (err, gz) => {
      if (err) { sendJson(res, 500, { error: "Compression error" }); return; }
      res.writeHead(200, {
        "Content-Type": "application/octet-stream", "Content-Encoding": "gzip", "Cache-Control": "no-store",
        "X-Day-Epoch": String(activeDayEpoch), "X-From-Index": String(from), "X-To-Index": String(nowIdx), "X-Pre-Length": String(preLen)
      });
      res.end(gz);
    });
    return;
  }
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

// Render free tier sleeps after 15 min without inbound HTTP: ping our own public URL (Render sets RENDER_EXTERNAL_URL).
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => { fetch(process.env.RENDER_EXTERNAL_URL + "/ping").catch(() => {}); }, 4 * 60 * 1000).unref();
}

function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  log("SHUTDOWN", `${sig} received`);
  clearTimers();
  clearTimeout(reconnectTimer);
  try { if (liveWs) liveWs.close(1000, "shutting down"); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

server.listen(PORT, () => {
  log("START", `tick server on port ${PORT}, activeDayEpoch ${activeDayEpoch}, ${ALL_PAIRS.length} pairs`);
  connectLiveWs(false);
  runBackfill("startup", null);
});
