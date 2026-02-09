// netlify/functions/trade-bot.mjs
// Node 18/20 (ESM). Netlify Functions.
// Data: CoinAPI OHLCV + CoinGecko Derivatives. State: Upstash Redis.
// Endpoints:
//  - GET  /.netlify/functions/trade-bot?mode=intrabar&min_conf=40
//  - GET  /.netlify/functions/trade-bot?mode=close&min_conf_confirmed=40
//  - POST /.netlify/functions/trade-bot?mode=wait_close  { "symbol":"BTCUSDT" }

import axios from "axios";
import { RSI, MACD, BollingerBands, ATR } from "technicalindicators";
import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const CONFIG = {
  MIN_CONF_SHOW: 0,
  MIN_CONF_CONFIRMED: 0,

  COOLDOWN_CONFIRMED_MIN: 30,
  COOLDOWN_INVALIDATED_MIN: 15,

  KLINE_LIMIT: 200,
  ATR_PERIOD: 14,

  SL_ATR_MULT_LOW: 2.0,
  SL_ATR_MULT_HIGH: 3.0,
  RR_BASE: 1.5,
  RR_HIGH: 2.0,

  MIN_SL_BPS: 10,
  MIN_TP_BPS: 15,

  TREND_UP_PCT: 2.0,
  TREND_DN_PCT: -2.0,

  FUNDING_RATE_ABS_WARN: 0.0002,

  DEDUPE_SCOPE_SECONDS: 120,
  DEDUPE_TTL_SECONDS: 120,

  TTL_COOLDOWN_SEC: 6 * 3600,
  TTL_PENDING_SEC: 2 * 3600,
  TTL_LASTSIG_SEC: 6 * 3600,
};

const COINAPI_BASE = process.env.COINAPI_BASE || "https://rest.coinapi.io";
const COINAPI_KEY = process.env.COINAPI_KEY || "";
const COINAPI_PERIOD_ID = process.env.COINAPI_PERIOD_ID || "1HRS";
const COINAPI_EXCHANGE = (process.env.COINAPI_EXCHANGE || "BYBIT").toUpperCase();
const COINAPI_MARKET_PREFIX = (process.env.COINAPI_MARKET_PREFIX || "PERP").toUpperCase();
const COINAPI_TIMEOUT_MS = Number(process.env.COINAPI_TIMEOUT_MS || 8500);

const COINGECKO_BASE = process.env.COINGECKO_BASE || "https://api.coingecko.com/api/v3";
const COINGECKO_KEY = process.env.COINGECKO_KEY || ""; // optional
const COINGECKO_MARKET_FILTER = (process.env.COINGECKO_MARKET_FILTER || "Bybit").toLowerCase();

const httpCoinapi = axios.create({
  baseURL: COINAPI_BASE,
  timeout: COINAPI_TIMEOUT_MS,
  headers: COINAPI_KEY ? { "X-CoinAPI-Key": COINAPI_KEY } : {},
});

const httpGecko = axios.create({
  baseURL: COINGECKO_BASE,
  timeout: 8500,
  headers: COINGECKO_KEY ? { "x-cg-demo-api-key": COINGECKO_KEY } : {},
});

const nowMs = () => Date.now();
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
const floorTimeBucket = (ms, bucketSec) => Math.floor(ms / (bucketSec * 1000)) * (bucketSec * 1000);
const bpsToDist = (price, bps) => price * (bps / 10000);

function parseWatchlist() {
  const raw = (process.env.WATCHLIST || "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

function symbolBaseQuote(symbol) {
  const m = String(symbol).toUpperCase().match(/^([A-Z0-9]+)(USDT|USDC|BUSD|FDUSD|TUSD)$/);
  if (!m) return null;
  return { base: m[1], quote: m[2] };
}

function buildCoinapiSymbolId(symbol) {
  const pq = symbolBaseQuote(symbol);
  if (!pq) return null;
  return `${COINAPI_EXCHANGE}_${COINAPI_MARKET_PREFIX}_${pq.base}_${pq.quote}`;
}

function smartDecimals(p) {
  if (!Number.isFinite(p) || p <= 0) return 2;
  if (p >= 10000) return 0;
  if (p >= 100) return 1;
  if (p >= 1) return 2;
  if (p >= 0.01) return 4;
  return 6;
}

const kCooldown = (sym) => `sig:cooldown:${sym}`;
const kPending = (sym) => `sig:pending:${sym}`;
const kLastSig = (sym) => `sig:lastsig:${sym}`;
const kDedupe = (id) => `sig:dedupe:${id}`;

async function getState(symbol) {
  const [cooldown, pending, lastsig] = await Promise.all([
    redis.get(kCooldown(symbol)),
    redis.get(kPending(symbol)),
    redis.get(kLastSig(symbol)),
  ]);
  return {
    cooldown_until_ts: cooldown?.cooldown_until_ts ?? 0,
    pending_confirm: pending ?? null,
    last_signal: lastsig ?? null,
  };
}

async function saveCooldown(symbol, cooldownUntilMs) {
  await redis.set(kCooldown(symbol), { cooldown_until_ts: cooldownUntilMs }, { ex: CONFIG.TTL_COOLDOWN_SEC });
}
async function savePending(symbol, pendingObj) {
  await redis.set(kPending(symbol), pendingObj, { ex: CONFIG.TTL_PENDING_SEC });
}
async function clearPending(symbol) {
  await redis.del(kPending(symbol));
}
async function saveLastSignal(symbol, obj) {
  await redis.set(kLastSig(symbol), obj, { ex: CONFIG.TTL_LASTSIG_SEC });
}
async function tryDedupeOrDrop(signalId) {
  const ok = await redis.set(kDedupe(signalId), 1, { nx: true, ex: CONFIG.DEDUPE_TTL_SECONDS });
  return ok === "OK";
}

async function fetchOHLCV(symbolId) {
  const { data } = await httpCoinapi.get(`/v1/ohlcv/${encodeURIComponent(symbolId)}/history`, {
    params: { period_id: COINAPI_PERIOD_ID, limit: CONFIG.KLINE_LIMIT },
  });
  return Array.isArray(data) ? data : [];
}

function parseCoinapiCandles(rows) {
  const open = [], high = [], low = [], close = [], volume = [], closeTime = [];
  for (const r of rows) {
    open.push(Number(r.price_open));
    high.push(Number(r.price_high));
    low.push(Number(r.price_low));
    close.push(Number(r.price_close));
    volume.push(Number(r.volume_traded));
    closeTime.push(Date.parse(r.time_period_end));
  }
  return { open, high, low, close, volume, closeTime };
}

function computeIndicators(c) {
  const closes = c.close, highs = c.high, lows = c.low, vols = c.volume;

  const rsiArr = RSI.calculate({ values: closes, period: 14 });
  const rsi = rsiArr.length ? rsiArr[rsiArr.length - 1] : null;

  const macdArr = MACD.calculate({
    values: closes,
    fastPeriod: 12,
    slowPeriod: 26,
    signalPeriod: 9,
    SimpleMAOscillator: false,
    SimpleMASignal: false,
  });
  const macd = macdArr.length ? macdArr[macdArr.length - 1] : null;

  const bbArr = BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 });
  const bb = bbArr.length ? bbArr[bbArr.length - 1] : null;

  const atrArr = ATR.calculate({ high: highs, low: lows, close: closes, period: CONFIG.ATR_PERIOD });
  const atr = atrArr.length ? atrArr[atrArr.length - 1] : null;

  const lookback = 20;
  const lastVol = vols[vols.length - 1];
  const avgVol =
    vols.length >= lookback
      ? vols.slice(vols.length - lookback).reduce((a, b) => a + b, 0) / lookback
      : vols.reduce((a, b) => a + b, 0) / Math.max(1, vols.length);
  const volumeRatio = avgVol > 0 ? lastVol / avgVol : 1;

  let bbWidthClass = "mid";
  let bbPos = 0;
  if (bb && bb.middle && bb.upper && bb.lower) {
    const widthPct = (bb.upper - bb.lower) / bb.middle;
    if (widthPct >= 0.04) bbWidthClass = "high";
    else if (widthPct <= 0.02) bbWidthClass = "low";

    const lastClose = closes[closes.length - 1];
    bbPos = lastClose > bb.middle ? 1 : lastClose < bb.middle ? -1 : 0;
  }

  return {
    rsi,
    macd,
    bb,
    bbWidthClass,
    bbPos,
    atr,
    volumeRatio,
    lastClose: closes[closes.length - 1],
    lastCandleCloseTime: c.closeTime[c.closeTime.length - 1],
  };
}

function computePctChange24hFromCandles(c) {
  if (!c?.close?.length || c.close.length < 26) return 0;
  const last = c.close[c.close.length - 1];
  const prev = c.close[c.close.length - 25];
  if (!(last > 0 && prev > 0)) return 0;
  return ((last - prev) / prev) * 100;
}

function classifyTrend(pct24, ind) {
  let base = "SIDE";
  if (pct24 > CONFIG.TREND_UP_PCT) base = "UP";
  else if (pct24 < CONFIG.TREND_DN_PCT) base = "DOWN";

  let score = 0;
  if (base === "UP") score += 2;
  if (base === "DOWN") score -= 2;
  if (ind?.macd?.histogram != null) {
    if (ind.macd.histogram > 0) score += 1;
    if (ind.macd.histogram < 0) score -= 1;
  }
  score += ind?.bbPos ?? 0;

  if (score >= 2) return "UP";
  if (score <= -2) return "DOWN";
  return "SIDE";
}

function computeMomentum(ind) {
  const rsi = ind.rsi, vr = ind.volumeRatio, bbw = ind.bbWidthClass;
  let score = 0;

  if (rsi != null) {
    if (rsi >= 60) score += 2;
    else if (rsi >= 52) score += 1;
    else if (rsi <= 40) score -= 2;
    else if (rsi <= 48) score -= 1;
  }

  if (vr >= 1.5) score += 2;
  else if (vr >= 1.1) score += 1;
  else if (vr <= 0.7) score -= 1;

  if (bbw === "high") score += 1;
  if (bbw === "low") score -= 1;

  let strength = "WEAK";
  if (Math.abs(score) >= 4) strength = "STRONG";
  else if (Math.abs(score) >= 2) strength = "MED";

  return { score, strength };
}

function directionCandidate(trend, ind) {
  const rsi = ind.rsi;
  const macdH = ind.macd?.histogram;

  if (trend === "UP") {
    if (rsi != null && rsi >= 70) return { dir: "SHORT_CANDIDATE", reason: "RSI_OVERBOUGHT_IN_UPTREND" };
    return { dir: "LONG", reason: "UPTREND_RSI_OK" };
  }

  if (trend === "DOWN") {
    if (rsi != null && rsi <= 30) return { dir: "LONG_CANDIDATE", reason: "RSI_OVERSOLD_IN_DOWNTREND" };
    return { dir: "SHORT", reason: "DOWNTREND_RSI_OK" };
  }

  if (rsi != null && macdH != null) {
    if (rsi > 55 && macdH > 0) return { dir: "LONG", reason: "SIDE_BULL_BIAS" };
    if (rsi < 45 && macdH < 0) return { dir: "SHORT", reason: "SIDE_BEAR_BIAS" };
  }

  return { dir: "NO_TRADE", reason: "SIDE_NO_EDGE" };
}

function baseConfidence(trend, momentum, volPct24hApprox) {
  let conf = 50;

  const aligned =
    (trend === "UP" && momentum.score > 0) ||
    (trend === "DOWN" && momentum.score < 0) ||
    (trend === "SIDE" && Math.abs(momentum.score) >= 2);

  conf += aligned ? 10 : -8;

  if (momentum.strength === "STRONG") conf += 15;
  else if (momentum.strength === "MED") conf += 8;
  else conf -= 5;

  if (Math.abs(volPct24hApprox) > 5) conf += 8;
  else if (Math.abs(volPct24hApprox) > 2) conf += 4;
  else conf -= 3;

  return clamp(conf, 0, 100);
}

function chooseRR(confidence, momentumStrength) {
  if (confidence >= 75 && momentumStrength === "STRONG") return CONFIG.RR_HIGH;
  return CONFIG.RR_BASE;
}

function chooseSLMult(confidence, isReversal, bbWidthClass) {
  let mult = CONFIG.SL_ATR_MULT_LOW;
  if (isReversal) mult = Math.max(mult, 2.5);
  if (confidence < 68) mult = Math.max(mult, 2.5);
  if (bbWidthClass === "high") mult = Math.max(mult, 2.5);
  return Math.min(CONFIG.SL_ATR_MULT_HIGH, mult);
}

function pickTpSlATR({ entry, atr, direction, confidence, momentumStrength, isReversal, bbWidthClass }) {
  if (!(atr > 0)) return null;

  const slMult = chooseSLMult(confidence, isReversal, bbWidthClass);
  const rr = chooseRR(confidence, momentumStrength);

  let slDist = slMult * atr;
  let tpDist = rr * slDist;

  slDist = Math.max(slDist, bpsToDist(entry, CONFIG.MIN_SL_BPS));
  tpDist = Math.max(tpDist, bpsToDist(entry, CONFIG.MIN_TP_BPS));

  let sl, tp;
  if (direction === "LONG") {
    sl = entry - slDist;
    tp = entry + tpDist;
  } else {
    sl = entry + slDist;
    tp = entry - tpDist;
  }

  return { slPrice: sl, tpPrice: tp, atr, slAtrMult: slMult, rr };
}

async function fetchDerivativesTickers() {
  const { data } = await httpGecko.get("/derivatives");
  return Array.isArray(data) ? data : [];
}

function normalizeSymbol(s) {
  return String(s || "").toUpperCase().replace("-", "").replace("_", "");
}

function pickDerivativeRowForSymbol(rows, symbol, marketFilterLower) {
  const target = normalizeSymbol(symbol);
  const matches = rows.filter((r) => normalizeSymbol(r.symbol) === target);
  if (!matches.length) return null;
  const preferred = matches.find((r) => String(r.market || "").toLowerCase().includes(marketFilterLower));
  return preferred || matches[0];
}

async function evaluateIntrabarForSymbol({ symbol, minConf }) {
  const st = await getState(symbol);
  if (nowMs() < st.cooldown_until_ts) return null;

  const coinapiSymbolId = buildCoinapiSymbolId(symbol);
  if (!coinapiSymbolId) return null;

  const ohlcv = await fetchOHLCV(coinapiSymbolId);
  if (!ohlcv.length) return null;

  const c = parseCoinapiCandles(ohlcv);
  if (c.close.length < CONFIG.ATR_PERIOD + 5) return null;

  const ind = computeIndicators(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return null;

  const pct24 = computePctChange24hFromCandles(c);
  const trend = classifyTrend(pct24, ind);
  const momentum = computeMomentum(ind);
  const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return null;

  let direction = cand.dir;
  let isReversal = false;
  if (cand.dir === "LONG_CANDIDATE") { direction = "LONG"; isReversal = true; }
  if (cand.dir === "SHORT_CANDIDATE") { direction = "SHORT"; isReversal = true; }

  let conf = baseConfidence(trend, momentum, pct24);
  if (Number.isFinite(minConf) && conf < minConf) return null;

  const entry = Number(ind.lastClose);
  if (!(entry > 0)) return null;

  const tpsl = pickTpSlATR({
    entry,
    atr: ind.atr,
    direction,
    confidence: conf,
    momentumStrength: momentum.strength,
    isReversal,
    bbWidthClass: ind.bbWidthClass,
  });
  if (!tpsl) return null;

  const bucketMs = floorTimeBucket(nowMs(), CONFIG.DEDUPE_SCOPE_SECONDS);
  const sigId = `PREVIEW|${symbol}|${direction}|${bucketMs}`;

  const deduped = await tryDedupeOrDrop(sigId);
  if (!deduped) return null;

  const pendingObj = {
    signal_id: sigId,
    direction,
    created_ts: nowMs(),
    confirm_requested: false,
    snapshot_metrics: {
      momentumScore: momentum.score,
      bbWidthClass: ind.bbWidthClass,
      atr: ind.atr,
      lastCandleCloseTime: ind.lastCandleCloseTime,
    },
  };

  await Promise.all([
    savePending(symbol, pendingObj),
    saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "PREVIEW" }),
  ]);

  return {
    id: sigId,
    symbol,
    mode: "PREVIEW",
    direction,
    confidence: Math.round(conf),
    entryRefPrice: entry,
    tpPrice: tpsl.tpPrice,
    slPrice: tpsl.slPrice,
    meta: {
      atr: tpsl.atr,
      slAtrMult: tpsl.slAtrMult,
      rr: tpsl.rr,
      price_precision: smartDecimals(entry),
      coinapi_symbol_id: coinapiSymbolId,
      coingecko_market: null,
    },
    reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "ATR_TPSL", "COINAPI_OHLCV_ENTRY"],
    warnings: [],
    timestamps: { created: nowMs() },
  };
}

async function userRequestConfirm(symbol) {
  const st = await getState(symbol);
  const p = st.pending_confirm;
  if (!p) return { ok: false, reason: "NO_PENDING_PREVIEW" };
  p.confirm_requested = true;
  await savePending(symbol, p);
  return { ok: true };
}

async function invalidate(symbol, reason) {
  const cooldownUntil = nowMs() + CONFIG.COOLDOWN_INVALIDATED_MIN * 60_000;
  const invId = `INVALID|${symbol}|${floorTimeBucket(nowMs(), 60)}|${reason}`;
  await Promise.all([
    saveCooldown(symbol, cooldownUntil),
    clearPending(symbol),
    saveLastSignal(symbol, { ts: nowMs(), id: invId, mode: "INVALIDATED", reason }),
  ]);
  return {
    id: invId,
    symbol,
    mode: "INVALIDATED",
    direction: "NONE",
    confidence: 0,
    entryRefPrice: null,
    tpPrice: null,
    slPrice: null,
    meta: {},
    reasons: [reason],
    warnings: [],
    timestamps: { created: nowMs() },
  };
}

async function confirmOnCloseForSymbol({ symbol, minConfConfirmed }) {
  const st = await getState(symbol);
  const p = st.pending_confirm;
  if (!p || p.confirm_requested !== true) return null;

  const coinapiSymbolId = buildCoinapiSymbolId(symbol);
  if (!coinapiSymbolId) return null;

  const ohlcv = await fetchOHLCV(coinapiSymbolId);
  if (!ohlcv.length) return await invalidate(symbol, "CLOSE_NO_OHLCV");

  const c = parseCoinapiCandles(ohlcv);
  if (c.close.length < CONFIG.ATR_PERIOD + 5) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const ind = computeIndicators(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const pct24 = computePctChange24hFromCandles(c);
  const trend = classifyTrend(pct24, ind);
  const momentum = computeMomentum(ind);
  const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return await invalidate(symbol, "CLOSE_NO_TRADE");

  let directionNow = cand.dir;
  let isReversalNow = false;
  if (cand.dir === "LONG_CANDIDATE") { directionNow = "LONG"; isReversalNow = true; }
  if (cand.dir === "SHORT_CANDIDATE") { directionNow = "SHORT"; isReversalNow = true; }
  if (directionNow !== p.direction) return await invalidate(symbol, "CLOSE_DIRECTION_CHANGED");

  let conf = baseConfidence(trend, momentum, pct24);
  if (Number.isFinite(minConfConfirmed) && conf < minConfConfirmed) return await invalidate(symbol, "CLOSE_CONFIDENCE_LOW");

  const entry = Number(ind.lastClose);
  if (!(entry > 0)) return await invalidate(symbol, "CLOSE_BAD_ENTRY");

  const tpsl = pickTpSlATR({
    entry,
    atr: ind.atr,
    direction: directionNow,
    confidence: conf,
    momentumStrength: momentum.strength,
    isReversal: isReversalNow,
    bbWidthClass: ind.bbWidthClass,
  });
  if (!tpsl) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const candleCloseTime = ind.lastCandleCloseTime;
  const sigId = `CONFIRMED|${symbol}|${directionNow}|${candleCloseTime}`;

  const deduped = await tryDedupeOrDrop(sigId);
  if (!deduped) return null;

  const cooldownUntil = nowMs() + CONFIG.COOLDOWN_CONFIRMED_MIN * 60_000;

  await Promise.all([
    saveCooldown(symbol, cooldownUntil),
    clearPending(symbol),
    saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "CONFIRMED" }),
  ]);

  return {
    id: sigId,
    symbol,
    mode: "CONFIRMED",
    direction: directionNow,
    confidence: Math.round(conf),
    entryRefPrice: entry,
    tpPrice: tpsl.tpPrice,
    slPrice: tpsl.slPrice,
    meta: {
      atr: tpsl.atr,
      slAtrMult: tpsl.slAtrMult,
      rr: tpsl.rr,
      price_precision: smartDecimals(entry),
      coinapi_symbol_id: coinapiSymbolId,
      coingecko_market: null,
    },
    reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "CONFIRMED_ON_CLOSE", "ATR_TPSL"],
    warnings: [],
    timestamps: { created: p.created_ts, confirmed: nowMs() },
  };
}

export async function handler(event) {
  const t0 = nowMs();

  const watchlist = parseWatchlist();
  if (!watchlist.length) return json(400, { ok: false, error: "WATCHLIST env is empty." });
  if (!COINAPI_KEY) return json(500, { ok: false, error: "COINAPI_KEY env is missing." });

  const qs = event?.queryStringParameters || {};
  const mode = String(qs.mode || "intrabar").toLowerCase();
  const minConf = Number.isFinite(Number(qs.min_conf)) ? clamp(Number(qs.min_conf), 0, 100) : CONFIG.MIN_CONF_SHOW;
  const minConfConfirmed = Number.isFinite(Number(qs.min_conf_confirmed))
    ? clamp(Number(qs.min_conf_confirmed), 0, 100)
    : minConf;

  if (event.httpMethod === "POST" && mode === "wait_close") {
    let body = {};
    try { body = event.body ? JSON.parse(event.body) : {}; } catch { body = {}; }
    const symbol = String(body.symbol || "").toUpperCase();
    if (!symbol || !watchlist.includes(symbol)) return json(400, { ok: false, error: "Invalid symbol" });

    const res = await userRequestConfirm(symbol);
    return json(200, { ok: true, result: res });
  }

  let geckoRows = [];
  try { geckoRows = await fetchDerivativesTickers(); } catch { geckoRows = []; }

  const attachFundingAndMarket = (sig) => {
    const row = pickDerivativeRowForSymbol(geckoRows, sig.symbol, COINGECKO_MARKET_FILTER);
    if (!row) return sig;

    sig.meta = sig.meta || {};
    sig.meta.coingecko_market = row.market || null;

    const funding = row.funding_rate;
    if (Number.isFinite(funding) && Math.abs(funding) >= CONFIG.FUNDING_RATE_ABS_WARN) {
      sig.warnings = sig.warnings || [];
      if (!sig.warnings.includes("FUNDING_RATE_HIGH")) sig.warnings.push("FUNDING_RATE_HIGH");
    }
    return sig;
  };

  try {
    if (mode === "close") {
      const tasks = watchlist.map((symbol) =>
        confirmOnCloseForSymbol({ symbol, minConfConfirmed })
          .then((x) => (x ? attachFundingAndMarket(x) : null))
          .catch((err) => ({ id: `ERR|${symbol}`, symbol, mode: "ERROR", error: String(err?.message || err) }))
      );

      const results = await Promise.all(tasks);
      return json(200, {
        ok: true,
        mode: "close",
        watchlist,
        signals: results.filter((x) => x && x.mode !== "ERROR"),
        errors: results.filter((x) => x && x.mode === "ERROR"),
        ms: nowMs() - t0,
      });
    }

    const tasks = watchlist.map((symbol) =>
      evaluateIntrabarForSymbol({ symbol, minConf })
        .then((x) => (x ? attachFundingAndMarket(x) : null))
        .catch((err) => ({ id: `ERR|${symbol}`, symbol, mode: "ERROR", error: String(err?.message || err) }))
    );

    const results = await Promise.all(tasks);
    return json(200, {
      ok: true,
      mode: "intrabar",
      watchlist,
      min_conf: minConf,
      signals: results.filter((x) => x && x.mode !== "ERROR" && x.mode !== "INVALIDATED"),
      errors: results.filter((x) => x && x.mode === "ERROR"),
      ms: nowMs() - t0,
    });
  } catch (e) {
    return json(500, { ok: false, error: String(e?.message || e), ms: nowMs() - t0 });
  }
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
    },
    body: JSON.stringify(body),
  };
}
