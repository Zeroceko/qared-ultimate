// netlify/functions/trade-bot.js
import axios from "axios";
import { RSI, MACD, BollingerBands, ATR } from "technicalindicators";
import { Redis } from "@upstash/redis";

const CONFIG = {
  MIN_CONF_SHOW: 0, MIN_CONF_CONFIRMED: 0,
  COOLDOWN_CONFIRMED_MIN: 30, COOLDOWN_INVALIDATED_MIN: 15,
  ATR_PERIOD: 14, SL_ATR_MULT_LOW: 2.0, SL_ATR_MULT_HIGH: 3.0,
  RR_BASE: 1.5, RR_HIGH: 2.0, MIN_SL_BPS: 10, MIN_TP_BPS: 15,
  LIQ_THR_HIGH: 8, LIQ_THR_MED: 6, LIQ_THR_LOW: 4,
  LIQ_PENALTY_HIGH: 10, LIQ_PENALTY_MED: 7, LIQ_PENALTY_LOW: 4, LIQ_BONUS_ALIGN: 5,
  FUNDING_WARN_MINUTES: 30, FUNDING_RATE_ABS_WARN: 0.0002,
  TTL_COOLDOWN_SEC: 6 * 3600, TTL_PENDING_SEC: 2 * 3600, TTL_LASTSIG_SEC: 6 * 3600,
  DEDUPE_TTL_SECONDS: 120, DEDUPE_SCOPE_SECONDS: 120, KLINE_LIMIT: 200,
  TREND_UP_PCT: 2.0, TREND_DN_PCT: -2.0,
};

// CoinAPI Configuration
const COINAPI_BASE = "https://rest.coinapi.io";
const COINAPI_KEY = process.env.COINAPI_KEY || "22800771-49de-40b7-a534-e29f555258d5"; // Must be set in environment
const COINAPI_EXCHANGE_ID = "BINANCEFTS"; // Binance Futures default in CoinAPI

const redis = Redis.fromEnv();
const http = axios.create({
  baseURL: COINAPI_BASE,
  timeout: 15000,
  headers: { "X-CoinAPI-Key": COINAPI_KEY }
});

function nowMs() { return Date.now(); }
function floorTimeBucket(ms, bucketSec) { const b = bucketSec * 1000; return Math.floor(ms / b) * b; }
function clamp(x, a, b) { return Math.max(a, Math.min(b, x)); }

function parseWatchlist() {
  const raw = (process.env.WATCHLIST || "").trim();
  if (!raw) return [];
  return raw.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
}

function minutesTo(tsMs) { return Math.floor((tsMs - nowMs()) / 60000); }
function bpsToDist(price, bps) { return price * (bps / 10000); }

// Redis Keys
const kCooldown = (sym) => `sig:cooldown:${sym}`;
const kPending = (sym) => `sig:pending:${sym}`;
const kLastSig = (sym) => `sig:lastsig:${sym}`;
const kDedupe = (id) => `sig:dedupe:${id}`;
const kLiq = (sym) => `liq:${sym}`;

async function getState(symbol) {
  const [cooldown, pending, lastsig] = await Promise.all([
    redis.get(kCooldown(symbol)), redis.get(kPending(symbol)), redis.get(kLastSig(symbol)),
  ]);
  return { cooldown_until_ts: cooldown?.cooldown_until_ts ?? 0, pending_confirm: pending ?? null, last_signal: lastsig ?? null };
}

async function saveCooldown(symbol, cooldownUntilMs) { await redis.set(kCooldown(symbol), { cooldown_until_ts: cooldownUntilMs }, { ex: CONFIG.TTL_COOLDOWN_SEC }); }
async function savePending(symbol, pendingObj) { await redis.set(kPending(symbol), pendingObj, { ex: CONFIG.TTL_PENDING_SEC }); }
async function clearPending(symbol) { await redis.del(kPending(symbol)); }
async function saveLastSignal(symbol, obj) { await redis.set(kLastSig(symbol), obj, { ex: CONFIG.TTL_LASTSIG_SEC }); }
async function tryDedupeOrDrop(signalId) { const ok = await redis.set(kDedupe(signalId), 1, { nx: true, ex: CONFIG.DEDUPE_TTL_SECONDS }); return ok === "OK"; }

// --- CoinAPI Data Fetching ---

async function fetchCoinApiSymbols() {
  const { data } = await http.get("/v1/symbols", {
    params: { filter_exchange_id: COINAPI_EXCHANGE_ID, filter_asset_id: "USDT" }
  });
  return data;
}

async function fetchTickerDaily(symbolId) {
  const { data } = await http.get(`/v1/ohlcv/${symbolId}/history`, {
    params: { period_id: "1DAY", limit: 1 }
  });
  if (!data || !data.length) throw new Error(`No daily OHLCV data for ${symbolId}`);
  const candle = data[0];
  return {
    symbol: symbolId,
    priceChangePercent: ((candle.price_close - candle.price_open) / candle.price_open) * 100,
    highPrice: candle.price_high,
    lowPrice: candle.price_low,
    lastPrice: candle.price_close,
    volume: candle.volume_traded
  };
}

async function fetchMarketData(symbolId) {
  try {
    return {
      markPrice: 0,
      lastFundingRate: 0,
      nextFundingTime: 0
    };
  } catch (err) {
    return { markPrice: 0, lastFundingRate: 0, nextFundingTime: 0 };
  }
}

async function fetchKlines1h(symbolId, limit) {
  const { data } = await http.get(`/v1/ohlcv/${symbolId}/history`, {
    params: { period_id: "1HRS", limit: limit }
  });
  if (!data || !data.length) throw new Error(`No hourly OHLCV data for ${symbolId}`);
  return data.sort((a, b) => new Date(a.time_period_start) - new Date(b.time_period_start));
}

function tickDecimals(tickSize) {
  if (!tickSize) return 2;
  const s = String(tickSize);
  if (!s.includes(".")) return 0;
  return s.split(".")[1].replace(/0+$/, "").length;
}

function roundToTick(price, tickSize, mode) {
  const step = Number(tickSize);
  if (!Number.isFinite(price) || !Number.isFinite(step) || step <= 0) return price;
  const eps = 1e-12;
  const raw = price / step;
  let n;
  if (mode === "up") n = Math.ceil(raw - eps);
  else if (mode === "down") n = Math.floor(raw + eps);
  else n = Math.round(raw);
  const dec = tickDecimals(tickSize);
  const out = n * step;
  return Number(out.toFixed(dec));
}

function roundSLTP(entry, sl, tp, tickSize, direction) {
  if (direction === "LONG") {
    const slR = roundToTick(sl, tickSize, "down");
    const tpR = roundToTick(tp, tickSize, "up");
    return { slPrice: Math.min(slR, entry - Number(tickSize)), tpPrice: Math.max(tpR, entry + Number(tickSize)) };
  }
  const slR = roundToTick(sl, tickSize, "up");
  const tpR = roundToTick(tp, tickSize, "down");
  return { slPrice: Math.max(slR, entry + Number(tickSize)), tpPrice: Math.min(tpR, entry - Number(tickSize)) };
}

function parseCandleRows(data) {
  const open = [], high = [], low = [], close = [], volume = [], closeTime = [];
  for (const d of data) {
    open.push(d.price_open);
    high.push(d.price_high);
    low.push(d.price_low);
    close.push(d.price_close);
    volume.push(d.volume_traded);
    closeTime.push(new Date(d.time_period_end).getTime());
  }
  return { open, high, low, close, volume, closeTime };
}

function computeIndicatorsFromCandles(c) {
  const closes = c.close, highs = c.high, lows = c.low, vols = c.volume;
  const rsiArr = RSI.calculate({ values: closes, period: 14 }); const rsi = rsiArr.length ? rsiArr[rsiArr.length - 1] : null;
  const macdArr = MACD.calculate({ values: closes, fastPeriod: 12, slowPeriod: 26, signalPeriod: 9, SimpleMAOscillator: false, SimpleMASignal: false }); const macd = macdArr.length ? macdArr[macdArr.length - 1] : null;
  const bbArr = BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 }); const bb = bbArr.length ? bbArr[bbArr.length - 1] : null;
  const atrArr = ATR.calculate({ high: highs, low: lows, close: closes, period: CONFIG.ATR_PERIOD }); const atr = atrArr.length ? atrArr[atrArr.length - 1] : null;
  const lookback = 20; const lastVol = vols[vols.length - 1];
  const avgVol = vols.length >= lookback ? vols.slice(vols.length - lookback).reduce((a, b) => a + b, 0) / lookback : vols.reduce((a, b) => a + b, 0) / Math.max(1, vols.length);
  const volumeRatio = avgVol > 0 ? lastVol / avgVol : 1;
  let bbWidthClass = "mid", bbPos = 0;
  if (bb && bb.middle && bb.upper && bb.lower) {
    const widthPct = (bb.upper - bb.lower) / bb.middle; if (widthPct >= 0.04) bbWidthClass = "high"; else if (widthPct <= 0.02) bbWidthClass = "low";
    const lastClose = closes[closes.length - 1]; if (lastClose > bb.middle) bbPos = 1; else if (lastClose < bb.middle) bbPos = -1;
  }
  return { rsi, macd, bb, bbWidthClass, bbPos, atr, volumeRatio, lastClose: closes[closes.length - 1], lastCandleCloseTime: c.closeTime[c.closeTime.length - 1] };
}

function classifyTrend(tickerDaily, ind) {
  const chg = Number(tickerDaily.priceChangePercent);
  let base = "SIDE";
  if (chg > CONFIG.TREND_UP_PCT) base = "UP";
  else if (chg < CONFIG.TREND_DN_PCT) base = "DOWN";

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
  const rsi = ind.rsi, vr = ind.volumeRatio, bbw = ind.bbWidthClass; let score = 0;
  if (rsi != null) { if (rsi >= 60) score += 2; else if (rsi >= 52) score += 1; else if (rsi <= 40) score -= 2; else if (rsi <= 48) score -= 1; }
  if (vr >= 1.5) score += 2; else if (vr >= 1.1) score += 1; else if (vr <= 0.7) score -= 1;
  if (bbw === "high") score += 1; if (bbw === "low") score -= 1;
  let strength = "WEAK"; if (Math.abs(score) >= 4) strength = "STRONG"; else if (Math.abs(score) >= 2) strength = "MED"; return { score, strength };
}

function directionCandidate(trend, ind) {
  const rsi = ind.rsi, macdH = ind.macd?.histogram;
  if (trend === "UP") { if (rsi != null && rsi >= 70) return { dir: "SHORT_CANDIDATE", reason: "RSI_OVERBOUGHT_IN_UPTREND" }; return { dir: "LONG", reason: "UPTREND_RSI_OK" }; }
  if (trend === "DOWN") { if (rsi != null && rsi <= 30) return { dir: "LONG_CANDIDATE", reason: "RSI_OVERSOLD_IN_DOWNTREND" }; return { dir: "SHORT", reason: "DOWNTREND_RSI_OK" }; }
  if (rsi != null && macdH != null) { if (rsi > 55 && macdH > 0) return { dir: "LONG", reason: "SIDE_BULL_BIAS" }; if (rsi < 45 && macdH < 0) return { dir: "SHORT", reason: "SIDE_BEAR_BIAS" }; }
  return { dir: "NO_TRADE", reason: "SIDE_NO_EDGE" };
}

function baseConfidence(trend, momentum, tickerDaily) {
  const high = Number(tickerDaily.highPrice), low = Number(tickerDaily.lowPrice), last = Number(tickerDaily.lastPrice);
  const vol = last > 0 ? ((high - low) / last) * 100 : 0;
  let conf = 50;
  const aligned = (trend === "UP" && momentum.score > 0) || (trend === "DOWN" && momentum.score < 0) || (trend === "SIDE" && Math.abs(momentum.score) >= 2);
  conf += aligned ? 10 : -8;
  if (momentum.strength === "STRONG") conf += 15; else if (momentum.strength === "MED") conf += 8; else conf -= 5;
  if (vol > 5) conf += 8; else if (vol > 2) conf += 4; else conf -= 3;
  return clamp(conf, 0, 100);
}

async function getLiquidationFromRedis(symbol) {
  const x = await redis.get(kLiq(symbol)); if (!x) return { intensityScore: 0, dirBias: 0, notionalSum: 0 };
  return { intensityScore: Number(x.intensityScore ?? 0), dirBias: Number(x.dirBias ?? 0), notionalSum: Number(x.notionalSum ?? 0) };
}

function applyLiquidationAdjustments(direction, confidence, liq) {
  const warnings = []; let veto = false; let conf = confidence;
  if (direction === "LONG" && liq.dirBias === +1 && liq.intensityScore >= CONFIG.LIQ_THR_HIGH) veto = true;
  if (direction === "SHORT" && liq.dirBias === -1 && liq.intensityScore >= CONFIG.LIQ_THR_HIGH) veto = true;
  if (liq.intensityScore >= CONFIG.LIQ_THR_HIGH) { conf -= CONFIG.LIQ_PENALTY_HIGH; warnings.push("LIQ_INTENSITY_HIGH"); }
  else if (liq.intensityScore >= CONFIG.LIQ_THR_MED) { conf -= CONFIG.LIQ_PENALTY_MED; warnings.push("LIQ_INTENSITY_MED"); }
  else if (liq.intensityScore >= CONFIG.LIQ_THR_LOW) { conf -= CONFIG.LIQ_PENALTY_LOW; warnings.push("LIQ_INTENSITY_LOW"); }
  const aligns = (direction === "SHORT" && liq.dirBias === +1) || (direction === "LONG" && liq.dirBias === -1);
  if (aligns) { conf += CONFIG.LIQ_BONUS_ALIGN; warnings.push("LIQ_ALIGNS_WITH_SIGNAL"); }
  return { confidence: clamp(conf, 0, 100), veto, warnings };
}

function chooseSLMult(confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass) {
  let mult = CONFIG.SL_ATR_MULT_LOW; if (isReversal) mult = Math.max(mult, 2.5); if (confidence < 68) mult = Math.max(mult, 2.5);
  if (liqIntensity >= CONFIG.LIQ_THR_MED) mult = Math.max(mult, 2.5); if (bbWidthClass === "high") mult = Math.max(mult, 2.5); return Math.min(CONFIG.SL_ATR_MULT_HIGH, mult);
}

function chooseRR(confidence, momentumStrength) { if (confidence >= 75 && momentumStrength === "STRONG") return CONFIG.RR_HIGH; return CONFIG.RR_BASE; }

function pickTpSlATR({ entry, atr, direction, confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass }) {
  if (!(atr > 0)) return null; const slMult = chooseSLMult(confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass); const rr = chooseRR(confidence, momentumStrength);
  let slDist = slMult * atr; let tpDist = rr * slDist; slDist = Math.max(slDist, bpsToDist(entry, CONFIG.MIN_SL_BPS)); tpDist = Math.max(tpDist, bpsToDist(entry, CONFIG.MIN_TP_BPS));
  let sl, tp; if (direction === "LONG") { sl = entry - slDist; tp = entry + tpDist; } else { sl = entry + slDist; tp = entry - tpDist; }
  return { slPrice: sl, tpPrice: tp, atr, slAtrMult: slMult, rr };
}

function makePreviewId(symbol, direction, bucketMs) { return `PREVIEW|${symbol}|${direction}|${bucketMs}`; }
function makeConfirmedId(symbol, direction, candleCloseTimeMs) { return `CONFIRMED|${symbol}|${direction}|${candleCloseTimeMs}`; }

async function evaluateIntrabarForSymbol({ symbol, symbolId, tickSize, tickerDaily, marketData }) {
  // Skip cooldown check temporarily for debugging
  // const st = await getState(symbol); if (nowMs() < st.cooldown_until_ts) return null;
  if (!tickerDaily) return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction: "NONE", confidence: 0, entryRefPrice: 0, reasons: ["NO_TICKER_DATA"], warnings: [], timestamps: { created: nowMs() } };

  const krows = await fetchKlines1h(symbolId, CONFIG.KLINE_LIMIT);
  const c = parseCandleRows(krows);

  if (c.close.length < CONFIG.ATR_PERIOD + 2) return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction: "NONE", confidence: 0, entryRefPrice: Number(tickerDaily.lastPrice), reasons: [`INSUFFICIENT_DATA_${c.close.length}_candles`], warnings: [], timestamps: { created: nowMs() } };

  const ind = computeIndicatorsFromCandles(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction: "NONE", confidence: 0, entryRefPrice: Number(tickerDaily.lastPrice), reasons: ["INDICATORS_NULL"], warnings: [], timestamps: { created: nowMs() } };

  const trend = classifyTrend(tickerDaily, ind); const momentum = computeMomentum(ind); const cand = directionCandidate(trend, ind);

  let direction = cand.dir, isReversal = false;
  if (cand.dir === "LONG_CANDIDATE") { direction = "LONG"; isReversal = true; } else if (cand.dir === "SHORT_CANDIDATE") { direction = "SHORT"; isReversal = true; }

  if (cand.dir === "NO_TRADE") {
    return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction: "NONE", confidence: 0, entryRefPrice: Number(tickerDaily.lastPrice), reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "NO_TRADE_EDGE"], warnings: [], timestamps: { created: nowMs() } };
  }

  let conf = baseConfidence(trend, momentum, tickerDaily);
  const liq = await getLiquidationFromRedis(symbol);
  const liqAdj = applyLiquidationAdjustments(direction, conf, liq); conf = liqAdj.confidence;

  const entry = Number(tickerDaily.lastPrice);
  if (!(entry > 0)) return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction, confidence: conf, entryRefPrice: 0, reasons: ["BAD_ENTRY_PRICE"], warnings: [], timestamps: { created: nowMs() } };

  const warnings = [...liqAdj.warnings];
  if (marketData.nextFundingTime > 0) {
    const nextFundingMs = Number(marketData.nextFundingTime);
    const lastFundingRate = Number(marketData.lastFundingRate);
    if (Number.isFinite(nextFundingMs) && minutesTo(nextFundingMs) <= CONFIG.FUNDING_WARN_MINUTES && Math.abs(lastFundingRate) >= CONFIG.FUNDING_RATE_ABS_WARN) { warnings.push("FUNDING_SOON_HIGH_RATE"); }
  }

  const tpsl = pickTpSlATR({ entry, atr: ind.atr, direction, confidence: conf, momentumStrength: momentum.strength, isReversal, liqIntensity: liq.intensityScore, bbWidthClass: ind.bbWidthClass });
  if (!tpsl) return { id: `SKIP|${symbol}`, symbol, mode: "PREVIEW", direction, confidence: conf, entryRefPrice: entry, reasons: ["ATR_TPSL_FAILED"], warnings, timestamps: { created: nowMs() } };

  const rounded = roundSLTP(entry, tpsl.slPrice, tpsl.tpPrice, tickSize, direction);
  const bucketMs = floorTimeBucket(nowMs(), CONFIG.DEDUPE_SCOPE_SECONDS);
  const sigId = makePreviewId(symbol, direction, bucketMs);
  // Skip dedupe for debugging - always show signals
  // const deduped = await tryDedupeOrDrop(sigId); if (!deduped) return null;

  const signal = { id: sigId, symbol, mode: "PREVIEW", direction, confidence: conf, entryRefPrice: entry, tpPrice: rounded.tpPrice, slPrice: rounded.slPrice, meta: { atr: tpsl.atr, slAtrMult: tpsl.slAtrMult, rr: tpsl.rr, tickSize }, reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "ATR_TPSL", "TICK_ROUNDED"], warnings, timestamps: { created: nowMs() }, };

  try {
    const pendingObj = { signal_id: sigId, direction, created_ts: nowMs(), confirm_requested: false, snapshot_metrics: { momentumScore: momentum.score, bbWidthClass: ind.bbWidthClass, atr: ind.atr, lastCandleCloseTime: ind.lastCandleCloseTime }, };
    await Promise.all([savePending(symbol, pendingObj), saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "PREVIEW" })]);
  } catch (redisErr) {
    signal.warnings.push("REDIS_SAVE_FAILED");
  }

  return signal;
}

async function userRequestConfirm(symbol) {
  const st = await getState(symbol); const p = st.pending_confirm; if (!p) return { ok: false, reason: "NO_PENDING_PREVIEW" };
  p.confirm_requested = true; await savePending(symbol, p); return { ok: true };
}

async function confirmOnCloseForSymbol({ symbol, symbolId, tickSize, tickerDaily, marketData }) {
  const st = await getState(symbol); const p = st.pending_confirm; if (!p) return null; if (p.confirm_requested !== true) return null;
  if (!tickerDaily) return null;

  const krows = await fetchKlines1h(symbolId, CONFIG.KLINE_LIMIT); const c = parseCandleRows(krows);
  if (c.close.length < CONFIG.ATR_PERIOD + 2) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const ind = computeIndicatorsFromCandles(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const trend = classifyTrend(tickerDaily, ind); const momentum = computeMomentum(ind); const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return await invalidate(symbol, "CLOSE_NO_TRADE");

  let directionNow = cand.dir, isReversalNow = false;
  if (cand.dir === "LONG_CANDIDATE") { directionNow = "LONG"; isReversalNow = true; } else if (cand.dir === "SHORT_CANDIDATE") { directionNow = "SHORT"; isReversalNow = true; }

  if (directionNow !== p.direction) return await invalidate(symbol, "CLOSE_DIRECTION_CHANGED");
  if (isReversalNow) { const ok = (directionNow === "SHORT" && ind.rsi < 70) || (directionNow === "LONG" && ind.rsi > 30) || (momentum.score < (p.snapshot_metrics?.momentumScore ?? momentum.score)) || (momentum.score > (p.snapshot_metrics?.momentumScore ?? momentum.score)); if (!ok) return await invalidate(symbol, "CLOSE_REVERSAL_NOT_CONFIRMED"); }

  let conf = baseConfidence(trend, momentum, tickerDaily); const liq = await getLiquidationFromRedis(symbol);
  const liqAdj = applyLiquidationAdjustments(directionNow, conf, liq); conf = liqAdj.confidence; if (liqAdj.veto) return await invalidate(symbol, "CLOSE_LIQ_VETO");
  if (conf < CONFIG.MIN_CONF_CONFIRMED) return await invalidate(symbol, "CLOSE_CONFIDENCE_LOW");

  const entry = Number(tickerDaily.lastPrice); if (!(entry > 0)) return await invalidate(symbol, "CLOSE_BAD_ENTRY");

  const warnings = [...liqAdj.warnings];
  if (marketData.nextFundingTime > 0) {
    const nextFundingMs = Number(marketData.nextFundingTime); const lastFundingRate = Number(marketData.lastFundingRate);
    if (Number.isFinite(nextFundingMs) && minutesTo(nextFundingMs) <= CONFIG.FUNDING_WARN_MINUTES && Math.abs(lastFundingRate) >= CONFIG.FUNDING_RATE_ABS_WARN) { warnings.push("FUNDING_SOON_HIGH_RATE"); }
  }

  const tpsl = pickTpSlATR({ entry, atr: ind.atr, direction: directionNow, confidence: conf, momentumStrength: momentum.strength, isReversal: isReversalNow, liqIntensity: liq.intensityScore, bbWidthClass: ind.bbWidthClass });
  if (!tpsl) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");

  const rounded = roundSLTP(entry, tpsl.slPrice, tpsl.tpPrice, tickSize, directionNow);
  const candleCloseTime = ind.lastCandleCloseTime; const sigId = makeConfirmedId(symbol, directionNow, candleCloseTime);
  const deduped = await tryDedupeOrDrop(sigId); if (!deduped) return null;

  const signal = { id: sigId, symbol, mode: "CONFIRMED", direction: directionNow, confidence: conf, entryRefPrice: entry, tpPrice: rounded.tpPrice, slPrice: rounded.slPrice, meta: { atr: tpsl.atr, slAtrMult: tpsl.slAtrMult, rr: tpsl.rr, tickSize }, reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "CONFIRMED_ON_CLOSE", "ATR_TPSL", "TICK_ROUNDED"], warnings, timestamps: { created: p.created_ts, confirmed: nowMs() }, };
  const cooldownUntil = nowMs() + CONFIG.COOLDOWN_CONFIRMED_MIN * 60_000;
  await Promise.all([saveCooldown(symbol, cooldownUntil), clearPending(symbol), saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "CONFIRMED" })]);
  return signal;
}

async function invalidate(symbol, reason) {
  const cooldownUntil = nowMs() + CONFIG.COOLDOWN_INVALIDATED_MIN * 60_000; const invId = `INVALID|${symbol}|${floorTimeBucket(nowMs(), 60)}|${reason}`;
  await Promise.all([saveCooldown(symbol, cooldownUntil), clearPending(symbol), saveLastSignal(symbol, { ts: nowMs(), id: invId, mode: "INVALIDATED", reason })]);
  return { id: invId, symbol, mode: "INVALIDATED", direction: "NONE", confidence: 0, entryRefPrice: null, tpPrice: null, slPrice: null, meta: {}, reasons: [reason], warnings: [], timestamps: { created: nowMs() } };
}

function buildSymbolMap(coinApiSymbols) {
  const map = new Map();
  for (const s of coinApiSymbols) {
    // Use asset_id fields for robust matching
    const base = s.asset_id_base || "";
    const quote = s.asset_id_quote || "";
    if (base && quote && s.symbol_type === "PERPETUAL") {
      const shortName = `${base}${quote}`;
      if (!map.has(shortName)) map.set(shortName, s);
    }
  }
  return map;
}

export async function handler(event) {
  const t0 = nowMs(); const watchlist = parseWatchlist();
  if (!watchlist.length) return json(400, { ok: false, error: "WATCHLIST env is empty." });

  if (!COINAPI_KEY) return json(500, { ok: false, error: "COINAPI_KEY is not set." });

  const mode = (event.queryStringParameters?.mode || "intrabar").toLowerCase();

  if (event.httpMethod === "POST" && mode === "wait_close") {
    let body = {}; try { body = event.body ? JSON.parse(event.body) : {}; } catch { body = {}; }
    const symbol = String(body.symbol || "").toUpperCase(); if (!symbol || !watchlist.includes(symbol)) return json(400, { ok: false, error: "Invalid symbol" });
    const res = await userRequestConfirm(symbol); return json(200, { ok: true, result: res });
  }

  try {
    const symbolsData = await fetchCoinApiSymbols();
    if (!symbolsData || !symbolsData.length) return json(500, { ok: false, error: `CoinAPI returned 0 symbols. Check API key.`, ms: nowMs() - t0 });
    const symbolMap = buildSymbolMap(symbolsData);
    const matchedCount = watchlist.filter(s => symbolMap.has(s)).length;

    const tasks = watchlist.map(async (shortSymbol) => {
      try {
        const symData = symbolMap.get(shortSymbol);
        if (!symData) return { id: `ERR|${shortSymbol}`, symbol: shortSymbol, mode: "ERROR", error: "Symbol not found in CoinAPI" };

        const symbolId = symData.symbol_id;
        const tickSize = symData.price_precision ? String(symData.price_precision) : "0.01";

        // These will now throw if they fail
        const tickerDaily = await fetchTickerDaily(symbolId);
        const marketData = await fetchMarketData(symbolId);

        if (mode === "close") {
          return await confirmOnCloseForSymbol({ symbol: shortSymbol, symbolId, tickSize, tickerDaily, marketData });
        } else {
          return await evaluateIntrabarForSymbol({ symbol: shortSymbol, symbolId, tickSize, tickerDaily, marketData });
        }
      } catch (err) {
        return { id: `ERR|${shortSymbol}`, symbol: shortSymbol, mode: "ERROR", error: String(err?.message || err) };
      }
    });

    const results = await Promise.all(tasks);

    return json(200, { ok: true, mode, watchlist, symbolCount: symbolsData.length, matchedCount, signals: results.filter((x) => x && x.mode !== "ERROR" && x.mode !== "INVALIDATED"), errors: results.filter((x) => x && x.mode === "ERROR"), ms: nowMs() - t0 });

  } catch (e) { return json(500, { ok: false, error: String(e?.message || e), ms: nowMs() - t0 }); }
}

function json(statusCode, body) { return { statusCode, headers: { "content-type": "application/json; charset=utf-8" }, body: JSON.stringify(body) }; }
