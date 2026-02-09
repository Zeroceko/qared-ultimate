// netlify/functions/trade-bot.js
import axios from "axios";
import { RSI, MACD, BollingerBands, ATR } from "technicalindicators";
import { Redis } from "@upstash/redis";
const CONFIG = {
  MIN_CONF_SHOW: 60, MIN_CONF_CONFIRMED: 60,
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
const BINANCE_FAPI_BASE = process.env.BINANCE_FAPI_BASE || "https://fapi.binance.me";
const redis = Redis.fromEnv();
const http = axios.create({ baseURL: BINANCE_FAPI_BASE, timeout: 8500 });
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
async function fetchExchangeInfo() { const { data } = await http.get("/fapi/v1/exchangeInfo"); return data; }
async function fetchTickers24hAll() { const { data } = await http.get("/fapi/v1/ticker/24hr"); return data; }
async function fetchPremiumIndexAll() { const { data } = await http.get("/fapi/v1/premiumIndex"); return data; }
async function fetchKlines1h(symbol, limit) { const { data } = await http.get("/fapi/v1/klines", { params: { symbol, interval: "1h", limit } }); return data; }
function tickDecimals(tickSizeStr) { const s = String(tickSizeStr); if (!s.includes(".")) return 0; return s.split(".")[1].replace(/0+$/, "").length; }
function roundToTick(price, tickSizeStr, mode) {
  const step = Number(tickSizeStr); if (!Number.isFinite(price) || !Number.isFinite(step) || step <= 0) return price;
  const eps = 1e-12; const raw = price / step; let n;
  if (mode === "up") n = Math.ceil(raw - eps); else if (mode === "down") n = Math.floor(raw + eps); else n = Math.round(raw);
  const dec = tickDecimals(tickSizeStr); const out = n * step; return Number(out.toFixed(dec));
}
function roundSLTP(entry, sl, tp, tickSizeStr, direction) {
  if (direction === "LONG") {
    const slR = roundToTick(sl, tickSizeStr, "down"); const tpR = roundToTick(tp, tickSizeStr, "up");
    return { slPrice: Math.min(slR, entry - Number(tickSizeStr)), tpPrice: Math.max(tpR, entry + Number(tickSizeStr)) };
  }
  const slR = roundToTick(sl, tickSizeStr, "up"); const tpR = roundToTick(tp, tickSizeStr, "down");
  return { slPrice: Math.max(slR, entry + Number(tickSizeStr)), tpPrice: Math.min(tpR, entry - Number(tickSizeStr)) };
}
function parseCandleRows(rows) {
  const open = [], high = [], low = [], close = [], volume = [], closeTime = [];
  for (const r of rows) {
    open.push(Number(r[1])); high.push(Number(r[2])); low.push(Number(r[3])); close.push(Number(r[4])); volume.push(Number(r[5])); closeTime.push(Number(r[6]));
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
function classifyTrend(ticker24, ind) {
  const chg = Number(ticker24.priceChangePercent); let base = "SIDE";
  if (chg > CONFIG.TREND_UP_PCT) base = "UP"; else if (chg < CONFIG.TREND_DN_PCT) base = "DOWN";
  let score = 0; if (base === "UP") score += 2; if (base === "DOWN") score -= 2;
  if (ind?.macd?.histogram != null) { if (ind.macd.histogram > 0) score += 1; if (ind.macd.histogram < 0) score -= 1; }
  score += ind?.bbPos ?? 0; if (score >= 2) return "UP"; if (score <= -2) return "DOWN"; return "SIDE";
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
function baseConfidence(trend, momentum, ticker24) {
  const high = Number(ticker24.highPrice), low = Number(ticker24.lowPrice), last = Number(ticker24.lastPrice || ticker24.last || ticker24.closePrice || ticker24.price);
  const vol = last > 0 ? ((high - low) / last) * 100 : 0; let conf = 50;
  const aligned = (trend === "UP" && momentum.score > 0) || (trend === "DOWN" && momentum.score < 0) || (trend === "SIDE" && Math.abs(momentum.score) >= 2);
  conf += aligned ? 10 : -8; if (momentum.strength === "STRONG") conf += 15; else if (momentum.strength === "MED") conf += 8; else conf -= 5;
  if (vol > 5) conf += 8; else if (vol > 2) conf += 4; else conf -= 3; return clamp(conf, 0, 100);
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
async function evaluateIntrabarForSymbol({ symbol, tickSize, tickerMap, premiumMap }) {
  const st = await getState(symbol); if (nowMs() < st.cooldown_until_ts) return null;
  const ticker24 = tickerMap.get(symbol); const prem = premiumMap.get(symbol); if (!ticker24 || !prem) return null;
  const krows = await fetchKlines1h(symbol, CONFIG.KLINE_LIMIT); const c = parseCandleRows(krows);
  if (c.close.length < CONFIG.ATR_PERIOD + 2) return null; const ind = computeIndicatorsFromCandles(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return null;
  const trend = classifyTrend(ticker24, ind); const momentum = computeMomentum(ind); const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return null; let direction = cand.dir, isReversal = false;
  if (cand.dir === "LONG_CANDIDATE") { direction = "LONG"; isReversal = true; } else if (cand.dir === "SHORT_CANDIDATE") { direction = "SHORT"; isReversal = true; }
  let conf = baseConfidence(trend, momentum, ticker24); const liq = await getLiquidationFromRedis(symbol);
  const liqAdj = applyLiquidationAdjustments(direction, conf, liq); conf = liqAdj.confidence; if (liqAdj.veto) return null; if (conf < CONFIG.MIN_CONF_SHOW) return null;
  const entry = Number(prem.markPrice); if (!(entry > 0)) return null;
  const warnings = [...liqAdj.warnings]; const nextFundingMs = Number(prem.nextFundingTime); const lastFundingRate = Number(prem.lastFundingRate);
  if (Number.isFinite(nextFundingMs) && minutesTo(nextFundingMs) <= CONFIG.FUNDING_WARN_MINUTES && Math.abs(lastFundingRate) >= CONFIG.FUNDING_RATE_ABS_WARN) { warnings.push("FUNDING_SOON_HIGH_RATE"); }
  const tpsl = pickTpSlATR({ entry, atr: ind.atr, direction, confidence: conf, momentumStrength: momentum.strength, isReversal, liqIntensity: liq.intensityScore, bbWidthClass: ind.bbWidthClass });
  if (!tpsl) return null; const rounded = roundSLTP(entry, tpsl.slPrice, tpsl.tpPrice, tickSize, direction);
  const bucketMs = floorTimeBucket(nowMs(), CONFIG.DEDUPE_SCOPE_SECONDS); const sigId = makePreviewId(symbol, direction, bucketMs);
  const deduped = await tryDedupeOrDrop(sigId); if (!deduped) return null;
  const signal = { id: sigId, symbol, mode: "PREVIEW", direction, confidence: conf, entryRefPrice: entry, tpPrice: rounded.tpPrice, slPrice: rounded.slPrice, meta: { atr: tpsl.atr, slAtrMult: tpsl.slAtrMult, rr: tpsl.rr, tickSize }, reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "ATR_TPSL", "TICK_ROUNDED"], warnings, timestamps: { created: nowMs() }, };
  const pendingObj = { signal_id: sigId, direction, created_ts: nowMs(), confirm_requested: false, snapshot_metrics: { momentumScore: momentum.score, bbWidthClass: ind.bbWidthClass, atr: ind.atr, lastCandleCloseTime: ind.lastCandleCloseTime }, };
  await Promise.all([savePending(symbol, pendingObj), saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "PREVIEW" })]);
  return signal;
}
async function userRequestConfirm(symbol) {
  const st = await getState(symbol); const p = st.pending_confirm; if (!p) return { ok: false, reason: "NO_PENDING_PREVIEW" };
  p.confirm_requested = true; await savePending(symbol, p); return { ok: true };
}
async function confirmOnCloseForSymbol({ symbol, tickSize, tickerMap, premiumMap }) {
  const st = await getState(symbol); const p = st.pending_confirm; if (!p) return null; if (p.confirm_requested !== true) return null;
  const ticker24 = tickerMap.get(symbol); const prem = premiumMap.get(symbol); if (!ticker24 || !prem) return null;
  const krows = await fetchKlines1h(symbol, CONFIG.KLINE_LIMIT); const c = parseCandleRows(krows);
  if (c.close.length < CONFIG.ATR_PERIOD + 2) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");
  const ind = computeIndicatorsFromCandles(c);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return await invalidate(symbol, "CLOSE_ATR_UNAVAILABLE");
  const trend = classifyTrend(ticker24, ind); const momentum = computeMomentum(ind); const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return await invalidate(symbol, "CLOSE_NO_TRADE");
  let directionNow = cand.dir, isReversalNow = false;
  if (cand.dir === "LONG_CANDIDATE") { directionNow = "LONG"; isReversalNow = true; } else if (cand.dir === "SHORT_CANDIDATE") { directionNow = "SHORT"; isReversalNow = true; }
  if (directionNow !== p.direction) return await invalidate(symbol, "CLOSE_DIRECTION_CHANGED");
  if (isReversalNow) { const ok = (directionNow === "SHORT" && ind.rsi < 70) || (directionNow === "LONG" && ind.rsi > 30) || (momentum.score < (p.snapshot_metrics?.momentumScore ?? momentum.score)) || (momentum.score > (p.snapshot_metrics?.momentumScore ?? momentum.score)); if (!ok) return await invalidate(symbol, "CLOSE_REVERSAL_NOT_CONFIRMED"); }
  let conf = baseConfidence(trend, momentum, ticker24); const liq = await getLiquidationFromRedis(symbol);
  const liqAdj = applyLiquidationAdjustments(directionNow, conf, liq); conf = liqAdj.confidence; if (liqAdj.veto) return await invalidate(symbol, "CLOSE_LIQ_VETO");
  if (conf < CONFIG.MIN_CONF_CONFIRMED) return await invalidate(symbol, "CLOSE_CONFIDENCE_LOW");
  const entry = Number(prem.markPrice); if (!(entry > 0)) return await invalidate(symbol, "CLOSE_BAD_ENTRY");
  const warnings = [...liqAdj.warnings]; const nextFundingMs = Number(prem.nextFundingTime); const lastFundingRate = Number(prem.lastFundingRate);
  if (Number.isFinite(nextFundingMs) && minutesTo(nextFundingMs) <= CONFIG.FUNDING_WARN_MINUTES && Math.abs(lastFundingRate) >= CONFIG.FUNDING_RATE_ABS_WARN) { warnings.push("FUNDING_SOON_HIGH_RATE"); }
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
function buildTickSizeMap(exchangeInfo) {
  const map = new Map(); const symbols = exchangeInfo?.symbols || [];
  for (const s of symbols) { const sym = s.symbol; const priceFilter = (s.filters || []).find((f) => f.filterType === "PRICE_FILTER"); const tickSize = priceFilter?.tickSize; if (sym && tickSize) map.set(sym, tickSize); }
  return map;
}
export async function handler(event) {
  const t0 = nowMs(); const watchlist = parseWatchlist();
  if (!watchlist.length) return json(400, { ok: false, error: "WATCHLIST env is empty." });
  const mode = (event.queryStringParameters?.mode || "intrabar").toLowerCase();
  if (event.httpMethod === "POST" && mode === "wait_close") {
    let body = {}; try { body = event.body ? JSON.parse(event.body) : {}; } catch { body = {}; }
    const symbol = String(body.symbol || "").toUpperCase(); if (!symbol || !watchlist.includes(symbol)) return json(400, { ok: false, error: "Invalid symbol" });
    const res = await userRequestConfirm(symbol); return json(200, { ok: true, result: res });
  }
  try {
    const [exchangeInfo, tickersAll, premiumAll] = await Promise.all([fetchExchangeInfo(), fetchTickers24hAll(), fetchPremiumIndexAll()]);
    const tickSizeMap = buildTickSizeMap(exchangeInfo);
    const tickerMap = new Map(); for (const t of tickersAll) { if (watchlist.includes(t.symbol)) tickerMap.set(t.symbol, t); }
    const premiumMap = new Map(); for (const p of premiumAll) { if (watchlist.includes(p.symbol)) premiumMap.set(p.symbol, p); }
    const missing = watchlist.filter((s) => !tickSizeMap.get(s)); if (missing.length) return json(500, { ok: false, error: "Missing tickSize for some symbols", missing });
    if (mode === "close") {
      const tasks = watchlist.map((symbol) => confirmOnCloseForSymbol({ symbol, tickSize: tickSizeMap.get(symbol), tickerMap, premiumMap }).catch((err) => ({ id: `ERR|${symbol}`, symbol, mode: "ERROR", error: String(err?.message || err) })));
      const results = await Promise.all(tasks);
      return json(200, { ok: true, mode: "close", signals: results.filter((x) => x && x.mode !== "ERROR"), errors: results.filter((x) => x && x.mode === "ERROR"), ms: nowMs() - t0 });
    }
    const tasks = watchlist.map((symbol) => evaluateIntrabarForSymbol({ symbol, tickSize: tickSizeMap.get(symbol), tickerMap, premiumMap }).catch((err) => ({ id: `ERR|${symbol}`, symbol, mode: "ERROR", error: String(err?.message || err) })));
    const results = await Promise.all(tasks);
    return json(200, { ok: true, mode: "intrabar", signals: results.filter((x) => x && x.mode !== "ERROR"), errors: results.filter((x) => x && x.mode === "ERROR"), ms: nowMs() - t0 });
  } catch (e) { return json(500, { ok: false, error: String(e?.message || e), ms: nowMs() - t0 }); }
}
function json(statusCode, body) { return { statusCode, headers: { "content-type": "application/json; charset=utf-8" }, body: JSON.stringify(body) }; }

