import axios from "axios";
import { RSI, MACD, BollingerBands, ATR } from "technicalindicators";
import { Redis } from "@upstash/redis";
import crypto from "node:crypto";

/**
 * Binance-free trade-bot (Netlify Functions, Promise.all-parallel)
 *
 * Data sources:
 * - CoinGecko Derivatives Tickers List: funding_rate + index + price (mark-like reference)
 *   GET https://api.coingecko.com/api/v3/derivatives
 * - CoinAPI OHLCV: 1h candles for indicators
 *   GET https://rest.coinapi.io/v1/ohlcv/:symbol_id/history?period_id=1HRS&limit=200
 *
 * State:
 * - Upstash Redis (cooldown, pending_confirm, last_signal, dedupe)
 * - Optional liquidation (rolling window) read from Redis keys: liq:<SYMBOL>
 */

const CONFIG = {
  // gates
  MIN_CONF_SHOW: 60,
  MIN_CONF_CONFIRMED: 60,

  // cooldowns
  COOLDOWN_CONFIRMED_MIN: 30,
  COOLDOWN_INVALIDATED_MIN: 15,

  // ATR/RR
  ATR_PERIOD: 14,
  SL_ATR_MULT_LOW: 2.0,
  SL_ATR_MULT_HIGH: 3.0,
  RR_BASE: 1.5,
  RR_HIGH: 2.0,
  MIN_SL_BPS: 10, // 0.10%
  MIN_TP_BPS: 15, // 0.15%

  // liquidation adjust
  LIQ_THR_HIGH: 8,
  LIQ_THR_MED: 6,
  LIQ_THR_LOW: 4,
  LIQ_PENALTY_HIGH: 10,
  LIQ_PENALTY_MED: 7,
  LIQ_PENALTY_LOW: 4,
  LIQ_BONUS_ALIGN: 5,

  // funding warn
  FUNDING_WARN_MINUTES: 30,
  FUNDING_RATE_ABS_WARN: 0.0002,

  // state TTL
  TTL_COOLDOWN_SEC: 6 * 3600,
  TTL_PENDING_SEC: 2 * 3600,
  TTL_LASTSIG_SEC: 6 * 3600,

  // dedupe
  DEDUPE_TTL_SECONDS: 120,
  DEDUPE_SCOPE_SECONDS: 120,

  // klines
  KLINE_LIMIT: 200,

  // trend thresholds
  TREND_UP_PCT: 2.0,
  TREND_DN_PCT: -2.0,

  // CoinAPI metadata cache
  META_TTL_SEC: 24 * 3600,
};

const redis = Redis.fromEnv();

const COINGECKO_BASE = process.env.COINGECKO_BASE || "https://api.coingecko.com";
const COINGECKO_API_KEY = (process.env.COINGECKO_API_KEY || "").trim(); // optional, but recommended
const COINGECKO_MARKET_FILTER = (process.env.COINGECKO_MARKET_FILTER || "").trim(); 
// e.g. "Binance (Futures)" or leave empty to accept any market

const COINAPI_BASE = process.env.COINAPI_BASE || "https://rest.coinapi.io";
const COINAPI_KEY = (process.env.COINAPI_KEY || "").trim();
const COINAPI_EXCHANGE_ID = (process.env.COINAPI_EXCHANGE_ID || "BINANCE").trim(); 
// exchange_id used to build symbol_id pattern: {exchange}_PERP_{base}_{quote}

if (!COINAPI_KEY) {
  // No throw at import time; handled in handler response.
}

const httpCG = axios.create({
  baseURL: COINGECKO_BASE,
  timeout: 8500,
});

const httpCA = axios.create({
  baseURL: COINAPI_BASE,
  timeout: 8500,
  headers: {
    "X-CoinAPI-Key": COINAPI_KEY,
  },
});

// -----------------------------
// Helpers
// -----------------------------
function nowMs() {
  return Date.now();
}
function clamp(x, a, b) {
  return Math.max(a, Math.min(b, x));
}
function floorBucket(ms, bucketSec) {
  const b = bucketSec * 1000;
  return Math.floor(ms / b) * b;
}
function minutesTo(tsMs) {
  return Math.floor((tsMs - nowMs()) / 60000);
}
function bpsToDist(price, bps) {
  return price * (bps / 10000);
}
function sha1(s) {
  return crypto.createHash("sha1").update(s).digest("hex");
}

// WATCHLIST from env (comma-separated)
function parseWatchlist() {
  const raw = (process.env.WATCHLIST || "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

// Parse symbol like BTCUSDT, ETHUSDT, SOLUSDT, etc.
function splitBaseQuote(sym) {
  const quotes = ["USDT", "USDC", "BUSD", "FDUSD", "TUSD"];
  for (const q of quotes) {
    if (sym.endsWith(q) && sym.length > q.length) {
      return { base: sym.slice(0, -q.length), quote: q };
    }
  }
  // fallback: assume last 4 is quote
  return { base: sym.slice(0, -4), quote: sym.slice(-4) };
}

// CoinAPI symbol_id for PERPETUAL is documented as:
// PERPETUAL `{exchange_id}_PERP_{asset_id_base}_{asset_id_quote}`
// (We generate it deterministically to avoid heavy symbol-list calls.)
function coinapiSymbolIdFor(sym) {
  const { base, quote } = splitBaseQuote(sym);
  return `${COINAPI_EXCHANGE_ID}_PERP_${base}_${quote}`;
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

// -----------------------------
// Redis keys + state
// -----------------------------
const kCooldown = (sym) => `sig:cooldown:${sym}`;
const kPending = (sym) => `sig:pending:${sym}`;
const kLastSig = (sym) => `sig:lastsig:${sym}`;
const kDedupe = (id) => `sig:dedupe:${id}`;

// Optional liquidation (written elsewhere)
const kLiq = (sym) => `liq:${sym}`;

// CoinAPI metadata cache (precision)
const kMeta = (sym) => `coinapi:meta:${sym}`;

async function getState(sym) {
  const [cooldown, pending, lastsig] = await Promise.all([
    redis.get(kCooldown(sym)),
    redis.get(kPending(sym)),
    redis.get(kLastSig(sym)),
  ]);
  return {
    cooldown_until_ts: cooldown?.cooldown_until_ts ?? 0,
    pending_confirm: pending ?? null,
    last_signal: lastsig ?? null,
  };
}

async function saveCooldown(sym, cooldownUntilMs) {
  await redis.set(kCooldown(sym), { cooldown_until_ts: cooldownUntilMs }, { ex: CONFIG.TTL_COOLDOWN_SEC });
}

async function savePending(sym, obj) {
  await redis.set(kPending(sym), obj, { ex: CONFIG.TTL_PENDING_SEC });
}

async function clearPending(sym) {
  await redis.del(kPending(sym));
}

async function saveLastSignal(sym, obj) {
  await redis.set(kLastSig(sym), obj, { ex: CONFIG.TTL_LASTSIG_SEC });
}

async function tryDedupeOrDrop(signalId) {
  const ok = await redis.set(kDedupe(signalId), 1, { nx: true, ex: CONFIG.DEDUPE_TTL_SECONDS });
  return ok === "OK";
}

async function getLiq(sym) {
  const x = await redis.get(kLiq(sym));
  if (!x) return { intensityScore: 0, dirBias: 0, notionalSum: 0 };
  return {
    intensityScore: Number(x.intensityScore ?? 0),
    dirBias: Number(x.dirBias ?? 0), // +1 = longs liquidating, -1 = shorts liquidating, 0 neutral
    notionalSum: Number(x.notionalSum ?? 0),
  };
}

// -----------------------------
// Precision rounding (CoinAPI price_precision)
// -----------------------------
function roundToDecimals(x, decimals) {
  if (!Number.isFinite(x)) return x;
  const d = Math.max(0, Math.min(18, Number(decimals) || 0));
  return Number(x.toFixed(d));
}

async function getCoinapiMeta(sym) {
  // Cached: { price_precision, size_precision }
  const cached = await redis.get(kMeta(sym));
  if (cached?.price_precision != null) return cached;

  // fallback: try to fetch from CoinAPI active symbols endpoint
  // We use documented endpoint and filter_symbol_id to reduce payload. :contentReference[oaicite:4]{index=4}
  const symbolId = coinapiSymbolIdFor(sym);
  // The endpoint is /v1/symbols/:exchange_id/active with optional filtering:
  // query filter_symbol_id=... (parts of symbol identifier) :contentReference[oaicite:5]{index=5}
  const url = `/v1/symbols/${encodeURIComponent(COINAPI_EXCHANGE_ID)}/active`;
  const { data } = await httpCA.get(url, {
    params: {
      filter_symbol_id: symbolId,
    },
  });

  const row = Array.isArray(data) ? data.find((r) => r.symbol_id === symbolId) : null;

  const meta = {
    symbol_id: symbolId,
    price_precision: row?.price_precision ?? 2,
    size_precision: row?.size_precision ?? 4,
  };

  await redis.set(kMeta(sym), meta, { ex: CONFIG.META_TTL_SEC });
  return meta;
}

// -----------------------------
// CoinGecko derivatives fetch + mapping
// -----------------------------
function normalizeDerivSymbol(s) {
  // Normalize "BTCUSDT", "BTC-PERPUSDT", "BTC-PERP" into something we can match
  return String(s || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "") // remove '-', '/'
    .replace(/PERP/g, "");     // drop PERP token
}

async function fetchCoinGeckoDerivatives() {
  const headers = {};
  // docs show 'x-cg-demo-api-key' for demo/pro keys :contentReference[oaicite:6]{index=6}
  if (COINGECKO_API_KEY) headers["x-cg-demo-api-key"] = COINGECKO_API_KEY;

  const { data } = await httpCG.get("/api/v3/derivatives", { headers });
  return Array.isArray(data) ? data : [];
}

function buildDerivMap(derivs, watchlist) {
  const want = new Set(watchlist.map((s) => normalizeDerivSymbol(s)));
  const m = new Map(); // watchSym -> best row

  for (const row of derivs) {
    if (COINGECKO_MARKET_FILTER && row.market !== COINGECKO_MARKET_FILTER) continue;

    const key = normalizeDerivSymbol(row.symbol);
    if (!want.has(key)) continue;

    // Prefer perpetual contracts (if multiple)
    const prev = m.get(key);
    const score = (r) => {
      let s = 0;
      if (String(r.contract_type || "").toLowerCase() === "perpetual") s += 10;
      if (r.index != null) s += 2;
      if (r.funding_rate != null) s += 2;
      if (r.open_interest != null) s += 1;
      return s;
    };

    if (!prev || score(row) > score(prev)) m.set(key, row);
  }

  // Return map from actual watchlist sym to row
  const out = new Map();
  for (const sym of watchlist) {
    out.set(sym, m.get(normalizeDerivSymbol(sym)) || null);
  }
  return out;
}

// -----------------------------
// CoinAPI OHLCV fetch + indicators
// -----------------------------
async function fetchOhlcv1hFromCoinapi(sym) {
  const symbolId = coinapiSymbolIdFor(sym);
  const { data } = await httpCA.get(`/v1/ohlcv/${encodeURIComponent(symbolId)}/history`, {
    params: {
      period_id: "1HRS",
      limit: CONFIG.KLINE_LIMIT,
    },
  });
  // data: [{ time_period_start, price_open, price_high, price_low, price_close, volume_traded, ... }, ...]
  if (!Array.isArray(data) || data.length < CONFIG.ATR_PERIOD + 2) return null;

  const open = [];
  const high = [];
  const low = [];
  const close = [];
  const volume = [];
  const closeTime = [];

  for (const r of data) {
    open.push(Number(r.price_open));
    high.push(Number(r.price_high));
    low.push(Number(r.price_low));
    close.push(Number(r.price_close));
    volume.push(Number(r.volume_traded || 0));
    // use period_end as "close time"
    const t = Date.parse(r.time_period_end || r.time_close || r.time_period_start);
    closeTime.push(Number.isFinite(t) ? t : nowMs());
  }

  return { open, high, low, close, volume, closeTime, symbolId };
}

function computeIndicators(c) {
  const closes = c.close;
  const highs = c.high;
  const lows = c.low;
  const vols = c.volume;

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

  // volume ratio
  const lookback = 20;
  const lastVol = vols[vols.length - 1];
  const avgVol =
    vols.length >= lookback
      ? vols.slice(vols.length - lookback).reduce((a, b) => a + b, 0) / lookback
      : vols.reduce((a, b) => a + b, 0) / Math.max(1, vols.length);
  const volumeRatio = avgVol > 0 ? lastVol / avgVol : 1;

  let bbWidthClass = "mid";
  let bbPos = 0;
  if (bb?.middle && bb?.upper && bb?.lower) {
    const widthPct = (bb.upper - bb.lower) / bb.middle;
    if (widthPct >= 0.04) bbWidthClass = "high";
    else if (widthPct <= 0.02) bbWidthClass = "low";

    const lastClose = closes[closes.length - 1];
    if (lastClose > bb.middle) bbPos = 1;
    else if (lastClose < bb.middle) bbPos = -1;
  }

  return {
    rsi,
    macd, // {MACD, signal, histogram}
    bb,
    atr,
    volumeRatio,
    bbWidthClass,
    bbPos,
    lastClose: closes[closes.length - 1],
    lastCandleCloseTime: c.closeTime[c.closeTime.length - 1],
  };
}

// -----------------------------
// Strategy logic (same core idea)
// -----------------------------
function classifyTrend(derivRow, ind) {
  // CoinGecko gives price_percentage_change_24h
  const chg = Number(derivRow?.price_percentage_change_24h ?? 0);
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
  const rsi = ind.rsi;
  const vr = ind.volumeRatio;
  const bbw = ind.bbWidthClass;

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

function baseConfidence(trend, momentum, ind, derivRow) {
  // Use BB width + momentum + 24h change magnitude as a volatility proxy
  const volProxy = Math.abs(Number(derivRow?.price_percentage_change_24h ?? 0)); // % change
  let conf = 50;

  const aligned =
    (trend === "UP" && momentum.score > 0) ||
    (trend === "DOWN" && momentum.score < 0) ||
    (trend === "SIDE" && Math.abs(momentum.score) >= 2);

  conf += aligned ? 10 : -8;

  if (momentum.strength === "STRONG") conf += 15;
  else if (momentum.strength === "MED") conf += 8;
  else conf -= 5;

  if (volProxy > 5) conf += 8;
  else if (volProxy > 2) conf += 4;
  else conf -= 3;

  // if ATR exists, reward non-zero ATR
  if (ind.atr != null && ind.atr > 0) conf += 2;

  return clamp(conf, 0, 100);
}

function applyLiquidationAdjustments(direction, confidence, liq) {
  const warnings = [];
  let veto = false;
  let conf = confidence;

  // Hard veto only on HIGH intensity for opposing danger
  if (direction === "LONG" && liq.dirBias === +1 && liq.intensityScore >= CONFIG.LIQ_THR_HIGH) veto = true;
  if (direction === "SHORT" && liq.dirBias === -1 && liq.intensityScore >= CONFIG.LIQ_THR_HIGH) veto = true;

  if (liq.intensityScore >= CONFIG.LIQ_THR_HIGH) {
    conf -= CONFIG.LIQ_PENALTY_HIGH;
    warnings.push("LIQ_INTENSITY_HIGH");
  } else if (liq.intensityScore >= CONFIG.LIQ_THR_MED) {
    conf -= CONFIG.LIQ_PENALTY_MED;
    warnings.push("LIQ_INTENSITY_MED");
  } else if (liq.intensityScore >= CONFIG.LIQ_THR_LOW) {
    conf -= CONFIG.LIQ_PENALTY_LOW;
    warnings.push("LIQ_INTENSITY_LOW");
  }

  const aligns =
    (direction === "SHORT" && liq.dirBias === +1) || (direction === "LONG" && liq.dirBias === -1);

  if (aligns) {
    conf += CONFIG.LIQ_BONUS_ALIGN;
    warnings.push("LIQ_ALIGNS_WITH_SIGNAL");
  }

  return { confidence: clamp(conf, 0, 100), veto, warnings };
}

function chooseSLMult(confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass) {
  let mult = CONFIG.SL_ATR_MULT_LOW; // 2.0

  if (isReversal) mult = Math.max(mult, 2.5);
  if (confidence < 68) mult = Math.max(mult, 2.5);

  if (liqIntensity >= CONFIG.LIQ_THR_MED) mult = Math.max(mult, 2.5);
  if (bbWidthClass === "high") mult = Math.max(mult, 2.5);

  return Math.min(CONFIG.SL_ATR_MULT_HIGH, mult);
}

function chooseRR(confidence, momentumStrength) {
  if (confidence >= 75 && momentumStrength === "STRONG") return CONFIG.RR_HIGH;
  return CONFIG.RR_BASE;
}

function pickTpSlATR({ entry, atr, direction, confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass }) {
  if (!(atr > 0)) return null;

  const slMult = chooseSLMult(confidence, momentumStrength, isReversal, liqIntensity, bbWidthClass);
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

// -----------------------------
// Signal IDs
// -----------------------------
function makePreviewId(symbol, direction, bucketMs) {
  return sha1(`PREVIEW|${symbol}|${direction}|${bucketMs}`);
}
function makeConfirmedId(symbol, direction, candleCloseTimeMs) {
  return sha1(`CONFIRMED|${symbol}|${direction}|${candleCloseTimeMs}`);
}

// -----------------------------
// Actions
// -----------------------------
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
  const invId = sha1(`INVALID|${symbol}|${floorBucket(nowMs(), 60)}|${reason}`);

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

// -----------------------------
// Intrabar evaluate (per symbol)
// -----------------------------
async function evaluateIntrabarForSymbol({ symbol, derivRow }) {
  const st = await getState(symbol);
  if (nowMs() < st.cooldown_until_ts) return null;
  if (!derivRow) return null;

  // Fetch OHLCV + meta + liq in parallel
  const [ohlcv, meta, liq] = await Promise.all([
    fetchOhlcv1hFromCoinapi(symbol),
    getCoinapiMeta(symbol),
    getLiq(symbol),
  ]);
  if (!ohlcv) return null;

  const ind = computeIndicators(ohlcv);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) return null;

  const trend = classifyTrend(derivRow, ind);
  const momentum = computeMomentum(ind);
  const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return null;

  let direction = cand.dir;
  let isReversal = false;
  if (cand.dir === "LONG_CANDIDATE") { direction = "LONG"; isReversal = true; }
  else if (cand.dir === "SHORT_CANDIDATE") { direction = "SHORT"; isReversal = true; }

  let conf = baseConfidence(trend, momentum, ind, derivRow);

  const liqAdj = applyLiquidationAdjustments(direction, conf, liq);
  conf = liqAdj.confidence;
  if (liqAdj.veto) return null;
  if (conf < CONFIG.MIN_CONF_SHOW) return null;

  // Entry reference: CoinGecko "index" (mark-like), fallback to "price"
  const entry = Number(derivRow.index ?? derivRow.price);
  if (!(entry > 0)) return null;

  // Funding warning
  const warnings = [...liqAdj.warnings];
  const fr = Number(derivRow.funding_rate);
  if (Number.isFinite(fr) && Math.abs(fr) >= CONFIG.FUNDING_RATE_ABS_WARN) {
    warnings.push("FUNDING_RATE_HIGH");
  }

  const tpsl = pickTpSlATR({
    entry,
    atr: ind.atr,
    direction,
    confidence: conf,
    momentumStrength: momentum.strength,
    isReversal,
    liqIntensity: liq.intensityScore,
    bbWidthClass: ind.bbWidthClass,
  });
  if (!tpsl) return null;

  // Round by CoinAPI price_precision (display precision; trading precision may differ) :contentReference[oaicite:7]{index=7}
  const dp = Number(meta.price_precision ?? 2);
  const slR = roundToDecimals(tpsl.slPrice, dp);
  const tpR = roundToDecimals(tpsl.tpPrice, dp);

  const bucketMs = floorBucket(nowMs(), CONFIG.DEDUPE_SCOPE_SECONDS);
  const sigId = makePreviewId(symbol, direction, bucketMs);
  if (!(await tryDedupeOrDrop(sigId))) return null;

  const signal = {
    id: sigId,
    symbol,
    mode: "PREVIEW",
    direction,
    confidence: conf,
    entryRefPrice: entry,
    tpPrice: tpR,
    slPrice: slR,
    meta: {
      atr: tpsl.atr,
      slAtrMult: tpsl.slAtrMult,
      rr: tpsl.rr,
      price_precision: dp,
      coinapi_symbol_id: ohlcv.symbolId,
      coingecko_market: derivRow.market,
    },
    reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "ATR_TPSL", "COINGECKO_INDEX_ENTRY"],
    warnings,
    timestamps: { created: nowMs() },
  };

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

  return signal;
}

// -----------------------------
// Close confirm (per symbol) - only if confirm_requested=true
// -----------------------------
async function confirmOnCloseForSymbol({ symbol, derivRow }) {
  const st = await getState(symbol);
  const p = st.pending_confirm;
  if (!p || p.confirm_requested !== true) return null;
  if (!derivRow) return await invalidate(symbol, "CLOSE_NO_DERIV_DATA");

  const [ohlcv, meta, liq] = await Promise.all([
    fetchOhlcv1hFromCoinapi(symbol),
    getCoinapiMeta(symbol),
    getLiq(symbol),
  ]);
  if (!ohlcv) return await invalidate(symbol, "CLOSE_NO_OHLCV");

  const ind = computeIndicators(ohlcv);
  if (ind.rsi == null || ind.macd == null || ind.bb == null || ind.atr == null) {
    return await invalidate(symbol, "CLOSE_BAD_INDICATORS");
  }

  const trend = classifyTrend(derivRow, ind);
  const momentum = computeMomentum(ind);
  const cand = directionCandidate(trend, ind);
  if (cand.dir === "NO_TRADE") return await invalidate(symbol, "CLOSE_NO_TRADE");

  let directionNow = cand.dir;
  let isReversalNow = false;
  if (cand.dir === "LONG_CANDIDATE") { directionNow = "LONG"; isReversalNow = true; }
  else if (cand.dir === "SHORT_CANDIDATE") { directionNow = "SHORT"; isReversalNow = true; }

  if (directionNow !== p.direction) return await invalidate(symbol, "CLOSE_DIRECTION_CHANGED");

  // Simple reversal confirmation
  if (isReversalNow) {
    const prev = Number(p.snapshot_metrics?.momentumScore ?? momentum.score);
    const ok =
      (directionNow === "SHORT" && ind.rsi < 70) ||
      (directionNow === "LONG" && ind.rsi > 30) ||
      (momentum.score !== prev);
    if (!ok) return await invalidate(symbol, "CLOSE_REVERSAL_NOT_CONFIRMED");
  }

  let conf = baseConfidence(trend, momentum, ind, derivRow);
  const liqAdj = applyLiquidationAdjustments(directionNow, conf, liq);
  conf = liqAdj.confidence;

  if (liqAdj.veto) return await invalidate(symbol, "CLOSE_LIQ_VETO");
  if (conf < CONFIG.MIN_CONF_CONFIRMED) return await invalidate(symbol, "CLOSE_CONFIDENCE_LOW");

  const entry = Number(derivRow.index ?? derivRow.price);
  if (!(entry > 0)) return await invalidate(symbol, "CLOSE_BAD_ENTRY");

  const tpsl = pickTpSlATR({
    entry,
    atr: ind.atr,
    direction: directionNow,
    confidence: conf,
    momentumStrength: momentum.strength,
    isReversal: isReversalNow,
    liqIntensity: liq.intensityScore,
    bbWidthClass: ind.bbWidthClass,
  });
  if (!tpsl) return await invalidate(symbol, "CLOSE_NO_ATR");

  const dp = Number(meta.price_precision ?? 2);
  const slR = roundToDecimals(tpsl.slPrice, dp);
  const tpR = roundToDecimals(tpsl.tpPrice, dp);

  const candleCloseTime = ind.lastCandleCloseTime;
  const sigId = makeConfirmedId(symbol, directionNow, candleCloseTime);
  if (!(await tryDedupeOrDrop(sigId))) return null;

  const signal = {
    id: sigId,
    symbol,
    mode: "CONFIRMED",
    direction: directionNow,
    confidence: conf,
    entryRefPrice: entry,
    tpPrice: tpR,
    slPrice: slR,
    meta: {
      atr: tpsl.atr,
      slAtrMult: tpsl.slAtrMult,
      rr: tpsl.rr,
      price_precision: dp,
      coinapi_symbol_id: ohlcv.symbolId,
      coingecko_market: derivRow.market,
    },
    reasons: [cand.reason, `TREND=${trend}`, `MOM=${momentum.strength}`, "CONFIRMED_ON_CLOSE", "ATR_TPSL"],
    warnings: [...liqAdj.warnings],
    timestamps: { created: p.created_ts, confirmed: nowMs() },
  };

  const cooldownUntil = nowMs() + CONFIG.COOLDOWN_CONFIRMED_MIN * 60_000;
  await Promise.all([
    saveCooldown(symbol, cooldownUntil),
    clearPending(symbol),
    saveLastSignal(symbol, { ts: nowMs(), id: sigId, mode: "CONFIRMED" }),
  ]);

  return signal;
}

// -----------------------------
// Netlify handler
// -----------------------------
export async function handler(event) {
  const t0 = nowMs();
  const watchlist = parseWatchlist();

  if (!COINAPI_KEY) {
    return json(500, { ok: false, error: "Missing env COINAPI_KEY", ms: nowMs() - t0 });
  }
  if (!watchlist.length) {
    return json(400, { ok: false, error: "WATCHLIST env is empty (e.g. BTCUSDT,ETHUSDT,...)", ms: nowMs() - t0 });
  }

  const mode = (event.queryStringParameters?.mode || "intrabar").toLowerCase();

  // User action: POST ?mode=wait_close { symbol }
  if (event.httpMethod === "POST" && mode === "wait_close") {
    let body = {};
    try { body = event.body ? JSON.parse(event.body) : {}; } catch { body = {}; }
    const symbol = String(body.symbol || "").toUpperCase();
    if (!symbol || !watchlist.includes(symbol)) {
      return json(400, { ok: false, error: "Invalid symbol or not in WATCHLIST", ms: nowMs() - t0 });
    }
    const res = await userRequestConfirm(symbol);
    return json(200, { ok: true, mode: "wait_close", result: res, ms: nowMs() - t0 });
  }

  try {
    // 1) Fetch CoinGecko derivatives ONCE
    const derivs = await fetchCoinGeckoDerivatives();
    const derivMap = buildDerivMap(derivs, watchlist); // sym -> row|null

    // 2) Per-symbol tasks MUST be parallel
    if (mode === "close") {
      const tasks = watchlist.map((symbol) =>
        confirmOnCloseForSymbol({ symbol, derivRow: derivMap.get(symbol) }).catch((err) => ({
          id: sha1(`ERR|${symbol}|${floorBucket(nowMs(), 60)}`),
          symbol,
          mode: "ERROR",
          error: String(err?.message || err),
        }))
      );
      const results = await Promise.all(tasks);
      const signals = results.filter((x) => x && x.mode !== "ERROR");
      const errors = results.filter((x) => x && x.mode === "ERROR");
      return json(200, { ok: true, mode: "close", watchlist, signals, errors, ms: nowMs() - t0 });
    }

    // default: intrabar preview
    const tasks = watchlist.map((symbol) =>
      evaluateIntrabarForSymbol({ symbol, derivRow: derivMap.get(symbol) }).catch((err) => ({
        id: sha1(`ERR|${symbol}|${floorBucket(nowMs(), 60)}`),
        symbol,
        mode: "ERROR",
        error: String(err?.message || err),
      }))
    );
    const results = await Promise.all(tasks);
    const signals = results.filter((x) => x && x.mode !== "ERROR");
    const errors = results.filter((x) => x && x.mode === "ERROR");

    return json(200, { ok: true, mode: "intrabar", watchlist, signals, errors, ms: nowMs() - t0 });
  } catch (e) {
    return json(500, { ok: false, error: String(e?.message || e), ms: nowMs() - t0 });
  }
}
