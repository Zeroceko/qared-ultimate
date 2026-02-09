import axios from "axios";
import { RSI, MACD, BollingerBands, ATR } from "technicalindicators";
import { Redis } from "@upstash/redis";
import crypto from "node:crypto";

// --- CONFIG & CONSTANTS ---
const CONFIG = {
  MIN_CONF_SHOW: 60,
  COOLDOWN_CONFIRMED_MIN: 30,
  ATR_PERIOD: 14,
  SYMBOLS: ["BTC", "ETH", "SOL", "AVAX", "LINK", "ARB", "OP", "RNDR", "TIA", "SUI"],
  COINAPI_BASE: "https://rest.coinapi.io/v1",
  COINGECKO_BASE: "https://api.coingecko.com/api/v3"
};

// API Anahtarlarını Buraya Gir veya Netlify Panelden Al
const COINAPI_KEY = "287d88ff-ccad-4354-828c-c35310fec621"; 

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || "",
  token: process.env.UPSTASH_REDIS_REST_TOKEN || "",
});

// Yardımcı Fonksiyonlar
const sha1 = (str) => crypto.createHash("sha1").update(str).digest("hex");
const nowMs = () => Date.now();
const json = (status, body) => ({
  statusCode: status,
  headers: { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" },
  body: JSON.stringify(body)
});

// Sembol Analiz Motoru
async function evaluateSymbol(symbol) {
  try {
    // 1. Veri Çekme (CoinAPI)
    const res = await axios.get(
      `${CONFIG.COINAPI_BASE}/ohlcv/BINANCE_SPOT_${symbol}_USDT/history?period_id=1HRS&limit=100`,
      { headers: { "X-CoinAPI-Key": COINAPI_KEY } }
    );
    
    const candles = res.data;
    const closes = candles.map(c => c.price_close);
    const highs = candles.map(c => c.price_high);
    const lows = candles.map(c => c.price_low);

    // 2. Teknik Göstergeler
    const rsiValues = RSI.calculate({ values: closes, period: 14 });
    const rsi = rsiValues[rsiValues.length - 1];
    
    const bb = BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 }).pop();
    const atrValues = ATR.calculate({ high: highs, low: lows, close: closes, period: CONFIG.ATR_PERIOD });
    const atr = atrValues[atrValues.length - 1];

    const lastPrice = closes[closes.length - 1];
    
    // 3. Strateji Mantığı (Senin trade-bot (2).mjs dosyasındaki mantık)
    let type = null;
    let confidence = 0;

    if (rsi < 30 && lastPrice <= bb.lower) {
      type = "LONG";
      confidence = 85;
    } else if (rsi > 70 && lastPrice >= bb.upper) {
      type = "SHORT";
      confidence = 85;
    }

    if (!type) return null;

    // 4. Sinyal Objesi Oluşturma
    return {
      id: sha1(`${symbol}|${type}|${Math.floor(nowMs() / 3600000)}`),
      symbol,
      type,
      entryPrice: lastPrice,
      confidence,
      tp: type === "LONG" ? lastPrice + (atr * 2) : lastPrice - (atr * 2),
      sl: type === "LONG" ? lastPrice - (atr * 1.5) : lastPrice + (atr * 1.5),
      timestamp: new Date().toISOString()
    };
  } catch (e) {
    return { symbol, error: e.message };
  }
}

export const handler = async (event) => {
  const t0 = nowMs();
  
  try {
    // Tüm sembolleri paralel analiz et (Performans için)
    const tasks = CONFIG.SYMBOLS.map(sym => evaluateSymbol(sym));
    const results = await Promise.all(tasks);
    
    const signals = results.filter(r => r && !r.error);
    const errors = results.filter(r => r && r.error);

    // Redis'e Kaydet (Deduplication - Aynı sinyali tekrar atma)
    for (const sig of signals) {
      await redis.set(`last_signal:${sig.symbol}`, sig, { ex: 3600 });
    }

    return json(200, {
      ok: true,
      signals,
      errors,
      ms: nowMs() - t0
    });
  } catch (err) {
    return json(500, { ok: false, error: err.message });
  }
};
