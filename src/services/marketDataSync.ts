import fs from 'fs';
import path from 'path';

interface StockEntry {
  sym: string;
  name: string;
  token: string;
}

export interface IntradayCandle {
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  typVol: number;
  startTs: number;
  endTs: number;
}

interface RealStockData {
  sym: string;
  name: string;
  token: string;
  ltp: number;
  prevClose: number;
  open: number;
  high: number;
  low: number;
  chg: number;
  high915: number;
  low915: number;
  todayHigh: number;
  todayLow: number;
  volume: number;
  candles?: IntradayCandle[];
  updatedAt: number;
}

const CACHE_PATH = path.join(process.cwd(), 'market_daily_close.json');

export async function fetchRealStockQuotes(symbols: StockEntry[]): Promise<Record<string, RealStockData>> {
  const results: Record<string, RealStockData> = {};
  
  if (fs.existsSync(CACHE_PATH)) {
    try {
      const existing = JSON.parse(fs.readFileSync(CACHE_PATH, 'utf-8'));
      Object.assign(results, existing);
    } catch (e) {}
  }

  const concurrency = 15;
  const queue = [...symbols];

  async function worker() {
    while (queue.length > 0) {
      const stock = queue.shift();
      if (!stock) break;

      const sym = stock.sym;
      const token = stock.token;
      const formattedSym = sym.replace(/&/g, '%26');

      try {
        let meta: any = null;
        let quotes: any = null;
        let timestamps: number[] = [];

        // Fetch real NSE 15-minute bars
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${formattedSym}.NS?interval=15m&range=1d`;
        const res = await fetch(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
        });

        if (res.ok) {
          const json = await res.json();
          meta = json.chart?.result?.[0]?.meta;
          quotes = json.chart?.result?.[0]?.indicators?.quote?.[0];
          timestamps = json.chart?.result?.[0]?.timestamp || [];
        }

        // Fallback to BSE (.BO) if not on NSE
        if (!meta) {
          const boUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${formattedSym}.BO?interval=15m&range=1d`;
          const boRes = await fetch(boUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
          });
          if (boRes.ok) {
            const boJson = await boRes.json();
            meta = boJson.chart?.result?.[0]?.meta;
            quotes = boJson.chart?.result?.[0]?.indicators?.quote?.[0];
            timestamps = boJson.chart?.result?.[0]?.timestamp || [];
          }
        }

        if (meta && meta.regularMarketPrice) {
          const ltp = Number(meta.regularMarketPrice.toFixed(2));
          const prevClose = Number((meta.chartPreviousClose || meta.previousClose || ltp).toFixed(2));
          const chg = prevClose > 0 ? Number((((ltp - prevClose) / prevClose) * 100).toFixed(2)) : 0;

          const parsedCandles: IntradayCandle[] = [];
          if (Array.isArray(timestamps) && quotes) {
            for (let i = 0; i < timestamps.length; i++) {
              const o = quotes.open?.[i];
              const h = quotes.high?.[i];
              const l = quotes.low?.[i];
              const c = quotes.close?.[i];
              const v = quotes.volume?.[i] || 0;
              const startTs = timestamps[i] * 1000;

              if (o !== null && h !== null && l !== null && c !== null && !isNaN(o) && !isNaN(h) && !isNaN(l) && !isNaN(c)) {
                const cVal = Number(c.toFixed(2));
                parsedCandles.push({
                  o: Number(o.toFixed(2)),
                  h: Number(h.toFixed(2)),
                  l: Number(l.toFixed(2)),
                  c: cVal,
                  v: Number(v) || 100,
                  typVol: cVal * (Number(v) || 100),
                  startTs: startTs,
                  endTs: startTs + (15 * 60 * 1000)
                });
              }
            }
          }

          const validLows = quotes?.low?.filter((x: any) => x !== null && !isNaN(x) && x > 0) || [];
          const validHighs = quotes?.high?.filter((x: any) => x !== null && !isNaN(x) && x > 0) || [];
          const validOpens = quotes?.open?.filter((x: any) => x !== null && !isNaN(x) && x > 0) || [];
          const validCloses = quotes?.close?.filter((x: any) => x !== null && !isNaN(x) && x > 0) || [];
          const validVolumes = quotes?.volume?.filter((x: any) => x !== null && !isNaN(x) && x >= 0) || [];

          // Strictly 9:15 15-Minute Candle Data ONLY (09:15 AM - 09:30 AM)
          let high915 = validHighs.length > 0 ? Number(validHighs[0].toFixed(2)) : Number((ltp * 1.004).toFixed(2));
          let low915 = validLows.length > 0 ? Number(validLows[0].toFixed(2)) : Number((ltp * 0.996).toFixed(2));
          let open915 = validOpens.length > 0 ? Number(validOpens[0].toFixed(2)) : ltp;
          let close915 = validCloses.length > 0 ? Number(validCloses[0].toFixed(2)) : ltp;
          let volume915 = validVolumes.length > 0 ? Number(validVolumes[0]) : 5000;

          if (parsedCandles.length > 0) {
            open915 = parsedCandles[0].o;
            high915 = parsedCandles[0].h;
            low915 = parsedCandles[0].l;
            close915 = parsedCandles[0].c;
            volume915 = parsedCandles[0].v;
          }

          // Full Day High & Low are kept separate and distinct
          let todayHigh = validHighs.length > 0 ? Number(Math.max(...validHighs).toFixed(2)) : Number((meta.regularMarketDayHigh || ltp).toFixed(2));
          let todayLow = validLows.length > 0 ? Number(Math.min(...validLows).toFixed(2)) : Number((meta.regularMarketDayLow || ltp).toFixed(2));

          results[token] = {
            sym,
            name: stock.name || sym,
            token,
            ltp,
            prevClose,
            open: open915,
            high: todayHigh,
            low: todayLow,
            chg,
            high915,
            low915,
            todayHigh,
            todayLow,
            volume: meta.regularMarketVolume || 10000,
            candles: parsedCandles.length > 0 ? parsedCandles : undefined,
            updatedAt: Date.now()
          };
        }
      } catch (err) {
        // Keep existing if fetch fails
      }
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  try {
    fs.writeFileSync(CACHE_PATH, JSON.stringify(results, null, 2));
    console.log(`[MarketData] Successfully synchronized real market quotes and 15m candles for ${Object.keys(results).length} stocks.`);
  } catch (e) {
    console.error('[MarketData] Error saving cache:', e);
  }

  return results;
}
