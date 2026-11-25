#!/usr/bin/env node
'use strict';
/*
 * Tokenized Stocks & RWA Reality Check: one file for the API, the live price check and the 24/7 history collector.
 * No dependencies. Needs Node 20.16 / 22.3 or newer.   Run:  node server.js   then open http://localhost:3000
 * (If you use an older Node, rename this file to server.cjs and run: node server.cjs)
 *
 * WHAT IT DOES
 *  - Reads Metric's public API for every chain and compares each tokenized stock, ETF and gold pool with the real market price
 *    (Yahoo Finance for stocks, Binance for crypto, Finnhub as an optional backup).
 *  - Keeps all chains hot in memory (refreshed every 10 s) so the page opens instantly.
 *  - If FIREBASE_SERVICE_ACCOUNT is set, it also records history and alerts to Firestore, around the clock.
 *
 * SET UP FIREBASE (optional, for history)
 *  1. Firebase Console > Build > Firestore Database > Create database.
 *  2. Firestore > Rules > paste the rules below > Publish (browsers can read, only this server can write):
 *       rules_version = '2';
 *       service cloud.firestore { match /databases/{db}/documents {
 *         match /pools/{id} { allow read: if true; allow write: if false; }
 *         match /pools/{id}/ticks/{t} { allow read: if true; allow write: if false; }
 *         match /alerts/{id} { allow read: if true; allow write: if false; }
 *         match /{document=**} { allow read, write: if false; }
 *       } }
 *  3. Project settings > Your apps > Web app > copy the config into FIREBASE_CONFIG at the top of index.html.
 *  4. Project settings > Service accounts > Generate new private key. Put the whole JSON in the environment
 *     variable FIREBASE_SERVICE_ACCOUNT (or base64 of it) where you run this server.
 *
 * HOSTING
 *  It must run all the time to collect history: Railway, Fly.io, a VPS, or your own PC. (Free plans that sleep,
 *  such as Render free, stop the collector while asleep.)  Start command: node server.js
 *
 * ENVIRONMENT (all optional)
 *  PORT 3000 | CHAINS e.g. "1,56,46630" (default: all) | ALERT_BPS 50 | MAX_POOLS 30 (recorded per chain)
 *  WRITE_SEC 600 (Firestore write interval) | RETENTION_DAYS 30 | FINNHUB_KEY (backup stock prices)
 *  COLLECT=0 (never write to Firestore) | WARM=0 (no background refresh) | MIN_TVL 100
 *  INCLUDE_CRYPTO=1 also lists crypto pools such as WETH or WBTC (by default only real-world assets are listed)
 *  Free Firestore allows 20,000 writes per day. Writes per day = chains x MAX_POOLS x (86400 / WRITE_SEC).
 *  The defaults (3 chains x 30 pools every 10 min) use about 13,000.
 */
const load = typeof require === 'function' ? require : process.getBuiltinModule; // works as CommonJS and as an ES module ("type": "module")
const http = load('http'), fs = load('fs'), path = load('path'), zlib = load('zlib'), crypto = load('crypto');
const HERE = typeof __dirname === 'string' ? __dirname : path.dirname(path.resolve(process.argv[1]));
const ENV = process.env;

/* ============ SETTINGS you may edit ============ */
// Force a market ticker when the automatic match is wrong, e.g. { TSLAX: 'TSLA', WETH: 'binance:ETH' }
const TICKERS = {};
// Issuer multiplier for tokenized stocks. Use it if a pool sits at a steady offset from the market, e.g. { TSLA: 1.012 }
const MULTIPLIERS = {};
/* ================================================= */

const API = (ENV.METRIC_API || 'https://api.metric.xyz').replace(/\/$/, '');
const YAHOO = ENV.YAHOO_BASE ? [ENV.YAHOO_BASE] : ['https://query1.finance.yahoo.com', 'https://query2.finance.yahoo.com'];
const BINANCE = ENV.BINANCE_BASE ? [ENV.BINANCE_BASE] : ['https://data-api.binance.vision', 'https://api.binance.com'];
const FINNHUB = ENV.FINNHUB_BASE || 'https://finnhub.io';
const MIN_TVL = +(ENV.MIN_TVL ?? 100), MAX_SNAP = +ENV.SNAPSHOT_POOLS || 300;
const TTL = +ENV.SNAPSHOT_TTL_MS || 10000, STALE_MAX = +ENV.SNAPSHOT_STALE_MS || 120000;
const MAX_POOLS = +ENV.MAX_POOLS || 30, THR = +ENV.ALERT_BPS || 50, WRITE = +ENV.WRITE_SEC || 600, POLL = +ENV.POLL_SEC || 15, RET_DAYS = +ENV.RETENTION_DAYS || 30;
const UA = { 'User-Agent': 'Mozilla/5.0 (compatible; rwa-reality-check)' };

const STABLE = new Set(['USDC', 'USDT', 'DAI', 'USDS', 'USDE', 'PYUSD', 'USD1', 'FDUSD', 'USDC.E', 'USDBC', 'BUSD', 'TUSD', 'FRAX', 'LUSD', 'USDG', 'RLUSD', 'USDT0']);
const CRYPTO = { WETH: 'ETH', ETH: 'ETH', WBTC: 'BTC', CBBTC: 'BTC', BTC: 'BTC', WBNB: 'BNB', BNB: 'BNB', SOL: 'SOL', WSOL: 'SOL', LINK: 'LINK', UNI: 'UNI', AAVE: 'AAVE', PAXG: 'PAXG', XAUT: 'XAUT', ARB: 'ARB', OP: 'OP' };
const GOLD = new Set(['PAXG', 'XAUT']); // tokenized gold counts as a real-world asset
const ETFS = new Set(['SPY', 'QQQ', 'IWM', 'DIA', 'VOO', 'VTI', 'IVV', 'GLD', 'IAU', 'SLV', 'TLT', 'IEF', 'SHY', 'ARKK', 'XLF', 'XLE', 'XLK', 'XLV', 'EEM', 'EFA', 'HYG', 'LQD', 'USO', 'BITO', 'IBIT', 'SMH', 'SOXL', 'TQQQ', 'SQQQ']);
const issuerOf = sym => /^[A-Z]{1,5}x$/.test(sym) ? 'xStocks' : /^[A-Z]{1,5}on$/.test(sym) ? 'Ondo' : /^[a-z]{1}[A-Z]{1,5}$/.test(sym) ? 'Backed' : null; // a guess from the token symbol
// Company names shown next to the token. Used when Yahoo does not return one. Add your own as needed.
const COMPANY = { SPCX: 'SpaceX', TSLA: 'Tesla', NVDA: 'NVIDIA', AAPL: 'Apple', MSFT: 'Microsoft', AMZN: 'Amazon', GOOGL: 'Alphabet', GOOG: 'Alphabet', META: 'Meta Platforms', COIN: 'Coinbase', MSTR: 'Strategy', AMD: 'Advanced Micro Devices', NFLX: 'Netflix', PLTR: 'Palantir', HOOD: 'Robinhood Markets', BABA: 'Alibaba', UBER: 'Uber', SHOP: 'Shopify', DIS: 'Walt Disney', INTC: 'Intel', JPM: 'JPMorgan Chase', V: 'Visa', MA: 'Mastercard', XOM: 'Exxon Mobil', WMT: 'Walmart', KO: 'Coca-Cola', PEP: 'PepsiCo', BA: 'Boeing', ORCL: 'Oracle', CRM: 'Salesforce', ADBE: 'Adobe', PYPL: 'PayPal', SQ: 'Block', SNAP: 'Snap', SPOT: 'Spotify', RIVN: 'Rivian', NIO: 'NIO', GME: 'GameStop', AMC: 'AMC Entertainment', CRCL: 'Circle Internet', MU: 'Micron', QCOM: 'Qualcomm', AVGO: 'Broadcom', TSM: 'TSMC', ASML: 'ASML', SMCI: 'Super Micro Computer', ARM: 'Arm Holdings', DELL: 'Dell Technologies', IBM: 'IBM', CSCO: 'Cisco', BAC: 'Bank of America', GS: 'Goldman Sachs', MCD: "McDonald's", NKE: 'Nike', SBUX: 'Starbucks', LLY: 'Eli Lilly', JNJ: 'Johnson & Johnson', PFE: 'Pfizer', UNH: 'UnitedHealth', CVX: 'Chevron', SPY: 'SPDR S&P 500 ETF', QQQ: 'Invesco QQQ Trust', IWM: 'iShares Russell 2000', DIA: 'SPDR Dow Jones ETF', VOO: 'Vanguard S&P 500 ETF', VTI: 'Vanguard Total Market', GLD: 'SPDR Gold Shares', SLV: 'iShares Silver Trust', TLT: 'iShares 20+ Year Treasury', IBIT: 'iShares Bitcoin Trust', ARKK: 'ARK Innovation ETF', PAXG: 'Paxos Gold', XAUT: 'Tether Gold' };
const cleanName = n => n ? String(n).replace(/\s+(Common Stock|Class [A-C]( Common Stock)?|Ordinary Shares?)$/i, '').trim() : null;
const INCLUDE_CRYPTO = ENV.INCLUDE_CRYPTO === '1';
// Crypto tokens are not real-world assets, so pools whose main token is one of these are left out (INCLUDE_CRYPTO=1 shows them).
const NON_RWA = new Set([...Object.keys(CRYPTO).filter(k => !GOLD.has(k)), 'STETH', 'WSTETH', 'WEETH', 'RETH', 'CBETH', 'LBTC', 'TBTC', 'SUSDE', 'AVAX', 'WAVAX', 'MATIC', 'POL', 'DOGE', 'SHIB', 'PEPE', 'CAKE', 'CRV', 'MKR', 'LDO', 'SUSHI', 'COMP', 'SNX', 'INJ', 'TON', 'XRP', 'ADA', 'DOT', 'NEAR']);
const MAXU = '340282366920938463463374607431768211455';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const med = a => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); return s[s.length >> 1]; };
const sd = a => { if (a.length < 2) return 0; const m = a.reduce((x, y) => x + y) / a.length; return Math.sqrt(a.reduce((x, y) => x + (y - m) ** 2, 0) / a.length); };
const sg = n => (n > 0 ? '+' : '') + n.toFixed(1);

async function getJSON(url, opt = {}, tries = 2) {
  let err;
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { ...opt, headers: { ...UA, ...(opt.headers || {}) }, signal: AbortSignal.timeout(+ENV.HTTP_TIMEOUT_MS || 12000) });
      if (r.status === 429 || r.status >= 500) { err = new Error(`${url.split('?')[0]} -> ${r.status}`); err.status = r.status; await sleep(500 * (i + 1)); continue; }
      if (!r.ok) { const e = new Error(`${url.split('?')[0]} -> ${r.status}`); e.status = r.status; e.body = await r.text().catch(() => ''); throw e; }
      return await r.json();
    } catch (e) { err = e; if (e.status && e.status < 429) throw e; }
  }
  throw err;
}
/* Q64.64 fixed point -> Number. bidAdj/askAdj are already decimals-adjusted: token1 per whole token0. */
const q2n = s => { try { return Number(BigInt(s) * 10n ** 18n >> 64n) / 1e18; } catch (e) { return NaN; } };

/* ================= Metric data ================= */
let chainsCache = null;
async function getChains() {
  if (chainsCache && Date.now() - chainsCache.t < 6e5) return chainsCache.v;
  const j = await getJSON(`${API}/public/v1/chains`);
  const arr = Array.isArray(j) ? j : j.data || j.chains || [];
  const v = arr.map(x => ({ id: String(x.chainId ?? x.chain_id ?? x.id), name: String(x.name ?? x.chain ?? x.chainId ?? x.id) })).filter(x => x.id && x.id !== 'undefined');
  chainsCache = { t: Date.now(), v };
  return v;
}
const defaultChain = chains => (chains.find(c => /robinhood/i.test(c.name)) || chains[0] || {}).id;
async function metaAll(chain) {
  const out = []; let off = 0;
  for (let i = 0; i < 4; i++) {
    const j = await getJSON(`${API}/public/v1/evm/${chain}/metadata?include24h=true&sortBy=tvl&sortOrder=desc&count=500&offset=${off}&minTvlFiat=${MIN_TVL}`);
    out.push(...(j.data || []));
    if (j.nextOffset == null || out.length >= MAX_SNAP) break;
    off = j.nextOffset;
  }
  return out.slice(0, MAX_SNAP);
}
/* depth:true is required to detect one-sided pools and to refresh the provider status */
async function quotesFor(chain, addrs) {
  const map = new Map(), chunks = [];
  for (let i = 0; i < addrs.length; i += 100) chunks.push(addrs.slice(i, i + 100));
  await Promise.all(chunks.map(async ids => {
    try {
      const j = await getJSON(`${API}/public/v1/evm/${chain}/bid_ask/batch`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pools: ids, depth: true }) });
      for (const r of j.results || []) map.set(String(r.poolAddress).toLowerCase(), r.quote || { error: r.error || { error: 'no quote' } });
    } catch (e) { for (const a of ids) map.set(a.toLowerCase(), { error: { error: e.message } }); }
  }));
  return map;
}

/* ================= market reference prices ================= */
function sessionOf(tp, now) {
  if (!tp) return null; const t = now / 1000;
  for (const k of ['pre', 'regular', 'post']) if (tp[k] && t >= tp[k].start && t < tp[k].end) return k;
  return 'closed';
}
async function yahoo(tk) {
  let err;
  for (const b of YAHOO) {
    try {
      const j = await getJSON(`${b}/v8/finance/chart/${encodeURIComponent(tk)}?interval=1m&range=1d&includePrePost=true`, {}, 1);
      const r = j.chart.result[0], m = r.meta || {};
      if (m.currency && m.currency !== 'USD') throw new Error('non-USD listing');
      const c = (r.indicators.quote[0].close) || [], ts = r.timestamp || [];
      let price = null, t = null;
      for (let i = c.length - 1; i >= 0; i--) if (c[i] != null) { price = c[i]; t = ts[i]; break; }
      if (price == null) { price = m.regularMarketPrice; t = m.regularMarketTime; }
      if (!(price > 0)) throw new Error('no price');
      return { price, ts: t, src: 'Yahoo', tk, name: m.longName || m.shortName || null, it: m.instrumentType || null, session: sessionOf(m.currentTradingPeriod, Date.now()) };
    } catch (e) { err = e; }
  }
  throw err;
}
async function finnhub(tk) {
  if (!ENV.FINNHUB_KEY) throw new Error('no FINNHUB_KEY');
  const j = await getJSON(`${FINNHUB}/api/v1/quote?symbol=${encodeURIComponent(tk)}&token=${ENV.FINNHUB_KEY}`, {}, 1);
  if (!(j.c > 0)) throw new Error('no data');
  return { price: j.c, ts: j.t, src: 'Finnhub', tk, session: null };
}
async function binance(id) {
  let err;
  for (const b of BINANCE) {
    try { const j = await getJSON(`${b}/api/v3/ticker/price?symbol=${id}USDT`, {}, 1); return { price: +j.price, ts: Math.floor(Date.now() / 1000), src: 'Binance', tk: id + 'USDT', session: 'regular' }; } catch (e) { err = e; }
  }
  throw err;
}
/* ================= Robinhood Stock Token API (official, read-only) ================= */
// https://docs.robinhood.com/chain/stock-token-apis  (assets, prices, corporate actions). Set RHJ_BASE to override, RH=0 to turn it off.
const RHJ = (ENV.RHJ_BASE || 'https://api.robinhood.com/rhj').replace(/\/$/, ''), RH_ON = ENV.RH !== '0';
const rh = { bySym: new Map(), byAddr: new Map(), alias: new Map(), t: 0, p: null, prices: new Map(), pt: 0, bareBad: 0, corp: new Map(), ct: 0, cp: null, warned: new Set() };
const rhWarn = (k, e) => { if (!rh.warned.has(k)) { rh.warned.add(k); console.error('Robinhood ' + k + ':', e.message); } };
const rhNum = x => { const n = +x; return Number.isFinite(n) ? n : null; };
function rhAssets() {
  if (!RH_ON) return Promise.resolve();
  if (rh.p) return rh.p;
  if (Date.now() - rh.t < 6e5 && rh.bySym.size) return Promise.resolve();
  rh.p = (async () => {
    try {
      const j = await getJSON(`${RHJ}/assets`, {}, 2), bySym = new Map(), byAddr = new Map();
      for (const a of j.assets || []) {
        const sym = String(a.tokenSymbol || '').toUpperCase(); if (!sym) continue;
        const dep = (a.deployments || []).map(d => String(d.contractAddress || '').toLowerCase()).filter(Boolean);
        const e = { sym, name: cleanName(String(a.tokenName || '').replace(/\s*[•·]\s*Robinhood Token$/i, '')) || null, mult: rhNum(a.currentMultiplier) || 1,
          pending: a.pendingMultiplier || '', pendingAt: a.pendingMultiplierEffectiveTime || null, active: a.status !== 'ASSET_STATUS_INACTIVE', allDay: a.tradingCapabilities ? a.tradingCapabilities.allDayTradability : null };
        bySym.set(sym, e); for (const d of dep) byAddr.set(d, e);
      }
      if (bySym.size) { rh.bySym = bySym; rh.byAddr = byAddr; }
      rh.t = Date.now();
    } catch (e) { rhWarn('/assets', e); rh.t = Date.now() - 5.5e5; } // retry in about a minute
    finally { rh.p = null; }
  })();
  return rh.p;
}
async function rhPrices() {
  if (Date.now() - rh.pt < 15000) return rh.prices;
  if (rh.bareBad > Date.now()) return rh.prices;
  try {
    const j = await getJSON(`${RHJ}/prices`, {}, 1), m = new Map();
    for (const q of j.quotes || []) m.set(String(q.tokenSymbol || '').toUpperCase(), q);
    if (m.size) { rh.prices = m; rh.pt = Date.now(); }
  } catch (e) { rhWarn('/prices', e); rh.bareBad = Date.now() + 6e5; }
  return rh.prices;
}
function nyNow() {
  const p = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: 'numeric', hour12: false }).formatToParts(new Date()), g = t => p.find(x => x.type === t).value;
  return { d: g('weekday'), m: (+g('hour') % 24) * 60 + +g('minute') };
}
function rhLive(a) { // is the underlying stock quoting right now? 24/5 assets: Sun 20:00 to Fri 20:00 ET. Others: weekdays 04:00 to 20:00 ET.
  const { d, m } = nyNow();
  if (a.allDay === 'tradable') return d === 'Sat' ? false : d === 'Sun' ? m >= 1200 : d === 'Fri' ? m < 1200 : true;
  return !['Sat', 'Sun'].includes(d) && m >= 240 && m < 1200;
}
async function robinhood(sym) {
  await rhAssets();
  const a = rh.bySym.get(sym); if (!a || !a.active) throw new Error('not an active Robinhood token');
  let q = (await rhPrices()).get(sym);
  if (!q) { const j = await getJSON(`${RHJ}/prices/${encodeURIComponent(sym)}`, {}, 1); q = (j.quotes || []).find(x => String(x.tokenSymbol).toUpperCase() === sym); }
  if (!q) throw new Error('no quote');
  const bid = rhNum(q.bid), ask = rhNum(q.ask), price = bid > 0 && ask > 0 ? (bid + ask) / 2 : ask > 0 ? ask : bid;
  if (!(price > 0)) throw new Error('no price');
  const halt = !!q.isTradingHalt, live = rhLive(a) && !halt;
  return { price, ts: Date.parse(q.generatedAt) / 1000 || Math.floor(Date.now() / 1000), src: 'Robinhood', tk: sym, it: null, halt, rhLive: live, session: halt ? 'halted' : live ? 'regular' : 'closed' };
}
const corpDesc = a => { const d = a.details || {}; return d.forwardSplit ? `${d.forwardSplit.newRate}-for-${d.forwardSplit.oldRate} forward split` : d.reverseSplit ? `${d.reverseSplit.newRate}-for-${d.reverseSplit.oldRate} reverse split` : d.cashDividend ? `$${d.cashDividend.rate} cash dividend` : d.stockDividend ? `${d.stockDividend.rate} stock dividend` : String(a.type || 'corporate action').replace('CORPORATE_ACTION_TYPE_', '').toLowerCase().replace(/_/g, ' '); };
function rhCorp() { // hourly: actions in progress, or dated within the last day or the next 7 days
  if (!RH_ON || rh.cp || Date.now() - rh.ct < 36e5) return;
  rh.cp = (async () => {
    try {
      const j = await getJSON(`${RHJ}/corporate-actions`, {}, 1), m = new Map(), now = Date.now();
      for (const a of j.corpActions || []) {
        const pd = a.processDate, t = pd ? Date.UTC(pd.year, pd.month - 1, pd.day) : null, days = t == null ? 0 : (t - now) / 864e5;
        if (!(a.status === 'CORPORATE_ACTION_STATUS_IN_PROGRESS' || (t != null && days >= -1 && days <= 7))) continue;
        const k = String(a.tokenSymbol || '').toUpperCase(); if (!m.has(k)) m.set(k, { desc: corpDesc(a), date: t });
      }
      rh.corp = m; rh.ct = Date.now();
    } catch (e) { rhWarn('/corporate-actions', e); rh.ct = Date.now() - 3e6; } finally { rh.cp = null; }
  })();
}
const rhSymFor = k => rh.alias.get(k) || (rh.bySym.has(k) ? k : null);
const FETCHERS = { y: yahoo, f: finnhub, b: binance, r: robinhood };
const refCache = new Map(), known = new Map(), fails = new Map();
function cachedRef(src, id) {
  const k = src + ':' + id, c = refCache.get(k);
  if (c && Date.now() - c.t < 12000) return c.p;
  const e = { t: Date.now(), p: FETCHERS[src](id), v: null };
  refCache.set(k, e);
  e.p.then(v => { e.v = v; }, () => { if (refCache.get(k) === e) refCache.delete(k); });
  return e.p;
}
const candidates = sym => {
  const s = sym.toUpperCase(), c = new Set([s]);
  for (const re of [/(ON|\.R|\.D|\.S|X)$/, /^(X|B)/]) { const t = s.replace(re, ''); if (t && t !== s) c.add(t); }
  return [...c].filter(x => /^[A-Z.\-=]{1,8}$/.test(x));
};
const multOf = k => { if (+MULTIPLIERS[k]) return +MULTIPLIERS[k]; const s = rhSymFor(k), a = s && rh.bySym.get(s); return a ? a.mult : 1; };
/* A candidate ticker is accepted only if its price is within about +-30% of Metric's own fiat price for the token. */
async function getRef(sym, baseFiat, deadline) {
  const k = sym.toUpperCase(), now = Date.now();
  const t = known.get(k);
  if (t) { try { return await cachedRef(t.src, t.id); } catch (e) { return null; } }
  if ((fails.get(k) || 0) > now || now > deadline) return null;
  const forced = TICKERS[k], crypto_ = !forced && CRYPTO[k], cand = candidates(k);
  const rounds = forced ? [String(forced).startsWith('binance:') ? [['b', String(forced).slice(8)]] : [['y', forced], ['f', forced]]]
    : crypto_ ? [[['b', CRYPTO[k]]]]
    : [...(rhSymFor(k) ? [[['r', rhSymFor(k)]]] : []), cand.map(id => ['y', id]), cand.map(id => ['f', id])];
  for (const round of rounds) {
    const res = await Promise.allSettled(round.map(([src, id]) => cachedRef(src, id)));
    for (let i = 0; i < round.length; i++) {
      const r = res[i].status === 'fulfilled' ? res[i].value : null;
      if (!r || !(r.price > 0)) continue;
      if (!forced && !crypto_ && baseFiat > 0) { const x = r.price * multOf(k) / baseFiat; if (x < 0.7 || x > 1.43) continue; }
      known.set(k, { src: round[i][0], id: round[i][1] });
      return r;
    }
  }
  fails.set(k, now + 6e5);
  return null;
}
function peekRef(sym) { // already-resolved price, never waits on the network
  const t = known.get(sym.toUpperCase()); if (!t) return null;
  const c = refCache.get(t.src + ':' + t.id);
  return c && c.v && Date.now() - c.t < 60000 ? c.v : null;
}
async function resolveRefs(symInfo, deadline) {
  const syms = [...symInfo.keys()].slice(0, 80), refs = {}; let n = 0;
  await Promise.all(Array.from({ length: 16 }, async () => { while (n < syms.length) { const s = syms[n++]; refs[s] = await getRef(s, symInfo.get(s).fiat, deadline); } }));
  return refs;
}

/* ================= per-pool tracking: staleness and reaction lag ================= */
function track(s, now, refPrice, usd) {
  s.lags = s.lags || [];
  if (s.mid == null || Math.abs(usd - s.mid) / s.mid > 2e-5) { s.mid = usd; s.chg = now; }
  const stale = (now - (s.chg || now)) / 1e3;
  if (s.r0 == null) s.r0 = refPrice;
  const d = (refPrice - s.r0) / s.r0 * 1e4;
  if (!s.pend && Math.abs(d) >= 8) { s.pend = { t: now, m: usd, dir: Math.sign(d), sz: Math.abs(d) }; s.r0 = refPrice; }
  if (s.pend) {
    const mv = (usd - s.pend.m) / s.pend.m * 1e4 * s.pend.dir;
    if (mv >= s.pend.sz * 0.5) { s.lags.push(Math.round((now - s.pend.t) / 1e3)); s.pend = null; }
    else if (now - s.pend.t > 18e4) { s.lags.push(180); s.pend = null; }
  }
  if (s.lags.length > 20) s.lags.shift();
  return { stale, lag: s.lags.length >= 3 ? med(s.lags) : null };
}

/* ================= snapshot (all pools of one chain) ================= */
const cache = new Map(), inflight = new Map();
function refresh(chain, fast) {
  const key = chain + (fast ? ':f' : '');
  if (inflight.has(key)) return inflight.get(key);
  const p = build(chain, { fast }).then(v => {
    const c = cache.get(chain);
    if (!fast || !c || !c.full) cache.set(chain, { t: Date.now(), v, full: !fast });
    return v;
  }).finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}
/* fast: whatever is cached right away. full: fresh data; slightly stale data is returned instantly while a refresh runs. */
function snapshot(chain, opt = {}) {
  const c = cache.get(chain), age = c ? Date.now() - c.t : Infinity;
  if (!opt.fresh && c) {
    if (opt.fast && age < STALE_MAX * 2.5) { if (age > TTL) refresh(chain, false).catch(() => {}); return Promise.resolve(c.v); }
    if (!opt.fast && c.full) {
      if (age < TTL) return Promise.resolve(c.v);
      if (age < STALE_MAX) { refresh(chain, false).catch(() => {}); return Promise.resolve(c.v); }
    }
  }
  return refresh(chain, !!opt.fast && !(c && c.full));
}
function warm() { // keeps every chain hot in memory
  const tick = async () => {
    try { const chains = await getChains(); await Promise.all(chains.map(c => refresh(c.id, false).catch(e => console.error('warm', c.id, e.message)))); }
    catch (e) { console.error('warm', e.message); }
  };
  tick(); return setInterval(tick, +ENV.WARM_MS || 10000);
}
async function build(chain, opt = {}) {
  const t0 = Date.now(), deadline = t0 + (+ENV.REF_BUDGET_MS || 8000);
  const meta = await metaAll(chain);
  if (opt.fast && rh.bySym.size) rhAssets(); else await rhAssets(); // the very first call waits, otherwise pools are priced before Robinhood tokens are known
  rhCorp();
  const pools = meta.map(m => {
    const s0 = (m.token0Symbol || '').toUpperCase(), s1 = (m.token1Symbol || '').toUpperCase();
    const isQuote = x => STABLE.has(x) || (!INCLUDE_CRYPTO && NON_RWA.has(x)); // the token the asset is priced in
    const bi = isQuote(s0) && !isQuote(s1) ? 1 : 0;
    return { m, bi, a: String(m.poolAddress).toLowerCase(), sym: (bi ? m.token1Symbol : m.token0Symbol) || '?' };
  }).filter(p => { const k = p.sym.toUpperCase(); return !STABLE.has(k) && (INCLUDE_CRYPTO || !NON_RWA.has(k)); });
  for (const p of pools) { const a = rh.byAddr.get(String(p.bi ? p.m.token1 : p.m.token0).toLowerCase()); if (a) rh.alias.set(p.sym.toUpperCase(), a.sym); }
  const symInfo = new Map();
  for (const p of pools) if (!symInfo.has(p.sym)) symInfo.set(p.sym, { fiat: p.bi ? p.m.token1FiatPrice : p.m.token0FiatPrice });
  let partial = false;
  const quotesP = quotesFor(chain, pools.map(p => p.m.poolAddress));
  const refsP = opt.fast
    ? Promise.resolve().then(() => {
        const r = {};
        for (const k of symInfo.keys()) { const v = peekRef(k); if (v) r[k] = v; else partial = true; }
        if (partial) resolveRefs(symInfo, deadline).catch(() => {}); // keep resolving for the next request
        return r;
      })
    : resolveRefs(symInfo, deadline);
  const [quotes, refs] = await Promise.all([quotesP, refsP]);

  const now = Date.now(), out = [];
  for (const p of pools) {
    const m = p.m, q = quotes.get(p.a) || {}, ref = refs[p.sym] || null, k = p.sym.toUpperCase(), bi = p.bi;
    const rhA = rh.byAddr.get(String(bi ? m.token1 : m.token0).toLowerCase()) || null, t0f = m.token0FiatPrice, t1f = m.token1FiatPrice, cs = m.classificationSummary || {}, an = m.analytics24h || {};
    const mk = m.markoutByWindow && m.markoutByWindow['7d'] && m.markoutByWindow['7d'].byHorizon && m.markoutByWindow['7d'].byHorizon['5m'];
    const pd = m.priceProviderDetails || {};
    const status = q.error ? 'unknown' : (q.priceProviderStatus || 'unknown');
    const bid = q.bidAdj != null ? q2n(q.bidAdj) : NaN, ask = q.askAdj != null && q.askAdj !== MAXU ? q2n(q.askAdj) : NaN;
    const bidOk = bid > 0, askOk = ask > 0;
    const oneSided = status === 'healthy' && q.depth != null && (bidOk !== askOk);
    let buy = null, sell = null;
    if (status === 'healthy' && bidOk && askOk) {
      if (!bi && t1f) { buy = ask * t1f; sell = bid * t1f; }
      else if (bi && t0f) { buy = t0f / bid; sell = t0f / ask; }
    }
    const usd = buy != null ? (buy + sell) / 2 : null, mult = multOf(k);
    let prem = null, edge = null, rr = null;
    if (ref) {
      const age = now / 1000 - ref.ts;
      rr = { price: ref.price, src: ref.src, tk: ref.tk, ts: ref.ts, age: Math.round(age), live: ref.src === 'Binance' || (ref.src === 'Robinhood' ? !!ref.rhLive : age <= 180), session: ref.session, halt: !!ref.halt };
      if (usd != null) {
        const fair = ref.price * mult;
        prem = (usd / fair - 1) * 1e4;                                   // mid price vs market
        edge = fair < sell ? (sell / fair - 1) * 1e4 : fair > buy ? (buy / fair - 1) * 1e4 : 0; // mispricing beyond the spread
      }
    }
    out.push({
      a: p.a, pair: m.pair, sym: p.sym, kind: GOLD.has(k) ? 'commodity' : CRYPTO[k] ? 'crypto' : !ref ? 'unknown' : (ref.it === 'ETF' || ETFS.has(String(ref.tk).toUpperCase())) ? 'etf' : 'stock', issuer: rhA ? 'Robinhood' : issuerOf(p.sym), bi, halt: !!(ref && ref.halt),
      corp: (() => { if (!rhA) return null; const c = rh.corp.get(rhA.sym); if (c) return c; return rhA.pending ? { desc: 'multiplier changes to ' + rhA.pending, date: rhA.pendingAt ? Date.parse(rhA.pendingAt) : null } : null; })(),
      logoKey: logoKeyOf(p.sym, rr),
      logo: (() => { const pre = bi ? 'token1' : 'token0'; let own = null; for (const f of Object.keys(m)) if (f.startsWith(pre) && /logo|image|icon/i.test(f) && typeof m[f] === 'string' && /^https?:\/\//.test(m[f])) { own = m[f]; break; }
        if (rhA || /robinhood/i.test(own || '')) return null; // that image is Robinhood branding, not the company's logo
        const t = logoKeyOf(p.sym, rr); if (own && t) ownLogos.set(t, own);
        return own; })(),
      company: (rhA && rhA.name) || cleanName(ref && ref.name) || COMPANY[String(ref ? ref.tk : '').toUpperCase()] || COMPANY[k.replace(/(ON|\.R|\.D|\.S|X)$/, '')] || COMPANY[k] || null,
      tier: cs.tierQualification || 'unclassified', reasons: cs.reasonCodes || [],
      tvl: m.tvlFiat ?? an.tvlFiat ?? null, vol24: an.volumeFiat ?? null, fees24: an.feesFiat ?? null, apr24: an.aprBps ?? null, apr7: m.aprBps7d ?? null,
      pnl7: m.tradingPnlAprBps7d ?? null, mk5: mk ? mk.aprBps ?? null : null,
      curator: m.adminUsername || null, verified: !!m.verifiedAdmin, cfee: m.spreadAdminFeeE6 != null ? m.spreadAdminFeeE6 / 1e4 : null,
      locked: !!m.lockedPricing, paused: !!m.pauseLevel, provider: pd.providerType || null, maxStale: pd.maxRefStaleness ?? pd.maxTimeDelta ?? null,
      status, quoteTs: q.serverTs || null, oneSided, buy, sell, usd, spread: usd ? (buy - sell) / usd * 1e4 : null,
      ref: rr, mult, prem, edge, mismatch: prem != null && Math.abs(prem) > 2000,
      t0: m.token0, t1: m.token1, d0: m.token0Decimals ?? 18, d1: m.token1Decimals ?? 18, f0: t0f, f1: t1f
    });
  }
  return { chain, ts: now, took: now - t0, total: pools.length, partial, pools: out };
}

/* ================= Firestore over REST (no SDK needed) ================= */
const SA = (() => {
  let s = ENV.FIREBASE_SERVICE_ACCOUNT; if (!s || ENV.COLLECT === '0') return null;
  try { return JSON.parse(s); } catch (e) { try { return JSON.parse(Buffer.from(s, 'base64').toString('utf8')); } catch (e2) { console.error('FIREBASE_SERVICE_ACCOUNT is not valid JSON or base64.'); return null; } }
})();
const FS_ROOT = ENV.FIRESTORE_BASE || 'https://firestore.googleapis.com', TOKEN_URL = ENV.TOKEN_URL || 'https://oauth2.googleapis.com/token';
const DOCS = () => `projects/${SA.project_id}/databases/(default)/documents`;
let tok = { v: null, exp: 0 };
async function token() {
  if (tok.v && Date.now() < tok.exp - 60000) return tok.v;
  const now = Math.floor(Date.now() / 1000), b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const head = b64({ alg: 'RS256', typ: 'JWT' }), claim = b64({ iss: SA.client_email, scope: 'https://www.googleapis.com/auth/datastore', aud: TOKEN_URL, iat: now, exp: now + 3600 });
  const sig = crypto.createSign('RSA-SHA256').update(head + '.' + claim).sign(SA.private_key, 'base64url');
  const r = await fetch(TOKEN_URL, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: `${head}.${claim}.${sig}` }) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error('Firebase auth failed: ' + (j.error_description || j.error || r.status));
  tok = { v: j.access_token, exp: Date.now() + (j.expires_in || 3600) * 1000 };
  return tok.v;
}
async function fsCall(method, url, body) {
  for (let i = 0; i < 2; i++) {
    const r = await fetch(url, { method, headers: { Authorization: 'Bearer ' + await token(), 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(20000) });
    if (r.status === 429 || r.status >= 500) { await sleep(800 * (i + 1)); continue; }
    if (r.status === 404) return null;
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error('Firestore ' + r.status + ': ' + ((j.error && j.error.message) || ''));
    return j;
  }
  throw new Error('Firestore unavailable');
}
const enc = v => v === null || v === undefined ? { nullValue: null } : typeof v === 'boolean' ? { booleanValue: v }
  : typeof v === 'number' ? (Number.isInteger(v) ? { integerValue: String(v) } : Number.isFinite(v) ? { doubleValue: v } : { nullValue: null })
  : typeof v === 'string' ? { stringValue: v } : Array.isArray(v) ? { arrayValue: { values: v.map(enc) } }
  : { mapValue: { fields: Object.fromEntries(Object.entries(v).filter(([, x]) => x !== undefined).map(([k, x]) => [k, enc(x)])) } };
const dec = v => 'nullValue' in v ? null : 'booleanValue' in v ? v.booleanValue : 'integerValue' in v ? +v.integerValue : 'doubleValue' in v ? v.doubleValue
  : 'stringValue' in v ? v.stringValue : 'arrayValue' in v ? (v.arrayValue.values || []).map(dec)
  : 'mapValue' in v ? Object.fromEntries(Object.entries(v.mapValue.fields || {}).map(([k, x]) => [k, dec(x)])) : null;
const fsGet = async p => { const d = await fsCall('GET', `${FS_ROOT}/v1/${DOCS()}/${p}`); return d ? dec({ mapValue: { fields: d.fields || {} } }) : null; };
const wSet = (p, obj) => ({ update: { name: `${DOCS()}/${p}`, fields: enc(obj).mapValue.fields } });
const wMerge = (p, obj) => ({ ...wSet(p, obj), updateMask: { fieldPaths: Object.keys(obj) } });
async function fsCommit(writes) { for (let i = 0; i < writes.length; i += 400) await fsCall('POST', `${FS_ROOT}/v1/${DOCS()}:commit`, { writes: writes.slice(i, i + 400) }); }
async function fsOldTicks(poolDoc, cut) {
  const r = await fsCall('POST', `${FS_ROOT}/v1/${DOCS()}/${poolDoc}:runQuery`, { structuredQuery: { from: [{ collectionId: 'ticks' }], where: { fieldFilter: { field: { fieldPath: 't' }, op: 'LESS_THAN', value: { integerValue: String(cut) } } }, limit: 400 } });
  return (r || []).filter(x => x.document).map(x => x.document.name);
}

/* ================= background collector ================= */
async function runChain(chain, st, write) {
  const now = Date.now(), sp = st.pools || (st.pools = {});
  const snap = await snapshot(chain);
  const tracked = snap.pools.filter(p => p.ref && !p.mismatch).slice(0, MAX_POOLS); // already sorted by TVL
  const ticks = [], alerts = [];
  for (const p of snap.pools.filter(x => x.mismatch && x.ref)) {
    const s = sp[p.a] || (sp[p.a] = { h: [], al: {} });
    if (now - (s.al.MAPPING || 0) > 36e5) { s.al.MAPPING = now; alerts.push({ t: now, pool: p.a, pair: p.pair, type: 'MAPPING', msg: 'Gap is over 20%. The market ticker or token multiplier is probably mapped wrong.', chain }); }
  }
  for (const p of tracked) {
    const s = sp[p.a] || (sp[p.a] = { h: [], al: {} });
    const add = (type, msg, cd = 3e5) => { if (now - (s.al[type] || 0) < cd) return; s.al[type] = now; alerts.push({ t: now, pool: p.a, pair: p.pair, type, msg, chain }); };
    if (p.status !== 'healthy') add('STATUS', s.status && s.status !== p.status ? `Price provider changed from ${s.status} to ${p.status}` : `Price provider is ${p.status}`);
    if (s.tier && s.tier !== p.tier) add('TIER', `Tier changed from ${s.tier} to ${p.tier}`);
    s.status = p.status; s.tier = p.tier;
    if (p.oneSided) add('ONE_SIDED', 'Only one side of the book is quoting');
    if (p.usd == null) continue;
    const tk = track(s, now, p.ref.price * p.mult, p.usd), live = p.ref.live, thr = live ? THR : THR * 3;
    if (Math.abs(p.edge) > thr) add('DEVIATION', `Pool is ${sg(p.edge)} bps outside the market price after spread (limit ${thr.toFixed(0)}${live ? '' : ', market closed'})`);
    if (s.h.length >= 20) {
      const mean = s.h.reduce((x, y) => x + y) / s.h.length;
      if (Math.abs(p.prem - mean) / (sd(s.h) || 1) > 3 && Math.abs(p.prem) > 15) add('ANOMALY', `Quote moved >3σ from its own history (${sg(p.prem)} bps)`);
      if (Math.abs(med(s.h)) > 40 && sd(s.h) < 20) add('OFFSET', `Constant offset of about ${sg(med(s.h))} bps. This often means an issuer multiplier. Set MULTIPLIERS in server.js.`, 36e5);
    }
    if (live && tk.stale > 120) add('STALE', `Pool price unchanged for ${Math.round(tk.stale)}s while the market is live`);
    s.h.push(+p.prem.toFixed(1)); if (s.h.length > 60) s.h.shift();
    ticks.push({ nm: !s.mw, id: p.a, pair: p.pair, sym: p.sym, tvl: p.tvl || 0, doc: { t: now, prem: +p.prem.toFixed(2), edge: +p.edge.toFixed(2), ok: p.status === 'healthy' ? 1 : 0, live: live ? 1 : 0, st: Math.round(tk.stale), usd: p.usd, ref: p.ref.price, src: p.ref.src, spread: +(p.spread || 0).toFixed(2), lag: tk.lag } });
  }
  const w = alerts.map(a => wSet('alerts/' + crypto.randomBytes(10).toString('hex'), a));
  if (alerts.length) console.log(`[${chain}] ${alerts.length} alert(s):`, alerts.map(a => a.pair + ' ' + a.type).join(', '));
  if (!write) { await fsCommit(w); return; }

  const metaDue = now - (st.metaAt || 0) > 864e5;
  for (const t of ticks) {
    w.push(wSet(`pools/${chain}_${t.id}/ticks/${now}`, t.doc));
    if (metaDue || t.nm) { sp[t.id].mw = 1; w.push(wMerge(`pools/${chain}_${t.id}`, { pair: t.pair, chain: String(chain), addr: t.id, sym: t.sym, tvl: t.tvl, updated: now })); }
  }
  if (metaDue) st.metaAt = now;
  st.t = now; st.tracked = ticks.length;
  w.push(wSet(`collector/${chain}`, st));
  await fsCommit(w);
  console.log(`[${chain}] wrote ${ticks.length} ticks (${snap.total} pools)`);
  if (now - (st.cleanAt || 0) > 864e5) { // daily retention cleanup
    const cut = now - RET_DAYS * 864e5;
    for (const t of ticks) for (;;) { const names = await fsOldTicks(`pools/${chain}_${t.id}`, cut); if (!names.length) break; await fsCommit(names.map(name => ({ delete: name }))); if (names.length < 400) break; }
    st.cleanAt = now;
  }
}
async function collect() {
  let chains = (ENV.CHAINS || '').split(',').map(s => s.trim()).filter(Boolean);
  while (!chains.length) { try { chains = (await getChains()).map(c => c.id); } catch (e) { console.error('collector waiting for chains:', e.message); await sleep(15000); } }
  console.log('Collector on. Chains:', chains.join(', '), '| writes every', WRITE, 's');
  const states = {};
  for (const c of chains) { try { states[c] = (await fsGet('collector/' + c)) || { pools: {} }; } catch (e) { console.error('collector state:', e.message); states[c] = { pools: {} }; } }
  try { const rm = await fsGet('collector/refmap'); if (rm) { (rm.known || []).forEach(x => known.set(x.k, { src: x.src, id: x.id })); (rm.fails || []).forEach(x => fails.set(x.k, x.u)); } } catch (e) { /* optional */ }
  let last = 0;
  for (;;) {
    const w = Date.now() - last >= WRITE * 1000;
    for (const c of chains) { try { await runChain(c, states[c], w); } catch (e) { console.error(`[${c}] collector:`, e.message); } }
    if (w) {
      try { await fsCommit([wSet('collector/refmap', { known: [...known].map(([k, v]) => ({ k, src: v.src, id: v.id })), fails: [...fails].filter(([, u]) => u > Date.now()).map(([k, u]) => ({ k, u })) }), wSet('collector/heartbeat', { t: Date.now(), chains })]); last = Date.now(); }
      catch (e) { console.error('collector:', e.message); last = Date.now() - WRITE * 1000 + 30000; }
    }
    await sleep(POLL * 1000);
  }
}

/* ================= token logos ================= */
// The server downloads, checks and caches logos, so the browser only ever asks this server (no blocked or slow third-party hosts).
const LOGO_BASES = ENV.LOGO_BASE ? [ENV.LOGO_BASE + '/a/{t}.png', ENV.LOGO_BASE + '/b/{t}.png'] : [
  'https://raw.githubusercontent.com/nvstly/icons/main/ticker_icons/{t}.png',
  'https://cdn.jsdelivr.net/gh/nvstly/icons@main/ticker_icons/{t}.png', // same files through a CDN
  'https://financialmodelingprep.com/image-stock/{t}.png',
  'https://assets.parqet.com/logos/symbol/{t}?format=png',
  'https://eodhd.com/img/logos/US/{t}.png'];
const ownLogos = new Map(), logoCache = new Map(); // ticker -> Metric's logo URL; ticker -> { until, p }
function imageType(b) { // trust the bytes, not the header: many hosts answer 200 with an HTML error page
  if (b.length < 100 || b.length > 1e6) return null;
  const hex = b.subarray(0, 4).toString('hex');
  if (hex === '89504e47') return 'image/png';
  if (hex.startsWith('ffd8ff')) return 'image/jpeg';
  if (hex === '47494638') return 'image/gif';
  if (hex === '52494646' && b.subarray(8, 12).toString() === 'WEBP') return 'image/webp';
  const head = b.subarray(0, 600).toString('utf8').trimStart().toLowerCase();
  return head.startsWith('<svg') || ((head.startsWith('<?xml') || head.startsWith('<!--')) && head.includes('<svg')) ? 'image/svg+xml' : null;
}
async function fetchImage(url) {
  try {
    const r = await fetch(url, { headers: { ...UA, Accept: 'image/*' }, signal: AbortSignal.timeout(+ENV.LOGO_TIMEOUT_MS || 6000) });
    if (!r.ok) return { retry: r.status === 429 || r.status >= 500 };
    const body = Buffer.from(await r.arrayBuffer()), type = imageType(body);
    return type ? { body, type, from: new URL(url).host } : { retry: false };
  } catch (e) { return { retry: true }; }
}
async function findLogo(t) {
  const names = [...new Set([t, t.replace(/\./g, '-')])];
  // Stock company logos come first. Robinhood's token artwork is never used; Metric's own image is only the last resort.
  const urls = [...names.flatMap(n => LOGO_BASES.map(b => b.replace('{t}', encodeURIComponent(n)))), ownLogos.get(t)].filter(Boolean);
  const tries = urls.map(fetchImage); // all at once, then take the best one in priority order
  let retry = false;
  for (const p of tries) { const r = await p; if (r.body) return r; retry = retry || r.retry; }
  return { retry };
}
function getLogo(t) {
  const c = logoCache.get(t);
  if (c && Date.now() < c.until) return c.p;
  if (logoCache.size >= 1000) logoCache.delete(logoCache.keys().next().value);
  const e = { until: Infinity, p: null };
  e.p = findLogo(t).catch(() => ({ retry: true })).then(r => { e.until = Date.now() + (r.body ? 864e5 : r.retry ? 3e4 : 6e5); return r; });
  logoCache.set(t, e);
  return e.p;
}
const TICKER_RE = /^[A-Z0-9.\-]{1,12}$/;
/* GET /api/logo/<ticker> */
async function apiLogo(res, t) {
  if (!TICKER_RE.test(t)) { res.statusCode = 400; return res.end('Invalid ticker'); }
  const r = await getLogo(t);
  if (!r.body) { res.statusCode = 404; res.setHeader('Cache-Control', 'no-store'); return res.end('No logo'); }
  res.setHeader('Content-Type', r.type); res.setHeader('Cache-Control', 'public, max-age=86400');
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox");
  res.setHeader('X-Logo-Source', r.from);
  res.end(r.body);
}
/* GET /api/logo-check?chain=<id> : which listed tokens have a logo, and from which host */
async function apiLogoCheck(res, u) {
  try {
    const want = u.searchParams.get('chain');
    if (want && !/^[0-9]{1,12}$/.test(want)) return send(res, 400, { error: 'Invalid chain' });
    const snap = await snapshot(want || defaultChain(await getChains()));
    const list = [...new Set(snap.pools.filter(p => p.kind !== 'commodity').map(p => p.logoKey).filter(Boolean))];
    const found = {}, missing = []; let n = 0;
    await Promise.all(Array.from({ length: 8 }, async () => { while (n < list.length) { const t = list[n++], r = await getLogo(t); if (r.body) found[t] = r.from; else missing.push(t + (r.retry ? ' (network error, will retry)' : ' (no source has it)')); } }));
    send(res, 200, { chain: snap.chain, total: list.length, ok: Object.keys(found).length, missing: missing.sort(), found });
  } catch (e) { send(res, 502, { error: e.message }); }
}
/* Same rule as baseTk() in index.html */
function logoKeyOf(sym, ref) {
  const raw = ref && ref.tk && ref.src !== 'Binance' ? ref.tk : String(sym || '').replace(/(\.[rds]|on|x)$/i, '').replace(/^[xb](?=[A-Z]{2,})/, '');
  const t = String(raw).toUpperCase().replace(/[^A-Z0-9.\-]/g, '');
  return TICKER_RE.test(t) ? t : null;
}

/* ================= web server ================= */
function send(res, code, obj, headers = {}) {
  const body = JSON.stringify(obj, (k, v) => (typeof v === 'number' && !Number.isInteger(v) ? +v.toPrecision(8) : v));
  res.statusCode = code;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Access-Control-Allow-Origin', '*');
  for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
  const ae = (res.req && res.req.headers['accept-encoding']) || '';
  if (body.length > 1024 && /\bgzip\b/.test(ae)) { const z = zlib.gzipSync(body); res.setHeader('Content-Encoding', 'gzip'); res.setHeader('Vary', 'Accept-Encoding'); res.setHeader('Content-Length', z.length); return res.end(z); }
  res.end(body);
}
async function readBody(req) { const chunks = []; let n = 0; for await (const c of req) { n += c.length; if (n > 1e5) throw new Error('Body too large'); chunks.push(c); } const s = Buffer.concat(chunks).toString('utf8'); return s ? JSON.parse(s) : {}; }

/* GET /api/snapshot?chain=<id>[&mode=fast] */
async function apiSnapshot(req, res, u) {
  const want = u.searchParams.get('chain'), fast = u.searchParams.get('mode') === 'fast';
  try {
    if (want && !/^[0-9]{1,12}$/.test(want)) return send(res, 400, { error: 'Invalid chain' });
    let chains, snap;
    if (want) [chains, snap] = await Promise.all([getChains(), snapshot(want, { fast })]);
    else { chains = await getChains(); snap = await snapshot(defaultChain(chains), { fast }); }
    send(res, 200, { chains, ...snap }, { 'Cache-Control': 'no-cache' });
  } catch (e) { send(res, e.status === 404 ? 404 : 502, { error: e.status === 404 ? 'Unknown chain ' + want : e.message }); }
}
/* POST /api/route { chain, pool, tokenIn, tokenOut, amountIn }: Metric's "Find best route" limited to one pool (exact fill) */
const ADDR = /^0x[0-9a-fA-F]{40}$/;
async function apiRoute(req, res) {
  try {
    const b = await readBody(req);
    if (!/^[0-9]+$/.test(String(b.chain)) || !ADDR.test(b.pool || '') || !ADDR.test(b.tokenIn || '') || !ADDR.test(b.tokenOut || '') || !/^[0-9]{1,40}$/.test(String(b.amountIn))) return send(res, 400, { error: 'Invalid request' });
    const r = await fetch(`${API}/public/v1/evm/${b.chain}/route`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(12000),
      body: JSON.stringify({ tokenIn: b.tokenIn, tokenOut: b.tokenOut, tradeType: 'exact_in', amountIn: String(b.amountIn), maxRoutes: 1, poolAddress: b.pool }) });
    const txt = await r.text(); let j; try { j = JSON.parse(txt); } catch (e) { j = { error: txt.slice(0, 200) }; }
    send(res, r.status, j);
  } catch (e) { send(res, 502, { error: e.message }); }
}
let page = null;
function servePage(req, res) {
  if (!page || ENV.NODE_ENV !== 'production') { try { const raw = fs.readFileSync(path.join(HERE, 'index.html')); page = { raw, gz: zlib.gzipSync(raw) }; } catch (e) { res.statusCode = 500; return res.end('index.html is missing next to server.js'); } }
  res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-cache');
  if (/\bgzip\b/.test(req.headers['accept-encoding'] || '')) { res.setHeader('Content-Encoding', 'gzip'); res.setHeader('Vary', 'Accept-Encoding'); return res.end(page.gz); }
  res.end(page.raw);
}
http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') return send(res, 204, {});
  if (u.pathname === '/api/snapshot') return apiSnapshot(req, res, u);
  if (u.pathname === '/api/route' && req.method === 'POST') return apiRoute(req, res);
  if (u.pathname.startsWith('/api/logo/')) return apiLogo(res, u.pathname.slice(10).toUpperCase());
  if (u.pathname === '/api/logo-check') return apiLogoCheck(res, u);
  if (u.pathname === '/healthz') return send(res, 200, { ok: true, chains: [...cache.keys()], collector: !!SA });
  if (u.pathname === '/' || u.pathname === '/index.html') return servePage(req, res);
  res.statusCode = 404; res.end('Not found');
}).listen(+ENV.PORT || 3000, () => {
  console.log('RWA Reality Check on http://localhost:' + (+ENV.PORT || 3000));
  getChains().catch(() => {});
  if (ENV.WARM !== '0') warm();
  if (SA) collect().catch(e => console.error('collector stopped:', e.message)); else console.log('Collector off (set FIREBASE_SERVICE_ACCOUNT to record history).');
});