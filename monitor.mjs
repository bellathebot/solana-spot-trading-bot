#!/usr/bin/env node
// Solana Trading Monitor — runs on cron, alerts on opportunities
// Usage: node monitor.mjs [--json] [--alert-only]
// Easter egg tag for future strategy metadata experiments: ethan was here

import fs from 'fs';
import { execSync, spawnSync } from 'child_process';
import { DATA_DIR, DB_CLI, DB_PATH, JUP_BIN, PATH_ENV, REPO_ROOT } from './runtime-config.mjs';

const PULLBACK_CONTINUATION_MAX_DISTANCE_PCT = parseFloat(process.env.AUTO_TRADER_PULLBACK_CONTINUATION_MAX_DISTANCE_PCT || '2');
const PULLBACK_CONTINUATION_MIN_CHANGE_24H = parseFloat(process.env.AUTO_TRADER_PULLBACK_CONTINUATION_MIN_CHANGE_24H || '1');
const PULLBACK_CONTINUATION_MAX_CHANGE_24H = parseFloat(process.env.AUTO_TRADER_PULLBACK_CONTINUATION_MAX_CHANGE_24H || '8');

const WALLET = 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu';
const PRICE_HISTORY = `${DATA_DIR}/price_history.json`;
const ALERTS_LOG = `${DATA_DIR}/alerts.log`;
const EASTER_EGG_TAG = 'ethan was here';

const flags = process.argv.slice(2);
const JSON_MODE = flags.includes('--json');
const ALERT_ONLY = flags.includes('--alert-only');

const SYSTEM_WATCHLIST = {
  'So11111111111111111111111111111111111111112': { symbol: 'SOL', alertBelow: 79.50, buyBelow: 77.50, sellAbove: 92.0, tier: 1, maxBuyUsd: 3.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
  '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh': { symbol: 'BTC', alertBelow: 65500.0, buyBelow: 64800.0, sellAbove: 74800.0, tier: 1, maxBuyUsd: 3.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': { symbol: 'USDC', alertBelow: null, buyBelow: null, sellAbove: null, tier: 0, maxBuyUsd: null, maxSymbolRiskUsd: null, paperEligible: false, liveEligible: false },
};

const TIERED_SYMBOLS = [
  {
    tier: 1,
    symbols: [
      { mint: 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', symbol: 'JUP', alertBelow: 0.1600, buyBelow: 0.1565, sellAbove: 0.2050, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: true },
      { mint: 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', symbol: 'PYTH', alertBelow: 0.0420, buyBelow: 0.0412, sellAbove: 0.0550, maxBuyUsd: 10.00, maxSymbolRiskUsd: 10.00, paperEligible: true, liveEligible: true },
      { mint: '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', symbol: 'RAY', alertBelow: 0.6100, buyBelow: 0.6020, sellAbove: 0.7500, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: true },
    ],
  },
  {
    tier: 2,
    symbols: [
      { mint: 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL', symbol: 'JTO', alertBelow: 0.2850, buyBelow: 0.2700, sellAbove: 0.3800, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
    ],
  },
  {
    tier: 3,
    symbols: [
      { mint: 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', symbol: 'BONK', alertBelow: 0.00000625, buyBelow: 0.00000610, sellAbove: 0.00000920, maxBuyUsd: 1.50, maxSymbolRiskUsd: 3.00, paperEligible: false, liveEligible: false },
      { mint: 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', symbol: 'WIF', alertBelow: 0.1760, buyBelow: 0.1725, sellAbove: 0.2350, maxBuyUsd: 1.50, maxSymbolRiskUsd: 3.00, paperEligible: true, liveEligible: true },
    ],
  },
];

const WATCHLIST = {
  ...SYSTEM_WATCHLIST,
  ...Object.fromEntries(
    TIERED_SYMBOLS.flatMap(({ tier, symbols }) => symbols.map(config => [config.mint, { ...config, tier }]))
  ),
};

const ALERT_THRESHOLDS = {
  nearBuyPct: 5,
  takeProfitWindowPct: 10,
  momentumPct: 3,
  bigDrop24h: -5,
  bigPump24h: 15,
};

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function inferRegimeContext({ change24h = 0, shortChange = null, marketChange24h = 0 }) {
  if (change24h <= -8 || (marketChange24h <= -5 && change24h <= -5)) {
    return { tag: 'panic_selloff', detail: 'risk_off_capitulation', explanation: 'Deep local drop or market-wide selloff detected.' };
  }
  if (change24h <= -3 || (shortChange !== null && shortChange <= -2)) {
    return { tag: 'trend_down', detail: 'stable_bearish', explanation: 'Symbol is trading with a bearish local or 24h trend.' };
  }
  if (change24h >= 8 || (shortChange !== null && shortChange >= 3)) {
    return { tag: 'trend_up', detail: 'expansion_bullish', explanation: 'Symbol is extended upward and mean reversion quality is weaker.' };
  }
  if (shortChange !== null && Math.abs(shortChange) >= 1.5) {
    return { tag: 'choppy', detail: 'unstable_rotation', explanation: 'Short-horizon movement is unstable enough to treat as choppy.' };
  }
  return { tag: 'stable', detail: 'stable_mean_reversion_friendly', explanation: 'Price action is calm enough for baseline mean reversion logic.' };
}

function inferRegimeTag({ change24h = 0, shortChange = null, marketChange24h = 0 }) {
  return inferRegimeContext({ change24h, shortChange, marketChange24h }).tag;
}

function thresholdDistanceScore(signalType, distancePct) {
  if (signalType === 'hard_buy') {
    const overshoot = Math.abs(Math.min(0, Number(distancePct || 0)));
    return clamp(14 + overshoot * 2, 0, 20);
  }
  const nearPct = Math.max(0, Number(distancePct || 0));
  return clamp(20 - (nearPct * 3), 0, 20);
}

function liquidityScore(liquidity) {
  const liq = Number(liquidity || 0);
  if (liq <= 0) return 0;
  if (liq < 100000) return 0;
  if (liq < 250000) return 5;
  if (liq < 1000000) return 9;
  if (liq < 5000000) return 12;
  return 15;
}

function regimeScore(regimeTag, side = 'buy') {
  if (side !== 'buy') return 5;
  switch (regimeTag) {
    case 'stable': return 5;
    case 'choppy': return 3;
    case 'trend_up': return 2;
    case 'trend_down': return 0;
    case 'panic_selloff': return 0;
    default: return 2;
  }
}

function tierScore(tier) {
  if (tier === 1) return 10;
  if (tier === 2) return 7;
  if (tier === 3) return 4;
  return 2;
}

function buildMonitorSignalScore({ signalType, distancePct, liquidity, regimeTag, tier }) {
  const components = {
    threshold_distance: thresholdDistanceScore(signalType, distancePct),
    liquidity: liquidityScore(liquidity),
    regime: regimeScore(regimeTag, 'buy'),
    tier: tierScore(tier),
  };
  return {
    score: clamp(Object.values(components).reduce((sum, value) => sum + value, 0), 0, 100),
    components,
  };
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function execJson(cmd) {
  try {
    const raw = execSync(cmd, {
      encoding: 'utf-8',
      timeout: 30000,
      env: { ...process.env, PATH: PATH_ENV },
      stdio: ['pipe', 'pipe', 'pipe'],
      maxBuffer: 1024 * 1024,
    });
    return JSON.parse(raw.trim());
  } catch (e) {
    try { return JSON.parse(e.stdout?.toString().trim()); } catch {}
    return null;
  }
}

function runDbCli(command, payload) {
  try {
    const result = spawnSync('python', [DB_CLI, command, '--db', DB_PATH], {
      input: JSON.stringify(payload),
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: REPO_ROOT },
      timeout: 30000,
    });
    if (result.status !== 0) {
      console.error(`DB CLI ${command} failed: ${(result.stderr || result.stdout || '').trim()}`);
      return false;
    }
    return true;
  } catch (e) {
    console.error(`DB CLI ${command} error: ${e.message}`);
    return false;
  }
}

function loadPriceHistory() {
  try { return JSON.parse(fs.readFileSync(PRICE_HISTORY, 'utf-8')); }
  catch { return { snapshots: [] }; }
}

function savePriceHistory(data) {
  data.snapshots = data.snapshots.slice(-720);
  fs.writeFileSync(PRICE_HISTORY, JSON.stringify(data, null, 2));
}

function logAlert(msg) {
  const ts = new Date().toISOString();
  fs.appendFileSync(ALERTS_LOG, `[${ts}] ${msg}\n`);
}

async function fetchPrices() {
  const mints = Object.keys(WATCHLIST).join(',');
  const res = await fetch(`https://lite-api.jup.ag/price/v3?ids=${mints}`);
  return res.json();
}

function getPortfolio() {
  return execJson(`${JUP_BIN} spot portfolio --address ${WALLET}`);
}

function formatPrice(price) {
  if (price == null) return '—';
  if (price < 0.01) return `$${price.toFixed(8)}`;
  if (price < 1) return `$${price.toFixed(6)}`;
  return `$${price.toFixed(2)}`;
}

function percentFromLevel(price, level) {
  if (!level) return null;
  return ((price - level) / level) * 100;
}

function recordSignalCandidate(candidate) {
  return runDbCli('record-signal', candidate);
}

async function main() {
  ensureDataDir();
  const now = new Date();
  const ts = now.toISOString();
  const alerts = [];
  const scoredSignals = [];

  const prices = await fetchPrices();
  const portfolio = getPortfolio();
  if (!portfolio) throw new Error('Could not fetch portfolio');

  const holdingsByMint = new Map();
  const holdingsBySymbol = new Map();
  for (const token of portfolio.tokens || []) {
    holdingsByMint.set(token.id, token);
    if (token.symbol) holdingsBySymbol.set(token.symbol, token);
  }

  const solHolding = holdingsBySymbol.get('SOL');
  const usdcHolding = holdingsBySymbol.get('USDC');
  const solBal = solHolding?.amount || 0;
  const usdcBal = usdcHolding?.amount || 0;
  const totalUsd = portfolio.totalValue || 0;

  const snapshot = { ts, prices: {}, wallet: { sol: solBal, usdc: usdcBal, totalUsd } };
  for (const [mint, config] of Object.entries(WATCHLIST)) {
    const p = prices[mint];
    if (!p) continue;
    const holding = holdingsByMint.get(mint) || holdingsBySymbol.get(config.symbol);
    snapshot.prices[config.symbol] = {
      price: p.usdPrice,
      change24h: p.priceChange24h || 0,
      liquidity: p.liquidity || 0,
      holdingAmount: holding?.amount || 0,
      holdingValue: holding?.value || 0,
      alertBelow: config.alertBelow,
      buyBelow: config.buyBelow,
      sellAbove: config.sellAbove,
      tier: config.tier,
      paperEligible: config.paperEligible,
      liveEligible: config.liveEligible,
    };
  }

  const history = loadPriceHistory();
  const prevSnapshot = history.snapshots.length > 0 ? history.snapshots[history.snapshots.length - 1] : null;
  const marketChange24h = prices['So11111111111111111111111111111111111111112']?.priceChange24h || 0;

  for (const [mint, config] of Object.entries(WATCHLIST)) {
    if (config.symbol === 'USDC') continue;
    const p = prices[mint];
    if (!p) continue;

    const price = p.usdPrice;
    const change24h = p.priceChange24h || 0;
    const prevPrice = prevSnapshot?.prices?.[config.symbol]?.price || null;
    const shortChange = prevPrice ? ((price - prevPrice) / prevPrice) * 100 : null;
    const regimeContext = inferRegimeContext({ change24h, shortChange, marketChange24h });
    const regimeTag = regimeContext.tag;
    const holding = holdingsByMint.get(mint) || holdingsBySymbol.get(config.symbol);
    const holdingValue = holding?.value || 0;
    const hasPosition = holdingValue > 0;

    if (change24h <= ALERT_THRESHOLDS.bigDrop24h) {
      alerts.push(`🔴 BIG DROP: ${config.symbol} ${change24h.toFixed(2)}% (24h) at ${formatPrice(price)}`);
    }

    if (change24h >= ALERT_THRESHOLDS.bigPump24h) {
      alerts.push(`🟢 BIG PUMP: ${config.symbol} ${change24h.toFixed(2)}% (24h) at ${formatPrice(price)}`);
    }

    if (config.buyBelow) {
      const distanceFromBuyPct = percentFromLevel(price, config.buyBelow);
      if (distanceFromBuyPct !== null && price > config.buyBelow && distanceFromBuyPct <= ALERT_THRESHOLDS.nearBuyPct) {
        const monitorScore = buildMonitorSignalScore({ signalType: 'near_buy', distancePct: distanceFromBuyPct, liquidity: p.liquidity || 0, regimeTag, tier: config.tier });
        alerts.push(`👀 NEAR-BUY: ${config.symbol} at ${formatPrice(price)} is within ${distanceFromBuyPct.toFixed(2)}% of auto-buy ${formatPrice(config.buyBelow)}`);
        const nearBuyCandidate = {
          ts,
          source: 'monitor.mjs',
          symbol: config.symbol,
          signal_type: 'near_buy',
          strategy_tag: 'mean_reversion_near_buy',
          side: 'buy',
          price,
          reference_level: config.buyBelow,
          distance_pct: distanceFromBuyPct,
          liquidity: p.liquidity || 0,
          score: monitorScore.score,
          regime_tag: regimeTag,
          status: 'candidate',
          reason: 'Near-buy signal from monitor',
          metadata: { change24h, shortChange, marketChange24h, tier: config.tier, paperEligible: config.paperEligible, liveEligible: config.liveEligible, score_components: monitorScore.components, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation }
        };
        scoredSignals.push({ symbol: config.symbol, signal_type: 'near_buy', score: monitorScore.score, regime_tag: regimeTag, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation, components: monitorScore.components });
        recordSignalCandidate(nearBuyCandidate);
        const pullbackContinuationEligible = config.tier === 1 && price > config.buyBelow && distanceFromBuyPct <= PULLBACK_CONTINUATION_MAX_DISTANCE_PCT && change24h >= PULLBACK_CONTINUATION_MIN_CHANGE_24H && change24h <= PULLBACK_CONTINUATION_MAX_CHANGE_24H && shortChange !== null && shortChange <= 0.75 && ['stable', 'trend_up'].includes(regimeTag);
        if (pullbackContinuationEligible) {
          const pullbackScore = buildMonitorSignalScore({ signalType: 'near_buy', distancePct: distanceFromBuyPct, liquidity: p.liquidity || 0, regimeTag, tier: config.tier });
          const pullbackCandidate = {
            ts,
            source: 'monitor.mjs',
            symbol: config.symbol,
            signal_type: 'pullback_buy',
            strategy_tag: 'pullback_continuation_tier1',
            side: 'buy',
            price,
            reference_level: config.buyBelow,
            distance_pct: distanceFromBuyPct,
            liquidity: p.liquidity || 0,
            score: pullbackScore.score,
            regime_tag: regimeTag,
            status: 'candidate',
            reason: 'Tier-1 pullback continuation setup from monitor',
            metadata: { change24h, shortChange, marketChange24h, tier: config.tier, paperEligible: config.paperEligible, liveEligible: config.liveEligible, score_components: pullbackScore.components, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation }
          };
          scoredSignals.push({ symbol: config.symbol, signal_type: 'pullback_buy', score: pullbackScore.score, regime_tag: regimeTag, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation, components: pullbackScore.components });
          recordSignalCandidate(pullbackCandidate);
        }
      }
      if (price <= config.buyBelow) {
        const hardBuyDistancePct = percentFromLevel(price, config.buyBelow);
        const monitorScore = buildMonitorSignalScore({ signalType: 'hard_buy', distancePct: hardBuyDistancePct, liquidity: p.liquidity || 0, regimeTag, tier: config.tier });
        alerts.push(`💰 BUY SIGNAL: ${config.symbol} at ${formatPrice(price)} is at/below auto-buy ${formatPrice(config.buyBelow)}`);
        const hardBuyCandidate = {
          ts,
          source: 'monitor.mjs',
          symbol: config.symbol,
          signal_type: 'hard_buy',
          strategy_tag: 'mean_reversion_near_buy',
          side: 'buy',
          price,
          reference_level: config.buyBelow,
          distance_pct: hardBuyDistancePct,
          liquidity: p.liquidity || 0,
          score: monitorScore.score,
          regime_tag: regimeTag,
          status: 'candidate',
          reason: 'Hard buy signal from monitor',
          metadata: { change24h, shortChange, marketChange24h, tier: config.tier, paperEligible: config.paperEligible, liveEligible: config.liveEligible, score_components: monitorScore.components, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation }
        };
        scoredSignals.push({ symbol: config.symbol, signal_type: 'hard_buy', score: monitorScore.score, regime_tag: regimeTag, regime_detail: regimeContext.detail, regime_explanation: regimeContext.explanation, components: monitorScore.components });
        recordSignalCandidate(hardBuyCandidate);
      }
    }

    if (hasPosition && config.sellAbove) {
      const distanceToSellPct = ((config.sellAbove - price) / config.sellAbove) * 100;
      if (price < config.sellAbove && distanceToSellPct >= 0 && distanceToSellPct <= ALERT_THRESHOLDS.takeProfitWindowPct) {
        alerts.push(`🎯 TAKE-PROFIT WATCH: ${config.symbol} at ${formatPrice(price)} is within ${distanceToSellPct.toFixed(2)}% of sell target ${formatPrice(config.sellAbove)}`);
      }
      if (price >= config.sellAbove) {
        alerts.push(`📈 SELL SIGNAL: ${config.symbol} at ${formatPrice(price)} is at/above sell target ${formatPrice(config.sellAbove)}`);
      }
    }

    if (prevPrice) {
      const shortChange = ((price - prevPrice) / prevPrice) * 100;
      if (Math.abs(shortChange) > ALERT_THRESHOLDS.momentumPct) {
        const monitorScore = buildMonitorSignalScore({ signalType: 'momentum', distancePct: shortChange, liquidity: p.liquidity || 0, regimeTag, tier: config.tier });
        const dir = shortChange > 0 ? '⬆️' : '⬇️';
        alerts.push(`${dir} MOMENTUM: ${config.symbol} moved ${shortChange.toFixed(2)}% since last check (${formatPrice(prevPrice)} → ${formatPrice(price)})`);
        const momentumCandidate = {
          ts,
          source: 'monitor.mjs',
          symbol: config.symbol,
          signal_type: 'momentum',
          strategy_tag: 'momentum',
          side: shortChange > 0 ? 'buy' : 'sell',
          price,
          reference_level: prevPrice,
          distance_pct: shortChange,
          liquidity: p.liquidity || 0,
          score: monitorScore.score,
          regime_tag: regimeTag,
          status: 'candidate',
          reason: 'Momentum signal from monitor',
          metadata: { change24h, shortChange, prevPrice, marketChange24h, tier: config.tier, paperEligible: config.paperEligible, liveEligible: config.liveEligible, score_components: monitorScore.components }
        };
        scoredSignals.push({ symbol: config.symbol, signal_type: 'momentum', score: monitorScore.score, regime_tag: regimeTag, components: monitorScore.components });
        recordSignalCandidate(momentumCandidate);
      }
    }
  }

  history.snapshots.push(snapshot);
  savePriceHistory(history);

  for (const alert of alerts) logAlert(alert);
  runDbCli('record-snapshot', { snapshot, alerts });

  if (JSON_MODE) {
    console.log(JSON.stringify({ ts, snapshot, alerts, scored_signals: scoredSignals }, null, 2));
    return;
  }

  if (ALERT_ONLY) {
    if (alerts.length > 0) {
      console.log(`\n=== TRADING ALERTS (${ts}) ===`);
      for (const alert of alerts) console.log(`  ${alert}`);
    }
    return;
  }

  console.log(`\n╔══════════════════════════════════════════════════════════════════╗`);
  console.log(`║  SOLANA TRADING MONITOR — ${now.toLocaleString('en-US', { timeZone: 'UTC' })} UTC`);
  console.log(`╠══════════════════════════════════════════════════════════════════╣`);
  console.log(`║  WALLET: ${WALLET.slice(0, 8)}...`);
  console.log(`║  SOL: ${solBal.toFixed(4)}  USDC: ${usdcBal.toFixed(2)}  Total: ${formatPrice(totalUsd)}`);
  console.log(`╠══════════════════════════════════════════════════════════════════╣`);
  console.log(`║  TOKEN   PRICE         24h      ALERT<      BUY<        SELL>   HOLDING`);
  console.log(`║  ─────────────────────────────────────────────────────────────────────`);

  for (const [mint, config] of Object.entries(WATCHLIST)) {
    if (config.symbol === 'USDC') continue;
    const p = prices[mint];
    if (!p) continue;
    const holding = holdingsByMint.get(mint) || holdingsBySymbol.get(config.symbol);
    const holdingStr = holding?.amount ? `${holding.amount}` : '—';
    const change = p.priceChange24h || 0;
    const sign = change >= 0 ? '+' : '';
    console.log(
      `║  ${config.symbol.padEnd(6)} ${formatPrice(p.usdPrice).padEnd(13)} ${(sign + change.toFixed(2) + '%').padStart(8)}  ${formatPrice(config.alertBelow).padEnd(10)} ${formatPrice(config.buyBelow).padEnd(10)} ${formatPrice(config.sellAbove).padEnd(8)} ${holdingStr}`
    );
  }

  console.log(`╠══════════════════════════════════════════════════════════════════╣`);
  if (alerts.length > 0) {
    console.log(`║  ⚡ ALERTS:`);
    for (const alert of alerts) console.log(`║    ${alert}`);
  } else {
    console.log(`║  No alerts — all quiet.`);
  }
  console.log(`╚══════════════════════════════════════════════════════════════════╝`);
}

main().catch(e => { console.error('Monitor error:', e.message); process.exit(1); });
