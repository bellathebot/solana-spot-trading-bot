#!/usr/bin/env node
/**
 * Auto-Trade Monitor
 * Checks prices against buy/sell targets and executes trades automatically.
 * Designed to run on a cron every few minutes.
 *
 * Rules enforced:
 *   - Max 1-2% of portfolio per trade (~$1-2)
 *   - Only trades 50% of SOL (rest is reserve)
 *   - Uses USDC dry powder for buys
 *   - Paper mode is the default unless --live or AUTO_TRADE_MODE=live is set
 *   - Logs all trades to trading data and trading.md
 *   - Easter egg preserved for morale: ethan was here
 */

import { execSync, spawnSync } from 'child_process';
import fs from 'fs';

const JUP_BIN = '/home/brimigs/.hermes/node/bin/jup';
const HELIUS_BIN = '/home/brimigs/.hermes/node/bin/helius';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const WALLET = 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu';
const JUP_KEY_NAME = 'trading';
const DATA_DIR = process.env.AUTO_TRADER_DATA_DIR || '/home/brimigs/.trading-data';
const DB_PATH = process.env.AUTO_TRADER_DB_PATH || `${DATA_DIR}/trading.db`;
const DB_CLI = '/home/brimigs/trading_system/trading_db_cli.py';
const TRADES_FILE = `${DATA_DIR}/auto_trades.json`;
const TRADING_MD = process.env.AUTO_TRADER_TRADING_MD || '/home/brimigs/trading.md';
const LOCK_FILE = `${DATA_DIR}/auto_trade.lock`;
const KILL_SWITCH_FILE = `${DATA_DIR}/auto_trader.disabled`;
const SMALL_ORDER_QUEUE_FILE = `${DATA_DIR}/small_order_queue.json`;
const SPOT_TRADE_JOURNAL_FILE = `${DATA_DIR}/spot_trade_journal.json`;
const SPOT_TRADE_JOURNAL_ARCHIVE_FILE = `${DATA_DIR}/spot_trade_journal.archive.jsonl`;
const SPOT_MANUAL_REVIEW_QUEUE_FILE = `${DATA_DIR}/spot_recovery_manual_review.jsonl`;
const TELEGRAM_BRIDGE_DIR = `${DATA_DIR}/telegram-bridge`;
const SPOT_LIVE_APPROVAL_FILE = `${TELEGRAM_BRIDGE_DIR}/spot_live_approval.json`;
const TX_STATUS_DIR = process.env.AUTO_TRADER_TX_STATUS_DIR || '';
const PATH_ENV = `/home/brimigs/.hermes/node/bin:/home/brimigs/.cargo/bin:${process.env.PATH}`;
const EASTER_EGG_TAG = 'ethan was here';
const MIN_SOL_FEE_RESERVE = 0.05;
const MIN_USDC_RESERVE = 5.0;
const MIN_TRADE_USD = 1.0;
const MAX_PORTFOLIO_RISK_PCT = 0.02;
const SYMBOL_MAX_PORTFOLIO_RISK_PCT = {
  PYTH: 0.12,
};
const MAX_TRADES_PER_CYCLE = 1;
const MAX_PRICE_IMPACT_PCT = 1.0;
const MIN_LIQUIDITY_USD = 100000;
const SYMBOL_MIN_LIQUIDITY_USD = {
  PYTH: 50000,
};
const STALE_LOCK_MS = 15 * 60 * 1000;
const MAX_DAILY_TRADES = parseInt(process.env.AUTO_TRADER_MAX_DAILY_TRADES || '20', 10);
const MAX_DAILY_NOTIONAL_USD = parseFloat(process.env.AUTO_TRADER_MAX_DAILY_NOTIONAL_USD || '25');
const MAX_ERROR_EVENTS_PER_RUN = parseInt(process.env.AUTO_TRADER_MAX_ERRORS_PER_RUN || '3', 10);
const STRATEGY_MIN_TRADES = parseInt(process.env.AUTO_TRADER_STRATEGY_MIN_TRADES || '2', 10);
const STRATEGY_MIN_REALIZED_PNL_USD = parseFloat(process.env.AUTO_TRADER_STRATEGY_MIN_REALIZED_PNL_USD || '0');
const MEAN_REVERSION_COOLDOWN_MS = parseInt(process.env.AUTO_TRADER_MEAN_REVERSION_COOLDOWN_HOURS || '4', 10) * 60 * 60 * 1000;
const DEFAULT_SYMBOL_RISK_CAP_USD = parseFloat(process.env.AUTO_TRADER_DEFAULT_SYMBOL_RISK_CAP_USD || '5');
const PAPER_STOP_LOSS_PCT = parseFloat(process.env.AUTO_TRADER_PAPER_STOP_LOSS_PCT || '8');
const PAPER_TIME_STOP_HOURS = parseInt(process.env.AUTO_TRADER_PAPER_TIME_STOP_HOURS || '6', 10);
const PAPER_NEAR_BUY_ENTRY_PCT = parseFloat(process.env.AUTO_TRADER_PAPER_NEAR_BUY_ENTRY_PCT || '3');
const PAPER_PARTIAL_PROFIT_PCT = parseFloat(process.env.AUTO_TRADER_PAPER_PARTIAL_PROFIT_PCT || '6');
const PAPER_PARTIAL_PROFIT_FRACTION = parseFloat(process.env.AUTO_TRADER_PAPER_PARTIAL_PROFIT_FRACTION || '0.5');
const PAPER_RECENT_CANDIDATE_LOOKBACK_MINUTES = parseInt(process.env.AUTO_TRADER_PAPER_RECENT_CANDIDATE_LOOKBACK_MINUTES || '180', 10);
const PAPER_RECENT_SAME_DIRECTION_CANDIDATE_CAP = parseInt(process.env.AUTO_TRADER_PAPER_RECENT_SAME_DIRECTION_CANDIDATE_CAP || '4', 10);
const MIN_ENTRY_SCORE = parseFloat(process.env.AUTO_TRADER_MIN_ENTRY_SCORE || '65');
const ALLOW_INSUFFICIENT_SAMPLE_PAPER = process.env.AUTO_TRADER_ALLOW_INSUFFICIENT_SAMPLE_PAPER === '1';
const PAPER_RANKED_MODE = process.env.AUTO_TRADER_PAPER_RANKED_MODE !== '0';
const PAPER_ALLOWLIST = new Set((process.env.AUTO_TRADER_PAPER_ALLOWLIST || 'WIF,JUP,RAY').split(',').map(s => s.trim()).filter(Boolean));
const DEFAULT_SYMBOL_SCORE_THRESHOLDS = { WIF: 65, JUP: 60, RAY: 60 };

function parseSymbolScoreThresholds() {
  const raw = process.env.AUTO_TRADER_SYMBOL_SCORE_THRESHOLDS;
  if (!raw) return DEFAULT_SYMBOL_SCORE_THRESHOLDS;
  try {
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_SYMBOL_SCORE_THRESHOLDS, ...(parsed || {}) };
  } catch {
    return DEFAULT_SYMBOL_SCORE_THRESHOLDS;
  }
}

const SYMBOL_SCORE_THRESHOLDS = parseSymbolScoreThresholds();
const TINY_LIVE_PILOT_ENABLED = process.env.AUTO_TRADER_TINY_LIVE_PILOT_ENABLED === '1';
const TINY_LIVE_PILOT_ALLOW_LIVE = process.env.AUTO_TRADER_TINY_LIVE_PILOT_ALLOW_LIVE === '1';
const TINY_LIVE_PILOT_ALLOW_NEAR_BUY_PROBES = process.env.AUTO_TRADER_TINY_LIVE_PILOT_ALLOW_NEAR_BUY_PROBES === '1';
const TINY_LIVE_PILOT_STRATEGY_TAG = process.env.AUTO_TRADER_TINY_LIVE_PILOT_STRATEGY_TAG || 'tiny_live_pilot_spot_long_mean_reversion';
const TINY_LIVE_PILOT_SYMBOLS = new Set((process.env.AUTO_TRADER_TINY_LIVE_PILOT_SYMBOLS || '').split(',').map(s => s.trim()).filter(Boolean));
const TINY_LIVE_PILOT_ALLOWED_REGIMES = new Set((process.env.AUTO_TRADER_TINY_LIVE_PILOT_ALLOWED_REGIMES || 'stable').split(',').map(s => s.trim()).filter(Boolean));
const TINY_LIVE_PILOT_RELAXED_CONTEXT_SYMBOLS = new Set((process.env.AUTO_TRADER_TINY_LIVE_PILOT_RELAXED_CONTEXT_SYMBOLS || 'JUP,RAY,WIF,PYTH').split(',').map(s => s.trim()).filter(Boolean));
const TINY_LIVE_PILOT_DEMOTION_OVERRIDE_SYMBOLS = new Set((process.env.AUTO_TRADER_TINY_LIVE_PILOT_DEMOTION_OVERRIDE_SYMBOLS || 'JUP,RAY,WIF,PYTH').split(',').map(s => s.trim()).filter(Boolean));
const TINY_LIVE_PILOT_REQUIRE_SUPPORTIVE_MARKET = process.env.AUTO_TRADER_TINY_LIVE_PILOT_REQUIRE_SUPPORTIVE_MARKET !== '0';
const TINY_LIVE_PILOT_MIN_MARKET_24H_PCT = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MIN_MARKET_24H_PCT || '0');
const TINY_LIVE_PILOT_MAX_ASSET_24H_CHASE_PCT = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MAX_ASSET_24H_CHASE_PCT || '2');
const TINY_LIVE_PILOT_MIN_ENTRY_SCORE_PAPER = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MIN_ENTRY_SCORE_PAPER || '75');
const TINY_LIVE_PILOT_MIN_ENTRY_SCORE_LIVE = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MIN_ENTRY_SCORE_LIVE || '85');
const TINY_LIVE_PILOT_CHOPPY_SCORE_BONUS = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_CHOPPY_SCORE_BONUS || '7');
const SYMBOL_TINY_LIVE_CHOPPY_SCORE_BONUS = {
  PYTH: 0,
};
const SYMBOL_TINY_LIVE_MIN_ENTRY_SCORE_LIVE = {
  PYTH: 69,
};
const TINY_LIVE_PILOT_MAX_BUY_USD = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MAX_BUY_USD || '1');
const TINY_LIVE_PILOT_MAX_SYMBOL_RISK_USD = parseFloat(process.env.AUTO_TRADER_TINY_LIVE_PILOT_MAX_SYMBOL_RISK_USD || '2');
const MIN_LIVE_ORDER_USD = parseFloat(process.env.AUTO_TRADER_MIN_LIVE_ORDER_USD || '5');
const SYMBOL_MIN_LIVE_TARGET_USD = {
  PYTH: 5.25,
};
const LIVE_DRAWDOWN_KILL_USD = parseFloat(process.env.AUTO_TRADER_LIVE_DRAWDOWN_KILL_USD || '2');
const PAPER_DRAWDOWN_WARN_USD = parseFloat(process.env.AUTO_TRADER_PAPER_DRAWDOWN_WARN_USD || '3');
const MAX_CONSECUTIVE_LOSSES = parseInt(process.env.AUTO_TRADER_MAX_CONSECUTIVE_LOSSES || '3', 10);
const SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE = parseInt(process.env.AUTO_TRADER_SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE || '1', 10);
const SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE = parseInt(process.env.AUTO_TRADER_SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE || '2', 10);

const STRATEGY_RULES = {
  mean_reversion_near_buy: {
    family: 'pullback_continuation',
    entry: 'hard_buy_only_below_threshold_and_supportive_regime',
    exit: 'target_or_stop_or_time_stop',
  },
  paper_near_buy_probe: {
    family: 'breakout_retest',
    entry: 'near_buy_probe_with_ranked_paper_gate',
    exit: 'partial_profit_or_stop_or_time_stop',
  },
  paper_partial_profit_exit: {
    family: 'exit',
    entry: 'n/a',
    exit: 'partial_profit',
  },
  paper_stop_loss_exit: {
    family: 'exit',
    entry: 'n/a',
    exit: 'stop_loss',
  },
  paper_time_stop_exit: {
    family: 'exit',
    entry: 'n/a',
    exit: 'time_stop',
  },
};

const POSITION_SIZING_FRAMEWORK = {
  baseRiskPctOfPortfolio: MAX_PORTFOLIO_RISK_PCT,
  minLiveOrderUsd: MIN_LIVE_ORDER_USD,
};

const flags = process.argv.slice(2);
const MODE = (flags.includes('--live') || process.env.AUTO_TRADE_MODE === 'live') ? 'live' : 'paper';

const PAPER_TIER_POLICY = {
  1: { sizeMultiplier: 1.0, allowNearBuyProbe: true, maxEntriesPerDay: 3, maxNotionalPerDay: 5.0, recentSameDirectionCandidateCap: 5 },
  2: { sizeMultiplier: 0.85, allowNearBuyProbe: true, maxEntriesPerDay: 2, maxNotionalPerDay: 4.0, recentSameDirectionCandidateCap: 4 },
  3: { sizeMultiplier: 0.5, allowNearBuyProbe: false, maxEntriesPerDay: 1, maxNotionalPerDay: 2.0, recentSameDirectionCandidateCap: 2 },
};

const TIERED_TARGET_GROUPS = [
  {
    tier: 1,
    symbols: [
      { symbol: 'SOL', mint: SOL_MINT, buyBelow: 77.50, sellAbove: 92.00, maxBuyUsd: 3.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
      { symbol: 'BTC', mint: '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh', buyBelow: 64800.00, sellAbove: 74800.00, maxBuyUsd: 3.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
      { symbol: 'JUP', mint: 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', buyBelow: 0.1565, sellAbove: 0.205, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: true },
      { symbol: 'PYTH', mint: 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', buyBelow: 0.0412, sellAbove: 0.055, maxBuyUsd: 10.00, maxSymbolRiskUsd: 10.00, paperEligible: true, liveEligible: true },
      { symbol: 'RAY', mint: '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', buyBelow: 0.602, sellAbove: 0.75, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: true },
    ],
  },
  {
    tier: 2,
    symbols: [
      { symbol: 'JTO', mint: 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL', buyBelow: 0.27, sellAbove: 0.38, maxBuyUsd: 2.00, maxSymbolRiskUsd: 5.00, paperEligible: true, liveEligible: false },
    ],
  },
  {
    tier: 3,
    symbols: [
      { symbol: 'BONK', mint: 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', buyBelow: 0.00000610, sellAbove: 0.00000920, maxBuyUsd: 1.50, maxSymbolRiskUsd: 3.00, paperEligible: false, liveEligible: false },
      { symbol: 'WIF', mint: 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', buyBelow: 0.1725, sellAbove: 0.235, maxBuyUsd: 1.50, maxSymbolRiskUsd: 3.00, paperEligible: true, liveEligible: true },
    ],
  },
];

const TARGETS = Object.fromEntries(
  TIERED_TARGET_GROUPS.flatMap(({ tier, symbols }) => symbols.map(target => {
    const paperPolicy = PAPER_TIER_POLICY[tier] || PAPER_TIER_POLICY[3];
    return [target.symbol, {
      ...target,
      tier,
      maxPaperEntriesPerDay: paperPolicy.maxEntriesPerDay,
      maxPaperNotionalPerDay: Math.min(target.maxSymbolRiskUsd, paperPolicy.maxNotionalPerDay),
      recentSameDirectionCandidateCap: paperPolicy.recentSameDirectionCandidateCap,
    }];
  }))
);

// Cooldown: don't buy the same token within 60 minutes
const COOLDOWN_MS = 60 * 60 * 1000;

// ── Helpers ──────────────────────────────────────────────────────

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(TELEGRAM_BRIDGE_DIR)) fs.mkdirSync(TELEGRAM_BRIDGE_DIR, { recursive: true });
}

function loadJsonFile(path, fallback) {
  try { return JSON.parse(fs.readFileSync(path, 'utf-8')); }
  catch { return fallback; }
}

function saveJsonFile(path, payload) {
  fs.writeFileSync(path, JSON.stringify(payload, null, 2));
}

function loadTrades() {
  return loadJsonFile(TRADES_FILE, []);
}

function saveTrades(trades) {
  fs.writeFileSync(TRADES_FILE, JSON.stringify(trades, null, 2));
}

function loadSmallOrderQueue() {
  return loadJsonFile(SMALL_ORDER_QUEUE_FILE, {});
}

function saveSmallOrderQueue(queue) {
  saveJsonFile(SMALL_ORDER_QUEUE_FILE, queue);
}

function spotApprovalExpired(approval) {
  if (!approval?.expires_at) return false;
  const expiresAt = new Date(approval.expires_at).getTime();
  return Number.isFinite(expiresAt) && expiresAt <= Date.now();
}

function loadSpotLiveApproval() {
  const approval = loadJsonFile(SPOT_LIVE_APPROVAL_FILE, { version: 1, status: 'idle' });
  if (approval?.status === 'pending' && spotApprovalExpired(approval)) {
    const expired = {
      ...approval,
      status: 'expired',
      expired_at: currentIso(),
    };
    saveJsonFile(SPOT_LIVE_APPROVAL_FILE, expired);
    return expired;
  }
  return approval;
}

function saveSpotLiveApproval(payload) {
  saveJsonFile(SPOT_LIVE_APPROVAL_FILE, payload);
}

function loadSpotTradeJournal() {
  return loadJsonFile(SPOT_TRADE_JOURNAL_FILE, { version: 1, entries: {} });
}

function saveSpotTradeJournal(payload) {
  saveJsonFile(SPOT_TRADE_JOURNAL_FILE, payload);
}

function archiveSpotTradeJournalEntry(entry) {
  fs.appendFileSync(SPOT_TRADE_JOURNAL_ARCHIVE_FILE, `${JSON.stringify(entry)}\n`);
}

function loadSpotManualReviewItems() {
  if (!fs.existsSync(SPOT_MANUAL_REVIEW_QUEUE_FILE)) return [];
  return fs.readFileSync(SPOT_MANUAL_REVIEW_QUEUE_FILE, 'utf-8')
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      try { return JSON.parse(line); } catch { return null; }
    })
    .filter(Boolean);
}

function queueFingerprint(item) {
  return JSON.stringify({
    review_type: item.review_type,
    entry_key: item.entry_key || null,
    decision_id: item.decision_id || null,
    symbol: item.symbol || null,
    signature: item.signature || null,
  });
}

function appendSpotManualReview(item) {
  const existing = loadSpotManualReviewItems();
  const fingerprint = queueFingerprint(item);
  const duplicate = existing.find(entry =>
    (entry.status || 'open') === 'open'
    && queueFingerprint(entry) === fingerprint
  );
  if (duplicate) return false;
  fs.appendFileSync(SPOT_MANUAL_REVIEW_QUEUE_FILE, `${JSON.stringify({ ...item, queued_at: currentIso(), status: item.status || 'open' })}\n`);
  return true;
}

function autoCloseSpotManualReviews(entry) {
  const items = loadSpotManualReviewItems();
  if (!items.length) return 0;
  const fingerprint = queueFingerprint({
    review_type: 'external_partial_fill',
    entry_key: entry.entry_key,
    decision_id: entry.decision_id,
    symbol: entry.symbol,
    signature: entry.signature,
  });
  let changed = 0;
  for (const item of items) {
    const matches = (
      item.entry_key && entry.entry_key && item.entry_key === entry.entry_key
    ) || (
      item.decision_id && entry.decision_id && item.decision_id === entry.decision_id
    ) || (
      item.signature && entry.signature && item.signature === entry.signature
    ) || queueFingerprint(item) === fingerprint;
    if (!matches) continue;
    if (!['open', 'snoozed'].includes(item.status || 'open')) continue;
    item.status = 'auto_resolved';
    item.auto_resolved_at = currentIso();
    item.auto_resolution_reason = `journal_${entry.status || 'verified'}`;
    const history = Array.isArray(item.history) ? item.history : [];
    history.push({ ts: currentIso(), action: 'auto_resolve', actor: 'system', note: `journal_${entry.status || 'verified'}` });
    item.history = history;
    changed += 1;
  }
  if (changed) {
    const lines = items.map(item => JSON.stringify(item)).join('\n');
    fs.writeFileSync(SPOT_MANUAL_REVIEW_QUEUE_FILE, lines + (lines ? '\n' : ''));
  }
  return changed;
}

function pruneSpotTradeJournal({ maxAgeDays = 7, keepRecentTerminal = 200 } = {}) {
  const journal = loadSpotTradeJournal();
  const entries = journal.entries || {};
  const terminal = ['verified', 'partially_verified', 'externally_observed', 'cancelled'];
  const sortedTerminal = Object.entries(entries)
    .filter(([, entry]) => entry && terminal.includes(entry.status))
    .sort((a, b) => new Date(b[1].updated_at || b[1].verified_at || 0).getTime() - new Date(a[1].updated_at || a[1].verified_at || 0).getTime());
  const cutoffMs = Date.now() - (maxAgeDays * 24 * 60 * 60 * 1000);
  let pruned = 0;
  for (const [index, [entryKey, entry]] of sortedTerminal.entries()) {
    const ts = new Date(entry.updated_at || entry.verified_at || entry.wallet_observed_at || 0).getTime();
    if (index < keepRecentTerminal) continue;
    if (!ts || ts > cutoffMs) continue;
    archiveSpotTradeJournalEntry({ ...entry, archived_at: currentIso() });
    delete entries[entryKey];
    pruned += 1;
  }
  journal.entries = entries;
  saveSpotTradeJournal(journal);
  if (pruned) {
    recordSystemEvent('spot_trade_journal_pruned', 'info', `Pruned ${pruned} terminal spot journal entries`, {
      pruned,
      archive_file: SPOT_TRADE_JOURNAL_ARCHIVE_FILE,
      journal_file: SPOT_TRADE_JOURNAL_FILE,
      max_age_days: maxAgeDays,
      keep_recent_terminal: keepRecentTerminal,
    });
  }
  return { pruned, remaining: Object.keys(entries).length, archive_file: SPOT_TRADE_JOURNAL_ARCHIVE_FILE };
}

function upsertSpotTradeJournalEntry(entryKey, patch) {
  const journal = loadSpotTradeJournal();
  const current = journal.entries?.[entryKey] || null;
  journal.entries = journal.entries || {};
  journal.entries[entryKey] = {
    ...(current || {}),
    entry_key: entryKey,
    ...(patch || {}),
    updated_at: currentIso(),
  };
  saveSpotTradeJournal(journal);
  return journal.entries[entryKey];
}

function reconcileSpotTradeJournal() {
  const journal = loadSpotTradeJournal();
  const fileTrades = loadTrades();
  const dbTrades = getRecordedTradesFromDb(300);
  const trades = [...fileTrades, ...dbTrades];
  const portfolio = getPortfolio();
  const entries = journal.entries || {};
  let verified = 0;
  let stillPending = 0;
  let staleSubmitted = 0;
  let externalConfirmed = 0;
  let walletObserved = 0;
  for (const [entryKey, entry] of Object.entries(entries)) {
    if (!entry || entry.status === 'verified' || entry.status === 'cancelled') continue;
    const matchedTrade = trades.find(t =>
      (entry.signature && t.signature === entry.signature)
      || (entry.decision_id && t.decisionId === entry.decision_id)
      || (entry.decision_id && t.decision_id === entry.decision_id)
    );
    if (matchedTrade) {
      entries[entryKey] = {
        ...entry,
        status: 'verified',
        verification_source: 'local_records',
        verified_at: currentIso(),
        trade_signature: matchedTrade.signature || entry.signature || null,
        mode: matchedTrade.mode || entry.mode,
        symbol: matchedTrade.symbol || entry.symbol,
        side: matchedTrade.side || entry.side,
        updated_at: currentIso(),
      };
      autoCloseSpotManualReviews(entries[entryKey]);
      verified += 1;
      continue;
    }

    if (entry.signature) {
      const txStatus = getExternalTxStatus(entry.signature);
      const txClassification = classifyExternalTxConfirmation(txStatus, entry);
      if (txClassification.confirmed) {
        entries[entryKey] = {
          ...entry,
          status: txClassification.verificationStatus,
          verification_source: 'external_tx_lookup',
          external_confirmation: txStatus,
          external_observed_out_amount: txClassification.observedOutAmount,
          expected_out_amount: txClassification.expectedOutAmount,
          exact_match_source: txClassification.exactMatchSource,
          partial_fill_detected: txClassification.partialFill,
          verified_at: currentIso(),
          trade_signature: entry.signature,
          updated_at: currentIso(),
        };
        if (txClassification.partialFill) {
          const reviewItem = {
            review_type: 'external_partial_fill',
            owner: 'operator',
            assignee: null,
            entry_key: entryKey,
            decision_id: entry.decision_id || null,
            symbol: entry.symbol || null,
            signature: entry.signature || null,
            expected_out_amount: txClassification.expectedOutAmount,
            observed_out_amount: txClassification.observedOutAmount,
            journal_file: SPOT_TRADE_JOURNAL_FILE,
            journal_row_id: entry.entry_key || entryKey,
            related_event_type: 'spot_trade_external_partial_fill_detected',
          };
          const eventResult = recordSystemEvent('spot_trade_external_partial_fill_detected', 'warning', `External reconciliation detected possible partial fill for ${entry.symbol || entry.decision_id || entryKey}`, reviewItem);
          reviewItem.journal_event_id = eventResult?.id ?? findRecentSystemEventId('spot_trade_external_partial_fill_detected', event => {
            const meta = event.metadata_json ? JSON.parse(event.metadata_json) : {};
            return (meta.decision_id || null) === (entry.decision_id || null) && (meta.signature || null) === (entry.signature || null);
          });
          appendSpotManualReview(reviewItem);
        } else {
          autoCloseSpotManualReviews(entries[entryKey]);
        }
        externalConfirmed += 1;
        continue;
      }
    }

    const portfolioToken = findPortfolioToken(portfolio, entry.symbol);
    const tokenAmount = Number(portfolioToken?.amount || 0);
    if (entry.side === 'buy' && tokenAmount > 0 && !entry.signature) {
      entries[entryKey] = {
        ...entry,
        status: 'externally_observed',
        verification_source: 'wallet_state',
        wallet_observed_at: currentIso(),
        wallet_token_amount: tokenAmount,
        updated_at: currentIso(),
      };
      autoCloseSpotManualReviews(entries[entryKey]);
      walletObserved += 1;
      continue;
    }

    if (entry.status === 'submitted') {
      staleSubmitted += 1;
      const submittedAt = entry.submitted_at ? new Date(entry.submitted_at).getTime() : 0;
      if (submittedAt && (Date.now() - submittedAt) > (15 * 60 * 1000)) {
        entries[entryKey] = {
          ...entry,
          status: 'stale_submitted',
          stale_detected_at: currentIso(),
          updated_at: currentIso(),
        };
        const reviewItem = {
          review_type: 'stale_submitted',
          owner: 'operator',
          assignee: null,
          entry_key: entryKey,
          decision_id: entry.decision_id || null,
          symbol: entry.symbol || null,
          signature: entry.signature || null,
          submitted_at: entry.submitted_at || null,
          journal_file: SPOT_TRADE_JOURNAL_FILE,
          journal_row_id: entry.entry_key || entryKey,
          related_event_type: 'spot_trade_journal_stale_submitted',
        };
        const eventResult = recordSystemEvent('spot_trade_journal_stale_submitted', 'warning', `Spot trade journal entry is stale after submit for ${entry.symbol || entry.decision_id || entryKey}`, reviewItem);
        reviewItem.journal_event_id = eventResult?.id ?? findRecentSystemEventId('spot_trade_journal_stale_submitted', event => {
          const meta = event.metadata_json ? JSON.parse(event.metadata_json) : {};
          return (meta.decision_id || null) === (entry.decision_id || null) && (meta.signature || null) === (entry.signature || null);
        });
        appendSpotManualReview(reviewItem);
      }
    } else {
      stillPending += 1;
    }
  }
  journal.entries = entries;
  saveSpotTradeJournal(journal);
  if (verified || staleSubmitted || externalConfirmed || walletObserved) {
    recordSystemEvent('spot_trade_journal_reconciled', 'info', 'Spot trade journal reconciliation completed', {
      verified,
      external_confirmed: externalConfirmed,
      wallet_observed: walletObserved,
      stale_submitted: staleSubmitted,
      still_pending: stillPending,
      journal_file: SPOT_TRADE_JOURNAL_FILE,
    });
  }
  return { verified, externalConfirmed, walletObserved, staleSubmitted, stillPending, journal };
}

function log(msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
  fs.appendFileSync(`${DATA_DIR}/auto_trade.log`, `[${ts}] ${msg}\n`);
}

function runDbCli(command, payload) {
  try {
    const result = spawnSync('python', [DB_CLI, command, '--db', DB_PATH], {
      input: JSON.stringify(payload),
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 30000,
    });
    if (result.status !== 0) {
      log(`Warning: DB CLI ${command} failed: ${(result.stderr || result.stdout || '').trim()}`);
      return false;
    }
    return true;
  } catch (e) {
    log(`Warning: DB CLI ${command} error: ${e.message}`);
    return false;
  }
}

function runDbCliJson(command, payload) {
  try {
    const result = spawnSync('python', [DB_CLI, command, '--db', DB_PATH], {
      input: JSON.stringify(payload),
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 30000,
    });
    if (result.status !== 0) {
      log(`Warning: DB CLI ${command} failed: ${(result.stderr || result.stdout || '').trim()}`);
      return null;
    }
    return JSON.parse(result.stdout || '{}');
  } catch (e) {
    log(`Warning: DB CLI ${command} error: ${e.message}`);
    return null;
  }
}

function recordSystemEvent(eventType, severity, message, metadata = {}) {
  return runDbCliJson('record-event', {
    ts: new Date().toISOString(),
    event_type: eventType,
    severity,
    message,
    source: 'auto-trade.mjs',
    metadata,
  });
}

function recordSignalCandidate(candidate) {
  return runDbCli('record-signal', candidate);
}

function getStrategyControls() {
  try {
    const result = spawnSync('python', [DB_CLI, 'strategy-controls', '--db', DB_PATH, '--min-trades', String(STRATEGY_MIN_TRADES), '--min-realized-pnl-usd', String(STRATEGY_MIN_REALIZED_PNL_USD)], {
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 120000,
      maxBuffer: 20 * 1024 * 1024,
    });
    if (result.status !== 0) {
      log(`Warning: strategy-controls failed: ${(result.stderr || result.stdout || '').trim()}`);
      return { paused_strategies: [], promotion_rules: [], symbol_promotion_rules: [], execution_scores: [], candidate_volume_by_symbol: [], candidate_volume_by_symbol_side: [] };
    }
    return JSON.parse(result.stdout || '{}');
  } catch (e) {
    log(`Warning: strategy-controls error: ${e.message}`);
    return { paused_strategies: [], promotion_rules: [], symbol_promotion_rules: [], execution_scores: [], candidate_volume_by_symbol: [], candidate_volume_by_symbol_side: [] };
  }
}

function getOpenPositions() {
  try {
    const result = spawnSync('python', [DB_CLI, 'open-positions', '--db', DB_PATH], {
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 30000,
    });
    if (result.status !== 0) {
      log(`Warning: open-positions failed: ${(result.stderr || result.stdout || '').trim()}`);
      return [];
    }
    return JSON.parse(result.stdout || '[]');
  } catch (e) {
    log(`Warning: open-positions error: ${e.message}`);
    return [];
  }
}

function getRecordedTradesFromDb(limit = 200) {
  try {
    const result = spawnSync('python', [DB_CLI, 'recent-trades', '--db', DB_PATH, '--limit', String(limit)], {
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 30000,
    });
    if (result.status !== 0) {
      log(`Warning: recent-trades failed: ${(result.stderr || result.stdout || '').trim()}`);
      return [];
    }
    return JSON.parse(result.stdout || '[]');
  } catch (e) {
    log(`Warning: recent-trades error: ${e.message}`);
    return [];
  }
}

function getRecentSystemEvents(eventType, limit = 50) {
  try {
    const args = ['python', DB_CLI, 'recent-system-events', '--db', DB_PATH, '--limit', String(limit)];
    if (eventType) args.push('--event-type', eventType);
    const result = spawnSync(args[0], args.slice(1), {
      encoding: 'utf-8',
      env: { ...process.env, PATH: PATH_ENV, PYTHONPATH: '/home/brimigs' },
      timeout: 30000,
    });
    if (result.status !== 0) {
      log(`Warning: recent-system-events failed: ${(result.stderr || result.stdout || '').trim()}`);
      return [];
    }
    return JSON.parse(result.stdout || '[]');
  } catch (e) {
    log(`Warning: recent-system-events error: ${e.message}`);
    return [];
  }
}

function findRecentSystemEventId(eventType, predicate = null) {
  const events = getRecentSystemEvents(eventType, 10);
  const match = predicate ? events.find(predicate) : events[0];
  return match?.id ?? null;
}

function acquireLock() {
  try {
    if (fs.existsSync(LOCK_FILE)) {
      const lock = JSON.parse(fs.readFileSync(LOCK_FILE, 'utf-8'));
      const age = Date.now() - new Date(lock.ts).getTime();
      if (age < STALE_LOCK_MS) {
        log(`Another auto-trade run is active (${lock.ts}). Skipping this cycle.`);
        return false;
      }
      log(`Removing stale lock from ${lock.ts}.`);
      fs.unlinkSync(LOCK_FILE);
    }
    fs.writeFileSync(LOCK_FILE, JSON.stringify({ ts: new Date().toISOString(), pid: process.pid }));
    return true;
  } catch (e) {
    log(`ERROR: Could not manage lock file: ${e.message}`);
    return false;
  }
}

function releaseLock() {
  try {
    if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
  } catch (e) {
    log(`Warning: could not remove lock file: ${e.message}`);
  }
}

function execJson(cmd) {
  try {
    const raw = execSync(cmd, { encoding: 'utf-8', timeout: 30000, env: { ...process.env, PATH: PATH_ENV }, stdio: ['pipe', 'pipe', 'pipe'] });
    return JSON.parse(raw.trim());
  } catch (e) {
    try { return JSON.parse(e.stdout?.toString().trim()); } catch {}
    return null;
  }
}

function getConfiguredJupKey(retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const keys = execJson(`${JUP_BIN} keys list`);
    if (Array.isArray(keys)) {
      const match = keys.find(key => key.name === JUP_KEY_NAME) || null;
      if (match) return match;
    }
  }
  return null;
}

function assertConfiguredWalletConsistency() {
  const configuredKey = getConfiguredJupKey();
  if (!configuredKey) {
    throw new Error(`Configured Jupiter key '${JUP_KEY_NAME}' was not found`);
  }
  if (configuredKey.address !== WALLET) {
    throw new Error(`Configured Jupiter key '${JUP_KEY_NAME}' resolves to ${configuredKey.address}, expected ${WALLET}`);
  }
  return configuredKey;
}

function resolveTokenIdentifier(token) {
  if (!token) return token;
  if (token === 'SOL') return SOL_MINT;
  if (token === 'USDC') return 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
  if (TARGETS[token]?.mint) return TARGETS[token].mint;
  return token;
}

function buildSpotSwapCommand(from, to, amount) {
  const fromId = resolveTokenIdentifier(from);
  const toId = resolveTokenIdentifier(to);
  return `${JUP_BIN} -f json spot swap --from ${fromId} --to ${toId} --amount ${amount} --key ${JUP_KEY_NAME}`;
}

function getQuote(from, to, amount) {
  const fromId = resolveTokenIdentifier(from);
  const toId = resolveTokenIdentifier(to);
  return execJson(`${JUP_BIN} -f json spot quote --from ${fromId} --to ${toId} --amount ${amount}`);
}

function getPortfolio(retries = 3) {
  const portfolioFile = process.env.AUTO_TRADER_PORTFOLIO_FILE;
  if (portfolioFile && fs.existsSync(portfolioFile)) {
    return loadJsonFile(portfolioFile, null);
  }
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const portfolio = execJson(`${JUP_BIN} -f json spot portfolio --key ${JUP_KEY_NAME}`);
    if (portfolio && typeof portfolio === 'object' && Array.isArray(portfolio.tokens)) {
      return portfolio;
    }
  }
  return null;
}

function getExternalTxStatus(signature) {
  if (!signature) return null;
  if (TX_STATUS_DIR) {
    const txPath = `${TX_STATUS_DIR}/${signature}.json`;
    if (fs.existsSync(txPath)) {
      return loadJsonFile(txPath, null);
    }
  }
  try {
    return execJson(`${HELIUS_BIN} tx ${signature} --json`);
  } catch {
    return null;
  }
}

function findPortfolioToken(portfolio, symbol) {
  if (!portfolio || !Array.isArray(portfolio.tokens)) return null;
  return portfolio.tokens.find(t => t.symbol === symbol || t.id === TARGETS[symbol]?.mint) || null;
}

function extractNumericFieldsDeep(value, keys, bucket = []) {
  if (value == null) return bucket;
  if (Array.isArray(value)) {
    for (const item of value) extractNumericFieldsDeep(item, keys, bucket);
    return bucket;
  }
  if (typeof value !== 'object') return bucket;
  for (const [k, v] of Object.entries(value)) {
    if (keys.has(k)) {
      const num = Number(v);
      if (Number.isFinite(num) && num > 0) bucket.push(num);
    }
    extractNumericFieldsDeep(v, keys, bucket);
  }
  return bucket;
}

function expectedOutputMintForEntry(entry = {}) {
  if (entry.side === 'sell') return USDC_MINT;
  if (entry.symbol === 'SOL') return SOL_MINT;
  return TARGETS[entry.symbol]?.mint || null;
}

function walletTokenDeltaFromTx(txStatus, wallet, targetMint) {
  if (!txStatus || !wallet || !targetMint) return null;
  let delta = 0;
  const transfers = Array.isArray(txStatus.tokenTransfers) ? txStatus.tokenTransfers : [];
  for (const transfer of transfers) {
    const mint = transfer.mint || transfer.mintToken || transfer.tokenMint || transfer.mint_address;
    if (mint !== targetMint) continue;
    const amount = Number(
      transfer.tokenAmount
      ?? transfer.amount
      ?? transfer.uiAmount
      ?? transfer.rawTokenAmount?.tokenAmount
      ?? transfer.tokenAmountUi
      ?? 0
    );
    if (!Number.isFinite(amount) || amount <= 0) continue;
    const toWallet = transfer.toUserAccount || transfer.toOwner || transfer.toWallet || transfer.destinationOwner;
    const fromWallet = transfer.fromUserAccount || transfer.fromOwner || transfer.fromWallet || transfer.sourceOwner;
    if (toWallet === wallet) delta += amount;
    if (fromWallet === wallet) delta -= amount;
  }
  if (targetMint === SOL_MINT) {
    const nativeTransfers = Array.isArray(txStatus.nativeTransfers) ? txStatus.nativeTransfers : [];
    for (const transfer of nativeTransfers) {
      const amount = Number(transfer.amount ?? transfer.lamports ?? 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      const uiAmount = amount > 1e6 ? amount / 1e9 : amount;
      if (transfer.toUserAccount === wallet || transfer.toWallet === wallet || transfer.to === wallet) delta += uiAmount;
      if (transfer.fromUserAccount === wallet || transfer.fromWallet === wallet || transfer.from === wallet) delta -= uiAmount;
    }
  }
  return delta !== 0 ? Math.abs(delta) : null;
}

function classifyExternalTxConfirmation(txStatus, entry = {}) {
  const confirmed = Boolean(
    txStatus && (
      txStatus.confirmed === true
      || txStatus.success === true
      || txStatus.status === 'confirmed'
      || txStatus.status === 'finalized'
      || txStatus.finalized === true
      || txStatus.signature === entry.signature
    )
  );
  const exactObservedOutAmount = walletTokenDeltaFromTx(txStatus, WALLET, expectedOutputMintForEntry(entry));
  const observedCandidates = extractNumericFieldsDeep(txStatus, new Set([
    'outAmount', 'out_amount', 'amountOut', 'amount_out', 'receivedAmount', 'received_amount',
    'uiAmount', 'tokenAmount', 'token_amount', 'confirmedOutAmount', 'confirmed_out_amount'
  ]));
  const observedOutAmount = exactObservedOutAmount ?? (observedCandidates.length ? Math.max(...observedCandidates) : null);
  const expectedOutAmount = Number(entry.out_amount ?? entry.expected_out_amount ?? 0) || null;
  const partialFill = Boolean(
    confirmed
    && observedOutAmount
    && expectedOutAmount
    && observedOutAmount < (expectedOutAmount * 0.98)
  );
  return {
    confirmed,
    observedOutAmount,
    expectedOutAmount,
    partialFill,
    exactMatchSource: exactObservedOutAmount != null ? 'wallet_token_delta' : 'numeric_fallback',
    verificationStatus: partialFill ? 'partially_verified' : (confirmed ? 'verified' : 'unconfirmed'),
  };
}

function normalizeNumericAmount(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function inferRegimeTag({ change24h = 0, marketChange24h = 0 }) {
  if (change24h <= -8 || (marketChange24h <= -5 && change24h <= -5)) return 'panic_selloff';
  if (change24h <= -3) return 'trend_down';
  if (change24h >= 8) return 'trend_up';
  if (Math.abs(change24h) >= 1.5) return 'choppy';
  return 'stable';
}

function thresholdDistanceComponent(signalKind, nearBuyPct) {
  if (signalKind === 'hard_buy') {
    const overshoot = Math.abs(Math.min(0, Number(nearBuyPct || 0)));
    return clamp(16 + (overshoot * 3), 0, 25);
  }
  return clamp(25 - (Math.max(0, Number(nearBuyPct || 0)) * 4), 0, 25);
}

function liquidityComponent(liquidity) {
  const liq = Number(liquidity || 0);
  if (liq < MIN_LIQUIDITY_USD) return 0;
  if (liq < 250000) return 5;
  if (liq < 1000000) return 9;
  if (liq < 5000000) return 13;
  return 15;
}

function priceImpactComponent(priceImpact) {
  const impact = Number(priceImpact);
  if (!Number.isFinite(impact) || impact >= MAX_PRICE_IMPACT_PCT) return 0;
  return clamp(((MAX_PRICE_IMPACT_PCT - Math.max(0, impact)) / MAX_PRICE_IMPACT_PCT) * 15, 0, 15);
}

function scaledHistoricalComponent(score, maxPoints) {
  const normalized = clamp(Number(score || 0), 0, 100) / 100;
  return normalized * maxPoints;
}

function pressureComponent(pressure, signalKind) {
  if (signalKind !== 'hard_buy' && signalKind !== 'near_buy_probe') return 4;
  if (pressure === 'aggressive') return 0;
  if (pressure === 'elevated') return 4;
  return 10;
}

function regimeComponent(regimeTag) {
  if (regimeTag === 'stable') return 10;
  if (regimeTag === 'choppy') return 6;
  if (regimeTag === 'trend_up') return 3;
  return 0;
}

function saturationComponent(recentSameDirectionCandidates, repeatedCandidateCap) {
  const recent = Number(recentSameDirectionCandidates || 0);
  const cap = Math.max(1, Number(repeatedCandidateCap || 1));
  const ratio = recent / cap;
  if (ratio <= 0.25) return 5;
  if (ratio <= 0.5) return 4;
  if (ratio <= 0.75) return 3;
  if (ratio < 1) return 1;
  return 0;
}

function buildEntryScore({ signalKind, nearBuyPct, liquidity, priceImpact, symbolExecutionScore, strategyExecutionScore, pressure, regimeTag, recentSameDirectionCandidates, repeatedCandidateCap }) {
  const components = {
    threshold_distance: thresholdDistanceComponent(signalKind, nearBuyPct),
    liquidity_quality: liquidityComponent(liquidity),
    quote_price_impact: priceImpactComponent(priceImpact),
    symbol_expectancy: scaledHistoricalComponent(symbolExecutionScore, 25),
    strategy_expectancy: scaledHistoricalComponent(strategyExecutionScore, 10),
    whale_pressure: pressureComponent(pressure, signalKind),
    regime: regimeComponent(regimeTag),
    saturation: saturationComponent(recentSameDirectionCandidates, repeatedCandidateCap),
  };
  return {
    score: clamp(Math.round(Object.values(components).reduce((sum, value) => sum + value, 0)), 0, 100),
    components,
  };
}

function isTinyLivePilotSymbol(symbol) {
  return TINY_LIVE_PILOT_ENABLED && TINY_LIVE_PILOT_SYMBOLS.has(symbol);
}

function classifyTinyLivePilotContext({ symbol, regimeTag, change24h, marketChange24h }) {
  const relaxedSymbol = TINY_LIVE_PILOT_RELAXED_CONTEXT_SYMBOLS.has(symbol);
  if (!TINY_LIVE_PILOT_ALLOWED_REGIMES.has(regimeTag)) {
    if (relaxedSymbol && regimeTag === 'choppy') {
      return { state: 'supportive', reason: 'relaxed_choppy_allowlist' };
    }
    return { state: 'blocked', reason: `regime_${regimeTag}_not_allowlisted` };
  }
  if (TINY_LIVE_PILOT_REQUIRE_SUPPORTIVE_MARKET && marketChange24h < TINY_LIVE_PILOT_MIN_MARKET_24H_PCT) {
    if (relaxedSymbol && marketChange24h >= -1.5) {
      return { state: 'supportive', reason: 'relaxed_market_support_override' };
    }
    return { state: 'mixed', reason: 'market_not_supportive' };
  }
  if (change24h > TINY_LIVE_PILOT_MAX_ASSET_24H_CHASE_PCT) {
    return { state: 'mixed', reason: 'asset_too_extended_for_mean_reversion' };
  }
  return { state: 'supportive', reason: 'supportive_regime' };
}

function getEntryScoreThreshold({ mode, symbol, isTinyPilot, regimeTag }) {
  if (isTinyPilot) {
    let threshold = mode === 'live'
      ? Number(SYMBOL_TINY_LIVE_MIN_ENTRY_SCORE_LIVE[symbol] ?? TINY_LIVE_PILOT_MIN_ENTRY_SCORE_LIVE)
      : TINY_LIVE_PILOT_MIN_ENTRY_SCORE_PAPER;
    if (mode === 'live' && regimeTag === 'choppy' && TINY_LIVE_PILOT_RELAXED_CONTEXT_SYMBOLS.has(symbol)) {
      threshold += Number(SYMBOL_TINY_LIVE_CHOPPY_SCORE_BONUS[symbol] ?? TINY_LIVE_PILOT_CHOPPY_SCORE_BONUS);
    }
    return threshold;
  }
  return Number(SYMBOL_SCORE_THRESHOLDS[symbol] ?? MIN_ENTRY_SCORE);
}

function isPaperAllowlisted(symbol, symbolRule = null, strategyRule = null) {
  if (!PAPER_RANKED_MODE) return true;
  if (!PAPER_ALLOWLIST.has(symbol)) return false;
  const symbolDecision = symbolRule?.decision || 'missing';
  const strategyDecision = strategyRule?.decision || 'missing';
  const symbolOkay = ['monitor', 'promote'].includes(symbolDecision);
  const strategyOkay = ['monitor', 'promote'].includes(strategyDecision);
  return symbolOkay && strategyOkay;
}

function minLiquidityThresholdUsd(symbol) {
  return Number(SYMBOL_MIN_LIQUIDITY_USD[symbol] ?? MIN_LIQUIDITY_USD);
}

function minLiveTargetUsd(symbol) {
  return Number(SYMBOL_MIN_LIVE_TARGET_USD[symbol] ?? 0);
}

function deriveExecutedBuyQuantity({ result, quote, price, buyAmount }) {
  const quotedOutAmount = normalizeNumericAmount(result?.outAmount ?? quote?.outAmount);
  if (quotedOutAmount > 0) {
    return { quantity: quotedOutAmount, source: 'quoted_out_amount' };
  }

  const inferredFromPrice = (Number(price) > 0 && Number(buyAmount) > 0)
    ? Number(buyAmount) / Number(price)
    : 0;
  if (Number.isFinite(inferredFromPrice) && inferredFromPrice > 0) {
    return { quantity: inferredFromPrice, source: 'size_usd_div_price_fallback' };
  }

  return { quantity: 0, source: 'missing' };
}

function canTradeWithCurrentSol(solBalance) {
  return solBalance > MIN_SOL_FEE_RESERVE;
}

function maybeExecuteSwap(command, meta) {
  if (MODE !== 'live') {
    const paperTrade = {
      ts: new Date().toISOString(),
      mode: 'paper',
      ...meta,
      simulated: true,
      command,
    };
    const signatureBase = meta?.decisionId || `${meta?.side || 'trade'}:${meta?.symbol || 'unknown'}:${meta?.ts || paperTrade.ts}`;
    log(`🧪 PAPER TRADE: ${meta.side.toUpperCase()} ${meta.symbol} | ${meta.reason}`);
    return {
      signature: `paper-${signatureBase}`,
      outAmount: meta.expectedOutAmount ?? null,
      paperTrade,
      submitStatus: 'submitted',
    };
  }
  try {
    const raw = execSync(command, { encoding: 'utf-8', timeout: 30000, env: { ...process.env, PATH: PATH_ENV }, stdio: ['pipe', 'pipe', 'pipe'] });
    const parsed = JSON.parse(raw.trim());
    return { ...parsed, submitStatus: 'submitted' };
  } catch (e) {
    try {
      const parsed = JSON.parse(e.stdout?.toString().trim());
      if (parsed && parsed.signature) return { ...parsed, submitStatus: 'submitted' };
      return { ...parsed, submitStatus: e.killed || /timed out/i.test(e.message || '') ? 'timeout_or_unknown' : 'failed', error: e.message || null };
    } catch {}
    return {
      submitStatus: e.killed || /timed out/i.test(e.message || '') ? 'timeout_or_unknown' : 'failed',
      error: e.message || null,
      stderr: e.stderr?.toString?.() || null,
      stdout: e.stdout?.toString?.() || null,
    };
  }
}

function dayStartIso(date = new Date()) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())).toISOString();
}

function getConsecutiveLossCount(trades) {
  let losses = 0;
  const relevant = trades
    .filter(t => Number.isFinite(Number(t.realizedPnlUsd ?? t.realized_pnl_usd)) && Number(t.realizedPnlUsd ?? t.realized_pnl_usd) !== 0)
    .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());
  for (const trade of relevant) {
    const pnl = Number(trade.realizedPnlUsd ?? trade.realized_pnl_usd ?? 0);
    if (pnl < 0) {
      losses += 1;
      continue;
    }
    if (pnl > 0) break;
  }
  return losses;
}

function todaysRealizedPnl(trades, mode = MODE) {
  const start = dayStartIso();
  return trades
    .filter(t => (t.ts || '') >= start && (t.mode || 'unknown') === mode)
    .reduce((sum, t) => sum + Number(t.realizedPnlUsd ?? t.realized_pnl_usd ?? 0), 0);
}

function planSpotSizing({ target, symbol, regimeTag, symbolRiskCap, currentSymbolExposure, usdcSpendable, portfolioTotalValue, tierPolicy, strategyRule, executionScore, tinyPilotSymbol, shouldPaperProbeBuy }) {
  const symbolRiskPct = Number(SYMBOL_MAX_PORTFOLIO_RISK_PCT[symbol] ?? POSITION_SIZING_FRAMEWORK.baseRiskPctOfPortfolio);
  const maxRisk = portfolioTotalValue * symbolRiskPct;
  let maxBuyUsd = target.maxBuyUsd;
  if (tinyPilotSymbol) {
    maxBuyUsd = TINY_LIVE_PILOT_MAX_BUY_USD;
  }
  let baseBuyAmount = Math.min(maxBuyUsd, usdcSpendable, maxRisk, Math.max(0, symbolRiskCap - currentSymbolExposure));
  if (strategyRule && strategyRule.decision === 'promote') {
    baseBuyAmount = tinyPilotSymbol
      ? Math.min(baseBuyAmount, maxBuyUsd, Math.max(0, symbolRiskCap - currentSymbolExposure))
      : Math.min(baseBuyAmount * 1.25, maxBuyUsd, Math.max(0, symbolRiskCap - currentSymbolExposure));
  } else if (strategyRule && strategyRule.decision === 'monitor') {
    baseBuyAmount = Math.min(baseBuyAmount, maxBuyUsd);
  }
  if (executionScore) {
    const scoreMultiplier = tinyPilotSymbol ? 1.0 : (executionScore.score >= 80 ? 1.3 : executionScore.score >= 60 ? 1.15 : executionScore.score >= 40 ? 1.0 : 0.75);
    baseBuyAmount = Math.min(baseBuyAmount * scoreMultiplier, maxBuyUsd, Math.max(0, symbolRiskCap - currentSymbolExposure));
  }
  if (MODE === 'paper') {
    baseBuyAmount = Math.min(baseBuyAmount * tierPolicy.sizeMultiplier, Math.max(0, target.maxPaperNotionalPerDay));
  }
  if (tinyPilotSymbol && regimeTag === 'choppy' && TINY_LIVE_PILOT_RELAXED_CONTEXT_SYMBOLS.has(symbol) && symbol !== 'PYTH') {
    baseBuyAmount = Math.min(baseBuyAmount * 0.5, maxBuyUsd, Math.max(0, symbolRiskCap - currentSymbolExposure));
  }
  const plannedBuyAmount = shouldPaperProbeBuy ? Math.max(MIN_TRADE_USD, Math.min(baseBuyAmount * 0.5, baseBuyAmount)) : baseBuyAmount;
  return {
    plannedBuyAmount,
    maxRisk,
    maxBuyUsd,
    symbolRiskRemainingUsd: Math.max(0, symbolRiskCap - currentSymbolExposure),
    tierSizeMultiplier: tierPolicy.sizeMultiplier,
    strategyDecision: strategyRule?.decision || 'missing',
    executionScore: executionScore?.score ?? null,
  };
}

function accumulateSmallOrder({ key, symbol, side, strategyTag, plannedUsd, metadata = {} }) {
  const queue = loadSmallOrderQueue();
  const entry = queue[key] || { symbol, side, strategyTag, accumulatedUsd: 0, attempts: 0, firstTs: new Date().toISOString(), lastTs: null, metadata: {} };
  entry.accumulatedUsd = Number(entry.accumulatedUsd || 0) + Number(plannedUsd || 0);
  entry.attempts = Number(entry.attempts || 0) + 1;
  entry.lastTs = new Date().toISOString();
  entry.metadata = { ...(entry.metadata || {}), ...metadata };
  queue[key] = entry;
  saveSmallOrderQueue(queue);
  return entry;
}

function consumeSmallOrderQueue(key) {
  const queue = loadSmallOrderQueue();
  const entry = queue[key] || null;
  if (entry) {
    delete queue[key];
    saveSmallOrderQueue(queue);
  }
  return entry;
}

function currentIso() {
  return new Date().toISOString();
}

function buildSpotApprovalIntent({ decisionId, symbol, strategyTag, signalType, buyAmount, price, regimeTag, entryScore, quotePriceImpact, reason }) {
  const approvalId = `spot-approval:${decisionId}`;
  return {
    version: 1,
    approval_id: approvalId,
    status: 'pending',
    requested_at: currentIso(),
    expires_at: new Date(Date.now() + (6 * 60 * 60 * 1000)).toISOString(),
    decision_id: decisionId,
    symbol,
    strategy_tag: strategyTag,
    signal_type: signalType,
    size_usd: buyAmount,
    entry_price: price,
    regime_tag: regimeTag,
    entry_score: entryScore?.score ?? null,
    quote_price_impact: quotePriceImpact,
    reason,
    commands: {
      approve: `APPROVE_SPOT ${approvalId}`,
      reject: `REJECT_SPOT ${approvalId}`,
    },
  };
}

function getExecutableSpotApprovalIntent(decisionId) {
  const approval = loadSpotLiveApproval();
  if (!approval || approval.status !== 'approved') return null;
  if (approval.decision_id !== decisionId) return null;
  if (approval.expires_at && new Date(approval.expires_at).getTime() <= Date.now()) return null;
  return approval;
}

function requestSpotLiveApproval(intent) {
  const current = loadSpotLiveApproval();
  if (current?.status === 'pending' && !spotApprovalExpired(current) && current.decision_id === intent.decision_id) {
    return current;
  }
  const payload = {
    ...intent,
    superseded_approval_id: current?.status === 'pending' && !spotApprovalExpired(current) ? current.approval_id : null,
  };
  saveSpotLiveApproval(payload);
  recordSystemEvent('spot_live_approval_requested', 'warning', `Spot live approval required for ${intent.symbol}`, {
    symbol: intent.symbol,
    decision_id: intent.decision_id,
    strategy_tag: intent.strategy_tag,
    signal_type: intent.signal_type,
    approval_intent: payload,
  });
  return payload;
}

function markSpotApprovalExecuted(intent, execution = {}) {
  const current = loadSpotLiveApproval();
  if (!current || current.approval_id !== intent?.approval_id) return;
  saveSpotLiveApproval({
    ...current,
    status: 'executed',
    executed_at: currentIso(),
    execution,
  });
}

function todaysEntryStats(trades, side = 'buy') {
  const start = dayStartIso();
  const relevant = trades.filter(t =>
    (t.ts || '') >= start
    && (t.mode || 'unknown') === MODE
    && (!side || t.side === side)
  );
  return {
    count: relevant.length,
    notionalUsd: relevant.reduce((sum, t) => sum + Number(t.sizeUsd || 0), 0),
  };
}

function getPaperPolicyForTier(tier) {
  return PAPER_TIER_POLICY[tier] || PAPER_TIER_POLICY[3];
}

function paperExecutionEligibility(target, signalKind) {
  if (!target.paperEligible) return false;
  if (signalKind === 'hard_buy') return true;
  const tierPolicy = getPaperPolicyForTier(target.tier);
  if (signalKind === 'near_buy_probe') return Boolean(tierPolicy.allowNearBuyProbe);
  return true;
}

function getSymbolTradeStatsForToday(trades, symbol, side = null) {
  const start = dayStartIso();
  const relevant = trades.filter(t =>
    (t.ts || '') >= start
    && (t.mode || 'unknown') === MODE
    && t.symbol === symbol
    && (!side || t.side === side)
  );
  return {
    count: relevant.length,
    notionalUsd: relevant.reduce((sum, t) => sum + Number(t.sizeUsd || 0), 0),
  };
}

function buildCandidateVolumeMaps(strategyControls) {
  const symbolSideRows = strategyControls.candidate_volume_by_symbol_side || [];
  const symbolRows = strategyControls.candidate_volume_by_symbol || [];
  return {
    bySymbolSide: new Map(symbolSideRows.map(row => [`${row.symbol}::${row.side || 'unknown'}`, row])),
    bySymbol: new Map(symbolRows.map(row => [row.symbol, row])),
  };
}

function getGateRule(ruleMap, name) {
  return ruleMap.get(`${name}::candidate`) || ruleMap.get(`${name}::executed`) || null;
}

function structuredSkip(eventType, message, metadata, signalCandidate = null) {
  recordSystemEvent(eventType, metadata?.severity || 'warning', message, metadata || {});
  if (signalCandidate) {
    recordSignalCandidate(signalCandidate);
  }
}

function isKillSwitchActive() {
  return fs.existsSync(KILL_SWITCH_FILE) || process.env.AUTO_TRADER_DISABLED === '1';
}

function activateKillSwitch(reason, metadata = {}) {
  const ts = new Date().toISOString();
  try {
    fs.writeFileSync(KILL_SWITCH_FILE, JSON.stringify({ ts, reason, metadata }, null, 2));
  } catch (e) {
    log(`ERROR: failed to write kill switch file: ${e.message}`);
  }
  recordSystemEvent('kill_switch', 'critical', reason, metadata);
}

function lastBuyTime(symbol) {
  const trades = loadTrades();
  for (let i = trades.length - 1; i >= 0; i--) {
    if (trades[i].symbol === symbol && trades[i].side === 'buy') {
      return new Date(trades[i].ts).getTime();
    }
  }
  return 0;
}

function lastTradeTimeForStrategy(trades, strategyTag) {
  for (let i = trades.length - 1; i >= 0; i--) {
    if ((trades[i].strategyTag || trades[i].strategy_tag) === strategyTag) {
      return new Date(trades[i].ts).getTime();
    }
  }
  return 0;
}

function appendToTradingMd(entry) {
  try {
    let content = fs.readFileSync(TRADING_MD, 'utf-8');
    const marker = '### Closed Trades Log';
    const idx = content.indexOf(marker);
    if (idx === -1) return;

    // Find the end of the table header (after the |---|---| line)
    const afterMarker = content.slice(idx);
    const lines = afterMarker.split('\n');
    let insertIdx = idx;
    for (let i = 0; i < lines.length; i++) {
      insertIdx += lines[i].length + 1;
      if (lines[i].startsWith('|---') || lines[i].startsWith('| #')) continue;
      if (lines[i].startsWith('| ')) {
        // Insert before first data row or at end of header
        break;
      }
    }

    const newRow = `| ${entry.id} | ${entry.date} | ${entry.pair} | ${entry.side} | $${entry.entryPrice} | — | $${entry.sizeUsd} | — | Auto: ${entry.reason} |\n`;
    content = content.slice(0, insertIdx) + newRow + content.slice(insertIdx);
    fs.writeFileSync(TRADING_MD, content);
  } catch (e) {
    log(`Warning: could not update trading.md: ${e.message}`);
  }
}

// ── Main ─────────────────────────────────────────────────────────

async function main() {
  ensureDataDir();

  if (flags.includes('--reconcile-spot-journal')) {
    console.log(JSON.stringify(reconcileSpotTradeJournal(), null, 2));
    return;
  }
  const pruneSpotJournalIndex = flags.indexOf('--prune-spot-journal');
  if (pruneSpotJournalIndex !== -1) {
    const maxAgeDays = Number(flags[pruneSpotJournalIndex + 1] || 7);
    console.log(JSON.stringify(pruneSpotTradeJournal({ maxAgeDays, keepRecentTerminal: 0 }), null, 2));
    return;
  }
  const spotApprovalCheckIndex = flags.indexOf('--spot-approval-check');
  if (spotApprovalCheckIndex !== -1) {
    const decisionId = flags[spotApprovalCheckIndex + 1];
    console.log(JSON.stringify(getExecutableSpotApprovalIntent(decisionId) || null, null, 2));
    return;
  }
  const spotApprovalExecuteIndex = flags.indexOf('--spot-approval-mark-executed');
  if (spotApprovalExecuteIndex !== -1) {
    const decisionId = flags[spotApprovalExecuteIndex + 1];
    const intent = getExecutableSpotApprovalIntent(decisionId);
    if (intent) {
      markSpotApprovalExecuted(intent, { decision_id: decisionId, source: 'cli_test_hook' });
    }
    console.log(JSON.stringify(loadSpotLiveApproval(), null, 2));
    return;
  }

  if (!acquireLock()) return;

  try {
    log(`Auto-trade scan starting... mode=${MODE}`);

    if (isKillSwitchActive()) {
      log('Kill switch active. Skipping this cycle.');
      recordSystemEvent('kill_switch_skip', 'warning', 'Skipped run because kill switch is active', { mode: MODE });
      return;
    }

    let errorEvents = 0;
    const configuredKey = assertConfiguredWalletConsistency();
    log(`Using Jupiter key '${configuredKey.name}' for wallet ${configuredKey.address}.`);
    const reconciliation = reconcileSpotTradeJournal();
    if (reconciliation.verified || reconciliation.staleSubmitted || reconciliation.externalConfirmed || reconciliation.walletObserved) {
      log(`Spot journal reconciliation: verified=${reconciliation.verified} external_confirmed=${reconciliation.externalConfirmed} wallet_observed=${reconciliation.walletObserved} stale_submitted=${reconciliation.staleSubmitted} pending=${reconciliation.stillPending}`);
    }
    if (MODE === 'live' && reconciliation.staleSubmitted >= SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE) {
      const reason = `Kill switch: spot recovery found ${reconciliation.staleSubmitted} stale submitted entry(s) (limit ${SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE})`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, reconciliation, spotRecoveryThresholds: { stale: SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE, ambiguous: SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE } });
      return;
    }
    const recentAmbiguousEvents = getRecentSystemEvents('spot_trade_submit_timeout_ambiguous', 20).filter(row => row.source === 'auto-trade.mjs');
    if (MODE === 'live' && recentAmbiguousEvents.length >= SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE) {
      const reason = `Kill switch: spot recovery observed ${recentAmbiguousEvents.length} ambiguous submit event(s) (limit ${SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE})`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, recentAmbiguousEvents: recentAmbiguousEvents.slice(0, 5), spotRecoveryThresholds: { stale: SPOT_RECOVERY_MAX_STALE_BEFORE_PAUSE, ambiguous: SPOT_RECOVERY_MAX_AMBIGUOUS_BEFORE_PAUSE } });
      return;
    }
    const existingTrades = loadTrades();
    const dailyStats = todaysEntryStats(existingTrades, 'buy');
    const dailyRealizedPnl = todaysRealizedPnl(existingTrades, MODE);
    const consecutiveLosses = getConsecutiveLossCount(existingTrades);
    const strategyControls = getStrategyControls();
    const riskGuardrails = strategyControls.risk_guardrails || {};
    const pausedStrategies = new Set((strategyControls.paused_strategies || []).map(s => s.strategy_tag));
    const promotionRules = new Map((strategyControls.promotion_rules || []).map(r => [`${r.name}::${r.status}`, r]));
    const symbolPromotionRules = new Map((strategyControls.symbol_promotion_rules || []).map(r => [`${r.name}::${r.status}`, r]));
    const executionScores = new Map((strategyControls.execution_scores || []).map(r => [`${r.symbol}::${r.status}`, r]));
    const candidateVolume = buildCandidateVolumeMaps(strategyControls);
    const openPositions = getOpenPositions();
    if (riskGuardrails.kill_switch_recommended && MODE === 'live') {
      const reason = `Risk guardrails recommend kill switch: ${(riskGuardrails.blockers || []).join(', ') || 'unspecified'}`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, riskGuardrails });
      return;
    }
    if (MODE === 'live' && dailyRealizedPnl <= -LIVE_DRAWDOWN_KILL_USD) {
      const reason = `Kill switch: live daily drawdown reached $${dailyRealizedPnl.toFixed(2)} (limit -$${LIVE_DRAWDOWN_KILL_USD.toFixed(2)})`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, dailyRealizedPnl, liveDrawdownKillUsd: LIVE_DRAWDOWN_KILL_USD });
      return;
    }
    if (MODE === 'paper' && dailyRealizedPnl <= -PAPER_DRAWDOWN_WARN_USD) {
      recordSystemEvent('paper_drawdown_warning', 'warning', `Paper drawdown warning: daily realized PnL is $${dailyRealizedPnl.toFixed(2)}`, { mode: MODE, dailyRealizedPnl, paperDrawdownWarnUsd: PAPER_DRAWDOWN_WARN_USD });
    }
    if (consecutiveLosses >= MAX_CONSECUTIVE_LOSSES && MODE === 'live') {
      const reason = `Kill switch: ${consecutiveLosses} consecutive losing closes reached the configured max of ${MAX_CONSECUTIVE_LOSSES}`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, consecutiveLosses, maxConsecutiveLosses: MAX_CONSECUTIVE_LOSSES });
      return;
    }
    const liveBuyTradeCapReached = MODE === 'live' && dailyStats.count >= MAX_DAILY_TRADES;
    const liveBuyNotionalCapReached = MODE === 'live' && dailyStats.notionalUsd >= MAX_DAILY_NOTIONAL_USD;
    if (liveBuyTradeCapReached) {
      const reason = `Daily trade cap reached for new buys (${dailyStats.count}/${MAX_DAILY_TRADES}); only exits may proceed.`;
      log(reason);
      recordSystemEvent('daily_buy_trade_cap_reached', 'warning', reason, { mode: MODE, dailyStats, maxDailyTrades: MAX_DAILY_TRADES });
    }
    if (liveBuyNotionalCapReached) {
      const reason = `Daily notional cap reached for new buys ($${dailyStats.notionalUsd.toFixed(2)}/$${MAX_DAILY_NOTIONAL_USD.toFixed(2)}); only exits may proceed.`;
      log(reason);
      recordSystemEvent('daily_buy_notional_cap_reached', 'warning', reason, { mode: MODE, dailyStats, maxDailyNotionalUsd: MAX_DAILY_NOTIONAL_USD });
    }

    // 1. Get portfolio to check available USDC and fee reserve
    const portfolio = getPortfolio();
    if (!portfolio) {
      log('ERROR: Could not fetch portfolio');
      return;
    }

    let usdcAvailable = 0;
    let solBalance = 0;
    for (const t of portfolio.tokens) {
      if (t.symbol === 'USDC') usdcAvailable = t.amount;
      if (t.symbol === 'SOL') solBalance = t.amount;
    }

    log(`Portfolio: $${portfolio.totalValue.toFixed(2)} | USDC: $${usdcAvailable.toFixed(2)} | SOL: ${solBalance.toFixed(4)}`);

    if (!canTradeWithCurrentSol(solBalance)) {
      log(`Not enough SOL for fee reserve (need > ${MIN_SOL_FEE_RESERVE} SOL). Skipping.`);
      return;
    }

    // Keep some USDC dry powder in reserve, but low USDC must not block de-risking sells.
    const usdcSpendable = Math.max(0, usdcAvailable - MIN_USDC_RESERVE);
    const canAttemptBuys = usdcSpendable >= MIN_TRADE_USD;
    if (!canAttemptBuys) {
      log(`Not enough spendable USDC after reserve ($${MIN_USDC_RESERVE.toFixed(2)} kept back). Buy entries disabled for this cycle; sell exits still allowed.`);
    }

    // 2. Get prices
    const mints = [SOL_MINT, ...Object.values(TARGETS).map(t => t.mint)].join(',');
    let prices;
    try {
      const res = await fetch(`https://lite-api.jup.ag/price/v3?ids=${mints}`);
      prices = await res.json();
    } catch (e) {
      log(`ERROR: Price fetch failed: ${e.message}`);
      return;
    }

    // 3. Check exits first across all symbols, then allow new entries with any remaining cycle budget.
    const now = Date.now();
    const trades = existingTrades;
    const targetEntries = Object.entries(TARGETS);
    let tradeCount = 0;
    let executedNotionalThisRun = 0;

    for (const [symbol, target] of targetEntries) {
      if (tradeCount >= MAX_TRADES_PER_CYCLE) {
        log(`Trade cap reached (${MAX_TRADES_PER_CYCLE} per cycle). Stopping further execution.`);
        break;
      }

      const priceData = prices[target.mint];
      if (!priceData) continue;

      const price = priceData.usdPrice;
      const liquidity = priceData.liquidity || 0;
      const change24h = priceData.priceChange24h || 0;
      const marketChange24h = prices[SOL_MINT]?.priceChange24h || 0;
      const regimeTag = inferRegimeTag({ change24h, marketChange24h });
      const signalTs = new Date().toISOString();
      const minLiquidityUsdForSymbol = minLiquidityThresholdUsd(symbol);

      if (liquidity < minLiquidityUsdForSymbol) {
        log(`${symbol} skipped: liquidity $${liquidity.toFixed(0)} below minimum $${minLiquidityUsdForSymbol.toFixed(0)}.`);
        continue;
      }

      // CHECK SELL TARGET (live holdings or paper open positions)
      let holding = portfolio.tokens.find(t => t.id === target.mint);
      let paperPosition = null;
      if ((!holding || holding.amount <= 0) && MODE !== 'live') {
        paperPosition = openPositions.find(p => p.mode === MODE && p.symbol === symbol && Number(p.remaining_amount || 0) > 0);
        if (paperPosition) {
          holding = {
            amount: Number(paperPosition.remaining_amount || 0),
            value: Number(paperPosition.market_value_usd || 0),
            id: target.mint,
          };
        }
      }

      let shouldSell = false;
      let sellReason = null;
      let sellFraction = 1.0;
      if (holding && holding.amount > 0) {
        if (target.sellAbove && price >= target.sellAbove) {
          shouldSell = true;
          sellReason = `Price $${price.toFixed(6)} >= target $${target.sellAbove}`;
        } else if (MODE !== 'live' && paperPosition) {
          const costBasis = Number(paperPosition.cost_basis_usd || 0);
          const openedTs = paperPosition.opened_ts ? new Date(paperPosition.opened_ts).getTime() : 0;
          const holdHours = openedTs ? (now - openedTs) / (1000 * 60 * 60) : 0;
          const unrealized = Number(paperPosition.unrealized_pnl_usd || 0);
          const gainPct = costBasis > 0 ? (unrealized / costBasis) * 100 : 0;
          const lossPct = costBasis > 0 ? (-unrealized / costBasis) * 100 : 0;
          if (costBasis > 0 && gainPct >= PAPER_PARTIAL_PROFIT_PCT && Number(paperPosition.remaining_amount || 0) > 0.000001) {
            shouldSell = true;
            sellFraction = PAPER_PARTIAL_PROFIT_FRACTION;
            sellReason = `Paper partial-profit: ${gainPct.toFixed(2)}% above cost basis`;
          } else if (costBasis > 0 && unrealized < 0 && lossPct >= PAPER_STOP_LOSS_PCT) {
            shouldSell = true;
            sellReason = `Paper stop-loss: ${lossPct.toFixed(2)}% below cost basis`;
          } else if (holdHours >= PAPER_TIME_STOP_HOURS && unrealized <= 0) {
            shouldSell = true;
            sellReason = `Paper time-stop: held ${holdHours.toFixed(1)}h without profit`;
          }
        }
      }

      if (shouldSell) {
        const entryAnchorTrade = [...trades].reverse().find(t => t.symbol === symbol && t.side === 'buy');
        const sellAmount = sellReason && sellReason.startsWith('Paper partial-profit')
          ? Math.max(holding.amount * sellFraction, 0.000001)
          : holding.amount;
        const quote = getQuote(symbol, 'USDC', sellAmount);
        const priceImpact = Number(quote?.priceImpact ?? Infinity);
        if (!quote) {
          log(`❌ SELL SKIPPED for ${symbol}: could not fetch quote.`);
          continue;
        }
        if (priceImpact > MAX_PRICE_IMPACT_PCT) {
          log(`❌ SELL SKIPPED for ${symbol}: quote price impact ${priceImpact.toFixed(4)} exceeds ${MAX_PRICE_IMPACT_PCT}% limit.`);
          continue;
        }

        recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'sell_setup', strategy_tag: paperPosition ? 'paper_exit_logic' : 'take_profit_exit', side: 'sell', price, reference_level: target.sellAbove, distance_pct: target.sellAbove ? ((price - target.sellAbove) / target.sellAbove) * 100 : null, liquidity, status: 'candidate', reason: sellReason, metadata: { mode: MODE, holding_amount: holding.amount, sell_amount: sellAmount, tier: target.tier, paperEligible: target.paperEligible, liveEligible: target.liveEligible } });
        log(`🔴 SELL SIGNAL: ${symbol} at $${price.toFixed(6)} — ${sellReason}`);

        try {
          const command = buildSpotSwapCommand(symbol, 'USDC', sellAmount);
          const sellStrategyTag = sellReason && sellReason.startsWith('Paper partial-profit')
            ? 'paper_partial_profit_exit'
            : (sellReason && sellReason.startsWith('Paper stop-loss')
              ? 'paper_stop_loss_exit'
              : (sellReason && sellReason.startsWith('Paper time-stop') ? 'paper_time_stop_exit' : 'take_profit_exit'));
          const expectedSellProceeds = normalizeNumericAmount(quote?.outAmount ?? holding.value ?? 0);
          if (MODE === 'live' && expectedSellProceeds < MIN_LIVE_ORDER_USD) {
            const queued = accumulateSmallOrder({ key: `${symbol}:sell:${sellStrategyTag}`, symbol, side: 'sell', strategyTag: sellStrategyTag, plannedUsd: expectedSellProceeds, metadata: { reason: sellReason, queueType: 'exit', minLiveOrderUsd: MIN_LIVE_ORDER_USD } });
            structuredSkip('small_live_exit_queued', `Queued small live exit for ${symbol} until notional clears exchange minimum`, { severity: 'warning', mode: MODE, symbol, strategyTag: sellStrategyTag, expectedSellProceeds, minLiveOrderUsd: MIN_LIVE_ORDER_USD, queued }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'sell_setup', strategy_tag: sellStrategyTag, side: 'sell', price, reference_level: target.sellAbove, distance_pct: target.sellAbove ? ((price - target.sellAbove) / target.sellAbove) * 100 : null, liquidity, status: 'skipped', reason: 'small_live_exit_queued', metadata: { mode: MODE, expectedSellProceeds, minLiveOrderUsd: MIN_LIVE_ORDER_USD, queued } });
            continue;
          }
          const rankedPaperSymbol = MODE === 'paper' && PAPER_ALLOWLIST.has(symbol);
          const sellDecisionId = rankedPaperSymbol ? `ranked-paper-exit:${symbol}:${signalTs}` : `paper-standard-exit:${symbol}:${signalTs}`;
          const sellJournalKey = `spot:${sellDecisionId}`;
          if (MODE === 'live') {
            upsertSpotTradeJournalEntry(sellJournalKey, {
              status: 'pending',
              created_at: currentIso(),
              decision_id: sellDecisionId,
              symbol,
              side: 'sell',
              strategy_tag: sellStrategyTag,
              command,
              planned_size_usd: expectedSellProceeds,
              expected_out_amount: quote?.outAmount ?? null,
              quote_price_impact: priceImpact,
            });
          }
          const result = maybeExecuteSwap(command, {
            symbol,
            side: 'sell',
            strategyTag: sellStrategyTag,
            strategyFamily: rankedPaperSymbol ? 'ranked_paper' : 'paper_standard',
            decisionId: sellDecisionId,
            reason: sellReason,
            expectedOutAmount: quote?.outAmount ?? null,
            price,
            sizeUsd: expectedSellProceeds,
            quotePriceImpact: priceImpact,
            validationMode: MODE === 'paper' ? (rankedPaperSymbol ? 'shadow' : 'paper') : 'live',
          });

          if (MODE === 'live' && result?.submitStatus === 'timeout_or_unknown') {
            upsertSpotTradeJournalEntry(sellJournalKey, {
              status: 'submitted',
              submitted_at: currentIso(),
              submission_state: 'timeout_or_unknown',
              submit_error: result.error || null,
            });
            recordSystemEvent('spot_trade_submit_timeout_ambiguous', 'warning', `Ambiguous live sell submission for ${symbol}`, {
              symbol,
              side: 'sell',
              decision_id: sellDecisionId,
              strategy_tag: sellStrategyTag,
              result,
            });
            continue;
          }

          if (result && result.signature) {
            if (MODE === 'live') {
              upsertSpotTradeJournalEntry(sellJournalKey, {
                status: 'submitted',
                submitted_at: currentIso(),
                signature: result.signature,
                out_amount: result.outAmount ?? quote?.outAmount ?? null,
              });
            }
            recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'sell_setup', strategy_tag: sellStrategyTag, side: 'sell', price, reference_level: target.sellAbove, distance_pct: target.sellAbove ? ((price - target.sellAbove) / target.sellAbove) * 100 : null, liquidity, quote_price_impact: priceImpact, status: 'executed', reason: sellReason, metadata: { mode: MODE, sell_fraction: sellFraction, sell_amount: sellAmount, tier: target.tier, paperEligible: target.paperEligible, liveEligible: target.liveEligible } });
            const proceedsUsd = normalizeNumericAmount(result.outAmount ?? quote?.outAmount ?? holding.value ?? 0);
            const trade = {
              ts: new Date().toISOString(),
              symbol,
              side: 'sell',
              mode: MODE,
              simulated: MODE !== 'live',
              strategyTag: sellStrategyTag,
              strategyFamily: rankedPaperSymbol ? 'ranked_paper' : 'paper_standard',
              decisionId: rankedPaperSymbol ? `ranked-paper-exit:${symbol}:${signalTs}` : `paper-standard-exit:${symbol}:${signalTs}`,
              price,
              amount: sellAmount,
              sizeUsd: proceedsUsd,
              outAmount: result.outAmount ?? quote?.outAmount ?? null,
              expectedOutAmount: quote?.outAmount ?? null,
              signature: result.signature,
              quotePriceImpact: priceImpact,
              reason: sellReason,
              entryStrategyTag: entryAnchorTrade?.strategyTag || entryAnchorTrade?.strategy_tag || null,
              entrySignalType: entryAnchorTrade?.entrySignalType || entryAnchorTrade?.entry_signal_type || null,
              entryRegimeTag: entryAnchorTrade?.entryRegimeTag || entryAnchorTrade?.entry_regime_tag || null,
              exitReason: sellStrategyTag,
              validationMode: MODE === 'paper' ? (rankedPaperSymbol ? 'shadow' : 'paper') : 'live',
              approvalStatus: MODE === 'live' ? 'approved' : 'paper_only',
            };
            trades.push(trade);
            tradeCount++;
            executedNotionalThisRun += proceedsUsd;
            runDbCli('record-trade', trade);

            if (MODE === 'live') {
              upsertSpotTradeJournalEntry(sellJournalKey, {
                status: 'verified',
                verified_at: currentIso(),
                trade_signature: result.signature,
                realized_size_usd: proceedsUsd,
              });
              log(`✅ SOLD ${symbol}: ${sellAmount} -> $${proceedsUsd.toFixed(2)} USDC | impact ${priceImpact.toFixed(4)}% | tx: ${result.signature}`);
            } else {
              log(`🧪 PAPER SELL RECORDED ${symbol}: ${sellAmount} -> ~${proceedsUsd.toFixed(2)} USDC | impact ${priceImpact.toFixed(4)}%`);
            }
          } else {
            errorEvents += 1;
            log(`❌ SELL FAILED for ${symbol}: ${JSON.stringify(result)}`);
            recordSystemEvent('trade_failure', 'warning', `Sell failed for ${symbol}`, { mode: MODE, symbol, result });
          }
        } catch (e) {
          errorEvents += 1;
          log(`❌ SELL ERROR for ${symbol}: ${e.message}`);
          recordSystemEvent('trade_error', 'error', `Sell error for ${symbol}: ${e.message}`, { mode: MODE, symbol });
        }
      }
    }

    for (const [symbol, target] of targetEntries) {
      if (tradeCount >= MAX_TRADES_PER_CYCLE) {
        log(`Trade cap reached (${MAX_TRADES_PER_CYCLE} per cycle). Stopping further execution.`);
        break;
      }

      const priceData = prices[target.mint];
      if (!priceData) continue;

      const price = priceData.usdPrice;
      const liquidity = priceData.liquidity || 0;
      const change24h = priceData.priceChange24h || 0;
      const marketChange24h = prices[SOL_MINT]?.priceChange24h || 0;
      const regimeTag = inferRegimeTag({ change24h, marketChange24h });
      const signalTs = new Date().toISOString();
      const minLiquidityUsdForSymbol = minLiquidityThresholdUsd(symbol);

      if (liquidity < minLiquidityUsdForSymbol) {
        log(`${symbol} skipped: liquidity $${liquidity.toFixed(0)} below minimum $${minLiquidityUsdForSymbol.toFixed(0)}.`);
        continue;
      }

      const nearBuyPct = target.buyBelow ? ((price - target.buyBelow) / target.buyBelow) * 100 : null;
      const tinyPilotSymbol = isTinyLivePilotSymbol(symbol);
      const wantsPaperProbeBuy = MODE !== 'live' && !tinyPilotSymbol && target.buyBelow && price > target.buyBelow && nearBuyPct !== null && nearBuyPct <= PAPER_NEAR_BUY_ENTRY_PCT;
      const wantsTinyPilotProbe = MODE !== 'live' && tinyPilotSymbol && TINY_LIVE_PILOT_ALLOW_NEAR_BUY_PROBES && target.buyBelow && price > target.buyBelow && nearBuyPct !== null && nearBuyPct <= PAPER_NEAR_BUY_ENTRY_PCT;
      const shouldPaperProbeBuy = (wantsPaperProbeBuy || wantsTinyPilotProbe) && paperExecutionEligibility(target, 'near_buy_probe');
      const buySignalKind = shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy';
      const shouldAttemptBuy = Boolean(target.buyBelow && price <= target.buyBelow) || shouldPaperProbeBuy;
      if ((MODE !== 'live') && wantsPaperProbeBuy && !shouldPaperProbeBuy) {
        log(`${symbol} paper near-buy probe skipped: tier ${target.tier} does not allow probe execution.`);
        recordSystemEvent('paper_tier_probe_skip', 'info', `Skipped paper near-buy probe for ${symbol} because tier policy disallows it`, { symbol, tier: target.tier, mode: MODE, signalKind: 'near_buy_probe' });
        recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: 'paper_near_buy_probe', side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'paper_tier_probe_ineligible', metadata: { mode: MODE, tier: target.tier, paperEligible: target.paperEligible, liveEligible: target.liveEligible } });
      }
      if (shouldAttemptBuy) {
        const baseStrategyTag = tinyPilotSymbol ? TINY_LIVE_PILOT_STRATEGY_TAG : (shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy');
        const isMeanReversionStrategy = baseStrategyTag === 'mean_reversion_near_buy' || baseStrategyTag === 'paper_near_buy_probe' || tinyPilotSymbol;
        if (tinyPilotSymbol) {
          const pilotContext = classifyTinyLivePilotContext({ symbol, regimeTag, change24h, marketChange24h });
          if (pilotContext.state !== 'supportive') {
            const reason = pilotContext.state === 'mixed' ? 'tiny_live_pilot_mixed_regime' : 'tiny_live_pilot_regime_blocked';
            log(`${symbol} buy signal blocked: tiny live pilot context is ${pilotContext.state} (${pilotContext.reason}).`);
            structuredSkip(reason, `Skipped buy for ${symbol} because tiny live pilot context is ${pilotContext.state}`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, regimeTag, change24h, marketChange24h, pilotContext }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, regime_tag: regimeTag, status: 'skipped', reason, metadata: { mode: MODE, tier: target.tier, regimeTag, change24h, marketChange24h, pilotContext } });
            continue;
          }
        } else if (isMeanReversionStrategy && (regimeTag === 'trend_down' || regimeTag === 'panic_selloff')) {
          log(`${symbol} buy signal blocked: regime ${regimeTag} disallows mean-reversion entries.`);
          structuredSkip('regime_block_skip', `Skipped buy for ${symbol} because regime ${regimeTag} blocks mean-reversion`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, regimeTag, change24h, marketChange24h }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, regime_tag: regimeTag, status: 'skipped', reason: 'regime_blocks_mean_reversion', metadata: { mode: MODE, tier: target.tier, regimeTag, change24h, marketChange24h } });
          continue;
        }
        if (!canAttemptBuys) {
          log(`${symbol} buy signal skipped: insufficient spendable USDC for new entries this cycle.`);
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'insufficient_spendable_usdc', metadata: { mode: MODE, usdcAvailable, usdcSpendable, minUsdcReserve: MIN_USDC_RESERVE, tier: target.tier } });
          continue;
        }
        const liveEnabledForSymbol = target.liveEligible || (tinyPilotSymbol && TINY_LIVE_PILOT_ALLOW_LIVE);
        if (MODE === 'live' && !liveEnabledForSymbol) {
          log(`${symbol} buy signal skipped: live execution is disabled for this symbol.`);
          structuredSkip('live_symbol_ineligible_skip', `Skipped live buy for ${symbol} because live execution is disabled`, { severity: 'warning', symbol, tier: target.tier, signalKind: buySignalKind, mode: MODE, liveEligible: liveEnabledForSymbol, tinyPilotSymbol }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'live_symbol_ineligible', metadata: { mode: MODE, tier: target.tier, paperEligible: target.paperEligible, liveEligible: liveEnabledForSymbol, tinyPilotSymbol } });
          continue;
        }
        if (MODE === 'paper' && !paperExecutionEligibility(target, buySignalKind)) {
          log(`${symbol} buy signal skipped: paper execution disabled for tier ${target.tier} ${buySignalKind}.`);
          recordSystemEvent('paper_symbol_ineligible_skip', 'info', `Skipped paper buy for ${symbol} because symbol/tier is not eligible for ${buySignalKind}`, { symbol, tier: target.tier, signalKind: buySignalKind, mode: MODE });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'paper_symbol_ineligible', metadata: { mode: MODE, tier: target.tier, paperEligible: target.paperEligible, liveEligible: target.liveEligible } });
          continue;
        }
        if (liveBuyTradeCapReached || liveBuyNotionalCapReached) {
          const buyCapReason = liveBuyTradeCapReached ? 'daily_trade_cap_reached' : 'daily_notional_cap_reached';
          log(`${symbol} buy signal skipped: live daily buy cap reached.`);
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: buyCapReason, metadata: { mode: MODE, dailyStats, maxDailyTrades: MAX_DAILY_TRADES, maxDailyNotionalUsd: MAX_DAILY_NOTIONAL_USD, tier: target.tier } });
          continue;
        }
        const strategyRule = getGateRule(promotionRules, baseStrategyTag);
        const symbolRulePreview = getGateRule(symbolPromotionRules, symbol);
        const rankedPaperPauseBypass = MODE === 'paper'
          && PAPER_ALLOWLIST.has(symbol)
          && ['monitor', 'promote'].includes(strategyRule?.decision || 'missing')
          && ['monitor', 'promote'].includes(symbolRulePreview?.decision || 'missing');
        if (pausedStrategies.has(baseStrategyTag) && !rankedPaperPauseBypass) {
          log(`${symbol} buy signal skipped: strategy ${baseStrategyTag} is paused by risk controls.`);
          recordSystemEvent('strategy_paused_skip', 'warning', `Skipped buy for ${symbol} because ${baseStrategyTag} is paused`, { symbol, strategyTag: baseStrategyTag, tier: target.tier });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'strategy_paused', metadata: { mode: MODE, tier: target.tier } });
          continue;
        }
        if (pausedStrategies.has(baseStrategyTag) && rankedPaperPauseBypass) {
          log(`${symbol} ranked-paper path bypassing paused strategy ${baseStrategyTag}.`);
          recordSystemEvent('ranked_paper_pause_bypass', 'info', `Ranked-paper path allowed ${symbol} despite paused strategy ${baseStrategyTag}`, { mode: MODE, symbol, strategyTag: baseStrategyTag, strategyRule, symbolRule: symbolRulePreview, tier: target.tier });
        }
        const strategySkipCandidate = {
          ts: signalTs,
          source: 'auto-trade.mjs',
          symbol,
          signal_type: 'buy_setup',
          strategy_tag: baseStrategyTag,
          side: 'buy',
          price,
          reference_level: target.buyBelow,
          distance_pct: nearBuyPct,
          liquidity,
          regime_tag: regimeTag,
          status: 'skipped',
        };
        const liveStrategyDemotionOverride = MODE === 'live' && tinyPilotSymbol && TINY_LIVE_PILOT_DEMOTION_OVERRIDE_SYMBOLS.has(symbol);
        if (strategyRule && strategyRule.decision === 'demote' && !liveStrategyDemotionOverride) {
          log(`${symbol} buy signal skipped: strategy ${baseStrategyTag} is demoted by promotion rules.`);
          structuredSkip('strategy_demoted_skip', `Skipped buy for ${symbol} because ${baseStrategyTag} is demoted`, { severity: 'warning', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, gate: strategyRule }, { ...strategySkipCandidate, reason: 'strategy_demoted', metadata: { mode: MODE, tier: target.tier, gate: strategyRule } });
          continue;
        }
        if (strategyRule && strategyRule.decision === 'demote' && liveStrategyDemotionOverride) {
          recordSystemEvent('tiny_live_strategy_demotion_override', 'warning', `Tiny live pilot override bypassed strategy demotion for ${symbol}`, {
            symbol,
            strategyTag: baseStrategyTag,
            tier: target.tier,
            gate: strategyRule,
          });
        }
        if (MODE === 'paper' && !strategyRule && !ALLOW_INSUFFICIENT_SAMPLE_PAPER) {
          log(`${symbol} buy signal observation-only: strategy ${baseStrategyTag} has no gate rule yet.`);
          structuredSkip('strategy_missing_gate_rule_skip', `Observation-only for ${symbol}: ${baseStrategyTag} has no gate rule yet`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER, missingGateRule: true }, { ...strategySkipCandidate, reason: 'strategy_missing_gate_rule_observation_only', metadata: { mode: MODE, tier: target.tier, missingGateRule: true, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER } });
          continue;
        }
        if (strategyRule && strategyRule.sample_sufficient === false && !ALLOW_INSUFFICIENT_SAMPLE_PAPER) {
          log(`${symbol} buy signal observation-only: strategy ${baseStrategyTag} has insufficient sample.`);
          structuredSkip('strategy_insufficient_sample_skip', `Observation-only for ${symbol}: ${baseStrategyTag} has insufficient sample`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER, gate: strategyRule }, { ...strategySkipCandidate, reason: 'strategy_insufficient_sample_observation_only', metadata: { mode: MODE, tier: target.tier, gate: strategyRule, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER } });
          continue;
        }
        const strategyLastTrade = lastTradeTimeForStrategy(trades, baseStrategyTag);
        if (strategyLastTrade && (now - strategyLastTrade) < MEAN_REVERSION_COOLDOWN_MS) {
          log(`${symbol} buy signal skipped: strategy ${baseStrategyTag} is in cooldown.`);
          recordSystemEvent('strategy_cooldown_skip', 'info', `Skipped buy for ${symbol} because ${baseStrategyTag} is cooling down`, { symbol, strategyTag: baseStrategyTag, cooldownMs: MEAN_REVERSION_COOLDOWN_MS, tier: target.tier });
          continue;
        }
        if (now - lastBuyTime(symbol) < COOLDOWN_MS) {
          log(`${symbol} hit buy target ($${price.toFixed(6)} <= $${target.buyBelow}) but is in cooldown. Skipping.`);
          continue;
        }

        const currentSymbolExposure = openPositions
          .filter(p => p.mode === MODE && p.symbol === symbol)
          .reduce((sum, p) => sum + Number(p.market_value_usd || 0), 0);
        const symbolRule = getGateRule(symbolPromotionRules, symbol);
        const executionScore = getGateRule(executionScores, symbol);
        const tierPolicy = getPaperPolicyForTier(target.tier);
        const symbolCandidateStats = candidateVolume.bySymbol.get(symbol) || null;
        const symbolSideStats = candidateVolume.bySymbolSide.get(`${symbol}::buy`) || null;
        const paperSymbolTradeStats = MODE === 'paper' ? getSymbolTradeStatsForToday(trades, symbol, 'buy') : { count: 0, notionalUsd: 0 };
        let symbolRiskCap = Number(target.maxSymbolRiskUsd || DEFAULT_SYMBOL_RISK_CAP_USD);
        if (tinyPilotSymbol) {
          symbolRiskCap = TINY_LIVE_PILOT_MAX_SYMBOL_RISK_USD;
        }
        const liveSymbolDemotionOverride = MODE === 'live' && tinyPilotSymbol && TINY_LIVE_PILOT_DEMOTION_OVERRIDE_SYMBOLS.has(symbol);
        if (symbolRule && symbolRule.decision === 'demote' && !liveSymbolDemotionOverride) {
          log(`${symbol} buy signal skipped: symbol ${symbol} is demoted by symbol-level rules.`);
          structuredSkip('symbol_demoted_skip', `Skipped buy for ${symbol} because symbol is demoted`, { severity: 'warning', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, gate: symbolRule }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'symbol_demoted', metadata: { mode: MODE, tier: target.tier, gate: symbolRule } });
          continue;
        }
        if (symbolRule && symbolRule.decision === 'demote' && liveSymbolDemotionOverride) {
          recordSystemEvent('tiny_live_symbol_demotion_override', 'warning', `Tiny live pilot override bypassed symbol demotion for ${symbol}`, {
            symbol,
            strategyTag: baseStrategyTag,
            tier: target.tier,
            gate: symbolRule,
          });
        }
        if (symbolRule && symbolRule.sample_sufficient === false && !ALLOW_INSUFFICIENT_SAMPLE_PAPER) {
          log(`${symbol} buy signal observation-only: symbol gate has insufficient sample.`);
          structuredSkip('symbol_insufficient_sample_skip', `Observation-only for ${symbol}: symbol gate has insufficient sample`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER, gate: symbolRule }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'symbol_insufficient_sample_observation_only', metadata: { mode: MODE, tier: target.tier, gate: symbolRule, allowInsufficientSamplePaper: ALLOW_INSUFFICIENT_SAMPLE_PAPER } });
          continue;
        }
        const livePromotionBypass = MODE === 'live' && tinyPilotSymbol;
        if (MODE === 'live' && !livePromotionBypass && ((strategyRule && strategyRule.decision !== 'promote') || (symbolRule && symbolRule.decision !== 'promote') || !strategyRule || !symbolRule)) {
          const liveGate = {
            strategyDecision: strategyRule?.decision || 'missing',
            symbolDecision: symbolRule?.decision || 'missing',
            strategyRule,
            symbolRule,
          };
          log(`${symbol} live buy skipped: live mode requires promoted strategy and promoted symbol.`);
          structuredSkip('live_promotion_gate_skip', `Skipped live buy for ${symbol} because live mode requires promoted strategy and symbol`, { severity: 'warning', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, liveGate }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'live_requires_promoted_strategy_and_symbol', metadata: { mode: MODE, tier: target.tier, liveGate } });
          continue;
        }
        if (livePromotionBypass && MODE === 'live') {
          recordSystemEvent('tiny_live_pilot_live_gate_bypass', 'info', `Tiny live pilot bypassed promotion gate for ${symbol}`, {
            symbol,
            strategyTag: baseStrategyTag,
            tier: target.tier,
            strategyDecision: strategyRule?.decision || 'missing',
            symbolDecision: symbolRule?.decision || 'missing',
          });
        }
        if (MODE === 'paper' && !isPaperAllowlisted(symbol, symbolRule, strategyRule)) {
          const rankedPaperGate = {
            allowlist: Array.from(PAPER_ALLOWLIST),
            symbolDecision: symbolRule?.decision || 'missing',
            strategyDecision: strategyRule?.decision || 'missing',
            rankedMode: PAPER_RANKED_MODE,
          };
          log(`${symbol} paper buy skipped: ranked-paper allowlist/gate excludes this symbol.`);
          structuredSkip('paper_allowlist_skip', `Skipped paper buy for ${symbol} because ranked-paper allowlist/gate excludes it`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, rankedPaperGate }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, regime_tag: regimeTag, status: 'skipped', reason: 'paper_allowlist_or_gate_excluded', metadata: { mode: MODE, tier: target.tier, rankedPaperGate } });
          continue;
        }
        if (symbolRule && symbolRule.decision === 'promote') {
          symbolRiskCap *= 1.25;
        }
        if (MODE === 'paper' && paperSymbolTradeStats.count >= target.maxPaperEntriesPerDay) {
          log(`${symbol} paper buy skipped: daily symbol entry cap reached (${paperSymbolTradeStats.count}/${target.maxPaperEntriesPerDay}).`);
          recordSystemEvent('paper_symbol_entry_cap_skip', 'warning', `Skipped paper buy for ${symbol} because daily symbol entry cap is reached`, { symbol, tier: target.tier, entryCountToday: paperSymbolTradeStats.count, maxPaperEntriesPerDay: target.maxPaperEntriesPerDay });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'paper_symbol_entry_cap', metadata: { mode: MODE, tier: target.tier, entryCountToday: paperSymbolTradeStats.count, maxPaperEntriesPerDay: target.maxPaperEntriesPerDay } });
          continue;
        }
        if (MODE === 'paper' && paperSymbolTradeStats.notionalUsd >= target.maxPaperNotionalPerDay) {
          log(`${symbol} paper buy skipped: daily symbol notional cap reached ($${paperSymbolTradeStats.notionalUsd.toFixed(2)}/$${target.maxPaperNotionalPerDay.toFixed(2)}).`);
          recordSystemEvent('paper_symbol_notional_cap_skip', 'warning', `Skipped paper buy for ${symbol} because daily symbol notional cap is reached`, { symbol, tier: target.tier, notionalUsdToday: paperSymbolTradeStats.notionalUsd, maxPaperNotionalPerDay: target.maxPaperNotionalPerDay });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'paper_symbol_notional_cap', metadata: { mode: MODE, tier: target.tier, notionalUsdToday: paperSymbolTradeStats.notionalUsd, maxPaperNotionalPerDay: target.maxPaperNotionalPerDay } });
          continue;
        }
        const recentSameDirectionCandidates = Number(symbolSideStats?.recent_candidate_count || 0);
        const repeatedCandidateCap = Number(target.recentSameDirectionCandidateCap || PAPER_RECENT_SAME_DIRECTION_CANDIDATE_CAP);
        if (MODE === 'paper' && recentSameDirectionCandidates >= repeatedCandidateCap) {
          log(`${symbol} paper buy skipped: too many recent same-direction candidates (${recentSameDirectionCandidates}/${repeatedCandidateCap}) in ${PAPER_RECENT_CANDIDATE_LOOKBACK_MINUTES}m.`);
          recordSystemEvent('paper_symbol_recent_candidate_skip', 'warning', `Skipped paper buy for ${symbol} because recent same-direction candidate volume is too high`, { symbol, tier: target.tier, side: 'buy', recentSameDirectionCandidates, repeatedCandidateCap, lookbackMinutes: PAPER_RECENT_CANDIDATE_LOOKBACK_MINUTES, symbolCandidateStats });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'paper_recent_same_direction_candidates', metadata: { mode: MODE, tier: target.tier, recentSameDirectionCandidates, repeatedCandidateCap, lookbackMinutes: PAPER_RECENT_CANDIDATE_LOOKBACK_MINUTES } });
          continue;
        }
        if (currentSymbolExposure >= symbolRiskCap) {
          log(`${symbol} buy signal skipped: symbol risk cap already reached ($${currentSymbolExposure.toFixed(2)} / $${symbolRiskCap.toFixed(2)}).`);
          recordSystemEvent('symbol_risk_cap_skip', 'warning', `Skipped buy for ${symbol} because symbol risk cap is reached`, { symbol, currentSymbolExposure, symbolRiskCap, tier: target.tier });
          continue;
        }

        if (executionScore) {
          const minExecutionScore = Number(symbolRule?.demotion_execution_score_threshold || symbolRule?.execution_score_threshold || executionScore?.demotion_execution_score_threshold || 35);
          if (Number(executionScore.score) < minExecutionScore) {
            log(`${symbol} buy signal skipped: execution score too low (${executionScore.score} < ${minExecutionScore}).`);
            structuredSkip('execution_score_skip', `Skipped buy for ${symbol} because execution score is too low`, { severity: 'warning', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, executionScore, minExecutionScore }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, status: 'skipped', reason: 'execution_score_low', metadata: { mode: MODE, executionScore, minExecutionScore, tier: target.tier } });
            continue;
          }
        }
        const sizingPlan = planSpotSizing({
          target,
          symbol,
          regimeTag,
          symbolRiskCap,
          currentSymbolExposure,
          usdcSpendable,
          portfolioTotalValue: portfolio.totalValue,
          tierPolicy,
          strategyRule,
          executionScore,
          tinyPilotSymbol,
          shouldPaperProbeBuy,
        });
        sizingPlan.strategyRulebook = STRATEGY_RULES[baseStrategyTag] || null;
        sizingPlan.paperNotionalRemainingUsd = Math.max(0, target.maxPaperNotionalPerDay - paperSymbolTradeStats.notionalUsd);
        let buyAmount = sizingPlan.plannedBuyAmount;
        if (MODE === 'paper') {
          buyAmount = Math.min(buyAmount, sizingPlan.paperNotionalRemainingUsd);
        }
        if (MODE === 'live') {
          const symbolMinTargetUsd = minLiveTargetUsd(symbol);
          if (symbolMinTargetUsd > 0) {
            buyAmount = Math.max(buyAmount, symbolMinTargetUsd);
          }
        }

        if (buyAmount < MIN_TRADE_USD) {
          log(`${symbol} hit buy target but buy amount too small ($${buyAmount.toFixed(2)}). Skipping.`);
          continue;
        }
        if (MODE === 'live' && buyAmount < MIN_LIVE_ORDER_USD) {
          const queueKey = `${symbol}:buy:${baseStrategyTag}`;
          const queued = accumulateSmallOrder({ key: queueKey, symbol, side: 'buy', strategyTag: baseStrategyTag, plannedUsd: buyAmount, metadata: { minLiveOrderUsd: MIN_LIVE_ORDER_USD, regimeTag, sizingPlan } });
          structuredSkip('small_live_entry_queued', `Queued small live entry for ${symbol} until notional clears exchange minimum`, { severity: 'warning', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, buyAmount, minLiveOrderUsd: MIN_LIVE_ORDER_USD, queued }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, regime_tag: regimeTag, status: 'skipped', reason: 'small_live_entry_queued', metadata: { mode: MODE, tier: target.tier, minLiveOrderUsd: MIN_LIVE_ORDER_USD, buyAmount, queued, sizingPlan } });
          continue;
        }
        if (MODE === 'live') {
          const queued = consumeSmallOrderQueue(`${symbol}:buy:${baseStrategyTag}`);
          if (queued && Number(queued.accumulatedUsd || 0) > 0) {
            buyAmount = Math.max(buyAmount, Number(queued.accumulatedUsd));
          }
        }

        const quote = getQuote('USDC', symbol, buyAmount.toFixed(2));
        const priceImpact = Number(quote?.priceImpact ?? Infinity);
        if (!quote) {
          log(`❌ BUY SKIPPED for ${symbol}: could not fetch quote.`);
          continue;
        }
        if (priceImpact > MAX_PRICE_IMPACT_PCT) {
          log(`❌ BUY SKIPPED for ${symbol}: quote price impact ${priceImpact.toFixed(4)} exceeds ${MAX_PRICE_IMPACT_PCT}% limit.`);
          continue;
        }

        const pressure = executionScore?.pressure || symbolRule?.pressure || 'neutral';
        const entryScore = buildEntryScore({
          signalKind: buySignalKind,
          nearBuyPct,
          liquidity,
          priceImpact,
          symbolExecutionScore: executionScore?.score || executionScore?.execution_score || 0,
          strategyExecutionScore: strategyRule?.execution_score || 0,
          pressure,
          regimeTag,
          recentSameDirectionCandidates,
          repeatedCandidateCap,
        });
        const scoreMetadata = {
          mode: MODE,
          tier: target.tier,
          paperEligible: target.paperEligible,
          liveEligible: target.liveEligible,
          change24h,
          marketChange24h,
          pressure,
          recentSameDirectionCandidates,
          repeatedCandidateCap,
          score_components: entryScore.components,
          symbol_gate: symbolRule,
          strategy_gate: strategyRule,
          sizing_plan: sizingPlan,
          strategy_rulebook: STRATEGY_RULES[baseStrategyTag] || null,
          position_sizing_framework: POSITION_SIZING_FRAMEWORK,
        };
        const minEntryScoreForSymbol = getEntryScoreThreshold({ mode: MODE, symbol, isTinyPilot: tinyPilotSymbol, regimeTag });
        if (entryScore.score < minEntryScoreForSymbol) {
          log(`${symbol} buy signal skipped: entry score too low (${entryScore.score} < ${minEntryScoreForSymbol}).`);
          structuredSkip('entry_score_skip', `Skipped buy for ${symbol} because entry score is below threshold`, { severity: 'info', mode: MODE, symbol, strategyTag: baseStrategyTag, tier: target.tier, entryScore, minEntryScore: minEntryScoreForSymbol, pressure, regimeTag }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: 'buy_setup', strategy_tag: baseStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, score: entryScore.score, regime_tag: regimeTag, status: 'skipped', reason: 'entry_score_below_threshold', metadata: { ...scoreMetadata, minEntryScore: minEntryScoreForSymbol } });
          continue;
        }

        const projectedBuyNotional = dailyStats.notionalUsd + executedNotionalThisRun + buyAmount;
        const projectedTradeCount = dailyStats.count + tradeCount + 1;
        if (MODE === 'live' && (projectedTradeCount > MAX_DAILY_TRADES || projectedBuyNotional > MAX_DAILY_NOTIONAL_USD)) {
          const reason = `Projected buy would exceed daily limits for ${symbol}; skipping buy.`;
          log(reason);
          recordSystemEvent('projected_buy_cap_skip', 'warning', reason, { mode: MODE, symbol, projectedTradeCount, projectedBuyNotional, maxDailyTrades: MAX_DAILY_TRADES, maxDailyNotionalUsd: MAX_DAILY_NOTIONAL_USD });
          recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy', strategy_tag: shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy', side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, score: entryScore.score, regime_tag: regimeTag, status: 'skipped', reason: 'projected_daily_limit', metadata: { ...scoreMetadata, projectedTradeCount, projectedBuyNotional, maxDailyTrades: MAX_DAILY_TRADES, maxDailyNotionalUsd: MAX_DAILY_NOTIONAL_USD } });
          continue;
        }

        const paperLane = tinyPilotSymbol ? 'tiny_live_pilot' : (MODE === 'paper' && PAPER_ALLOWLIST.has(symbol) ? 'ranked_paper' : 'paper_standard');
        const decisionId = `${paperLane}:${symbol}:${signalTs}`;
        const buyReason = shouldPaperProbeBuy
          ? `Paper probe buy: ${symbol} is within ${nearBuyPct.toFixed(2)}% of buy target $${target.buyBelow}`
          : `Price $${price.toFixed(6)} <= target $${target.buyBelow}`;
        recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy', strategy_tag: shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy', side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, score: entryScore.score, regime_tag: regimeTag, decision_id: decisionId, status: 'candidate', reason: buyReason, metadata: { ...scoreMetadata, paper_lane: paperLane, planned_size_usd: buyAmount, minEntryScore: minEntryScoreForSymbol } });
        log(`🟢 BUY SIGNAL: ${symbol} at $${price.toFixed(6)} — buying $${buyAmount.toFixed(2)} worth | ${buyReason}`);

        const liveSpotApproval = MODE === 'live'
          ? getExecutableSpotApprovalIntent(decisionId)
          : null;
        if (MODE === 'live' && !liveSpotApproval) {
          const approvalIntent = requestSpotLiveApproval(buildSpotApprovalIntent({
            decisionId,
            symbol,
            strategyTag: shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy',
            signalType: shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy',
            buyAmount,
            price,
            regimeTag,
            entryScore,
            quotePriceImpact: priceImpact,
            reason: buyReason,
          }));
          structuredSkip('spot_live_approval_required', `Awaiting Telegram approval for live spot buy ${symbol}`, { severity: 'warning', mode: MODE, symbol, decisionId, approvalIntent, buyAmount, price, regimeTag }, { ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy', strategy_tag: shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy', side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, score: entryScore.score, regime_tag: regimeTag, status: 'skipped', reason: 'awaiting_spot_live_approval', metadata: { ...scoreMetadata, approval_intent: approvalIntent, planned_size_usd: buyAmount, minEntryScore: minEntryScoreForSymbol } });
          continue;
        }

        try {
          const command = buildSpotSwapCommand('USDC', symbol, buyAmount.toFixed(2));
          const buyStrategyTag = shouldPaperProbeBuy ? 'paper_near_buy_probe' : 'mean_reversion_near_buy';
          const buyJournalKey = `spot:${decisionId}`;
          if (MODE === 'live') {
            upsertSpotTradeJournalEntry(buyJournalKey, {
              status: 'pending',
              created_at: currentIso(),
              decision_id: decisionId,
              symbol,
              side: 'buy',
              strategy_tag: buyStrategyTag,
              command,
              planned_size_usd: buyAmount,
              expected_out_amount: quote?.outAmount ?? null,
              quote_price_impact: priceImpact,
              approval_id: liveSpotApproval?.approval_id || null,
            });
          }
          const result = maybeExecuteSwap(command, {
            symbol,
            side: 'buy',
            strategyTag: buyStrategyTag,
            strategyFamily: paperLane,
            decisionId,
            reason: buyReason,
            expectedOutAmount: quote?.outAmount ?? null,
            price,
            sizeUsd: buyAmount,
            quotePriceImpact: priceImpact,
            validationMode: MODE === 'paper' ? (paperLane === 'paper_standard' ? 'paper' : 'shadow') : 'live',
          });
          if (MODE === 'live' && result?.submitStatus === 'timeout_or_unknown') {
            upsertSpotTradeJournalEntry(buyJournalKey, {
              status: 'submitted',
              submitted_at: currentIso(),
              submission_state: 'timeout_or_unknown',
              submit_error: result.error || null,
            });
            recordSystemEvent('spot_trade_submit_timeout_ambiguous', 'warning', `Ambiguous live buy submission for ${symbol}`, {
              symbol,
              side: 'buy',
              decision_id: decisionId,
              strategy_tag: buyStrategyTag,
              result,
            });
            continue;
          }
          if (result && result.signature) {
            if (MODE === 'live') {
              upsertSpotTradeJournalEntry(buyJournalKey, {
                status: 'submitted',
                submitted_at: currentIso(),
                signature: result.signature,
                out_amount: result.outAmount ?? quote?.outAmount ?? null,
              });
            }
            const executedBuy = deriveExecutedBuyQuantity({ result, quote, price, buyAmount });
            if (executedBuy.quantity <= 0) {
              errorEvents += 1;
              log(`❌ BUY RECORDING ERROR for ${symbol}: could not derive token quantity from quote/swap result.`);
              recordSystemEvent('trade_recording_error', 'error', `Buy quantity missing for ${symbol}`, { mode: MODE, symbol, price, buyAmount, result, quote });
              continue;
            }
            if (executedBuy.source !== 'quoted_out_amount') {
              log(`⚠️ ${symbol} buy quantity inferred via fallback (${executedBuy.source}). qty=${executedBuy.quantity}`);
              recordSystemEvent('trade_quantity_fallback', 'warning', `Inferred buy quantity for ${symbol} using fallback path`, { mode: MODE, symbol, quantity: executedBuy.quantity, source: executedBuy.source, price, buyAmount });
            }
            recordSignalCandidate({ ts: signalTs, source: 'auto-trade.mjs', symbol, signal_type: shouldPaperProbeBuy ? 'near_buy_probe' : 'hard_buy', strategy_tag: buyStrategyTag, side: 'buy', price, reference_level: target.buyBelow, distance_pct: nearBuyPct, liquidity, quote_price_impact: priceImpact, score: entryScore.score, regime_tag: regimeTag, decision_id: decisionId, status: 'executed', reason: buyReason, metadata: { ...scoreMetadata, paper_lane: paperLane, planned_size_usd: buyAmount, executed_quantity: executedBuy.quantity, quantity_source: executedBuy.source, minEntryScore: minEntryScoreForSymbol } });
            const trade = {
              ts: new Date().toISOString(),
              symbol,
              side: 'buy',
              mode: MODE,
              simulated: MODE !== 'live',
              strategyTag: buyStrategyTag,
              strategyFamily: paperLane,
              decisionId,
              price,
              amount: executedBuy.quantity,
              sizeUsd: buyAmount,
              outAmount: result.outAmount ?? quote?.outAmount ?? null,
              expectedOutAmount: quote?.outAmount ?? null,
              signature: result.signature,
              quotePriceImpact: priceImpact,
              reason: buyReason,
              entrySignalType: buySignalKind,
              entryRegimeTag: regimeTag,
              entryStrategyTag: buyStrategyTag,
              validationMode: MODE === 'paper' ? (paperLane === 'paper_standard' ? 'paper' : 'shadow') : 'live',
              approvalStatus: MODE === 'live' ? 'telegram_approved' : 'paper_only',
            };
            trades.push(trade);
            tradeCount++;
            executedNotionalThisRun += buyAmount;
            runDbCli('record-trade', trade);

            if (MODE === 'live') {
              upsertSpotTradeJournalEntry(buyJournalKey, {
                status: 'verified',
                verified_at: currentIso(),
                trade_signature: result.signature,
                realized_size_usd: buyAmount,
                executed_quantity: executedBuy.quantity,
              });
              markSpotApprovalExecuted(liveSpotApproval, { decision_id: decisionId, symbol, signature: result.signature, size_usd: buyAmount });
              log(`✅ BOUGHT ${symbol}: $${buyAmount.toFixed(2)} USDC -> ${result.outAmount} ${symbol} | impact ${priceImpact.toFixed(4)}% | tx: ${result.signature}`);
              appendToTradingMd({
                id: trades.length,
                date: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
                pair: `USDC→${symbol}`,
                side: 'buy',
                entryPrice: price.toFixed(6),
                sizeUsd: buyAmount.toFixed(2),
                reason: `hit buy target $${target.buyBelow}`,
              });
            } else {
              log(`🧪 PAPER BUY RECORDED ${symbol}: $${buyAmount.toFixed(2)} USDC -> ~${result.outAmount || quote?.outAmount || '?'} ${symbol} | impact ${priceImpact.toFixed(4)}%`);
            }
          } else {
            errorEvents += 1;
            log(`❌ SWAP FAILED for ${symbol}: ${JSON.stringify(result)}`);
            recordSystemEvent('trade_failure', 'warning', `Buy failed for ${symbol}`, { mode: MODE, symbol, result });
          }
        } catch (e) {
          errorEvents += 1;
          log(`❌ SWAP ERROR for ${symbol}: ${e.message}`);
          recordSystemEvent('trade_error', 'error', `Buy error for ${symbol}: ${e.message}`, { mode: MODE, symbol });
        }
      }
    }

    saveTrades(trades);

    if (MODE === 'live' && errorEvents >= MAX_ERROR_EVENTS_PER_RUN) {
      const reason = `Kill switch: too many trade errors this run (${errorEvents}/${MAX_ERROR_EVENTS_PER_RUN})`;
      log(reason);
      activateKillSwitch(reason, { mode: MODE, errorEvents });
    }

    if (tradeCount === 0) {
      log('No targets hit. All quiet.');
    } else {
      log(`Executed ${tradeCount} trade(s) this cycle.`);
    }
  } finally {
    releaseLock();
  }
}

main().catch(e => { log(`FATAL: ${e.message}`); process.exit(1); });
