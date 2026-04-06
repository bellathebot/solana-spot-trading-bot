# Architecture

## Overview

The spot trading bot has four main layers:

1. Market monitoring
2. Execution and risk gating
3. SQLite analytics and policy generation
4. Operator messaging / approval workflows

## 1. Monitoring layer

File: `monitor.mjs`

Responsibilities:

- polls current market data
- evaluates symbol thresholds (`alertBelow`, `buyBelow`, `sellAbove`)
- produces near-buy / hard-buy alerts
- snapshots wallet state and watchlist state
- emits signal candidates used by downstream analytics and reporting

The monitor is the operator-facing truth source for current spot candidate state.

## 2. Execution layer

File: `auto-trade.mjs`

Responsibilities:

- runs paper mode by default
- optionally runs supervised live mode
- applies reserves, caps, cooldowns, liquidity checks, drawdown controls, and price-impact checks
- loads strategy and symbol controls from SQLite analytics
- writes system events and candidate results back into SQLite
- requests Telegram approval before supervised live spot entries

Important current behavior:

- BONK is monitored but not live-tradable
- supervised live spot entries are intentionally narrow and symbol-specific
- PYTH currently has the strongest narrow live pilot path in this export

## 3. Analytics layer

Main files:

- `trading_system/trading_db.py`
- `trading_system/trading_db_cli.py`
- `trading_system/sync_trading_db.py`
- `trading_system/daily_analytics_report.py`

Responsibilities:

- stores prices, trades, candidate rows, system events, and whale observations
- computes strategy promotion / demotion state
- exposes execution controls back to the executor
- supports operator reports and health summaries

The executor relies on this layer for:

- paused strategies
- demoted symbols
- execution scores
- promotion gates
- risk guardrail status

## 4. Messaging / operator approval layer

Main files:

- `trading_system/telegram_trade_reply_bridge.py`
- `trading_system/spot_telegram_notifier.py`
- `trading_system/spot_notifier_health_check.py`
- `trading_system/spot_live_candidate_summary.py`
- `trading_system/spot_recovery_dashboard.py`

Responsibilities:

- sends Telegram approval requests and anomaly alerts
- receives operator commands / YES / NO replies
- keeps approval state in bridge files under `.trading-data/telegram-bridge/`
- exposes operator-facing summaries of current live candidate quality and notifier health

## 5. Whale / flow context

File: `trading_system/whale_scanner.py`

Responsibilities:

- scans tracked wallets and symbols
- stores whale observations into SQLite
- contributes contextual market pressure information

This is currently an overlay/context source, not the primary alpha engine.

## Data flow

1. `monitor.mjs` evaluates current prices and thresholds
2. candidate and state data are recorded into SQLite
3. `auto-trade.mjs` loads policy controls from `trading_db_cli.py`
4. the executor either:
   - skips a trade and records the reason, or
   - requests Telegram approval for a supervised live candidate, or
   - executes a paper trade
5. notifier and summary scripts report current state back to the operator

## Current design principles

- paper-first validation
- regime/context-aware gating
- symbol-specific overrides instead of basket-wide loosening
- explicit operator visibility for blockers
- fail-closed live execution with approval requirements
