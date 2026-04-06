# Solana Spot Trading Bot

![Tests](https://github.com/bellathebot/solana-spot-trading-bot/actions/workflows/tests.yml/badge.svg)

A paper-first, supervised-live Solana spot trading system built around Jupiter CLI execution, SQLite-backed analytics, and Telegram approval workflows.

This repository packages the current spot-trading codebase into a standalone project without local runtime secrets, wallet keys, or trading data.

## What it does

- Monitors a curated Solana spot watchlist
- Tracks near-buy and hard-buy thresholds
- Runs paper-first spot execution with strict risk controls
- Supports supervised tiny-live spot approvals over Telegram
- Persists signals, trades, positions, and system events into SQLite
- Generates operator summaries and health checks
- Tracks whale-flow observations as contextual signal input

## Core components

- `monitor.mjs` — market/watchlist monitor and signal generation
- `auto-trade.mjs` — spot executor with paper/live gating and risk controls
- `trading_system/trading_db.py` — SQLite schema and analytics logic
- `trading_system/trading_db_cli.py` — CLI bridge used by Node scripts
- `trading_system/telegram_trade_reply_bridge.py` — Telegram YES/NO / command approval bridge
- `trading_system/spot_telegram_notifier.py` — outbound Telegram spot alerts
- `trading_system/spot_live_candidate_summary.py` — ranked supervised-live candidate summary
- `trading_system/daily_analytics_report.py` — operator analytics report
- `trading_system/whale_scanner.py` — whale observation pipeline

## Repository layout

- `auto-trade.mjs`
- `monitor.mjs`
- `trading_system/`
- `docs/`
- `config/`
- `package.json`

See `docs/architecture.md`, `docs/operations.md`, and `docs/setup.md` for more detail.

## Safety posture

This system is designed to be paper-first.

Important defaults and principles:

- paper trading remains the default operating mode
- live execution should stay tiny and supervised
- Telegram approval is required for supervised live spot entries
- kill-switches, reserve checks, and daily caps are expected to remain enabled
- symbol- and strategy-level promotion/demotion analytics gate execution
- BONK is intentionally observation-only for live spot execution in the current configuration

## Current supervised-live spot posture

Current live-eligible watchlist focus in this export:

- `PYTH`
- `RAY`
- `JUP`
- `WIF`

Current operational stance:

- `PYTH` is the primary supervised-live candidate path
- `BONK` is monitored but not live-tradable
- the stack uses narrow symbol-specific overrides instead of loosening the whole basket

## Dependencies

Runtime dependencies in this repo are intentionally minimal:

- Node.js
- Python 3
- `@solana/web3.js`
- external CLIs expected by the system:
  - Jupiter CLI (`jup`)
  - Helius CLI (`helius`)

The current code also expects a local environment with SQLite and access to Telegram Bot API credentials if Telegram workflows are enabled. Core entrypoints now use env-driven runtime config helpers so the project no longer depends on the original `/home/brimigs` layout for basic operation.

## Quick start

1. Install Node dependencies

```bash
npm install
```

2. Ensure required external tools are available on `PATH`

- `jup`
- `helius`

3. Optionally copy `config/spot-bot.example.json` to `config/local.json` and adjust paths / credentials

4. Create a writable trading data directory

Example expected runtime paths in the current code:

- `~/.trading-data/trading.db`
- `~/.trading-data/telegram-bridge/`

4. Initialize / sync the SQLite database

```bash
PYTHONPATH=. python trading_system/sync_trading_db.py --db ~/.trading-data/trading.db --data-dir ~/.trading-data
```

5. Run the monitor

```bash
node monitor.mjs --json
```

6. Run the spot executor in paper mode

```bash
node auto-trade.mjs
```

7. Run the spot executor in supervised live mode

```bash
node auto-trade.mjs --live
```

## Validation commands

```bash
node --check monitor.mjs
node --check auto-trade.mjs
python -m py_compile trading_system/trading_db.py trading_system/daily_analytics_report.py trading_system/whale_scanner.py trading_system/trading_db_cli.py
PYTHONPATH=. python -m unittest trading_system.tests.test_trading_db
PYTHONPATH=. python -m unittest trading_system.tests.test_accounting_audit
```

## What is intentionally not included

This export excludes local secrets and machine-specific runtime data, including:

- wallet keys
- Telegram bot tokens
- local SQLite DB contents
- `.trading-data/` runtime state
- cron job state
- local systemd/unit state

## Notes

This repository is an extracted application snapshot from an actively iterated trading environment. A portability pass added repo-local runtime config helpers for the main spot entrypoints, but some secondary helper scripts and test fixtures still reflect the source environment and may need a later cleanup.
