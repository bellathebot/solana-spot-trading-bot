# Operations

## Expected local tools

The current code expects the following tools to be installed locally:

- Node.js / npm
- Python 3
- Jupiter CLI (`jup`)
- Helius CLI (`helius`)

## Expected runtime directories

The current code assumes a writable data area similar to:

- `~/.trading-data/`
- `~/.trading-data/trading.db`
- `~/.trading-data/telegram-bridge/`

## Common commands

### Market snapshot

```bash
node monitor.mjs --json
```

### Paper spot execution scan

```bash
node auto-trade.mjs
```

### Supervised live spot execution scan

```bash
node auto-trade.mjs --live
```

### Sync file-based runtime data into SQLite

```bash
PYTHONPATH=. python trading_system/sync_trading_db.py --db ~/.trading-data/trading.db --data-dir ~/.trading-data
```

### Daily analytics report

```bash
PYTHONPATH=. python trading_system/daily_analytics_report.py --db ~/.trading-data/trading.db
```

### Live candidate summary

```bash
PYTHONPATH=. python trading_system/spot_live_candidate_summary.py --db ~/.trading-data/trading.db
```

### Telegram reply bridge

```bash
python trading_system/telegram_trade_reply_bridge.py
```

### Spot Telegram notifier

```bash
PYTHONPATH=. python trading_system/spot_telegram_notifier.py --db ~/.trading-data/trading.db
```

## Validation

```bash
node --check monitor.mjs
node --check auto-trade.mjs
python -m py_compile trading_system/trading_db.py trading_system/daily_analytics_report.py trading_system/whale_scanner.py trading_system/trading_db_cli.py
PYTHONPATH=. python -m unittest trading_system.tests.test_trading_db
PYTHONPATH=. python -m unittest trading_system.tests.test_accounting_audit
```

## Safety notes

- Keep paper mode as the default mode
- Require operator approval for supervised live spot entries
- Preserve fee reserve / dry powder checks
- Do not bypass kill-switch logic casually
- Do not widen symbol overrides globally when a symbol-specific fix will do

## Current operator interpretation

At the time of export:

- BONK is observation-only for live spot
- PYTH is the primary narrow supervised-live candidate
- RAY can be blocked by demotion analytics
- JUP can be blocked by context / regime gating

## Portability note

The main spot-bot entrypoints now use shared runtime config helpers:

- `runtime-config.mjs`
- `trading_system/runtime_config.py`
- `scripts/bootstrap.sh`

Those helpers remove the original `/home/brimigs/...` dependency from the core exported spot workflow. Some secondary helper/report scripts and tests still reflect the source environment and can be cleaned up in a later pass.
