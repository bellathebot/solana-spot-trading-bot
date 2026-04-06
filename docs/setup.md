# Setup

This repository now includes lightweight portability helpers so it can run outside the original source machine layout.

## Portability model

Main entrypoints no longer require `/home/brimigs/...` hard-coded paths.

The main spot bot now reads runtime paths from environment variables with sensible defaults:

- `HERMES_SPOT_BOT_HOME`
- `AUTO_TRADER_DATA_DIR`
- `AUTO_TRADER_DB_PATH`
- `AUTO_TRADER_DB_CLI`
- `AUTO_TRADER_TRADING_MD`
- `JUP_BIN`
- `HELIUS_BIN`
- `TELEGRAM_TOKEN_FILE`
- `TELEGRAM_TRADE_CHAT_ID`
- `SPOT_MONITOR_PATH`
- `SPOT_BOT_REPO_ROOT`

## New config helpers

- `runtime-config.mjs`
  - used by the main Node spot entrypoints
- `trading_system/runtime_config.py`
  - used by the main Python spot operator scripts
- `config/spot-bot.example.json`
  - example JSON config file for file-based configuration

If you prefer a config file over environment variables, copy the example file to `config/local.json` or point `SPOT_BOT_CONFIG_FILE` at your own JSON file.

## Quick bootstrap

```bash
./scripts/bootstrap.sh
```

## Example manual setup

```bash
export HERMES_SPOT_BOT_HOME="$HOME"
export AUTO_TRADER_DATA_DIR="$HOME/.trading-data"
export AUTO_TRADER_DB_PATH="$HOME/.trading-data/trading.db"
export JUP_BIN="jup"
export HELIUS_BIN="helius"
export TELEGRAM_TOKEN_FILE="$PWD/telegram.txt"
export TELEGRAM_TRADE_CHAT_ID="123456789"
```

Then initialize the DB:

```bash
PYTHONPATH="$PWD" python trading_system/sync_trading_db.py --db "$AUTO_TRADER_DB_PATH" --data-dir "$AUTO_TRADER_DATA_DIR"
```

Run the main entrypoints:

```bash
node monitor.mjs --json
node auto-trade.mjs
node auto-trade.mjs --live
```

## What was updated in this cleanup pass

The portability pass specifically updated the most important spot-bot entrypoints:

- `monitor.mjs`
- `auto-trade.mjs`
- `trading_system/spot_live_candidate_summary.py`
- `trading_system/daily_analytics_report.py`
- `trading_system/sync_trading_db.py`
- `trading_system/trading_db_cli.py`
- `trading_system/telegram_trade_alert_bot.py`
- `trading_system/telegram_trade_reply_bridge.py`
- `trading_system/spot_telegram_notifier.py`
- `trading_system/spot_notifier_health_check.py`
- `trading_system/spot_trading_runtime_health_check.py`
- `trading_system/whale_scanner.py`

## Remaining portability debt

Some secondary helper/report scripts and test fixtures still reflect the original operating environment. They are safe to keep in the repo, but a later cleanup can continue migrating them to the shared config helpers.
