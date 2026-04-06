#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AUTO_TRADER_DATA_DIR:-$HOME/.trading-data}"
DB_PATH="${AUTO_TRADER_DB_PATH:-$DATA_DIR/trading.db}"

mkdir -p "$DATA_DIR/telegram-bridge"

echo "Repo root: $ROOT"
echo "Data dir:   $DATA_DIR"
echo "DB path:    $DB_PATH"

echo "Installing Node dependencies..."
npm install

echo
echo "Bootstrap complete. Suggested exports:"
cat <<EOF
export HERMES_SPOT_BOT_HOME="${HERMES_SPOT_BOT_HOME:-$HOME}"
export AUTO_TRADER_DATA_DIR="$DATA_DIR"
export AUTO_TRADER_DB_PATH="$DB_PATH"
export JUP_BIN="${JUP_BIN:-jup}"
export HELIUS_BIN="${HELIUS_BIN:-helius}"
export TELEGRAM_TOKEN_FILE="${TELEGRAM_TOKEN_FILE:-$ROOT/telegram.txt}"
EOF

echo
echo "Next suggested command:"
echo "  PYTHONPATH=\"$ROOT\" python \"$ROOT/trading_system/sync_trading_db.py\" --db \"$DB_PATH\" --data-dir \"$DATA_DIR\""
