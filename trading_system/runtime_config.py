from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(os.environ.get('SPOT_BOT_REPO_ROOT', Path(__file__).resolve().parents[1]))
BOT_HOME = Path(os.environ.get('HERMES_SPOT_BOT_HOME', os.environ.get('HOME', str(REPO_ROOT))))
DATA_DIR = Path(os.environ.get('AUTO_TRADER_DATA_DIR', str(BOT_HOME / '.trading-data')))
DB_PATH = Path(os.environ.get('AUTO_TRADER_DB_PATH', str(DATA_DIR / 'trading.db')))
BRIDGE_DIR = DATA_DIR / 'telegram-bridge'
QUEUE_FILE = DATA_DIR / 'spot_recovery_manual_review.jsonl'
TELEGRAM_TOKEN_FILE = Path(os.environ.get('TELEGRAM_TOKEN_FILE', str(REPO_ROOT / 'telegram.txt')))
TELEGRAM_TRADE_CHAT_ID = os.environ.get('TELEGRAM_TRADE_CHAT_ID', '2116422114')
JUP_BIN = os.environ.get('JUP_BIN', 'jup')
HELIUS_BIN = os.environ.get('HELIUS_BIN', 'helius')
MONITOR_PATH = Path(os.environ.get('SPOT_MONITOR_PATH', str(REPO_ROOT / 'monitor.mjs')))
SESSIONS_INDEX = REPO_ROOT / '.hermes' / 'sessions' / 'sessions.json'
SESSIONS_DIR = REPO_ROOT / '.hermes' / 'sessions'


def build_path_env() -> str:
    extras = []
    home = os.environ.get('HOME')
    if os.environ.get('JUP_BIN_DIR'):
        extras.append(os.environ['JUP_BIN_DIR'])
    if os.environ.get('HELIUS_BIN_DIR'):
        extras.append(os.environ['HELIUS_BIN_DIR'])
    if home:
        extras.append(str(Path(home) / '.hermes' / 'node' / 'bin'))
        extras.append(str(Path(home) / '.cargo' / 'bin'))
    extras = [p for p in extras if p]
    return ':'.join(extras + [os.environ.get('PATH', '')]) if extras else os.environ.get('PATH', '')
