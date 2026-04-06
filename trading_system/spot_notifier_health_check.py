#!/usr/bin/env python3
import argparse
import json
import sqlite3
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
BRIDGE_DIR = DATA_DIR / 'telegram-bridge'
STATE_FILE = BRIDGE_DIR / 'spot_executor_alert_state.json'
HEALTH_STATE_FILE = BRIDGE_DIR / 'spot_notifier_health_state.json'
DEFAULT_DB = DATA_DIR / 'trading.db'
EVENT_TYPES = (
    'spot_trade_submit_timeout_ambiguous',
    'spot_trade_journal_stale_submitted',
    'spot_trade_external_partial_fill_detected',
    'spot_live_approval_requested',
    'kill_switch',
)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Check health of the spot recovery Telegram notifier')
    parser.add_argument('--db', default=str(DEFAULT_DB))
    parser.add_argument('--max-lag-events', type=int, default=3)
    args = parser.parse_args()

    state = load_json(STATE_FILE, {})
    last_seen = int(state.get('last_system_event_id', 0) or 0)
    conn = sqlite3.connect(str(Path(args.db)))
    try:
        placeholders = ','.join('?' for _ in EVENT_TYPES)
        max_relevant = conn.execute(
            f"SELECT COALESCE(MAX(id), 0) FROM system_events WHERE COALESCE(source, '')='auto-trade.mjs' AND event_type IN ({placeholders})",
            EVENT_TYPES,
        ).fetchone()[0]
    finally:
        conn.close()

    lag = max(0, int(max_relevant) - last_seen)
    status = 'healthy' if lag <= args.max_lag_events else 'degraded'
    payload = {
        'status': status,
        'last_system_event_id': last_seen,
        'max_relevant_event_id': int(max_relevant),
        'lag_events': lag,
        'max_lag_events': args.max_lag_events,
        'state_file': str(STATE_FILE),
        'health_state_file': str(HEALTH_STATE_FILE),
    }
    save_json(HEALTH_STATE_FILE, payload)
    print(json.dumps(payload))
    if status != 'healthy':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
