#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
BRIDGE_DIR = DATA_DIR / 'telegram-bridge'
STATE_FILE = BRIDGE_DIR / 'spot_executor_alert_state.json'
TOKEN_FILE = ROOT / '.telegram' / 'telegram.txt'
CHAT_ID = os.environ.get('TELEGRAM_TRADE_CHAT_ID', '2116422114')
DEFAULT_DB = DATA_DIR / 'trading.db'

SYSTEM_EVENT_TYPES = {
    'spot_trade_submit_timeout_ambiguous',
    'spot_trade_journal_stale_submitted',
    'spot_trade_external_partial_fill_detected',
    'spot_live_approval_requested',
    'kill_switch',
}


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


def shorten(value, limit=3000):
    text = '' if value is None else str(value)
    return text if len(text) <= limit else text[:limit] + ' ...[truncated]'


def telegram_send(message: str):
    token = TOKEN_FILE.read_text().strip()
    data = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': shorten(message)}).encode()
    req = urllib.request.urlopen(f'https://api.telegram.org/bot{token}/sendMessage', data=data, timeout=20)
    return json.loads(req.read().decode())


def parse_json(text: str | None):
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def load_state():
    return load_json(STATE_FILE, {'last_system_event_id': 0})


def fetch_pending_events(db_path: Path, state: dict):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            '''
            SELECT id, ts, event_type, severity, message, metadata_json
            FROM system_events
            WHERE id > ?
              AND event_type IN ({})
              AND COALESCE(source, '') = 'auto-trade.mjs'
            ORDER BY id ASC
            '''.format(','.join('?' for _ in SYSTEM_EVENT_TYPES)),
            [state.get('last_system_event_id', 0), *sorted(SYSTEM_EVENT_TYPES)],
        ).fetchall()
        max_id = conn.execute('SELECT COALESCE(MAX(id), 0) FROM system_events').fetchone()[0]
        return list(rows), max_id
    finally:
        conn.close()


def format_message(row: sqlite3.Row, metadata: dict) -> str:
    event_type = row['event_type']
    if event_type == 'spot_trade_submit_timeout_ambiguous':
        return '\n'.join([
            'Spot recovery anomaly: ambiguous submit',
            f"time: {row['ts']}",
            f"symbol: {metadata.get('symbol') or 'n/a'}",
            f"side: {metadata.get('side') or 'n/a'}",
            f"decision: {metadata.get('decision_id') or 'n/a'}",
            f"message: {shorten(row['message'], 500)}",
        ])
    if event_type == 'spot_trade_journal_stale_submitted':
        return '\n'.join([
            'Spot recovery anomaly: stale submitted trade',
            f"time: {row['ts']}",
            f"symbol: {metadata.get('symbol') or 'n/a'}",
            f"decision: {metadata.get('decision_id') or 'n/a'}",
            f"signature: {metadata.get('signature') or 'n/a'}",
            f"submitted_at: {metadata.get('submitted_at') or 'n/a'}",
        ])
    if event_type == 'spot_trade_external_partial_fill_detected':
        return '\n'.join([
            'Spot recovery anomaly: external partial fill detected',
            f"time: {row['ts']}",
            f"symbol: {metadata.get('symbol') or 'n/a'}",
            f"decision: {metadata.get('decision_id') or 'n/a'}",
            f"signature: {metadata.get('signature') or 'n/a'}",
            f"observed_out: {metadata.get('observed_out_amount') or 'n/a'}",
            f"expected_out: {metadata.get('expected_out_amount') or 'n/a'}",
        ])
    if event_type == 'spot_live_approval_requested':
        intent = metadata.get('approval_intent') or {}
        return '\n'.join([
            'Spot live approval required',
            f"time: {row['ts']}",
            f"symbol: {intent.get('symbol') or 'n/a'}",
            f"signal: {intent.get('signal_type') or 'n/a'}",
            f"decision: {intent.get('decision_id') or 'n/a'}",
            f"size_usd: {intent.get('size_usd') if intent.get('size_usd') is not None else 'n/a'}",
            'Reply YES to approve or NO to reject.',
            f"Approve: {intent.get('commands', {}).get('approve', 'n/a')}",
            f"Reject: {intent.get('commands', {}).get('reject', 'n/a')}",
        ])
    return '\n'.join([
        'Spot trader kill switch activated',
        f"time: {row['ts']}",
        f"severity: {row['severity']}",
        f"message: {shorten(row['message'], 500)}",
    ])


def prime_state(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        max_id = conn.execute('SELECT COALESCE(MAX(id), 0) FROM system_events').fetchone()[0]
    finally:
        conn.close()
    state = {'last_system_event_id': max_id}
    save_json(STATE_FILE, state)
    return state


def main():
    parser = argparse.ArgumentParser(description='Send Telegram alerts for spot recovery anomalies')
    parser.add_argument('--db', default=str(DEFAULT_DB))
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--prime-only', action='store_true')
    args = parser.parse_args()

    db_path = Path(args.db)
    if args.prime_only or not STATE_FILE.exists():
        state = prime_state(db_path)
        if args.prime_only:
            print(json.dumps({'status': 'PRIMED', **state}))
            return

    state = load_state()
    rows, max_id = fetch_pending_events(db_path, state)
    sent = []
    for row in rows:
        metadata = parse_json(row['metadata_json'])
        message = format_message(row, metadata)
        if args.dry_run:
            sent.append({'id': row['id'], 'message': message})
        else:
            telegram_send(message)
            sent.append({'id': row['id'], 'message': message})

    save_json(STATE_FILE, {'last_system_event_id': max_id})
    print(json.dumps({'status': 'OK', 'sent': sent, 'last_system_event_id': max_id}))


if __name__ == '__main__':
    main()
