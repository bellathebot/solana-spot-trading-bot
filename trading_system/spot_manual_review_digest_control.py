#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_system.runtime_config import BRIDGE_DIR

STATE_FILE = BRIDGE_DIR / 'spot_manual_review_digest_state.json'


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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description='Control spot manual review digest state')
    parser.add_argument('action', choices=['show', 'ack', 'mute'])
    parser.add_argument('--hours', type=int, default=24)
    args = parser.parse_args()

    state = load_json(STATE_FILE, {'last_open_count': 0, 'acked_at': None, 'mute_until': None})

    if args.action == 'show':
        print(json.dumps(state, indent=2))
        return
    if args.action == 'ack':
        state['acked_at'] = now_iso()
        save_json(STATE_FILE, state)
        print(json.dumps({'status': 'ACKED', 'acked_at': state['acked_at']}, indent=2))
        return
    state['mute_until'] = (datetime.now(timezone.utc) + timedelta(hours=args.hours)).isoformat()
    save_json(STATE_FILE, state)
    print(json.dumps({'status': 'MUTED', 'mute_until': state['mute_until']}, indent=2))


if __name__ == '__main__':
    main()
