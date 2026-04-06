#!/usr/bin/env python3
import argparse
import json
import sqlite3
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
DB_PATH = DATA_DIR / 'trading.db'
QUEUE_FILE = DATA_DIR / 'spot_recovery_manual_review.jsonl'
NOTIFIER_HEALTH_FILE = DATA_DIR / 'telegram-bridge' / 'spot_notifier_health_state.json'


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def load_queue_items(path: Path):
    if not path.exists():
        return []
    items = []
    for idx, line in enumerate(path.read_text().splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
            if isinstance(item, dict):
                item.setdefault('queue_id', idx)
                item.setdefault('status', 'open')
                items.append(item)
        except Exception:
            continue
    return items


def get_recent_spot_events(db_path: Path, limit: int = 5):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts, event_type, severity, message
            FROM system_events
            WHERE COALESCE(source, '')='auto-trade.mjs'
              AND event_type IN ('spot_trade_submit_timeout_ambiguous','spot_trade_journal_stale_submitted','spot_trade_external_partial_fill_detected','spot_live_approval_requested','kill_switch')
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Compact local dashboard for spot recovery health')
    parser.add_argument('--db', default=str(DB_PATH))
    args = parser.parse_args()

    queue_items = [item for item in load_queue_items(QUEUE_FILE) if item.get('status', 'open') == 'open']
    by_type = {}
    for item in queue_items:
        key = item.get('review_type', 'unknown')
        by_type[key] = by_type.get(key, 0) + 1
    notifier = load_json(NOTIFIER_HEALTH_FILE, {})
    events = get_recent_spot_events(Path(args.db), limit=5)
    payload = {
        'manual_review_open_count': len(queue_items),
        'manual_review_by_type': by_type,
        'notifier_health': notifier,
        'recent_events': events,
    }
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    main()
