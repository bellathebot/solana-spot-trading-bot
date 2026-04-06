#!/usr/bin/env python3
import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from trading_system.runtime_config import BRIDGE_DIR, QUEUE_FILE, TELEGRAM_TOKEN_FILE, TELEGRAM_TRADE_CHAT_ID

STATE_FILE = BRIDGE_DIR / 'spot_manual_review_digest_state.json'
TOKEN_FILE = TELEGRAM_TOKEN_FILE
CHAT_ID = TELEGRAM_TRADE_CHAT_ID

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


def load_items(path: Path):
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


def shorten(text: str, limit: int = 3000) -> str:
    text = (text or '').strip()
    return text if len(text) <= limit else text[:limit] + ' ...[truncated]'


def telegram_send(message: str):
    token = TOKEN_FILE.read_text().strip()
    data = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': shorten(message)}).encode()
    req = urllib.request.urlopen(f'https://api.telegram.org/bot{token}/sendMessage', data=data, timeout=20)
    return json.loads(req.read().decode())


def render_digest(items):
    lines = ['Spot manual-review queue digest', '']
    lines.append(f'open_items: {len(items)}')
    by_type = {}
    by_severity = {}
    by_assignee = {}
    sla_breached_by_assignee = {}
    snoozed_count = 0
    unassigned_open = 0
    all_items = load_items(QUEUE_FILE)
    for item in all_items:
        if item.get('status') == 'snoozed':
            snoozed_count += 1
    for item in items:
        key = item.get('review_type', 'unknown')
        sev = item.get('severity', 'medium')
        assignee = item.get('assignee') or 'unassigned'
        by_type[key] = by_type.get(key, 0) + 1
        by_severity[sev] = by_severity.get(sev, 0) + 1
        by_assignee[assignee] = by_assignee.get(assignee, 0) + 1
        if item.get('sla_breached'):
            sla_breached_by_assignee[assignee] = sla_breached_by_assignee.get(assignee, 0) + 1
        if assignee == 'unassigned':
            unassigned_open += 1
    for key, count in sorted(by_type.items()):
        lines.append(f'- {key}: {count}')
    for sev, count in sorted(by_severity.items()):
        lines.append(f'- severity_{sev}: {count}')
    for assignee, count in sorted(by_assignee.items()):
        lines.append(f'- assignee_{assignee}: {count}')
    for assignee, count in sorted(sla_breached_by_assignee.items()):
        lines.append(f'- sla_breached_{assignee}: {count}')
    lines.append(f'- unassigned_open: {unassigned_open}')
    lines.append(f'- snoozed_items: {snoozed_count}')
    needs_reassignment = [item for item in items if item.get('needs_reassignment')]
    if by_assignee:
        busiest = max(by_assignee.items(), key=lambda kv: kv[1])
        lightest_assigned = min([(k, v) for k, v in by_assignee.items() if k != 'unassigned'], key=lambda kv: kv[1], default=None)
        if unassigned_open and lightest_assigned:
            lines.append(f'- reassign_suggestion: assign one unassigned item to {lightest_assigned[0]} (lowest open backlog among assigned operators)')
        elif unassigned_open and busiest[1] > 0:
            lines.append(f'- reassign_suggestion: assign one unassigned item to {busiest[0]} only if they have spare capacity; otherwise pick the lowest-load operator')
    if needs_reassignment:
        lines.append(f'- needs_reassignment_count: {len(needs_reassignment)}')
    if items:
        lines.append('')
        for item in items[:5]:
            lines.append(f"- #{item.get('queue_id')} {item.get('review_type')} decision={item.get('decision_id') or 'n/a'} symbol={item.get('symbol') or 'n/a'}")
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Send batched Telegram digests for spot manual-review queue items')
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    queue_path = Path(args.queue_file)
    items = [item for item in load_items(queue_path) if item.get('status', 'open') == 'open']
    state = load_json(STATE_FILE, {'last_open_count': 0, 'acked_at': None, 'mute_until': None})
    open_count = len(items)
    mute_until = state.get('mute_until')
    if open_count == 0:
        if not args.dry_run:
            save_json(STATE_FILE, {'last_open_count': 0, 'acked_at': state.get('acked_at'), 'mute_until': state.get('mute_until')})
        print(json.dumps({'status': 'NO_OPEN_ITEMS', 'open_count': 0}))
        return
    if mute_until:
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(mute_until):
                print(json.dumps({'status': 'MUTED', 'open_count': open_count, 'mute_until': mute_until}))
                return
        except Exception:
            pass
    if not args.dry_run and open_count == state.get('last_open_count', 0):
        print(json.dumps({'status': 'UNCHANGED', 'open_count': open_count}))
        return
    message = render_digest(items)
    if not args.dry_run:
        telegram_send(message)
        save_json(STATE_FILE, {'last_open_count': open_count, 'acked_at': state.get('acked_at'), 'mute_until': state.get('mute_until')})
    print(json.dumps({'status': 'DIGEST_SENT' if not args.dry_run else 'DRY_RUN', 'open_count': open_count}))


if __name__ == '__main__':
    main()
