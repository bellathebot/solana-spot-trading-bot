#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
QUEUE_FILE = DATA_DIR / 'spot_recovery_manual_review.jsonl'
HEALTH_FILE = DATA_DIR / 'telegram-bridge' / 'spot_notifier_health_state.json'


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


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


def main():
    parser = argparse.ArgumentParser(description='Compact dashboard summary for spot review/operator workload health')
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--health-file', default=str(HEALTH_FILE))
    args = parser.parse_args()

    items = [item for item in load_items(Path(args.queue_file)) if item.get('status', 'open') == 'open']
    notifier = load_json(Path(args.health_file), {})
    unassigned = sum(1 for item in items if not item.get('assignee'))
    needs_reassignment = sum(1 for item in items if item.get('needs_reassignment'))
    assignee_counts = {}
    for item in items:
        key = item.get('assignee') or 'unassigned'
        assignee_counts[key] = assignee_counts.get(key, 0) + 1
    payload = {
        'open_items': len(items),
        'unassigned_open': unassigned,
        'needs_reassignment': needs_reassignment,
        'assignee_backlog': assignee_counts,
        'notifier_status': notifier.get('status', 'unknown'),
        'notifier_lag_events': notifier.get('lag_events', 'n/a'),
    }
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    main()
