#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from trading_system.runtime_config import QUEUE_FILE


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
    parser = argparse.ArgumentParser(description='Summarize top stale assigned spot manual-review items')
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--limit', type=int, default=5)
    args = parser.parse_args()

    items = [item for item in load_items(Path(args.queue_file)) if item.get('status') == 'open' and item.get('assignee') and item.get('sla_breached')]
    items.sort(key=lambda item: (-float(item.get('age_hours') or 0), -int(item.get('priority_score') or 0)))
    payload = {'count': len(items), 'items': items[:args.limit]}
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    main()
