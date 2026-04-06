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
    parser = argparse.ArgumentParser(description='Assignee-specific spot review dashboard summary')
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--assignee', required=True)
    args = parser.parse_args()

    items = [item for item in load_items(Path(args.queue_file)) if item.get('status', 'open') == 'open' and (item.get('assignee') or '') == args.assignee]
    payload = {
        'assignee': args.assignee,
        'open_items': len(items),
        'needs_reassignment': sum(1 for item in items if item.get('needs_reassignment')),
        'sla_breached': sum(1 for item in items if item.get('sla_breached')),
    }
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    main()
