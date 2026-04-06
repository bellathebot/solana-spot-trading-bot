#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
QUEUE_FILE = DATA_DIR / 'spot_recovery_manual_review.jsonl'


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
                queued_at = item.get('queued_at')
                if queued_at and item.get('sla_breached') is None:
                    try:
                        age_hours = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(queued_at)).total_seconds() / 3600.0)
                        item['sla_breached'] = age_hours >= 12 and item.get('status', 'open') == 'open'
                    except Exception:
                        item['sla_breached'] = False
                items.append(item)
        except Exception:
            continue
    return items


def main():
    parser = argparse.ArgumentParser(description='Alert on spot manual-review queue thresholds')
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--max-unassigned-open', type=int, default=2)
    parser.add_argument('--max-sla-breached', type=int, default=1)
    parser.add_argument('--max-sla-breached-per-assignee', type=int, default=1)
    args = parser.parse_args()

    items = [item for item in load_items(Path(args.queue_file)) if item.get('status', 'open') == 'open']
    unassigned_open = sum(1 for item in items if not item.get('assignee'))
    sla_breached = sum(1 for item in items if item.get('sla_breached'))
    sla_breached_by_assignee = {}
    for item in items:
        if item.get('sla_breached') and item.get('assignee'):
            key = item.get('assignee')
            sla_breached_by_assignee[key] = sla_breached_by_assignee.get(key, 0) + 1
    status = 'healthy'
    blockers = []
    if unassigned_open > args.max_unassigned_open:
        status = 'degraded'
        blockers.append('unassigned_open_threshold')
    if sla_breached > args.max_sla_breached:
        status = 'degraded'
        blockers.append('sla_breached_threshold')
    if any(count > args.max_sla_breached_per_assignee for count in sla_breached_by_assignee.values()):
        status = 'degraded'
        blockers.append('sla_breached_per_assignee_threshold')
    payload = {
        'status': status,
        'open_count': len(items),
        'unassigned_open': unassigned_open,
        'sla_breached_open': sla_breached,
        'max_unassigned_open': args.max_unassigned_open,
        'max_sla_breached': args.max_sla_breached,
        'max_sla_breached_per_assignee': args.max_sla_breached_per_assignee,
        'sla_breached_by_assignee': sla_breached_by_assignee,
        'blockers': blockers,
    }
    print(json.dumps(payload))
    if status != 'healthy':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
