#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_system.runtime_config import QUEUE_FILE
SNOOZE_EXPIRE_HOURS = 24
SLA_BREACH_HOURS = 12
PRIORITY_RANK = {
    'critical': 0,
    'high': 1,
    'medium': 2,
    'low': 3,
}
REVIEW_PRIORITY = {
    'external_partial_fill': ('high', 90),
    'stale_submitted': ('critical', 100),
}


def enrich_item(item):
    if not isinstance(item, dict):
        return item
    severity, score = REVIEW_PRIORITY.get(item.get('review_type', 'unknown'), ('medium', 50))
    item.setdefault('severity', severity)
    item.setdefault('priority_score', score)
    item.setdefault('owner', 'operator')
    item.setdefault('assignee', None)
    queued_at = item.get('queued_at')
    age_hours = None
    if queued_at:
        try:
            age_hours = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(queued_at)).total_seconds() / 3600.0)
        except Exception:
            age_hours = None
    item['age_hours'] = round(age_hours, 2) if age_hours is not None else None
    item['sla_breached'] = bool(age_hours is not None and age_hours >= SLA_BREACH_HOURS and item.get('status', 'open') == 'open')
    item['needs_reassignment'] = bool(item.get('status', 'open') == 'open' and item.get('assignee') and item.get('sla_breached'))
    return item


def sort_items(items):
    return sorted(
        items,
        key=lambda item: (
            PRIORITY_RANK.get(item.get('severity', 'medium'), 9),
            -int(item.get('priority_score', 0) or 0),
            str(item.get('queued_at', '')),
        )
    )


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
                items.append(enrich_item(item))
        except Exception:
            continue
    return items


def append_history(item, action, note=None, actor=None):
    history = item.setdefault('history', [])
    history.append({
        'ts': datetime.now(timezone.utc).isoformat(),
        'action': action,
        'actor': actor or 'system',
        'note': note or '',
    })


def save_items(path: Path, items):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for idx, item in enumerate(items, start=1):
        payload = dict(item)
        payload['queue_id'] = idx
        lines.append(json.dumps(payload, sort_keys=True))
    path.write_text('\n'.join(lines) + ('\n' if lines else ''))


def expire_snoozed_items(items, hours=SNOOZE_EXPIRE_HOURS):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    changed = 0
    for item in items:
        if item.get('status') != 'snoozed':
            continue
        raw = item.get('snoozed_at')
        if not raw or raw == 'manual':
            continue
        try:
            snoozed_at = datetime.fromisoformat(raw)
        except Exception:
            continue
        if snoozed_at <= cutoff:
            item['status'] = 'open'
            item['reopened_at'] = datetime.now(timezone.utc).isoformat()
            item['auto_reopen_reason'] = 'snooze_expired'
            append_history(item, 'auto_reopen', 'snooze_expired')
            changed += 1
    return changed


def main():
    parser = argparse.ArgumentParser(description='Inspect and resolve spot recovery manual-review queue items')
    parser.add_argument('action', choices=['list', 'resolve', 'summary', 'snooze', 'reopen', 'expire-snoozed', 'assign', 'claim', 'unassign', 'bulk-assign', 'bulk-snooze'])
    parser.add_argument('--queue-file', default=str(QUEUE_FILE))
    parser.add_argument('--status', default='open')
    parser.add_argument('--queue-id', type=int)
    parser.add_argument('--decision-id')
    parser.add_argument('--resolution', default='resolved')
    parser.add_argument('--note', default='')
    parser.add_argument('--hours', type=int, default=SNOOZE_EXPIRE_HOURS)
    parser.add_argument('--assignee')
    parser.add_argument('--severity')
    args = parser.parse_args()

    queue_path = Path(args.queue_file)
    items = load_items(queue_path)

    if args.action == 'list':
        filtered = sort_items([item for item in items if args.status in {'all', item.get('status', 'open')}])
        print(json.dumps({'count': len(filtered), 'items': filtered}, indent=2))
        return

    if args.action == 'summary':
        filtered = sort_items([item for item in items if args.status in {'all', item.get('status', 'open')}])
        by_type = {}
        by_severity = {}
        by_assignee = {}
        sla_breached_by_assignee = {}
        snoozed_count = sum(1 for item in items if item.get('status') == 'snoozed')
        sla_breached_count = 0
        for item in filtered:
            key = item.get('review_type', 'unknown')
            sev = item.get('severity', 'medium')
            assignee = item.get('assignee') or 'unassigned'
            by_type[key] = by_type.get(key, 0) + 1
            by_severity[sev] = by_severity.get(sev, 0) + 1
            by_assignee[assignee] = by_assignee.get(assignee, 0) + 1
            if item.get('sla_breached'):
                sla_breached_count += 1
                sla_breached_by_assignee[assignee] = sla_breached_by_assignee.get(assignee, 0) + 1
        print(json.dumps({'count': len(filtered), 'by_type': by_type, 'by_severity': by_severity, 'by_assignee': by_assignee, 'snoozed_count': snoozed_count, 'sla_breached_count': sla_breached_count, 'sla_breached_by_assignee': sla_breached_by_assignee}, indent=2))
        return

    if args.action == 'expire-snoozed':
        changed = expire_snoozed_items(items, hours=args.hours)
        save_items(queue_path, items)
        print(json.dumps({'reopened': changed, 'hours': args.hours, 'queue_file': str(queue_path)}, indent=2))
        return

    if args.action in {'bulk-assign', 'bulk-snooze'}:
        changed = 0
        for item in items:
            if item.get('status', 'open') != 'open':
                continue
            if args.action == 'bulk-assign' and args.severity and item.get('severity', 'medium') != args.severity:
                continue
            if args.action == 'bulk-snooze' and (item.get('assignee') or '') != (args.assignee or ''):
                continue
            if args.action == 'bulk-assign':
                item['assignee'] = args.assignee
                item['assigned_at'] = 'manual'
                if args.note:
                    item['assignment_note'] = args.note
                append_history(item, f"bulk_assign:{args.assignee}", args.note, 'operator')
            else:
                item['status'] = 'snoozed'
                item['snoozed_at'] = 'manual'
                if args.note:
                    item['snooze_note'] = args.note
                append_history(item, 'bulk_snooze', args.note, 'operator')
            changed += 1
        save_items(queue_path, items)
        print(json.dumps({'updated': changed, 'queue_file': str(queue_path)}, indent=2))
        return

    changed = 0
    for item in items:
        if args.queue_id is not None and item.get('queue_id') != args.queue_id:
            continue
        if args.decision_id and item.get('decision_id') != args.decision_id:
            continue
        if args.action == 'snooze':
            item['status'] = 'snoozed'
            item['snoozed_at'] = 'manual'
            if args.note:
                item['snooze_note'] = args.note
            append_history(item, 'snooze', args.note, 'operator')
        elif args.action == 'reopen':
            item['status'] = 'open'
            item['reopened_at'] = 'manual'
            if args.note:
                item['reopen_note'] = args.note
            append_history(item, 'reopen', args.note, 'operator')
        elif args.action == 'assign':
            item['assignee'] = args.assignee
            item['assigned_at'] = 'manual'
            if args.note:
                item['assignment_note'] = args.note
            append_history(item, f"assign:{args.assignee}", args.note, 'operator')
        elif args.action == 'claim':
            assignee = args.assignee or 'operator'
            item['assignee'] = assignee
            item['claimed_at'] = 'manual'
            if args.note:
                item['claim_note'] = args.note
            append_history(item, f"claim:{assignee}", args.note, 'operator')
        elif args.action == 'unassign':
            item['assignee'] = None
            item['unassigned_at'] = 'manual'
            if args.note:
                item['unassign_note'] = args.note
            append_history(item, 'unassign', args.note, 'operator')
        else:
            item['status'] = args.resolution
            if args.note:
                item['resolution_note'] = args.note
            append_history(item, f"resolve:{args.resolution}", args.note, 'operator')
        changed += 1
    save_items(queue_path, items)
    print(json.dumps({'updated': changed, 'queue_file': str(queue_path)}, indent=2))


if __name__ == '__main__':
    main()
