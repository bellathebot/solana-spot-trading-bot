#!/usr/bin/env python3
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path('/home/brimigs')
DATA_DIR = ROOT / '.trading-data'
BRIDGE_DIR = DATA_DIR / 'telegram-bridge'
PENDING_FILE = BRIDGE_DIR / 'pending_trade_alert.json'
SPOT_APPROVAL_FILE = BRIDGE_DIR / 'spot_live_approval.json'
SPOT_REVIEW_QUEUE_FILE = DATA_DIR / 'spot_recovery_manual_review.jsonl'
SPOT_DIGEST_STATE_FILE = BRIDGE_DIR / 'spot_manual_review_digest_state.json'
STATE_FILE = BRIDGE_DIR / 'reply_bridge_state.json'
EXEC_LOG = BRIDGE_DIR / 'execution_history.jsonl'
TOKEN_FILE = Path(os.environ.get('TELEGRAM_TRADE_TOKEN_FILE', str(ROOT / 'telegram.txt')))
CHAT_ID = os.environ.get('TELEGRAM_TRADE_CHAT_ID', '2116422114')
SESSIONS_INDEX = ROOT / '.hermes' / 'sessions' / 'sessions.json'
SESSIONS_DIR = ROOT / '.hermes' / 'sessions'
SESSION_KEY = f'agent:main:telegram:dm:{CHAT_ID}'


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def current_operator_label():
    return os.environ.get('SPOT_REVIEW_OPERATOR') or f'telegram:{CHAT_ID}'


def shorten(text: str, limit: int = 3000) -> str:
    text = (text or '').strip()
    return text if len(text) <= limit else text[:limit] + ' ...[truncated]'


def telegram_send(message: str):
    token = TOKEN_FILE.read_text().strip()
    data = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': shorten(message)}).encode()
    req = urllib.request.urlopen(f'https://api.telegram.org/bot{token}/sendMessage', data=data, timeout=20)
    return json.loads(req.read().decode())


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def current_session_file():
    idx = load_json(SESSIONS_INDEX, {})
    meta = idx.get(SESSION_KEY)
    if not meta:
        return None
    session_id = meta.get('session_id')
    if not session_id:
        return None
    path = SESSIONS_DIR / f'session_{session_id}.json'
    return path if path.exists() else None


def poll_telegram_updates(state):
    try:
        token = TOKEN_FILE.read_text().strip()
        query = {'timeout': 0}
        if state.get('last_update_id') is not None:
            query['offset'] = int(state['last_update_id']) + 1
        url = f"https://api.telegram.org/bot{token}/getUpdates?{urllib.parse.urlencode(query)}"
        payload = json.loads(urllib.request.urlopen(url, timeout=20).read().decode())
        results = payload.get('result') or []
        messages = []
        max_update_id = state.get('last_update_id')
        for item in results:
            update_id = item.get('update_id')
            if update_id is not None:
                max_update_id = max(update_id, max_update_id or update_id)
            msg = item.get('message') or {}
            chat = msg.get('chat') or {}
            if str(chat.get('id')) != str(CHAT_ID):
                continue
            text = (msg.get('text') or '').strip()
            if text:
                messages.append(text)
        if max_update_id is not None:
            state['last_update_id'] = max_update_id
        return messages, state
    except Exception:
        return [], state


def extract_new_user_messages():
    state = load_json(STATE_FILE, {'last_session_path': None, 'last_user_count': 0, 'last_processed_command': None, 'last_update_id': None})
    session_path = current_session_file()
    session_messages = []
    if session_path:
        session = load_json(session_path, {})
        user_messages = [m.get('content', '').strip() for m in session.get('messages', []) if m.get('role') == 'user']
        last_count = state['last_user_count'] if state.get('last_session_path') == str(session_path) else 0
        session_messages = user_messages[last_count:]
        state['last_session_path'] = str(session_path)
        state['last_user_count'] = len(user_messages)
    update_messages, state = poll_telegram_updates(state)
    merged = []
    for text in [*session_messages, *update_messages]:
        if text and text not in merged:
            merged.append(text)
    save_json(STATE_FILE, state)
    return merged, state


def parse_command(text: str):
    text = (text or '').strip()
    if not text:
        return None
    m = re.fullmatch(r'(?i)SKIP\s+([A-Za-z0-9_:\-]+)', text)
    if m:
        return {'action': 'skip', 'alert_id': m.group(1)}
    m = re.fullmatch(r'(?i)EXECUTE\s+([A-Za-z0-9_:\-]+)\s+(TOP|1|2)', text)
    if m:
        choice = m.group(2).upper()
        return {'action': 'execute', 'alert_id': m.group(1), 'choice': '1' if choice == 'TOP' else choice}
    m = re.fullmatch(r'(?i)APPROVE_SPOT\s+([A-Za-z0-9_:\-]+)', text)
    if m:
        return {'action': 'approve_spot', 'approval_id': m.group(1)}
    m = re.fullmatch(r'(?i)REJECT_SPOT\s+([A-Za-z0-9_:\-]+)', text)
    if m:
        return {'action': 'reject_spot', 'approval_id': m.group(1)}
    if text.upper() == 'YES':
        return {'action': 'approve_spot_simple'}
    if text.upper() == 'NO':
        return {'action': 'reject_spot_simple'}
    m = re.fullmatch(r'(?i)RESOLVE_SPOT_REVIEW\s+([0-9]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'resolve_spot_review', 'queue_id': int(m.group(1)), 'note': (m.group(2) or '').strip()}
    m = re.fullmatch(r'(?i)SNOOZE_SPOT_REVIEW\s+([0-9]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'snooze_spot_review', 'queue_id': int(m.group(1)), 'note': (m.group(2) or '').strip()}
    m = re.fullmatch(r'(?i)REOPEN_SPOT_REVIEW\s+([0-9]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'reopen_spot_review', 'queue_id': int(m.group(1)), 'note': (m.group(2) or '').strip()}
    m = re.fullmatch(r'(?i)CLAIM_SPOT_REVIEW\s+([0-9]+)(?:\s+([A-Za-z0-9_\-:]+))?(?:\s+(.+))?', text)
    if m:
        return {'action': 'claim_spot_review', 'queue_id': int(m.group(1)), 'assignee': (m.group(2) or None), 'note': (m.group(3) or '').strip()}
    m = re.fullmatch(r'(?i)UNASSIGN_SPOT_REVIEW\s+([0-9]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'unassign_spot_review', 'queue_id': int(m.group(1)), 'note': (m.group(2) or '').strip()}
    m = re.fullmatch(r'(?i)LIST_SPOT_REVIEWS(?:\s+(CRITICAL|HIGH|MEDIUM|LOW))?', text)
    if m:
        return {'action': 'list_spot_reviews', 'severity': (m.group(1) or '').lower() or None}
    m = re.fullmatch(r'(?i)MY_SPOT_REVIEWS(?:\s+([A-Za-z0-9_\-]+))?', text)
    if m:
        return {'action': 'my_spot_reviews', 'assignee': (m.group(1) or None)}
    m = re.fullmatch(r'(?i)ASSIGNED_SPOT_REVIEWS\s+([A-Za-z0-9_\-:]+)', text)
    if m:
        return {'action': 'assigned_spot_reviews', 'assignee': m.group(1)}
    if text.upper() == 'UNASSIGNED_SPOT_REVIEWS':
        return {'action': 'unassigned_spot_reviews'}
    if text.upper() == 'NEEDS_REASSIGNMENT_SPOT_REVIEWS':
        return {'action': 'needs_reassignment_spot_reviews'}
    if text.upper() == 'STALE_ASSIGNED_SPOT_REVIEWS':
        return {'action': 'stale_assigned_spot_reviews'}
    m = re.fullmatch(r'(?i)SHOW_SPOT_REVIEW\s+([0-9]+)', text)
    if m:
        return {'action': 'show_spot_review', 'queue_id': int(m.group(1))}
    m = re.fullmatch(r'(?i)ASSIGN_SPOT_REVIEW\s+([0-9]+)\s+([A-Za-z0-9_\-]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'assign_spot_review', 'queue_id': int(m.group(1)), 'assignee': m.group(2), 'note': (m.group(3) or '').strip()}
    m = re.fullmatch(r'(?i)BULK_ASSIGN_SPOT_REVIEWS\s+(CRITICAL|HIGH|MEDIUM|LOW)\s+([A-Za-z0-9_\-]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'bulk_assign_spot_reviews', 'severity': m.group(1).lower(), 'assignee': m.group(2), 'note': (m.group(3) or '').strip()}
    m = re.fullmatch(r'(?i)BULK_SNOOZE_SPOT_REVIEWS\s+([A-Za-z0-9_\-:]+)(?:\s+(.+))?', text)
    if m:
        return {'action': 'bulk_snooze_spot_reviews', 'assignee': m.group(1), 'note': (m.group(2) or '').strip()}
    if text.upper() == 'ACK_SPOT_DIGEST':
        return {'action': 'ack_spot_digest'}
    m = re.fullmatch(r'(?i)MUTE_SPOT_DIGEST(?:\s+([0-9]+))?', text)
    if m:
        return {'action': 'mute_spot_digest', 'hours': int(m.group(1) or 24)}
    return None


def load_pending():
    if not PENDING_FILE.exists():
        return None
    return json.loads(PENDING_FILE.read_text())


def approval_expired(approval: dict) -> bool:
    expires = approval.get('expires_at')
    if not expires:
        return False
    try:
        return datetime.fromisoformat(expires) <= datetime.now(timezone.utc)
    except Exception:
        return False


def load_spot_approval():
    approval = load_json(SPOT_APPROVAL_FILE, {'version': 1, 'status': 'idle'})
    if approval.get('status') == 'pending' and approval_expired(approval):
        approval['status'] = 'expired'
        approval['expired_at'] = now_iso()
        save_json(SPOT_APPROVAL_FILE, approval)
    return approval


def save_spot_approval(payload):
    save_json(SPOT_APPROVAL_FILE, payload)


def load_spot_review_items():
    if not SPOT_REVIEW_QUEUE_FILE.exists():
        return []
    items = []
    for idx, line in enumerate(SPOT_REVIEW_QUEUE_FILE.read_text().splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
        except Exception:
            continue
        if isinstance(item, dict):
            item.setdefault('queue_id', idx)
            item.setdefault('status', 'open')
            items.append(item)
    return items


def save_spot_review_items(items):
    lines = []
    for idx, item in enumerate(items, start=1):
        payload = dict(item)
        payload['queue_id'] = idx
        lines.append(json.dumps(payload, sort_keys=True))
    SPOT_REVIEW_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    SPOT_REVIEW_QUEUE_FILE.write_text('\n'.join(lines) + ('\n' if lines else ''))


def pending_valid(pending):
    if not pending:
        return False
    if pending.get('status') != 'pending':
        return False
    expires = pending.get('expires_at')
    if not expires:
        return True
    return datetime.fromisoformat(expires) > datetime.now(timezone.utc)


def append_exec_log(entry):
    BRIDGE_DIR.mkdir(parents=True, exist_ok=True)
    with EXEC_LOG.open('a') as f:
        f.write(json.dumps(entry) + '\n')


def execute_command(command: str):
    env = {**os.environ, 'PATH': f"/home/brimigs/.hermes/node/bin:/home/brimigs/.cargo/bin:{os.environ.get('PATH','')}"}
    res = subprocess.run(command, shell=True, text=True, capture_output=True, env=env, timeout=180)
    return {
        'exit_code': res.returncode,
        'stdout': (res.stdout or '').strip(),
        'stderr': (res.stderr or '').strip(),
    }


def _review_priority(item: dict) -> tuple:
    severity_rank = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    return (
        severity_rank.get(item.get('severity', 'medium'), 9),
        -int(item.get('priority_score', 0) or 0),
        str(item.get('queued_at', '')),
    )


def handle(parsed):
    if parsed['action'] in {'ack_spot_digest', 'mute_spot_digest'}:
        digest_state = load_json(SPOT_DIGEST_STATE_FILE, {'last_open_count': 0, 'acked_at': None, 'mute_until': None})
        if parsed['action'] == 'ack_spot_digest':
            digest_state['acked_at'] = now_iso()
            save_json(SPOT_DIGEST_STATE_FILE, digest_state)
            telegram_send(f"Acknowledged spot manual-review digest at {digest_state['acked_at']}")
        else:
            mute_until = (datetime.now(timezone.utc) + timedelta(hours=parsed.get('hours', 24))).isoformat()
            digest_state['mute_until'] = mute_until
            save_json(SPOT_DIGEST_STATE_FILE, digest_state)
            telegram_send(f"Muted spot manual-review digest until {mute_until}")
        return

    if parsed['action'] in {'resolve_spot_review', 'snooze_spot_review', 'reopen_spot_review', 'claim_spot_review', 'unassign_spot_review', 'list_spot_reviews', 'my_spot_reviews', 'assigned_spot_reviews', 'unassigned_spot_reviews', 'needs_reassignment_spot_reviews', 'stale_assigned_spot_reviews', 'show_spot_review', 'assign_spot_review', 'bulk_assign_spot_reviews', 'bulk_snooze_spot_reviews'}:
        items = load_spot_review_items()
        if parsed['action'] in {'list_spot_reviews', 'my_spot_reviews', 'assigned_spot_reviews', 'unassigned_spot_reviews', 'needs_reassignment_spot_reviews', 'stale_assigned_spot_reviews'}:
            open_items = sorted([item for item in items if item.get('status', 'open') == 'open'], key=_review_priority)
            if parsed['action'] == 'list_spot_reviews' and parsed.get('severity'):
                open_items = [item for item in open_items if item.get('severity', 'medium') == parsed['severity']]
            if parsed['action'] == 'my_spot_reviews':
                assignee = parsed.get('assignee') or current_operator_label()
                open_items = [item for item in open_items if (item.get('assignee') or '') == assignee]
            if parsed['action'] == 'assigned_spot_reviews':
                open_items = [item for item in open_items if (item.get('assignee') or '') == parsed.get('assignee')]
            if parsed['action'] == 'unassigned_spot_reviews':
                open_items = [item for item in open_items if not item.get('assignee')]
            if parsed['action'] == 'needs_reassignment_spot_reviews':
                open_items = [item for item in open_items if item.get('needs_reassignment')]
            if parsed['action'] == 'stale_assigned_spot_reviews':
                open_items = [item for item in open_items if item.get('assignee') and item.get('sla_breached')]
            if not open_items:
                telegram_send('No matching open spot manual-review items right now.')
                return
            if parsed['action'] == 'my_spot_reviews':
                title = f"Spot reviews assigned to {parsed.get('assignee') or current_operator_label()}:"
            elif parsed['action'] == 'assigned_spot_reviews':
                title = f"Spot reviews assigned to {parsed.get('assignee')}:"
            elif parsed['action'] == 'unassigned_spot_reviews':
                title = 'Unassigned spot manual-review items:'
            elif parsed['action'] == 'needs_reassignment_spot_reviews':
                title = 'Spot manual-review items needing reassignment:'
            elif parsed['action'] == 'stale_assigned_spot_reviews':
                title = 'Top stale assigned spot manual-review items:'
            else:
                title = 'Open spot manual-review items:' if not parsed.get('severity') else f"Open {parsed['severity']} spot manual-review items:"
            lines = [title]
            for item in open_items[:8]:
                lines.append(f"- #{item.get('queue_id')} [{item.get('severity', 'medium')}] {item.get('review_type')} decision={item.get('decision_id') or 'n/a'} symbol={item.get('symbol') or 'n/a'} assignee={item.get('assignee') or 'unassigned'}")
            telegram_send('\n'.join(lines))
            return
        if parsed['action'] == 'show_spot_review':
            for item in items:
                if item.get('queue_id') == parsed['queue_id']:
                    lines = [
                        f"Spot review #{item.get('queue_id')}",
                        f"status: {item.get('status', 'open')}",
                        f"severity: {item.get('severity', 'medium')}",
                        f"priority_score: {item.get('priority_score', 'n/a')}",
                        f"type: {item.get('review_type', 'unknown')}",
                        f"decision: {item.get('decision_id') or 'n/a'}",
                        f"symbol: {item.get('symbol') or 'n/a'}",
                        f"signature: {item.get('signature') or 'n/a'}",
                        f"owner: {item.get('owner') or 'n/a'}",
                        f"assignee: {item.get('assignee') or 'unassigned'}",
                        f"entry_key: {item.get('entry_key') or 'n/a'}",
                        f"related_event_type: {item.get('related_event_type') or 'n/a'}",
                        f"journal_event_id: {item.get('journal_event_id') or 'n/a'}",
                        f"journal_row_id: {item.get('journal_row_id') or 'n/a'}",
                        f"needs_reassignment: {item.get('needs_reassignment', False)}",
                    ]
                    history = item.get('history') or []
                    for entry in history[-5:]:
                        lines.append(f"history: {entry.get('ts')} {entry.get('action')} actor={entry.get('actor')} note={entry.get('note')}")
                    telegram_send('\n'.join(lines))
                    return
            telegram_send(f"Spot manual-review item #{parsed['queue_id']} was not found.")
            return
        if parsed['action'] in {'bulk_assign_spot_reviews', 'bulk_snooze_spot_reviews'}:
            changed = 0
            for item in items:
                if item.get('status', 'open') != 'open':
                    continue
                if parsed['action'] == 'bulk_assign_spot_reviews' and item.get('severity', 'medium') != parsed.get('severity'):
                    continue
                if parsed['action'] == 'bulk_snooze_spot_reviews' and (item.get('assignee') or '') != parsed.get('assignee'):
                    continue
                if parsed['action'] == 'bulk_assign_spot_reviews':
                    item['assignee'] = parsed.get('assignee')
                    item['assigned_at'] = now_iso()
                    if parsed.get('note'):
                        item['assignment_note'] = parsed['note']
                    action_word = f"Bulk assigned to {parsed.get('assignee')}"
                else:
                    item['status'] = 'snoozed'
                    item['snoozed_at'] = now_iso()
                    if parsed.get('note'):
                        item['snooze_note'] = parsed['note']
                    action_word = f"Bulk snoozed reviews for {parsed.get('assignee')}"
                changed += 1
            save_spot_review_items(items)
            telegram_send(f"{action_word}. Updated {changed} spot manual-review items.")
            return

        for item in items:
            if item.get('queue_id') == parsed['queue_id']:
                if parsed['action'] == 'resolve_spot_review':
                    item['status'] = 'resolved'
                    item['resolved_at'] = now_iso()
                    if parsed.get('note'):
                        item['resolution_note'] = parsed['note']
                    action_word = 'Resolved'
                elif parsed['action'] == 'snooze_spot_review':
                    item['status'] = 'snoozed'
                    item['snoozed_at'] = now_iso()
                    if parsed.get('note'):
                        item['snooze_note'] = parsed['note']
                    action_word = 'Snoozed'
                elif parsed['action'] == 'assign_spot_review':
                    item['assignee'] = parsed.get('assignee')
                    item['assigned_at'] = now_iso()
                    if parsed.get('note'):
                        item['assignment_note'] = parsed['note']
                    action_word = f"Assigned to {parsed.get('assignee')}"
                elif parsed['action'] == 'claim_spot_review':
                    assignee = parsed.get('assignee') or current_operator_label()
                    item['assignee'] = assignee
                    item['claimed_at'] = now_iso()
                    if parsed.get('note'):
                        item['claim_note'] = parsed['note']
                    action_word = f"Claimed by {assignee}"
                elif parsed['action'] == 'unassign_spot_review':
                    item['assignee'] = None
                    item['unassigned_at'] = now_iso()
                    if parsed.get('note'):
                        item['unassign_note'] = parsed['note']
                    action_word = 'Unassigned'
                else:
                    item['status'] = 'open'
                    item['reopened_at'] = now_iso()
                    if parsed.get('note'):
                        item['reopen_note'] = parsed['note']
                    action_word = 'Reopened'
                save_spot_review_items(items)
                telegram_send(f"{action_word} spot manual-review item #{parsed['queue_id']}.\n{item.get('review_type')} decision={item.get('decision_id') or 'n/a'}")
                return
        telegram_send(f"Spot manual-review item #{parsed['queue_id']} was not found.")
        return

    if parsed['action'] in {'approve_spot', 'reject_spot', 'approve_spot_simple', 'reject_spot_simple'}:
        approval = load_spot_approval()
        if approval.get('status') != 'pending':
            telegram_send('No pending spot live approval request right now.')
            return
        if parsed['action'] in {'approve_spot', 'reject_spot'} and parsed['approval_id'] != approval.get('approval_id'):
            telegram_send(f"Spot approval ID mismatch. Current pending approval is {approval.get('approval_id')}")
            return
        approval['status'] = 'approved' if parsed['action'] in {'approve_spot', 'approve_spot_simple'} else 'rejected'
        approval['resolved_at'] = now_iso()
        approval['approved_at' if approval['status'] == 'approved' else 'rejected_at'] = now_iso()
        save_spot_approval(approval)
        telegram_send(f"Spot live approval {approval['approval_id']} {approval['status']}.\n{approval.get('symbol')} {approval.get('signal_type')} size=${approval.get('size_usd')}")
        return

    pending = load_pending()
    if not pending_valid(pending):
        telegram_send('No pending executable trade alert right now.')
        return
    if parsed.get('alert_id') and parsed['alert_id'] != pending.get('alert_id'):
        telegram_send(f"Alert ID mismatch. Current pending alert is {pending.get('alert_id')}")
        return

    if parsed['action'] == 'skip':
        pending['status'] = 'skipped'
        pending['resolved_at'] = now_iso()
        save_json(PENDING_FILE, pending)
        append_exec_log({'ts': now_iso(), 'action': 'skip', 'alert_id': pending['alert_id']})
        telegram_send(f"Skipped alert {pending['alert_id']}.")
        return

    choice = int(parsed.get('choice', '1'))
    idx = choice - 1
    candidates = pending.get('candidates', [])
    if idx < 0 or idx >= len(candidates):
        telegram_send('That alert does not have that many candidates.')
        return
    candidate = candidates[idx]
    result = execute_command(candidate['command'])
    pending['status'] = 'executed' if result['exit_code'] == 0 else 'execution_failed'
    pending['resolved_at'] = now_iso()
    pending['executed_candidate'] = candidate
    pending['execution_result'] = result
    save_json(PENDING_FILE, pending)
    append_exec_log({'ts': now_iso(), 'action': 'execute', 'alert_id': pending['alert_id'], 'candidate': candidate, 'result': result})

    if result['exit_code'] == 0:
        msg = f"Executed {candidate['instrument']} {candidate['side'].upper()} from alert {pending['alert_id']}."
        if result['stdout']:
            msg += f"\n\nResult:\n{result['stdout'][:3000]}"
        telegram_send(msg)
    else:
        telegram_send(f"Execution failed for alert {pending['alert_id']} candidate {choice}.\n\nSTDERR:\n{result['stderr'][:3000] or '(none)'}")


def main():
    messages, state = extract_new_user_messages()
    acted = False
    for text in messages:
        parsed = parse_command(text)
        if parsed:
            state['last_processed_command'] = text
            save_json(STATE_FILE, state)
            handle(parsed)
            acted = True
    if not acted:
        print('NO_COMMAND')
    else:
        print('COMMAND_PROCESSED')


if __name__ == '__main__':
    main()
