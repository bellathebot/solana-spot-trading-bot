#!/usr/bin/env python3
import argparse
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from trading_system.runtime_config import BRIDGE_DIR, DB_PATH, MONITOR_PATH, build_path_env

SPOT_APPROVAL_FILE = BRIDGE_DIR / 'spot_live_approval.json'
LIVE_SYMBOLS = ['SOL', 'BTC', 'JUP', 'PYTH', 'RAY', 'WIF']
MIN_LIQUIDITY_USD = 100000
SYMBOL_MIN_LIQUIDITY_USD = {
    'PYTH': 50000,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_monitor_json() -> dict:
    res = subprocess.run(
        ['node', str(MONITOR_PATH), '--json'],
        capture_output=True,
        text=True,
        timeout=180,
        env={'PATH': build_path_env(), **__import__('os').environ},
    )
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or res.stdout.strip() or 'monitor failed')
    return json.loads(res.stdout)


def min_liquidity_threshold_usd(symbol: str) -> float:
    return float(SYMBOL_MIN_LIQUIDITY_USD.get(symbol, MIN_LIQUIDITY_USD))


def approval_expired(approval: dict) -> bool:
    expires_at = approval.get('expires_at')
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) <= datetime.now(timezone.utc)
    except Exception:
        return False


def current_pending_spot_approval_symbol() -> str | None:
    if not SPOT_APPROVAL_FILE.exists():
        return None
    try:
        approval = json.loads(SPOT_APPROVAL_FILE.read_text())
        if not isinstance(approval, dict):
            return None
    except Exception:
        return None
    if approval.get('status') != 'pending' or approval_expired(approval):
        return None
    symbol = approval.get('symbol')
    return str(symbol) if symbol else None


def status_and_distance(row: dict):
    price = float(row.get('price') or 0)
    buy = float(row.get('buyBelow') or 0)
    if buy <= 0:
        return 'watch', 999.0
    distance = ((price - buy) / buy) * 100
    if price <= buy:
        return 'buy_ready', abs(distance)
    if distance <= 5:
        return 'near_buy', distance
    return 'watch', distance


def recent_blockers(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT ts, event_type, message, metadata_json FROM system_events WHERE source='auto-trade.mjs' ORDER BY id DESC LIMIT 250"
        ).fetchall()
    finally:
        conn.close()
    per_symbol = {}
    for row in rows:
        try:
            meta = json.loads(row['metadata_json'] or '{}')
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}
        symbol = meta.get('symbol')
        if not symbol or symbol in per_symbol:
            continue
        event_type = row['event_type'] or ''
        pilot = meta.get('pilotContext') or {}
        reason = pilot.get('reason') or meta.get('reason') or row['message'] or event_type
        if event_type in {
            'tiny_live_pilot_mixed_regime',
            'tiny_live_pilot_regime_blocked',
            'spot_live_approval_required',
            'small_live_entry_queued',
            'execution_score_skip',
            'entry_score_skip',
            'live_promotion_gate_skip',
            'live_symbol_ineligible_skip',
            'symbol_demoted_skip',
            'tiny_live_symbol_demotion_override',
            'tiny_live_strategy_demotion_override',
        }:
            blocker_type = 'other'
            if event_type in {'tiny_live_pilot_mixed_regime', 'tiny_live_pilot_regime_blocked'}:
                blocker_type = 'choppy_or_context'
            elif event_type == 'symbol_demoted_skip':
                blocker_type = 'demotion'
            elif event_type == 'spot_live_approval_required':
                blocker_type = 'awaiting_approval'
            elif event_type == 'small_live_entry_queued':
                blocker_type = 'size_too_small'
            elif event_type in {'execution_score_skip', 'entry_score_skip'}:
                blocker_type = 'score_gate'
            elif event_type in {'live_promotion_gate_skip', 'live_symbol_ineligible_skip'}:
                blocker_type = 'policy_gate'
            elif event_type in {'tiny_live_symbol_demotion_override', 'tiny_live_strategy_demotion_override'}:
                blocker_type = 'demotion_overridden'
            per_symbol[symbol] = {'event_type': event_type, 'reason': str(reason), 'blocker_type': blocker_type}
    return per_symbol


def persist_summary(db_path: Path, rows: list[dict], payload: dict) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO system_events(ts, event_type, severity, message, source, metadata_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                now_iso(),
                'spot_live_candidate_summary',
                'info',
                'Hourly live spot candidate summary persisted',
                'spot_live_candidate_summary.py',
                json.dumps(payload, sort_keys=True),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Summarize top live spot candidates and blockers')
    parser.add_argument('--db', default=str(DB_PATH))
    parser.add_argument('--no-persist', action='store_true')
    args = parser.parse_args()

    payload = run_monitor_json()
    prices = payload.get('snapshot', {}).get('prices', {})
    blockers = recent_blockers(Path(args.db))
    pending_approval_symbol = current_pending_spot_approval_symbol()

    rows = []
    for symbol in LIVE_SYMBOLS:
        row = prices.get(symbol)
        if not row:
            continue
        status, distance = status_and_distance(row)
        blocker = blockers.get(symbol)
        if float(row.get('liquidity') or 0) < min_liquidity_threshold_usd(symbol):
            blocker_reason = 'liquidity_below_threshold'
            blocker_type = 'liquidity'
        elif blocker:
            blocker_reason = blocker['reason']
            blocker_type = blocker.get('blocker_type', 'other')
            if blocker_type == 'awaiting_approval':
                if pending_approval_symbol == symbol:
                    blocker_type = 'approval_pending'
                else:
                    blocker_reason = 'no_recent_blocker'
                    blocker_type = 'none'
        else:
            blocker_reason = 'no_recent_blocker'
            blocker_type = 'none'
        rows.append({
            'symbol': symbol,
            'price': float(row.get('price') or 0),
            'buy_below': float(row.get('buyBelow') or 0),
            'sell_above': float(row.get('sellAbove') or 0),
            'liquidity': float(row.get('liquidity') or 0),
            'status': status,
            'distance_pct': distance,
            'blocker': blocker_reason,
            'blocker_type': blocker_type,
        })

    rank = {'buy_ready': 0, 'near_buy': 1, 'watch': 2}
    rows.sort(key=lambda r: (rank.get(r['status'], 9), r['distance_pct'], -r['liquidity']))

    promotion_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker'] == 'no_recent_blocker']
    demotion_overridden_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker_type'] == 'demotion_overridden']
    context_cleared_but_demotion_blocked = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker_type'] == 'demotion']
    blocked_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker'] != 'no_recent_blocker' and row['blocker_type'] != 'demotion_overridden']

    summary_payload = {
        'summary_ts': payload.get('ts') or now_iso(),
        'top_candidate': rows[0] if rows else None,
        'ranked_candidates': rows,
        'promotion_candidates': promotion_candidates,
        'demotion_overridden_candidates': demotion_overridden_candidates,
        'context_cleared_but_demotion_blocked': context_cleared_but_demotion_blocked,
        'blocked_candidates': blocked_candidates,
    }
    if not args.no_persist:
        persist_summary(Path(args.db), rows, summary_payload)

    lines = ['Hourly live spot candidate summary']
    if not rows:
        lines.append('- no live candidate data')
        print('\n'.join(lines))
        return

    top = rows[0]
    lines.append(f"- top candidate: {top['symbol']} status={top['status']} price=${top['price']:.6f} buy<${top['buy_below']:.6f} blocker_type={top['blocker_type']} blocker={top['blocker']}")
    for idx, row in enumerate(rows[:3], start=1):
        lines.append(f"- #{idx} {row['symbol']} status={row['status']} dist={row['distance_pct']:.2f}% liq=${row['liquidity']:.0f} blocker_type={row['blocker_type']} blocker={row['blocker']}")
    promo_text = ', '.join(f"{row['symbol']}({row['status']})" for row in promotion_candidates[:3]) or 'none'
    demotion_blocked_text = ', '.join(f"{row['symbol']}({row['blocker']})" for row in context_cleared_but_demotion_blocked[:3]) or 'none'
    demotion_overridden_text = ', '.join(f"{row['symbol']}({row['status']})" for row in demotion_overridden_candidates[:3]) or 'none'
    blocked_text = ', '.join(f"{row['symbol']}({row['blocker_type']}:{row['blocker']})" for row in blocked_candidates[:3]) or 'none'
    lines.append(f"- promotion_candidates: {promo_text}")
    lines.append(f"- context_cleared_but_demotion_blocked: {demotion_blocked_text}")
    lines.append(f"- demotion_overridden_candidates: {demotion_overridden_text}")
    lines.append(f"- blocked_candidates: {blocked_text}")
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
