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


def candidate_visibility_fields(symbol: str, row: dict, blocker_type: str, blocker_reason: str) -> dict:
    price = float(row.get('price') or 0)
    buy = float(row.get('buyBelow') or 0)
    liquidity = float(row.get('liquidity') or 0)
    threshold_gap = max(0.0, price - buy) if buy > 0 else 0.0
    next_price_move_pct = max(0.0, ((price - buy) / price) * 100) if price > 0 and buy > 0 else 0.0
    secondary_blocker = 'none'
    if blocker_type == 'none' and buy > 0 and price > buy:
        blocker_type = 'price_not_at_threshold'
        blocker_reason = f'Needs ${threshold_gap:.6f} lower price to reach buy trigger'
    if blocker_type == 'none' and liquidity < min_liquidity_threshold_usd(symbol):
        secondary_blocker = 'liquidity'
    elif blocker_type == 'price_not_at_threshold':
        secondary_blocker = 'none'
    return {
        'next_trigger_price': buy,
        'distance_to_buy_pct': next_price_move_pct,
        'next_price_move_pct': next_price_move_pct,
        'price_gap_abs': threshold_gap,
        'top_blocker': blocker_type,
        'secondary_blocker': secondary_blocker,
        'blocker_explanation': blocker_reason,
        'live_eligible': bool(row.get('liveEligible', False)),
        'paper_eligible': bool(row.get('paperEligible', False)),
    }


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
        visibility = candidate_visibility_fields(symbol, row, blocker_type, blocker_reason)
        rows.append({
            'symbol': symbol,
            'price': float(row.get('price') or 0),
            'buy_below': float(row.get('buyBelow') or 0),
            'sell_above': float(row.get('sellAbove') or 0),
            'liquidity': float(row.get('liquidity') or 0),
            'status': status,
            'distance_pct': distance,
            'blocker': visibility['blocker_explanation'],
            'blocker_type': visibility['top_blocker'],
            **visibility,
        })

    rank = {'buy_ready': 0, 'near_buy': 1, 'watch': 2}
    rows.sort(key=lambda r: (rank.get(r['status'], 9), r['distance_pct'], -r['liquidity']))

    promotion_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker'] == 'no_recent_blocker']
    demotion_overridden_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker_type'] == 'demotion_overridden']
    context_cleared_but_demotion_blocked = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker_type'] == 'demotion']
    blocked_candidates = [row for row in rows if row['status'] in {'buy_ready', 'near_buy'} and row['blocker'] != 'no_recent_blocker' and row['blocker_type'] != 'demotion_overridden']

    family_competition = {}
    for row in payload.get('scored_signals', []):
        symbol = row.get('symbol')
        if symbol not in LIVE_SYMBOLS:
            continue
        family_competition.setdefault(symbol, []).append({
            'strategy_tag': row.get('strategy_tag') or ('pullback_continuation_tier1' if row.get('signal_type') == 'pullback_buy' else 'mean_reversion_near_buy'),
            'signal_type': row.get('signal_type'),
            'score': row.get('score'),
            'regime_tag': row.get('regime_tag'),
            'regime_detail': row.get('regime_detail'),
        })
    strategy_family_competition = []
    for symbol, lanes in sorted(family_competition.items()):
        ranked_lanes = sorted(lanes, key=lambda lane: float(lane.get('score') or 0), reverse=True)
        top_lane = ranked_lanes[0] if ranked_lanes else None
        runner_up = ranked_lanes[1] if len(ranked_lanes) > 1 else None
        strategy_family_competition.append({
            'symbol': symbol,
            'top_strategy': top_lane.get('strategy_tag') if top_lane else None,
            'top_signal_type': top_lane.get('signal_type') if top_lane else None,
            'top_score': top_lane.get('score') if top_lane else None,
            'runner_up_strategy': runner_up.get('strategy_tag') if runner_up else None,
            'runner_up_signal_type': runner_up.get('signal_type') if runner_up else None,
            'runner_up_score': runner_up.get('score') if runner_up else None,
            'lanes': ranked_lanes,
        })

    summary_payload = {
        'summary_ts': payload.get('ts') or now_iso(),
        'top_candidate': rows[0] if rows else None,
        'ranked_candidates': rows,
        'promotion_candidates': promotion_candidates,
        'demotion_overridden_candidates': demotion_overridden_candidates,
        'context_cleared_but_demotion_blocked': context_cleared_but_demotion_blocked,
        'blocked_candidates': blocked_candidates,
        'strategy_family_competition': strategy_family_competition,
    }
    if not args.no_persist:
        persist_summary(Path(args.db), rows, summary_payload)

    lines = ['Hourly live spot candidate summary']
    if not rows:
        lines.append('- no live candidate data')
        print('\n'.join(lines))
        return

    top = rows[0]
    lines.append(
        f"- top candidate: {top['symbol']} status={top['status']} price=${top['price']:.6f} buy<${top['buy_below']:.6f} "
        f"move_to_trigger={top['next_price_move_pct']:.2f}% blocker_type={top['blocker_type']} blocker={top['blocker']}"
    )
    for idx, row in enumerate(rows[:3], start=1):
        lines.append(
            f"- #{idx} {row['symbol']} status={row['status']} dist={row['distance_pct']:.2f}% move_to_trigger={row['next_price_move_pct']:.2f}% "
            f"liq=${row['liquidity']:.0f} live={row['live_eligible']} paper={row['paper_eligible']} "
            f"blocker_type={row['blocker_type']} blocker={row['blocker']}"
        )
    promo_text = ', '.join(f"{row['symbol']}({row['status']})" for row in promotion_candidates[:3]) or 'none'
    demotion_blocked_text = ', '.join(f"{row['symbol']}({row['blocker']})" for row in context_cleared_but_demotion_blocked[:3]) or 'none'
    demotion_overridden_text = ', '.join(f"{row['symbol']}({row['status']})" for row in demotion_overridden_candidates[:3]) or 'none'
    blocked_text = ', '.join(f"{row['symbol']}({row['blocker_type']}:{row['blocker']})" for row in blocked_candidates[:3]) or 'none'
    lines.append(f"- promotion_candidates: {promo_text}")
    lines.append(f"- context_cleared_but_demotion_blocked: {demotion_blocked_text}")
    lines.append(f"- demotion_overridden_candidates: {demotion_overridden_text}")
    lines.append(f"- blocked_candidates: {blocked_text}")
    family_text = ', '.join(
        f"{row['symbol']}({row.get('top_strategy')}:{row.get('top_score')} vs {row.get('runner_up_strategy')}:{row.get('runner_up_score')})"
        for row in strategy_family_competition[:3]
    ) or 'none'
    lines.append(f"- strategy_family_competition: {family_text}")
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
