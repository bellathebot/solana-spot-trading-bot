#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from trading_system.runtime_config import DATA_DIR, DB_PATH
SPOT_NOTIFIER_HEALTH_STATE = DATA_DIR / 'telegram-bridge' / 'spot_notifier_health_state.json'
SPOT_MANUAL_REVIEW_QUEUE = DATA_DIR / 'spot_recovery_manual_review.jsonl'
LIVE_TRADABLE_ASSETS = ['SOL', 'BTC', 'JUP', 'PYTH', 'RAY', 'WIF']


from trading_system.trading_db import get_daily_analytics


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _load_jsonl(path: Path):
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


def _display_money(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _latest_target_points(db_path: Path, symbols: list[str]) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f'''
            SELECT pp.symbol, pp.price, pp.change24h, pp.liquidity, pp.alert_below, pp.buy_below, pp.sell_above,
                   pp.holding_amount, pp.holding_value, ps.ts
            FROM price_points pp
            JOIN price_snapshots ps ON ps.id = pp.snapshot_id
            WHERE pp.snapshot_id = (SELECT id FROM price_snapshots ORDER BY ts DESC LIMIT 1)
              AND pp.symbol IN ({','.join('?' for _ in symbols)})
            ORDER BY CASE pp.symbol {' '.join(f"WHEN ? THEN {idx}" for idx, _ in enumerate(symbols))} ELSE 999 END
            ''',
            [*symbols, *symbols],
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _live_asset_status(row: dict) -> tuple[str, float]:
    price = _display_money(row.get('price'))
    buy_below = _display_money(row.get('buy_below'))
    if buy_below <= 0:
        return 'watch', 999.0
    distance_pct = ((price - buy_below) / buy_below) * 100
    if price <= buy_below:
        return 'buy_ready', abs(distance_pct)
    if distance_pct <= 5:
        return 'near_buy', distance_pct
    return 'watch', distance_pct


def _rank_live_assets(rows: list[dict]) -> list[dict]:
    status_rank = {'buy_ready': 0, 'near_buy': 1, 'watch': 2}
    ranked = []
    for row in rows:
        status, distance_pct = _live_asset_status(row)
        ranked.append({**row, 'live_status': status, 'distance_to_buy_pct': distance_pct})
    ranked.sort(key=lambda row: (status_rank.get(row['live_status'], 9), row['distance_to_buy_pct'], -_display_money(row.get('liquidity'))))
    return ranked


def _latest_live_candidate_summary(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT ts, metadata_json FROM system_events WHERE event_type = 'spot_live_candidate_summary' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {}
        payload = json.loads(row['metadata_json'] or '{}')
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Print daily trading analytics from SQLite')
    parser.add_argument('--db', default=str(DB_PATH))
    parser.add_argument('--since-ts', default=None)
    args = parser.parse_args()

    report = get_daily_analytics(Path(args.db), args.since_ts)

    print('Daily trading analytics')
    print(f"Since: {report['since_ts']}")
    latest = report.get('latest_snapshot')
    if latest:
        print(f"Latest snapshot: {latest['ts']} | total=${latest['wallet_total_usd']:.2f} | SOL={latest['wallet_sol']:.4f} | USDC={latest['wallet_usdc']:.2f}")
        print(f"Unrealized PnL: ${report['portfolio_pnl']['unrealized_pnl_usd']:.2f}")
    else:
        print('Latest snapshot: none')

    print('\nMonitored spot targets:')
    target_points = _latest_target_points(Path(args.db), LIVE_TRADABLE_ASSETS)
    if target_points:
        for row in target_points:
            print(
                f"- {row['symbol']}: price=${_display_money(row['price']):.6f} 24h={_display_money(row['change24h']):.2f}% "
                f"alert<${_display_money(row['alert_below']):.6f} buy<${_display_money(row['buy_below']):.6f} "
                f"sell>${_display_money(row['sell_above']):.6f} liq=${_display_money(row['liquidity']):.2f} holding=${_display_money(row['holding_value']):.2f}"
            )
    else:
        print('- none')

    print('\nLive tradable assets:')
    ranked_live_assets = _rank_live_assets(target_points)
    if ranked_live_assets:
        for row in ranked_live_assets:
            print(f"- {row['symbol']}: status={row['live_status']} live_tradable=yes buy<${_display_money(row['buy_below']):.6f} sell>${_display_money(row['sell_above']):.6f} liq=${_display_money(row['liquidity']):.2f}")
    else:
        print('- none')

    print('\nRanked live spot candidates:')
    if ranked_live_assets:
        for idx, row in enumerate(ranked_live_assets[:5], start=1):
            print(f"- #{idx} {row['symbol']}: status={row['live_status']} distance_to_buy={row['distance_to_buy_pct']:.2f}% liq=${_display_money(row['liquidity']):.2f}")
    else:
        print('- none')

    latest_live_summary = _latest_live_candidate_summary(Path(args.db))
    print('\nPromotion candidates vs blocked candidates:')
    promotion_candidates = latest_live_summary.get('promotion_candidates') or []
    demotion_blocked = latest_live_summary.get('context_cleared_but_demotion_blocked') or []
    demotion_overridden = latest_live_summary.get('demotion_overridden_candidates') or []
    blocked_candidates = latest_live_summary.get('blocked_candidates') or []
    if promotion_candidates:
        for row in promotion_candidates[:5]:
            print(f"- promotion {row['symbol']}: status={row.get('status')} blocker_type={row.get('blocker_type', 'none')} blocker={row.get('blocker')} dist={_display_money(row.get('distance_pct')):.2f}%")
    else:
        print('- promotion: none')
    if demotion_blocked:
        for row in demotion_blocked[:5]:
            print(f"- context-cleared/demotion-blocked {row['symbol']}: status={row.get('status')} blocker={row.get('blocker')} dist={_display_money(row.get('distance_pct')):.2f}%")
    else:
        print('- context-cleared/demotion-blocked: none')
    if demotion_overridden:
        for row in demotion_overridden[:5]:
            print(f"- demotion-overridden {row['symbol']}: status={row.get('status')} blocker_type={row.get('blocker_type')} blocker={row.get('blocker')} dist={_display_money(row.get('distance_pct')):.2f}%")
    else:
        print('- demotion-overridden: none')
    if blocked_candidates:
        for row in blocked_candidates[:5]:
            print(f"- blocked {row['symbol']}: status={row.get('status')} blocker_type={row.get('blocker_type', 'other')} blocker={row.get('blocker')} dist={_display_money(row.get('distance_pct')):.2f}%")
    else:
        print('- blocked: none')

    print('\nTrade performance by mode:')
    for mode in ['paper', 'live', 'unknown']:
        stats = report['trade_performance'].get(mode)
        if not stats:
            continue
        print(f"- {mode}: trades={stats['trade_count']} notional=${stats['notional_usd']:.2f} simulated={stats['simulated_count']} realized_pnl=${stats['realized_pnl_usd']:.2f}")

    print('\nTrusted trade summary:')
    trusted = report.get('trusted_trade_summary', {})
    print(f"- trusted_trades={trusted.get('trusted_trade_count', 0)} trusted_notional=${trusted.get('trusted_notional_usd', 0):.2f} trusted_realized_pnl=${trusted.get('trusted_realized_pnl_usd', 0):.2f} untrusted_trades={trusted.get('untrusted_trade_count', 0)}")
    if trusted.get('untrusted_rows'):
        for row in trusted['untrusted_rows'][:3]:
            print(f"- untrusted {row['symbol']} {row['side']} [{row['mode']}] reason={row.get('trust_reason')}")
    else:
        print('- no untrusted trade rows detected')

    print('\nStrategy breakdown:')
    if report['strategy_breakdown']:
        for row in report['strategy_breakdown']:
            print(f"- {row['strategy_tag']}: trades={row['trade_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nRanked-paper lane summary:')
    family_rows = report.get('strategy_family_breakdown') or []
    ranked_family = next((row for row in family_rows if row['strategy_family'] == 'ranked_paper'), None)
    if ranked_family:
        print(f"- ranked_paper: trades={ranked_family['trade_count']} notional=${ranked_family['notional_usd']:.2f} realized_pnl=${ranked_family['realized_pnl_usd']:.2f}")
    else:
        print('- ranked_paper: no executed trades yet')

    print('\nStrategy family breakdown:')
    if family_rows:
        for row in family_rows:
            print(f"- {row['strategy_family']}: trades={row['trade_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nStrategy health:')
    if report['strategy_health']:
        for row in report['strategy_health']:
            print(f"- {row['strategy_tag']}: avg_pnl=${row['avg_pnl_per_trade_usd']:.2f} health={row['health_score']:.2f}")
    else:
        print('- none')

    print('\nSymbol breakdown:')
    if report['symbol_breakdown']:
        for row in report['symbol_breakdown']:
            print(f"- {row['symbol']}: trades={row['trade_count']} wins={row['win_count']} losses={row['loss_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nOrder-flow signals:')
    if report['order_flow_signals']:
        for row in report['order_flow_signals']:
            print(f"- {row['symbol']}: wallets={row['wallet_count']} tx_activity={row['tx_activity']} self_fee={row['self_fee_activity']} pressure={row['pressure']}")
    else:
        print('- none')

    print('\nExecution quality:')
    if report['execution_quality']:
        for row in report['execution_quality']:
            print(f"- {row['mode']}: avg_slippage_bps={row['avg_slippage_bps']:.2f} max_slippage_bps={row['max_slippage_bps']:.2f} run_over_rate={row['run_over_rate']:.2f} observations={row['post_trade_drift_observations']}")
    else:
        print('- none')

    print('\nFill quality summary:')
    fill_quality = report.get('fill_quality_summary', {})
    if fill_quality.get('spot'):
        for row in fill_quality['spot']:
            print(f"- spot {row['mode']}: trades={row['trade_count']} avg_slippage_bps={row['avg_slippage_bps']:.2f} max_slippage_bps={row['max_slippage_bps']:.2f} avg_quote_impact_pct={row['avg_quote_impact_pct']:.4f}")
    else:
        print('- spot: none')
    if fill_quality.get('perps'):
        for row in fill_quality['perps']:
            print(f"- perps {row['mode']}: orders={row['order_count']} avg_order_slippage_bps={row['avg_order_slippage_bps']:.2f} max_order_slippage_bps={row['max_order_slippage_bps']:.2f} avg_fee_bps={row['avg_fee_bps']:.2f}")
    else:
        print('- perps: none')

    print('\nValidation status:')
    validation = report.get('validation_status', {})
    print(f"- paper={validation.get('paper_validation_status')} shadow={validation.get('shadow_validation_status')} accounting={validation.get('accounting_status')} trusted_trades={validation.get('trusted_trade_count', 0)} untrusted_trades={validation.get('untrusted_trade_count', 0)}")
    if validation.get('blockers'):
        print(f"- blockers: {', '.join(validation['blockers'])}")
    for row in (validation.get('validation_modes') or [])[:6]:
        print(f"- mode={row['validation_mode']} trades={row['trade_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f}")
    for row in (validation.get('approval_statuses') or [])[:6]:
        print(f"- approval={row['approval_status']} trades={row['trade_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f}")

    print('\nRisk guardrails:')
    guardrails = report.get('risk_guardrails', {})
    print(f"- spot_live_realized=${guardrails.get('spot_live_realized_pnl_usd', 0):.2f} spot_paper_realized=${guardrails.get('spot_paper_realized_pnl_usd', 0):.2f} perps_paper_realized=${guardrails.get('perps_paper_realized_pnl_usd', 0):.2f} consecutive_losses={guardrails.get('consecutive_losses', 0)} kill_switch_recommended={guardrails.get('kill_switch_recommended')}")
    if guardrails.get('blockers'):
        print(f"- blockers: {', '.join(guardrails['blockers'])}")
    spot_journal = guardrails.get('spot_journal_summary') or {}
    print(f"- spot_journal: ambiguous={spot_journal.get('ambiguous_submit_count', 0)} stale={spot_journal.get('stale_submitted_count', 0)} partial={spot_journal.get('partial_fill_count', 0)} approvals={spot_journal.get('approval_request_count', 0)} reconciliations={spot_journal.get('reconciliation_count', 0)}")
    for event in (spot_journal.get('recent_events') or [])[:4]:
        print(f"- spot_event {event['event_type']} severity={event['severity']} ts={event['ts']} msg={event['message']}")

    print('\nSpot recovery health:')
    blockers = guardrails.get('blockers') or []
    health = 'degraded' if any(token.startswith('spot_') for token in blockers) else 'healthy'
    print(f"- status={health} ambiguous={spot_journal.get('ambiguous_submit_count', 0)} stale={spot_journal.get('stale_submitted_count', 0)} partial={spot_journal.get('partial_fill_count', 0)}")
    spot_blockers = [token for token in blockers if token.startswith('spot_')]
    print(f"- blockers: {', '.join(spot_blockers) if spot_blockers else 'none'}")
    notifier_health = _load_json(SPOT_NOTIFIER_HEALTH_STATE, {})
    if notifier_health:
        print(f"- notifier_health: status={notifier_health.get('status', 'unknown')} lag_events={notifier_health.get('lag_events', 'n/a')} max_lag={notifier_health.get('max_lag_events', 'n/a')}")
    review_items = [item for item in _load_jsonl(SPOT_MANUAL_REVIEW_QUEUE) if item.get('status', 'open') == 'open']
    review_items.sort(key=lambda item: ({'critical': 0, 'high': 1, 'medium': 2, 'low': 3}.get(item.get('severity', 'medium'), 9), -int(item.get('priority_score', 0) or 0), str(item.get('queued_at', ''))))
    assignee_counts = {}
    needs_reassignment_count = 0
    for item in review_items:
        key = item.get('assignee') or 'unassigned'
        assignee_counts[key] = assignee_counts.get(key, 0) + 1
        if item.get('needs_reassignment'):
            needs_reassignment_count += 1
    print(f"- manual_review_open={len(review_items)}")
    print(f"- needs_reassignment_open={needs_reassignment_count}")
    for assignee, count in sorted(assignee_counts.items()):
        print(f"- assignee_backlog {assignee}={count}")
    for item in review_items[:3]:
        print(f"- top_review #{item.get('queue_id')} [{item.get('severity', 'medium')}] {item.get('review_type')} decision={item.get('decision_id') or 'n/a'} symbol={item.get('symbol') or 'n/a'} assignee={item.get('assignee') or 'unassigned'}")

    print('\nOpen positions:')
    if report['open_positions']:
        for row in report['open_positions']:
            print(f"- {row['mode']} {row['symbol']}: qty={row['remaining_amount']:.6f} cost_basis=${row['cost_basis_usd']:.2f} market_value=${row['market_value_usd']:.2f} unrealized=${row['unrealized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nAccounting audit:')
    audit = report.get('accounting_audit', {})
    audit_summary = audit.get('summary', {})
    print(f"- status={audit.get('status', 'unknown')} flagged_trades={audit_summary.get('flagged_trade_count', 0)} findings={audit_summary.get('finding_count', 0)}")
    if audit_summary.get('top_issues'):
        for issue in audit_summary['top_issues'][:3]:
            print(f"- issue {issue['issue_type']}: {issue['count']}")
    else:
        print('- none')

    print('\nPerps markets:')
    if report['perp_market_overview']:
        for row in report['perp_market_overview']:
            print(f"- {row['asset']}: price=${row['price_usd']:.4f} 24h={row['change_pct_24h']:.2f}% volume=${row['volume_usd_24h']:.2f}")
    else:
        print('- none')

    print('\nPerps open positions:')
    if report['perp_position_summary']:
        for row in report['perp_position_summary']:
            print(f"- {row['mode']} {row['asset']} {row['side']}: size=${(row['notional_usd'] or row['size_usd'] or 0):.2f} lev={row['leverage'] or 0:.2f} unrealized=${row['unrealized_pnl_usd'] or 0:.2f}")
    else:
        print('- none')

    print('\nPerps trade performance:')
    if report['perp_trade_performance']:
        for row in report['perp_trade_performance']:
            print(f"- {row['mode']}: fills={row['fill_count']} notional=${row['notional_usd']:.2f} realized_pnl=${row['realized_pnl_usd']:.2f} fees=${row['fees_usd']:.2f} funding=${row['funding_usd']:.2f}")
    else:
        print('- none')

    print('\nPerps risk summary:')
    perp_risk = report['perp_risk_summary']
    print(f"- open_positions={perp_risk['open_position_count']} open_notional=${perp_risk['open_notional_usd']:.2f} unrealized=${perp_risk['unrealized_pnl_usd']:.2f} avg_leverage={perp_risk['avg_leverage']:.2f} closest_liq_buffer_pct={perp_risk['closest_liquidation_buffer_pct']}")

    print('\nCombined exposure summary:')
    combined = report['combined_exposure_summary']
    print(f"- spot_open_value=${combined['spot_open_market_value_usd']:.2f} perp_open_notional=${combined['perp_open_notional_usd']:.2f} spot_unrealized=${combined['spot_unrealized_pnl_usd']:.2f} perp_unrealized=${combined['perp_unrealized_pnl_usd']:.2f}")

    print('\nSignal candidates:')
    if report['signal_candidates']:
        for row in report['signal_candidates']:
            print(f"- {row['symbol']} {row['signal_type']} [{row['status']}] x{row['candidate_count']}")
    else:
        print('- none')

    print('\nCandidate outcomes (60m horizon):')
    if report['candidate_outcomes']:
        for row in report['candidate_outcomes']:
            print(f"- {row['signal_type']} [{row['status']}]: avg_forward_return={row['avg_forward_return_pct']:.2f}% favorable_rate={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nPerp candidate outcomes (60m horizon):')
    if report['perp_candidate_outcomes']:
        for row in report['perp_candidate_outcomes']:
            print(f"- {row['signal_type']} [{row['status']}]: avg_forward_return={row['avg_forward_return_pct']:.2f}% favorable_rate={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nCandidate outcomes (multi-horizon):')
    if report['candidate_outcomes_multi']:
        for row in report['candidate_outcomes_multi'][:12]:
            print(f"- {row['signal_type']} [{row['status']}] @{row['horizon_minutes']}m: avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nPerp candidate outcomes (multi-horizon):')
    if report['perp_candidate_outcomes_multi']:
        for row in report['perp_candidate_outcomes_multi'][:12]:
            print(f"- {row['signal_type']} [{row['status']}] @{row['horizon_minutes']}m: avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nCandidate status comparison:')
    if report['candidate_status_comparison']:
        for row in report['candidate_status_comparison']:
            print(f"- {row['signal_type']}: total={row['candidate_count']} executed={row['executed_count']} skipped={row['skipped_count']} conversion={row['conversion_rate']:.2f}")
    else:
        print('- none')

    print('\nCandidate score summary:')
    if report['candidate_score_summary']:
        for row in report['candidate_score_summary'][:10]:
            print(f"- {row['symbol']} [{row['status']}]: avg_score={row['avg_score']:.2f} min={row['min_score']:.2f} max={row['max_score']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nCandidate regime summary:')
    if report['candidate_regime_summary']:
        for row in report['candidate_regime_summary'][:10]:
            avg_score = row['avg_score'] if row['avg_score'] is not None else 0
            print(f"- {row['regime_tag']} [{row['status']}]: avg_score={avg_score:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nRecent score explanations:')
    if report.get('recent_scored_candidates'):
        for row in report['recent_scored_candidates'][:10]:
            liquidity_component = row.get('liquidity_component_execution') if row.get('liquidity_component_execution') is not None else row.get('liquidity_component_monitor')
            parts = [
                f"threshold={row.get('threshold_distance')}",
                f"liquidity={liquidity_component}",
                f"impact={row.get('quote_price_impact')}",
                f"symbol_exp={row.get('symbol_expectancy')}",
                f"strategy_exp={row.get('strategy_expectancy')}",
                f"pressure={row.get('whale_pressure')}",
                f"regime={row.get('regime_component')}",
                f"saturation={row.get('saturation')}",
            ]
            print(f"- {row['symbol']} {row['signal_type']} [{row['status']}] score={row['score']:.2f} regime={row.get('regime_tag')} :: " + ', '.join(parts))
    else:
        print('- none')

    print('\nCandidate outcomes by strategy:')
    if report['candidate_outcomes_by_strategy']:
        for row in report['candidate_outcomes_by_strategy'][:10]:
            gating = 'ready' if row['sample_sufficient'] else f"insufficient(n<{row['min_sample_size']})"
            print(f"- {row['strategy_tag']} [{row['status']}]: avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']} gate={gating}")
    else:
        print('- none')

    print('\nCandidate outcomes by symbol:')
    if report['candidate_outcomes_by_symbol']:
        for row in report['candidate_outcomes_by_symbol'][:10]:
            print(f"- {row['symbol']} [{row['status']}]: avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nExecution scores:')
    if report['execution_scores']:
        for row in report['execution_scores'][:10]:
            print(f"- {row['symbol']} [{row['status']}]: score={row['score']} tier={row['tier']} pressure={row['pressure']}")
    else:
        print('- none')

    print('\nTop missed opportunities:')
    if report['missed_opportunities']:
        for row in report['missed_opportunities'][:10]:
            print(f"- {row['symbol']} {row['signal_type']} [{row['status']}] @{row['horizon_minutes']}m -> {row['forward_return_pct']:.2f}%")
    else:
        print('- none')

    print('\nPost-trade attribution:')
    attribution = report.get('post_trade_attribution', {}).get('rows', [])
    if attribution:
        for row in attribution[:12]:
            print(f"- {row['product_type']} {row['setup']} regime={row['entry_regime_tag']} exit={row['exit_reason']} validation={row['validation_mode']} trades={row['trade_count']} pnl=${row['realized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nTop alerts:')
    if report['top_alerts']:
        for row in report['top_alerts']:
            print(f"- {row['cnt']}x {row['message']}")
    else:
        print('- none')

    print('\nTop whale observations:')
    if report['top_whales']:
        for row in report['top_whales']:
            print(f"- {row['wallet_label']} ({row['focus_symbol']}): txs={row['recent_tx_count']} self-paid={row['self_fee_payer_count']} | {row['summary']}")
    else:
        print('- none')

    print('\nPromotion / demotion rules:')
    if report['promotion_rules']:
        for row in report['promotion_rules']:
            print(f"- {row['name']} [{row['status']}]: {row['decision']} score={row['execution_score']} n={row['candidate_count']} sample_ok={row['sample_sufficient']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} — {row['decision_reason']}")
    else:
        print('- none')

    print('\nPerp candidate volume by symbol:')
    if report.get('perp_candidate_volume_by_symbol'):
        for row in report['perp_candidate_volume_by_symbol'][:10]:
            print(f"- {row['symbol']}: candidates={row['candidate_count']} executed={row['executed_count']} skipped={row['skipped_count']} execution_rate={row['execution_rate']:.2f}")
    else:
        print('- none')

    print('\nPerp candidate score summary:')
    if report.get('perp_candidate_score_summary'):
        for row in report['perp_candidate_score_summary'][:10]:
            print(f"- {row['symbol']} [{row['status']}]: avg_score={row['avg_score']:.2f} min={row['min_score']:.2f} max={row['max_score']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nPerp recent score explanations:')
    if report.get('perp_recent_scored_candidates'):
        for row in report['perp_recent_scored_candidates'][:10]:
            liquidity_component = row.get('liquidity_component_execution') if row.get('liquidity_component_execution') is not None else row.get('liquidity_component_monitor')
            parts = [
                f"threshold={row.get('threshold_distance')}",
                f"liquidity={liquidity_component}",
                f"impact={row.get('quote_price_impact')}",
                f"symbol_exp={row.get('symbol_expectancy')}",
                f"strategy_exp={row.get('strategy_expectancy')}",
                f"pressure={row.get('whale_pressure')}",
                f"regime={row.get('regime_component')}",
                f"saturation={row.get('saturation')}",
            ]
            print(f"- {row['symbol']} {row['signal_type']} [{row['status']}] score={row['score']:.2f} regime={row.get('regime_tag')} :: " + ', '.join(parts))
    else:
        print('- none')

    print('\nPerp pilot lane competition (60m focus):')
    perp_competition = report.get('perp_candidate_competition') or []
    rows_60_symbol = [r for r in perp_competition if r['horizon_minutes'] == 60 and r.get('competition_scope') == 'symbol']
    rows_60_basket = [r for r in perp_competition if r['horizon_minutes'] == 60 and r.get('competition_scope') != 'symbol']
    if rows_60_symbol:
        for row in rows_60_symbol[:10]:
            print(f"- {row['signal_type']} {row.get('symbol')} @60m: wins={row['win_count']}/{row['decision_count']} win_rate={row['win_rate']:.2f} avg_edge={row['avg_edge_pct']:.2f}% best_short_edge={row['avg_best_short_edge_pct']:.2f}%")
        if rows_60_basket:
            print('- basket rollup:')
            for row in rows_60_basket[:3]:
                print(f"  - {row['signal_type']} {row.get('symbol')} @60m: wins={row['win_count']}/{row['decision_count']} win_rate={row['win_rate']:.2f} avg_edge={row['avg_edge_pct']:.2f}%")
    elif rows_60_basket:
        for row in rows_60_basket[:10]:
            print(f"- {row['signal_type']} {row.get('symbol')} @60m: wins={row['win_count']}/{row['decision_count']} win_rate={row['win_rate']:.2f} avg_edge={row['avg_edge_pct']:.2f}% best_short_edge={row['avg_best_short_edge_pct']:.2f}%")
    elif perp_competition:
        horizons = sorted({row['horizon_minutes'] for row in perp_competition})
        print(f"- no 60m labeled competition yet; available_horizons={horizons}")
        for row in perp_competition[:10]:
            symbol_text = f" {row.get('symbol')}" if row.get('symbol') else ''
            print(f"- {row['signal_type']}{symbol_text} @{row['horizon_minutes']}m: wins={row['win_count']}/{row['decision_count']} win_rate={row['win_rate']:.2f} avg_edge={row['avg_edge_pct']:.2f}%")
    else:
        print('- none')

    print('\nPerp executor paper state:')
    perp_executor = report.get('perp_executor_state', {})
    daily_metrics = perp_executor.get('daily_paper_metrics', {})
    readiness = perp_executor.get('decision_readiness', {})
    print(f"- pending_decisions={len(perp_executor.get('decisions', []))} open_positions={len(perp_executor.get('open_positions', []))} fills_today={daily_metrics.get('fill_count', 0)} realized_today=${daily_metrics.get('realized_pnl_usd', 0):.2f} notional_today=${daily_metrics.get('trade_notional_usd', 0):.2f}")
    if readiness:
        print(
            f"- readiness: trade_ready={readiness.get('trade_ready_count', 0)} "
            f"blocked={readiness.get('blocked_count', 0)} "
            f"no_trade_selected={readiness.get('no_trade_selected_count', 0)}"
        )
        if readiness.get('signal_age_limit_minutes') is not None:
            print(f"- actionability_guardrails: max_signal_age_min={readiness.get('signal_age_limit_minutes')}")
        if readiness.get('top_blockers'):
            print('- top_blockers: ' + ', '.join(f"{row['blocker']}={row['count']}" for row in readiness['top_blockers']))
        best_current = readiness.get('best_current_opportunity') or perp_executor.get('best_current_opportunity') or {}
        if best_current:
            print(
                f"- best_current_opportunity {best_current.get('symbol')} / {best_current.get('decision_id')}: "
                f"status={best_current.get('operator_status')} selected={best_current.get('selected_signal_type')} "
                f"best_short={best_current.get('best_short_signal_type')} best_short_score={best_current.get('best_short_score')} "
                f"no_trade_score={best_current.get('no_trade_score')} gap_vs_no_trade={best_current.get('score_gap_vs_no_trade')} "
                f"age_min={best_current.get('age_minutes')} blockers={','.join(best_current.get('blockers') or []) or 'none'}"
            )
            if any(best_current.get(field) is not None for field in ('paper_notional_usd', 'planned_size_usd', 'stop_loss_price', 'take_profit_price', 'invalidation_price', 'market_age_minutes')):
                print(
                    f"- best_current_plan: planned_size=${best_current.get('planned_size_usd') or 0:.2f} "
                    f"notional=${best_current.get('paper_notional_usd') or 0:.2f} market_age_min={best_current.get('market_age_minutes')} "
                    f"stop=${best_current.get('stop_loss_price') or 0:.4f} target=${best_current.get('take_profit_price') or 0:.4f} "
                    f"invalidate=${best_current.get('invalidation_price') or 0:.4f}"
                )
        if readiness.get('top_opportunities'):
            for row in readiness['top_opportunities'][:3]:
                print(
                    f"- opportunity {row['symbol']} / {row['decision_id']}: status={row['operator_status']} "
                    f"selected={row.get('selected_signal_type')} best_short={row.get('best_short_signal_type')} "
                    f"gap_vs_no_trade={row.get('score_gap_vs_no_trade')} blockers={','.join(row.get('blockers') or []) or 'none'}"
                )
    latest_choice = perp_executor.get('latest_candidate_choice') or {}
    latest_pilot_policy = perp_executor.get('latest_pilot_policy') or {}
    decision_rows = (readiness.get('decisions') or perp_executor.get('decisions') or [])
    latest_choice_state = next(
        (row for row in decision_rows if row.get('decision_id') == latest_choice.get('decision_id')),
        {},
    ) if latest_choice else {}
    if latest_pilot_policy:
        candidate_strategy = latest_pilot_policy.get('candidate_strategy') or 'none'
        candidate_symbol = latest_pilot_policy.get('candidate_symbol') or 'none'
        print(
            f"- pilot_policy: status={latest_pilot_policy.get('policy_status')} "
            f"live_eligible={latest_pilot_policy.get('live_eligible_for_executor')} "
            f"denial_reason={latest_pilot_policy.get('denial_reason') or 'none'} "
            f"candidate={candidate_strategy} / {candidate_symbol}"
        )
        pilot_decision = latest_pilot_policy.get('tiny_live_pilot_decision') or {}
        if pilot_decision:
            print(
                f"- pilot_approval: approved={pilot_decision.get('approved')} mode={pilot_decision.get('mode')} "
                f"approved_candidate={pilot_decision.get('product_type') or 'none'} "
                f"{pilot_decision.get('strategy') or 'none'} / {pilot_decision.get('symbol') or 'none'}"
            )
    else:
        print('- pilot_policy: none recorded in recent executor cycles')
    if latest_choice:
        print(
            f"- current_choice {latest_choice.get('symbol')} / {latest_choice.get('decision_id')}: "
            f"selected={latest_choice.get('selected_signal_type')} type={latest_choice.get('type')} "
            f"selected_score={latest_choice.get('selected_score')} best_short={latest_choice.get('best_short_signal_type')} "
            f"best_short_score={latest_choice.get('best_short_score')} no_trade_score={latest_choice.get('no_trade_score')} "
            f"gap_vs_no_trade={latest_choice.get('score_gap_vs_no_trade')}"
        )
        current_choice_blockers = latest_choice_state.get('blockers') or []
        current_choice_reason = current_choice_blockers[0] if current_choice_blockers else latest_choice.get('block_reason')
        current_choice_market_age = latest_choice_state.get('market_age_minutes')
        if current_choice_market_age is None:
            current_choice_market_age = latest_choice.get('market_age_minutes')
        if latest_choice.get('blocked_trade') or latest_choice_state.get('operator_status') == 'blocked':
            print(f"- current_choice_blocked: reason={current_choice_reason} market_age_min={current_choice_market_age}")
        elif latest_choice.get('type') == 'trade':
            planned_size = _display_money(
                latest_choice.get('planned_size_usd')
                if latest_choice.get('planned_size_usd') is not None
                else latest_choice_state.get('planned_size_usd')
            )
            notional = _display_money(
                latest_choice.get('paper_notional_usd')
                if latest_choice.get('paper_notional_usd') is not None
                else latest_choice_state.get('paper_notional_usd')
                if latest_choice_state.get('paper_notional_usd') is not None
                else latest_choice.get('planned_size_usd')
                if latest_choice.get('planned_size_usd') is not None
                else latest_choice_state.get('planned_size_usd')
            )
            print(
                f"- current_trade_plan: planned_size=${planned_size:.2f} notional=${notional:.2f} "
                f"stop=${latest_choice.get('stop_loss_price') or latest_choice_state.get('stop_loss_price') or 0:.4f} "
                f"target=${latest_choice.get('take_profit_price') or latest_choice_state.get('take_profit_price') or 0:.4f} "
                f"invalidate=${latest_choice.get('invalidation_price') or latest_choice_state.get('invalidation_price') or 0:.4f}"
            )
    else:
        latest_decision = (perp_executor.get('decisions') or [None])[0]
        if latest_decision:
            best_short = latest_decision.get('best_short_lane') or {}
            no_trade_lane = next((lane for lane in latest_decision.get('lanes', []) if lane.get('signal_type') == 'perp_no_trade'), None)
            print(f"- latest_decision {latest_decision.get('symbol')} / {latest_decision.get('decision_id')}: best_short={best_short.get('signal_type')} score={best_short.get('score')} no_trade_score={None if no_trade_lane is None else no_trade_lane.get('score')}")
        else:
            print('- no recent perp executor decisions')
    recent_cycles = perp_executor.get('recent_cycles') or []
    live_adapter_cycle = next((cycle for cycle in recent_cycles if cycle.get('live_adapter_request')), {})
    print('Recent executor cycles:')
    if recent_cycles:
        for cycle in recent_cycles[:3]:
            entry_bits = [cycle.get('entry_outcome') or 'unknown']
            if cycle.get('entry_symbol'):
                entry_bits.append(str(cycle.get('entry_symbol')))
            if cycle.get('entry_signal_type'):
                entry_bits.append(str(cycle.get('entry_signal_type')))
            close_reasons = cycle.get('close_reasons') or []
            close_text = f" closes={','.join(close_reasons)}" if close_reasons else ''
            print(f"- {cycle.get('event_ts')}: entry={' '.join(entry_bits)} pos={cycle.get('position_action') or 'unknown'}{close_text}")
    else:
        print('- none')
    entry_counts = perp_executor.get('recent_entry_reason_counts') or {}
    if entry_counts:
        print('Entry outcomes:')
        print('- ' + ', '.join(f"{reason}={count}" for reason, count in list(entry_counts.items())[:6]))
    else:
        print('Entry outcomes:')
        print('- none')
    close_counts = perp_executor.get('recent_close_reason_counts') or {}
    if close_counts:
        print('Close reasons:')
        print('- ' + ', '.join(f"{reason}={count}" for reason, count in list(close_counts.items())[:6]))
    else:
        print('Close reasons:')
        print('- none')
    recent_risk = perp_executor.get('recent_risk_events') or []
    if recent_risk:
        for row in recent_risk[:5]:
            print(f"- risk [{row['severity']}] {row['event_type']}: {row['message']}")
    else:
        print('- no recent perp risk events')
    if live_adapter_cycle:
        adapter_request = live_adapter_cycle.get('live_adapter_request') or {}
        adapter_attempt = live_adapter_cycle.get('live_submit_attempt') or {}
        adapter_caps = live_adapter_cycle.get('live_adapter_capabilities') or {}
        print('Live adapter scaffold:')
        print(
            f"- adapter={adapter_caps.get('adapter_name') or adapter_request.get('adapter_name') or 'unknown'} "
            f"implemented={adapter_caps.get('implemented')} live_supported={adapter_caps.get('live_order_submission_supported')}"
        )
        print(
            f"- request action={adapter_request.get('action')} symbol={adapter_request.get('market', {}).get('symbol')} "
            f"decision_id={adapter_request.get('approval', {}).get('decision_id')} notional=${_display_money(adapter_request.get('risk', {}).get('entry_notional_usd')):.2f}"
        )
        print(
            f"- submit_attempt action={adapter_attempt.get('action')} reason={adapter_attempt.get('reason')}"
        )
    else:
        print('Live adapter scaffold:')
        print('- none captured in recent cycles')

    print('\nTiny live pilot decision:')
    pilot = report.get('tiny_live_pilot_decision', {})
    print(f"- approved={pilot.get('approved')} mode={pilot.get('mode')} product_type={pilot.get('product_type')} strategy={pilot.get('strategy')} symbol={pilot.get('symbol')} horizon={pilot.get('evaluation_horizon_minutes')}")
    print(f"- reason: {pilot.get('reason')}")
    if pilot.get('blockers'):
        print(f"- blockers: {', '.join(pilot['blockers'])}")
    requirements = pilot.get('approval_requirements') or {}
    if requirements:
        print(f"- requirements: min_n={requirements.get('min_candidate_count')} perp_win_rate>={requirements.get('perp_min_win_rate')} perp_avg_edge>={requirements.get('perp_min_avg_edge_pct')} perp_env_edge>={requirements.get('perp_min_environment_edge_pct')} spot_score>={requirements.get('spot_min_execution_score')}")
    evidence = pilot.get('evidence') or {}
    best_perp = evidence.get('best_perp_candidate') or {}
    if best_perp:
        best_perp_symbol = best_perp.get('symbol') or pilot.get('symbol')
        print(f"- best_perp_candidate: {best_perp.get('signal_type')} / {best_perp_symbol} @{best_perp.get('horizon_minutes', pilot.get('evaluation_horizon_minutes'))}m wins={best_perp.get('win_count')}/{best_perp.get('decision_count')} win_rate={best_perp.get('win_rate'):.2f} avg_edge={best_perp.get('avg_edge_pct'):.2f}%")
    elif evidence.get('perp_available_horizons'):
        print(f"- perp available horizons: {evidence.get('perp_available_horizons')}")
    actionable_candidate = pilot.get('actionable_candidate') or pilot.get('blocked_candidate') or pilot.get('recommended_candidate') or {}
    no_trade_benchmark = pilot.get('no_trade_benchmark') or {}
    if actionable_candidate:
        metrics = actionable_candidate.get('metrics') or {}
        comparison = actionable_candidate.get('comparison_vs_no_trade') or {}
        horizon = actionable_candidate.get('horizon_minutes')
        horizon_text = f" @{horizon}m" if horizon is not None else ''
        print(
            f"- actionable_candidate: {actionable_candidate.get('product_type')} {actionable_candidate.get('strategy')} / "
            f"{actionable_candidate.get('symbol')}{horizon_text} avg={float(metrics.get('avg_forward_return_pct') or 0):.2f}% "
            f"fav={float(metrics.get('favorable_rate') or 0):.2f} score={metrics.get('execution_score')} n={metrics.get('candidate_count')}"
        )
        if actionable_candidate.get('blockers'):
            print(f"- actionable_candidate_blockers: {', '.join(actionable_candidate.get('blockers') or [])}")
        if comparison:
            print(
                f"- actionable_candidate_vs_no_trade: signal={comparison.get('signal_type')} "
                f"win_rate_gap={comparison.get('win_rate_gap')} avg_edge_gap_pct={comparison.get('avg_edge_gap_pct')}"
            )
    if no_trade_benchmark:
        benchmark_metrics = no_trade_benchmark.get('metrics') or {}
        benchmark_horizon = no_trade_benchmark.get('horizon_minutes')
        benchmark_horizon_text = f" @{benchmark_horizon}m" if benchmark_horizon is not None else ''
        print(
            f"- no_trade_benchmark: {no_trade_benchmark.get('product_type')} {no_trade_benchmark.get('strategy')} / "
            f"{no_trade_benchmark.get('symbol')}{benchmark_horizon_text} avg={float(benchmark_metrics.get('avg_forward_return_pct') or 0):.2f}% "
            f"fav={float(benchmark_metrics.get('favorable_rate') or 0):.2f} score={benchmark_metrics.get('execution_score')} n={benchmark_metrics.get('candidate_count')}"
        )
    if 'perp_short_lane_not_beating_no_trade_win_rate' in (pilot.get('blockers') or []):
        print('- promotion_suppressed_by_no_trade: yes — no-trade is still winning on win rate, so the actionable short lane stays paper-only')
    if pilot.get('best_candidates'):
        for row in pilot['best_candidates']:
            horizon = row.get('horizon_minutes')
            horizon_text = f" @{horizon}m" if horizon is not None else ''
            print(f"- candidate {row['product_type']} {row['strategy']} / {row['symbol']}{horizon_text}: score={row['execution_score']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- no pilot candidates yet')

    print('\nSymbol promotion / demotion rules:')
    if report['symbol_promotion_rules']:
        for row in report['symbol_promotion_rules']:
            print(f"- {row['name']} [{row['status']}]: {row['decision']} score={row['execution_score']} n={row['candidate_count']} sample_ok={row['sample_sufficient']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} — {row['decision_reason']}")
    else:
        print('- none')

    print('\nPromoted strategies:')
    if report['promoted_strategies']:
        for row in report['promoted_strategies']:
            print(f"- {row['name']} [{row['status']}] score={row['execution_score']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nDemoted strategies:')
    if report['demoted_strategies']:
        for row in report['demoted_strategies']:
            print(f"- {row['name']} [{row['status']}] score={row['execution_score']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nPromoted symbols:')
    if report['promoted_symbols']:
        for row in report['promoted_symbols']:
            print(f"- {row['name']} [{row['status']}] score={row['execution_score']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nDemoted symbols:')
    if report['demoted_symbols']:
        for row in report['demoted_symbols']:
            print(f"- {row['name']} [{row['status']}] score={row['execution_score']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f} n={row['candidate_count']}")
    else:
        print('- none')

    print('\nInsufficient-sample strategies:')
    if report['insufficient_sample_strategies']:
        for row in report['insufficient_sample_strategies']:
            print(f"- {row['name']} [{row['status']}] score={row['execution_score']} sample={row['candidate_count']}/{row['min_candidate_count']} avg={row['avg_forward_return_pct']:.2f}% fav={row['favorable_rate']:.2f}")
    else:
        print('- none')

    print('\nPaused strategies:')
    if report['strategy_controls']['paused_strategies']:
        for row in report['strategy_controls']['paused_strategies']:
            print(f"- {row['strategy_tag']}: trades={row['trade_count']} realized_pnl=${row['realized_pnl_usd']:.2f}")
    else:
        print('- none')

    print('\nRecommendations:')
    for rec in report['recommendations']:
        print(f"- {rec}")

    print(f"\nDeployment mode recommendation: {report['deployment_mode_recommendation']}")

    print('\nSystem events:')
    if report['system_events']:
        for row in report['system_events'][:10]:
            print(f"- [{row['severity']}] {row['event_type']}: {row['message']}")
    else:
        print('- none')


if __name__ == '__main__':
    main()
