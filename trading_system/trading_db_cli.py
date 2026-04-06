#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from trading_system.runtime_config import DB_PATH
from trading_system.trading_db import (
    get_accounting_audit,
    get_open_positions,
    get_perp_executor_state,
    get_perp_open_positions,
    get_perp_summary,
    get_recent_perp_market_history,
    get_strategy_execution_policy,
    get_strategy_risk_controls,
    record_auto_trade,
    record_perp_account_snapshot,
    record_perp_fill,
    record_perp_market_snapshot,
    record_perp_order,
    record_risk_event,
    record_signal_candidate,
    record_snapshot_and_alerts,
    record_system_event,
    record_whale_observation,
    upsert_perp_position,
)


def read_payload() -> dict:
    raw = sys.stdin.read().strip()
    if not raw:
        raise SystemExit('Expected JSON payload on stdin')
    return json.loads(raw)


def main() -> None:
    parser = argparse.ArgumentParser(description='Record trading events into SQLite')
    parser.add_argument('command', choices=['record-snapshot', 'record-trade', 'record-signal', 'record-whale', 'record-event', 'strategy-controls', 'open-positions', 'accounting-audit', 'record-perp-market-snapshot', 'record-perp-account-snapshot', 'record-perp-position', 'record-perp-order', 'record-perp-fill', 'record-risk-event', 'perp-positions', 'perp-summary', 'record-perp-candidate', 'perp-market-history', 'perp-executor-state', 'recent-trades', 'recent-system-events'])
    parser.add_argument('--db', default=str(DB_PATH))
    parser.add_argument('--min-trades', type=int, default=2)
    parser.add_argument('--min-realized-pnl-usd', type=float, default=0.0)
    parser.add_argument('--minutes', type=int, default=180)
    parser.add_argument('--assets', default='')
    parser.add_argument('--limit-per-asset', type=int, default=120)
    parser.add_argument('--analytics-lookback-hours', type=int, default=24)
    parser.add_argument('--recent-fill-limit', type=int, default=25)
    parser.add_argument('--recent-order-limit', type=int, default=25)
    parser.add_argument('--limit', type=int, default=200)
    parser.add_argument('--event-type', default='')
    args = parser.parse_args()

    db_path = Path(args.db)

    if args.command == 'strategy-controls':
        print(json.dumps(get_strategy_execution_policy(db_path, min_trades=args.min_trades, min_realized_pnl_usd=args.min_realized_pnl_usd), sort_keys=True))
        return
    if args.command == 'open-positions':
        print(json.dumps(get_open_positions(db_path)))
        return
    if args.command == 'accounting-audit':
        print(json.dumps(get_accounting_audit(db_path)))
        return
    if args.command == 'perp-positions':
        print(json.dumps(get_perp_open_positions(db_path)))
        return
    if args.command == 'perp-summary':
        print(json.dumps(get_perp_summary(db_path)))
        return
    if args.command == 'perp-market-history':
        assets = [asset.strip() for asset in (args.assets or '').split(',') if asset.strip()]
        print(json.dumps(get_recent_perp_market_history(db_path, minutes=args.minutes, assets=assets or None, limit_per_asset=args.limit_per_asset)))
        return
    if args.command == 'perp-executor-state':
        print(json.dumps(get_perp_executor_state(
            db_path,
            lookback_minutes=args.minutes,
            analytics_lookback_hours=args.analytics_lookback_hours,
            recent_fill_limit=args.recent_fill_limit,
            recent_order_limit=args.recent_order_limit,
        )))
        return
    if args.command == 'recent-trades':
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT ts, symbol, side, mode, decision_id, signature, trade_key FROM auto_trades ORDER BY ts DESC, id DESC LIMIT ?",
                (args.limit,),
            ).fetchall()
            print(json.dumps([dict(row) for row in rows]))
        finally:
            conn.close()
        return
    if args.command == 'recent-system-events':
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            if args.event_type:
                rows = conn.execute(
                    "SELECT id, ts, event_type, severity, message, source, metadata_json FROM system_events WHERE event_type = ? ORDER BY ts DESC, id DESC LIMIT ?",
                    (args.event_type, args.limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, ts, event_type, severity, message, source, metadata_json FROM system_events ORDER BY ts DESC, id DESC LIMIT ?",
                    (args.limit,),
                ).fetchall()
            print(json.dumps([dict(row) for row in rows]))
        finally:
            conn.close()
        return

    payload = read_payload()

    if args.command == 'record-snapshot':
        record_snapshot_and_alerts(db_path, payload['snapshot'], payload.get('alerts', []))
    elif args.command == 'record-trade':
        record_auto_trade(db_path, payload)
    elif args.command == 'record-signal':
        record_signal_candidate(db_path, payload)
    elif args.command == 'record-whale':
        record_whale_observation(db_path, payload)
    elif args.command == 'record-event':
        event_id = record_system_event(db_path, payload)
        print(json.dumps({'id': event_id}))
        return
    elif args.command == 'record-perp-market-snapshot':
        record_perp_market_snapshot(db_path, payload)
    elif args.command == 'record-perp-account-snapshot':
        record_perp_account_snapshot(db_path, payload)
    elif args.command == 'record-perp-position':
        upsert_perp_position(db_path, payload)
    elif args.command == 'record-perp-order':
        record_perp_order(db_path, payload)
    elif args.command == 'record-perp-fill':
        record_perp_fill(db_path, payload)
    elif args.command == 'record-risk-event':
        record_risk_event(db_path, payload)
    elif args.command == 'record-perp-candidate':
        record_signal_candidate(db_path, payload)


if __name__ == '__main__':
    main()
