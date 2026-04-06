import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_system.trading_db import (
    get_accounting_audit,
    get_daily_analytics,
    get_open_positions,
    get_perp_candidate_competition,
    get_perp_executor_state,
    get_perp_open_positions,
    get_perp_summary,
    get_signal_candidate_outcomes,
    get_strategy_execution_policy,
    get_strategy_risk_controls,
    get_trade_performance_split,
    get_trusted_trade_summary,
    init_db,
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
    sync_from_files,
    upsert_perp_position,
)


class TradingDbTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.db_path = self.base / 'trading.db'
        self.data_dir = self.base / 'data'
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.tmp.cleanup()

    def _write_fixture_files(self):
        (self.data_dir / 'price_history.json').write_text(
            '{"snapshots": [{"ts": "2026-03-17T21:45:37.888Z", "prices": {"SOL": {"price": 95.33, "change24h": -1.2, "liquidity": 722505892.77, "holdingAmount": 0.84, "holdingValue": 80.82, "alertBelow": 92, "buyBelow": null, "sellAbove": 110}, "BONK": {"price": 0.0000067, "change24h": 1.5, "liquidity": 2517123.63, "holdingAmount": 0, "holdingValue": 0, "alertBelow": 0.0000068, "buyBelow": 0.0000065, "sellAbove": 0.00001}}, "wallet": {"sol": 0.8478, "usdc": 19.2006, "totalUsd": 100.01}}]}'
        )
        (self.data_dir / 'auto_trades.json').write_text(
            '[{"ts": "2026-03-17T21:00:00.000Z", "symbol": "BONK", "side": "buy", "mode": "paper", "simulated": true, "price": 0.0000065, "amount": 1.5, "sizeUsd": 1.5, "outAmount": "223880", "signature": "paper-123", "quotePriceImpact": 0.05, "reason": "Price <= target", "strategyTag": "mean_reversion_near_buy"}]'
        )
        (self.data_dir / 'alerts.log').write_text(
            '[2026-03-17T21:45:37.888Z] 👀 NEAR-BUY: BONK at $0.00000670 is within 3.07% of auto-buy $0.00000650\n'
        )

    def _run_perps_auto_trade(self, extra_env=None, args=None):
        env = {
            **os.environ,
            'PERPS_AUTO_TRADE_DATA_DIR': str(self.data_dir),
            'PERPS_AUTO_TRADE_DB_PATH': str(self.db_path),
            'PERPS_AUTO_TRADE_MAX_MARKET_STALENESS_MINUTES': '999999',
            'PYTHONPATH': '/home/brimigs',
        }
        if extra_env:
            env.update(extra_env)
        result = subprocess.run(
            ['node', '/home/brimigs/perps-auto-trade.mjs', *(args or [])],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        return result, json.loads(result.stdout.strip().splitlines()[-1])

    def _run_auto_trade(self, extra_env=None, args=None):
        env = {
            **os.environ,
            'AUTO_TRADER_DATA_DIR': str(self.data_dir),
            'AUTO_TRADER_DB_PATH': str(self.db_path),
            'AUTO_TRADER_TRADING_MD': str(self.base / 'trading.md'),
            'AUTO_TRADER_PORTFOLIO_FILE': str(self.data_dir / 'portfolio.json'),
            'PYTHONPATH': '/home/brimigs',
        }
        if extra_env:
            env.update(extra_env)
        result = subprocess.run(
            ['node', '/home/brimigs/auto-trade.mjs', *(args or [])],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        stdout = result.stdout.strip()
        payload = None
        if stdout:
            try:
                payload = json.loads(stdout)
            except Exception:
                try:
                    payload = json.loads(stdout.splitlines()[-1])
                except Exception:
                    payload = None
        return result, payload

    def _seed_approved_perp_pilot_history(self, symbol='SOL', future_price=95.0):
        for i in range(8):
            hour = i * 2
            ts = f'2099-03-17T{hour:02d}:00:00.000Z'
            future_ts = f'2099-03-17T{hour:02d}:45:00.000Z'
            decision_id = f'perp-history-{symbol.lower()}-{i}'
            record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': symbol, 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
            record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': symbol, 'priceUsd': future_price, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
            for signal_type, score in [('perp_short_continuation', 82), ('perp_short_failed_bounce', 61), ('perp_no_trade', 35)]:
                record_signal_candidate(self.db_path, {
                    'ts': ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': symbol,
                    'market': symbol,
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'no_trade_min_short_edge_pct': 0.25},
                })

    def _write_strategy_controls_shim(self, mutation_lines):
        shim_path = self.base / 'shim_trading_db_cli.py'
        joined_mutation = '\n'.join(f'    {line}' for line in mutation_lines)
        shim_path.write_text(
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import subprocess\n"
            "import sys\n"
            f"REAL_CLI = {str('/home/brimigs/trading_system/trading_db_cli.py')!r}\n"
            "payload = sys.stdin.read()\n"
            "result = subprocess.run(['python', REAL_CLI, *sys.argv[1:]], input=payload, capture_output=True, text=True)\n"
            "if result.returncode != 0:\n"
            "    sys.stdout.write(result.stdout)\n"
            "    sys.stderr.write(result.stderr)\n"
            "    raise SystemExit(result.returncode)\n"
            "if len(sys.argv) > 1 and sys.argv[1] == 'strategy-controls':\n"
            "    data = json.loads(result.stdout or '{}')\n"
            f"{joined_mutation}\n"
            "    sys.stdout.write(json.dumps(data))\n"
            "else:\n"
            "    sys.stdout.write(result.stdout)\n"
        )
        return shim_path

    def _record_perp_executor_cycle_event(self, ts, summary, message='Perp paper executor cycle completed'):
        record_system_event(
            self.db_path,
            {
                'ts': ts,
                'event_type': 'perp_executor_cycle',
                'severity': 'info',
                'message': message,
                'source': 'perps-auto-trade.mjs',
                'metadata': {
                    'strategy_family': 'tiny_live_pilot',
                    'product_type': 'perps',
                    'summary': summary,
                },
            },
        )

    def _iso_ts(self, dt):
        return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _run_daily_analytics_report(self, since_ts='2026-03-17T00:00:00.000Z'):
        result = subprocess.run(
            ['python', '/home/brimigs/trading_system/daily_analytics_report.py', '--db', str(self.db_path), '--since-ts', since_ts],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, 'PYTHONPATH': '/home/brimigs', 'AUTO_TRADER_DATA_DIR': str(self.data_dir)},
        )
        return result.stdout

    def _read_perps_journal(self):
        path = self.data_dir / 'perps_auto_trade_journal.json'
        if not path.exists():
            return {}
        return json.loads(path.read_text())

    def _read_perps_live_approval(self):
        path = self.data_dir / 'telegram-bridge' / 'perps_live_approval.json'
        if not path.exists():
            return {}
        return json.loads(path.read_text())

    def _write_perps_live_approval(self, payload):
        path = self.data_dir / 'telegram-bridge' / 'perps_live_approval.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))

    def _write_perps_live_command(self, payload):
        path = self.data_dir / 'telegram-bridge' / 'perps_live_commands.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))

    def test_init_db_creates_expected_tables(self):
        init_db(self.db_path)
        conn = sqlite3.connect(self.db_path)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            conn.close()
        self.assertTrue({'schema_meta', 'price_snapshots', 'price_points', 'alerts', 'auto_trades', 'signal_candidates', 'whale_observations', 'system_events', 'open_positions', 'perp_market_snapshots', 'perp_account_snapshots', 'perp_positions', 'perp_orders', 'perp_fills', 'risk_limits', 'risk_events'}.issubset(tables))

    def test_sync_from_files_imports_snapshot_alert_and_trade_data(self):
        self._write_fixture_files()
        init_db(self.db_path)
        counts = sync_from_files(self.db_path, self.data_dir)
        self.assertEqual(counts['price_snapshots'], 1)
        self.assertEqual(counts['price_points'], 2)
        self.assertEqual(counts['alerts'], 1)
        self.assertEqual(counts['auto_trades'], 1)
        self.assertEqual(counts['signal_candidates'], 0)
        conn = sqlite3.connect(self.db_path)
        try:
            symbols = [row[0] for row in conn.execute('SELECT symbol FROM price_points ORDER BY symbol')]
            self.assertEqual(symbols, ['BONK', 'SOL'])
            row = conn.execute('SELECT mode, strategy_tag, product_type FROM auto_trades').fetchone()
            self.assertEqual(row[0], 'paper')
            self.assertEqual(row[1], 'mean_reversion_near_buy')
            self.assertEqual(row[2], 'spot')
        finally:
            conn.close()

    def test_sync_from_files_handles_malformed_json_without_crashing(self):
        (self.data_dir / 'price_history.json').write_text('{bad json')
        (self.data_dir / 'auto_trades.json').write_text('[{"ts": "2026-03-17T21:00:00.000Z", "symbol": "BONK", "side": "buy", "mode": "paper", "simulated": true, "sizeUsd": 1.5, "amount": 1.0}]')
        init_db(self.db_path)
        counts = sync_from_files(self.db_path, self.data_dir)
        self.assertEqual(counts['price_snapshots'], 0)
        self.assertEqual(counts['auto_trades'], 1)
        conn = sqlite3.connect(self.db_path)
        try:
            event = conn.execute(
                "SELECT event_type, source, message FROM system_events WHERE event_type='malformed_json_ingest' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            trade_count = conn.execute('SELECT COUNT(*) FROM auto_trades').fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(trade_count, 1)
        self.assertEqual(event[0], 'malformed_json_ingest')
        self.assertEqual(event[1], 'sync_from_files')
        self.assertIn('price_history.json', event[2])

    def test_record_auto_trade_persists_metadata_without_signature_and_dedupes_by_trade_key(self):
        init_db(self.db_path)
        trade = {
            'ts': '2026-03-17T00:00:00.000Z',
            'symbol': 'SOL',
            'side': 'buy',
            'mode': 'paper',
            'simulated': True,
            'product_type': 'spot',
            'amount': 1.0,
            'sizeUsd': 10.0,
            'decisionId': 'spot-decision-1',
            'strategyTag': 'mean_reversion_near_buy',
            'entrySignalType': 'hard_buy',
            'entryRegimeTag': 'stable',
            'entryStrategyTag': 'mean_reversion_near_buy',
            'exitReason': 'open_or_entry',
            'validationMode': 'shadow',
            'approvalStatus': 'paper_only',
        }
        record_auto_trade(self.db_path, trade)
        record_auto_trade(self.db_path, trade)
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT COUNT(*), trade_key, entry_signal_type, entry_regime_tag, entry_strategy_tag, exit_reason, validation_mode, approval_status FROM auto_trades WHERE decision_id='spot-decision-1'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 1)
        self.assertTrue(row[1])
        self.assertEqual(row[2], 'hard_buy')
        self.assertEqual(row[3], 'stable')
        self.assertEqual(row[4], 'mean_reversion_near_buy')
        self.assertEqual(row[5], 'open_or_entry')
        self.assertEqual(row[6], 'shadow')
        self.assertEqual(row[7], 'paper_only')

    def test_daily_analytics_validation_status_includes_approval_rollups(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {
            'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True,
            'amount': 1.0, 'sizeUsd': 10.0, 'decisionId': 'approval-rollup-paper', 'validationMode': 'paper', 'approvalStatus': 'paper_only'
        })
        record_auto_trade(self.db_path, {
            'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'BTC', 'side': 'buy', 'mode': 'live', 'simulated': False,
            'amount': 0.1, 'sizeUsd': 12.0, 'decisionId': 'approval-rollup-live', 'validationMode': 'tiny_live_stub', 'approvalStatus': 'telegram_approved'
        })
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        approval_rows = {row['approval_status']: row for row in report['validation_status']['approval_statuses']}
        self.assertEqual(approval_rows['paper_only']['trade_count'], 1)
        self.assertEqual(approval_rows['telegram_approved']['trade_count'], 1)
        rendered = self._run_daily_analytics_report('2026-03-17T00:00:00.000Z')
        self.assertIn('approval=paper_only trades=1', rendered)
        self.assertIn('approval=telegram_approved trades=1', rendered)

    def test_auto_trade_reconcile_spot_journal_marks_matching_submitted_trade_verified(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:test-decision': {
                    'entry_key': 'spot:test-decision',
                    'status': 'submitted',
                    'decision_id': 'test-decision',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'sig-spot-1',
                }
            }
        }, indent=2))
        (self.data_dir / 'auto_trades.json').write_text(json.dumps([
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'live', 'decisionId': 'test-decision', 'signature': 'sig-spot-1'}
        ], indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(args=['--reconcile-spot-journal'])
        self.assertEqual(payload['verified'], 1)
        reconciled = json.loads(journal_path.read_text())
        entry = reconciled['entries']['spot:test-decision']
        self.assertEqual(entry['status'], 'verified')
        self.assertEqual(entry['trade_signature'], 'sig-spot-1')

    def test_auto_trade_spot_approval_check_and_mark_executed(self):
        approval_path = self.data_dir / 'telegram-bridge' / 'spot_live_approval.json'
        approval_path.parent.mkdir(parents=True, exist_ok=True)
        approval_path.write_text(json.dumps({
            'version': 1,
            'approval_id': 'spot-approval:test-decision',
            'status': 'approved',
            'decision_id': 'test-decision',
            'symbol': 'SOL',
            'signal_type': 'hard_buy',
            'size_usd': 5.0,
            'expires_at': '2099-03-18T00:00:00.000Z'
        }, indent=2))
        _, check_payload = self._run_auto_trade(args=['--spot-approval-check', 'test-decision'])
        self.assertEqual(check_payload['approval_id'], 'spot-approval:test-decision')
        _, executed_payload = self._run_auto_trade(args=['--spot-approval-mark-executed', 'test-decision'])
        self.assertEqual(executed_payload['status'], 'executed')
        self.assertEqual(executed_payload['execution']['decision_id'], 'test-decision')

    def test_auto_trade_reconcile_spot_journal_verifies_against_sqlite_auto_trades(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:db-decision': {
                    'entry_key': 'spot:db-decision',
                    'status': 'submitted',
                    'decision_id': 'db-decision',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        init_db(self.db_path)
        record_auto_trade(self.db_path, {
            'ts': '2026-03-17T00:01:00.000Z',
            'symbol': 'SOL',
            'side': 'buy',
            'mode': 'live',
            'simulated': False,
            'amount': 1.0,
            'sizeUsd': 5.0,
            'decisionId': 'db-decision',
            'signature': 'db-sig-1',
            'approvalStatus': 'telegram_approved',
        })
        _, payload = self._run_auto_trade(args=['--reconcile-spot-journal'])
        self.assertEqual(payload['verified'], 1)
        reconciled = json.loads(journal_path.read_text())
        self.assertEqual(reconciled['entries']['spot:db-decision']['status'], 'verified')
        self.assertEqual(reconciled['entries']['spot:db-decision']['trade_signature'], 'db-sig-1')

    def test_auto_trade_reconcile_spot_journal_marks_stale_submitted_without_match(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:stale-decision': {
                    'entry_key': 'spot:stale-decision',
                    'status': 'submitted',
                    'decision_id': 'stale-decision',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(args=['--reconcile-spot-journal'])
        self.assertEqual(payload['staleSubmitted'], 1)
        reconciled = json.loads(journal_path.read_text())
        self.assertEqual(reconciled['entries']['spot:stale-decision']['status'], 'stale_submitted')
        conn = sqlite3.connect(self.db_path)
        try:
            event = conn.execute("SELECT event_type FROM system_events WHERE event_type='spot_trade_journal_stale_submitted' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            conn.close()
        self.assertEqual(event[0], 'spot_trade_journal_stale_submitted')

    def test_daily_analytics_report_surfaces_spot_journal_risk_visibility(self):
        init_db(self.db_path)
        record_system_event(self.db_path, {
            'ts': '2026-03-17T01:00:00.000Z',
            'event_type': 'spot_trade_submit_timeout_ambiguous',
            'severity': 'warning',
            'message': 'Ambiguous live buy submission for SOL',
            'source': 'auto-trade.mjs',
            'metadata': {'decision_id': 'spot-ambiguous-1', 'symbol': 'SOL'},
        })
        record_system_event(self.db_path, {
            'ts': '2026-03-17T01:05:00.000Z',
            'event_type': 'spot_trade_journal_stale_submitted',
            'severity': 'warning',
            'message': 'Spot trade journal entry is stale after submit for SOL',
            'source': 'auto-trade.mjs',
            'metadata': {'decision_id': 'spot-stale-1', 'symbol': 'SOL'},
        })
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        spot_summary = report['risk_guardrails']['spot_journal_summary']
        self.assertEqual(spot_summary['ambiguous_submit_count'], 1)
        self.assertEqual(spot_summary['stale_submitted_count'], 1)
        self.assertIn('spot_ambiguous_submit_present', report['risk_guardrails']['blockers'])
        self.assertIn('spot_stale_submitted_present', report['risk_guardrails']['blockers'])
        rendered = self._run_daily_analytics_report('2026-03-17T00:00:00.000Z')
        self.assertIn('spot_journal: ambiguous=1 stale=1', rendered)
        self.assertIn('spot_event spot_trade_journal_stale_submitted', rendered)
        self.assertIn('spot_event spot_trade_submit_timeout_ambiguous', rendered)

    def test_auto_trade_reconcile_spot_journal_verifies_against_external_tx_lookup(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        tx_dir = self.data_dir / 'tx-status'
        tx_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:external-decision': {
                    'entry_key': 'spot:external-decision',
                    'status': 'submitted',
                    'decision_id': 'external-decision',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'external-sig-1',
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (tx_dir / 'external-sig-1.json').write_text(json.dumps({'signature': 'external-sig-1', 'status': 'confirmed', 'confirmed': True}, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        self.assertEqual(payload['externalConfirmed'], 1)
        reconciled = json.loads(journal_path.read_text())
        entry = reconciled['entries']['spot:external-decision']
        self.assertEqual(entry['status'], 'verified')
        self.assertEqual(entry['verification_source'], 'external_tx_lookup')

    def test_auto_trade_reconcile_spot_journal_marks_wallet_observed_buy_without_signature(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        (self.data_dir / 'portfolio.json').write_text(json.dumps({
            'tokens': [{'symbol': 'SOL', 'id': 'So11111111111111111111111111111111111111112', 'amount': 1.25}],
            'totalValue': 100.0,
        }, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:wallet-decision': {
                    'entry_key': 'spot:wallet-decision',
                    'status': 'pending',
                    'decision_id': 'wallet-decision',
                    'symbol': 'SOL',
                    'side': 'buy'
                }
            }
        }, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(args=['--reconcile-spot-journal'])
        self.assertEqual(payload['walletObserved'], 1)
        reconciled = json.loads(journal_path.read_text())
        entry = reconciled['entries']['spot:wallet-decision']
        self.assertEqual(entry['status'], 'externally_observed')
        self.assertEqual(entry['verification_source'], 'wallet_state')

    def test_auto_trade_reconcile_spot_journal_detects_external_partial_fill(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        tx_dir = self.data_dir / 'tx-status'
        tx_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:partial-decision': {
                    'entry_key': 'spot:partial-decision',
                    'status': 'submitted',
                    'decision_id': 'partial-decision',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'partial-sig-1',
                    'out_amount': 100.0,
                    'expected_out_amount': 100.0,
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (tx_dir / 'partial-sig-1.json').write_text(json.dumps({
            'signature': 'partial-sig-1',
            'status': 'confirmed',
            'confirmed': True,
            'tokenTransfers': [
                {
                    'mint': 'So11111111111111111111111111111111111111112',
                    'toUserAccount': 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu',
                    'tokenAmount': 40.0
                }
            ],
            'result': {'outAmount': 999.0}
        }, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        self.assertEqual(payload['externalConfirmed'], 1)
        reconciled = json.loads(journal_path.read_text())
        entry = reconciled['entries']['spot:partial-decision']
        self.assertEqual(entry['status'], 'partially_verified')
        self.assertEqual(entry['verification_source'], 'external_tx_lookup')
        self.assertEqual(entry['exact_match_source'], 'wallet_token_delta')
        self.assertTrue(entry['partial_fill_detected'])
        self.assertEqual(entry['external_observed_out_amount'], 40.0)
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(report['risk_guardrails']['spot_journal_summary']['partial_fill_count'], 1)
        self.assertIn('spot_partial_fill_present', report['risk_guardrails']['blockers'])
        rendered = self._run_daily_analytics_report('2026-03-17T00:00:00.000Z')
        self.assertIn('spot_journal: ambiguous=0 stale=0 partial=1', rendered)
        self.assertIn('spot_event spot_trade_external_partial_fill_detected', rendered)
        conn = sqlite3.connect(self.db_path)
        try:
            event = conn.execute("SELECT event_type FROM system_events WHERE event_type='spot_trade_external_partial_fill_detected' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            conn.close()
        self.assertEqual(event[0], 'spot_trade_external_partial_fill_detected')

    def test_auto_trade_reconcile_spot_journal_uses_exact_usdc_delta_for_sell_confirmation(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        tx_dir = self.data_dir / 'tx-status'
        tx_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:sell-delta-decision': {
                    'entry_key': 'spot:sell-delta-decision',
                    'status': 'submitted',
                    'decision_id': 'sell-delta-decision',
                    'symbol': 'SOL',
                    'side': 'sell',
                    'signature': 'sell-delta-sig-1',
                    'out_amount': 25.0,
                    'expected_out_amount': 25.0,
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (tx_dir / 'sell-delta-sig-1.json').write_text(json.dumps({
            'signature': 'sell-delta-sig-1',
            'status': 'confirmed',
            'confirmed': True,
            'tokenTransfers': [
                {
                    'mint': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                    'toUserAccount': 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu',
                    'tokenAmount': 25.0
                }
            ],
            'result': {'outAmount': 2.0}
        }, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        self.assertEqual(payload['externalConfirmed'], 1)
        reconciled = json.loads(journal_path.read_text())
        entry = reconciled['entries']['spot:sell-delta-decision']
        self.assertEqual(entry['status'], 'verified')
        self.assertEqual(entry['exact_match_source'], 'wallet_token_delta')
        self.assertEqual(entry['external_observed_out_amount'], 25.0)

    def test_auto_trade_prune_spot_journal_archives_old_terminal_entries(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        archive_path = self.data_dir / 'spot_trade_journal.archive.jsonl'
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:old-verified': {
                    'entry_key': 'spot:old-verified',
                    'status': 'verified',
                    'verified_at': '2026-03-01T00:00:00.000Z',
                    'updated_at': '2026-03-01T00:00:00.000Z'
                },
                'spot:recent-verified': {
                    'entry_key': 'spot:recent-verified',
                    'status': 'verified',
                    'verified_at': '2099-03-17T00:00:00.000Z',
                    'updated_at': '2099-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        init_db(self.db_path)
        _, payload = self._run_auto_trade(args=['--prune-spot-journal', '7'])
        self.assertEqual(payload['pruned'], 1)
        remaining = json.loads(journal_path.read_text())
        self.assertIn('spot:recent-verified', remaining['entries'])
        self.assertNotIn('spot:old-verified', remaining['entries'])
        self.assertTrue(archive_path.exists())
        self.assertIn('spot:old-verified', archive_path.read_text())

    def test_daily_analytics_report_prints_compact_spot_recovery_health_section(self):
        init_db(self.db_path)
        health_path = self.data_dir / 'telegram-bridge' / 'spot_notifier_health_state.json'
        health_path.parent.mkdir(parents=True, exist_ok=True)
        health_path.write_text(json.dumps({'status': 'healthy', 'lag_events': 0, 'max_lag_events': 3}, indent=2))
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'top-1', 'symbol': 'SOL', 'status': 'open', 'severity': 'critical', 'priority_score': 100, 'assignee': 'alice', 'needs_reassignment': True}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'top-2', 'symbol': 'JUP', 'status': 'open', 'severity': 'high', 'priority_score': 90, 'assignee': None}),
        ]) + '\n')
        record_system_event(self.db_path, {
            'ts': '2026-03-17T01:00:00.000Z',
            'event_type': 'spot_trade_external_partial_fill_detected',
            'severity': 'warning',
            'message': 'External reconciliation detected possible partial fill for SOL',
            'source': 'auto-trade.mjs',
            'metadata': {'decision_id': 'spot-health-1', 'symbol': 'SOL'},
        })
        rendered = self._run_daily_analytics_report('2026-03-17T00:00:00.000Z')
        self.assertIn('Spot recovery health:', rendered)
        self.assertIn('status=degraded', rendered)
        self.assertIn('spot_partial_fill_present', rendered)
        self.assertIn('notifier_health: status=healthy lag_events=0 max_lag=3', rendered)
        self.assertIn('manual_review_open=2', rendered)
        self.assertIn('needs_reassignment_open=1', rendered)
        self.assertIn('assignee_backlog alice=1', rendered)
        self.assertIn('assignee_backlog unassigned=1', rendered)
        self.assertIn('top_review #1 [critical] stale_submitted decision=top-1 symbol=SOL assignee=alice', rendered)

    def test_stale_and_partial_anomalies_append_manual_review_queue(self):
        journal_path = self.data_dir / 'spot_trade_journal.json'
        tx_dir = self.data_dir / 'tx-status'
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        tx_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:stale-review': {
                    'entry_key': 'spot:stale-review',
                    'status': 'submitted',
                    'decision_id': 'stale-review',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                },
                'spot:partial-review': {
                    'entry_key': 'spot:partial-review',
                    'status': 'submitted',
                    'decision_id': 'partial-review',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'partial-review-sig',
                    'out_amount': 100.0,
                    'expected_out_amount': 100.0,
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (tx_dir / 'partial-review-sig.json').write_text(json.dumps({
            'signature': 'partial-review-sig',
            'status': 'confirmed',
            'confirmed': True,
            'tokenTransfers': [{'mint': 'So11111111111111111111111111111111111111112', 'toUserAccount': 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu', 'tokenAmount': 30.0}]
        }, indent=2))
        init_db(self.db_path)
        self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        lines = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        review_types = {item['review_type'] for item in lines}
        self.assertIn('stale_submitted', review_types)
        self.assertIn('external_partial_fill', review_types)
        for item in lines:
            self.assertEqual(item.get('owner'), 'operator')
            self.assertIn('journal_row_id', item)
            self.assertIn('related_event_type', item)

    def test_manual_review_queue_cli_supports_bulk_assign_and_bulk_snooze(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'one', 'status': 'open', 'severity': 'critical'}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'two', 'status': 'open', 'severity': 'high'}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'three', 'status': 'open', 'severity': 'high'}),
        ]) + '\n')
        result_assign = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'bulk-assign', '--queue-file', str(queue_path), '--severity', 'high', '--assignee', 'alice', '--note', 'bulk'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        payload_assign = json.loads(result_assign.stdout)
        self.assertEqual(payload_assign['updated'], 2)
        result_snooze = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'bulk-snooze', '--queue-file', str(queue_path), '--assignee', 'alice', '--note', 'pause'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        payload_snooze = json.loads(result_snooze.stdout)
        self.assertEqual(payload_snooze['updated'], 2)
        items = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        high_items = [item for item in items if item['decision_id'] in {'two', 'three'}]
        self.assertTrue(all(item['status'] == 'snoozed' for item in high_items))
        self.assertTrue(all(item['assignee'] == 'alice' for item in high_items))

    def test_manual_review_queue_cli_lists_resolves_snoozes_reopens_and_assigns_items(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'one', 'status': 'open', 'owner': 'operator', 'assignee': None, 'related_event_type': 'spot_trade_journal_stale_submitted', 'journal_row_id': 'spot:one'}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'two', 'status': 'open', 'owner': 'operator', 'assignee': None, 'related_event_type': 'spot_trade_external_partial_fill_detected', 'journal_row_id': 'spot:two'}),
        ]) + '\n')
        list_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'list', '--queue-file', str(queue_path), '--status', 'open'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        listed = json.loads(list_result.stdout)
        self.assertEqual(listed['count'], 2)
        self.assertEqual(listed['items'][0]['severity'], 'critical')
        summary_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'summary', '--queue-file', str(queue_path), '--status', 'open'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        summary = json.loads(summary_result.stdout)
        self.assertEqual(summary['count'], 2)
        self.assertEqual(summary['by_type']['stale_submitted'], 1)
        self.assertEqual(summary['by_severity']['critical'], 1)
        snooze_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'snooze', '--queue-file', str(queue_path), '--decision-id', 'two', '--note', 'later'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        snoozed = json.loads(snooze_result.stdout)
        self.assertEqual(snoozed['updated'], 1)
        reopen_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'reopen', '--queue-file', str(queue_path), '--decision-id', 'two', '--note', 'back'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        reopened = json.loads(reopen_result.stdout)
        self.assertEqual(reopened['updated'], 1)
        assign_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'assign', '--queue-file', str(queue_path), '--decision-id', 'two', '--assignee', 'alice', '--note', 'take-it'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        assigned = json.loads(assign_result.stdout)
        self.assertEqual(assigned['updated'], 1)
        claim_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'claim', '--queue-file', str(queue_path), '--decision-id', 'two', '--assignee', 'bob', '--note', 'mine'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        claimed = json.loads(claim_result.stdout)
        self.assertEqual(claimed['updated'], 1)
        unassign_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'unassign', '--queue-file', str(queue_path), '--decision-id', 'two', '--note', 'handoff'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        unassigned = json.loads(unassign_result.stdout)
        self.assertEqual(unassigned['updated'], 1)
        resolve_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'resolve', '--queue-file', str(queue_path), '--decision-id', 'one', '--resolution', 'resolved', '--note', 'checked'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        resolved = json.loads(resolve_result.stdout)
        self.assertEqual(resolved['updated'], 1)
        post = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        item_one = next(item for item in post if item['decision_id'] == 'one')
        item_two = next(item for item in post if item['decision_id'] == 'two')
        self.assertEqual(item_one['status'], 'resolved')
        self.assertEqual(item_one['resolution_note'], 'checked')
        self.assertEqual(item_two['status'], 'open')
        self.assertEqual(item_two['reopen_note'], 'back')
        self.assertIsNone(item_two['assignee'])
        self.assertEqual(item_two['assignment_note'], 'take-it')
        self.assertEqual(item_two['claim_note'], 'mine')
        self.assertEqual(item_two['unassign_note'], 'handoff')
        self.assertTrue(len(item_two.get('history', [])) >= 4)

    def test_manual_review_digest_reports_open_snoozed_assignment_and_sla_counts_in_dry_run(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'one', 'status': 'open', 'severity': 'critical', 'assignee': 'alice', 'queued_at': '2026-03-17T00:00:00+00:00'}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'two', 'status': 'open', 'severity': 'high', 'assignee': None}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'three', 'status': 'snoozed', 'severity': 'high', 'assignee': 'bob'}),
        ]) + '\n')
        result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_digest.py', '--queue-file', str(queue_path), '--dry-run'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload['status'], 'DRY_RUN')
        self.assertEqual(payload['open_count'], 2)
        summary_result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'summary', '--queue-file', str(queue_path), '--status', 'open'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        summary = json.loads(summary_result.stdout)
        self.assertEqual(summary['by_assignee']['alice'], 1)
        self.assertEqual(summary['by_assignee']['unassigned'], 1)
        self.assertEqual(summary['sla_breached_count'], 1)
        self.assertEqual(summary['sla_breached_by_assignee']['alice'], 1)

    def test_manual_review_queue_dedupes_identical_open_items(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        journal_path = self.data_dir / 'spot_trade_journal.json'
        tx_dir = self.data_dir / 'tx-status'
        tx_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'portfolio.json').write_text(json.dumps({'tokens': [], 'totalValue': 0}, indent=2))
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:dup-partial': {
                    'entry_key': 'spot:dup-partial',
                    'status': 'submitted',
                    'decision_id': 'dup-partial',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'dup-partial-sig',
                    'out_amount': 100.0,
                    'expected_out_amount': 100.0,
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (tx_dir / 'dup-partial-sig.json').write_text(json.dumps({
            'signature': 'dup-partial-sig', 'status': 'confirmed', 'confirmed': True,
            'tokenTransfers': [{'mint': 'So11111111111111111111111111111111111111112', 'toUserAccount': 'jTsP9QPb7b8XKhiexDCoA9DadkocsvFxgaabBCWxCZu', 'tokenAmount': 30.0}]
        }, indent=2))
        init_db(self.db_path)
        self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        self._run_auto_trade(extra_env={'AUTO_TRADER_TX_STATUS_DIR': str(tx_dir)}, args=['--reconcile-spot-journal'])
        lines = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)

    def test_auto_reconciliation_auto_closes_open_manual_review_item(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        journal_path = self.data_dir / 'spot_trade_journal.json'
        queue_path.write_text(json.dumps({'review_type': 'stale_submitted', 'entry_key': 'spot:auto-close', 'decision_id': 'auto-close', 'symbol': 'SOL', 'status': 'open'}) + '\n')
        journal_path.write_text(json.dumps({
            'version': 1,
            'entries': {
                'spot:auto-close': {
                    'entry_key': 'spot:auto-close',
                    'status': 'submitted',
                    'decision_id': 'auto-close',
                    'symbol': 'SOL',
                    'side': 'buy',
                    'signature': 'auto-close-sig',
                    'submitted_at': '2026-03-17T00:00:00.000Z'
                }
            }
        }, indent=2))
        (self.data_dir / 'auto_trades.json').write_text(json.dumps([
            {'ts': '2026-03-17T00:01:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'live', 'decisionId': 'auto-close', 'signature': 'auto-close-sig'}
        ], indent=2))
        init_db(self.db_path)
        self._run_auto_trade(args=['--reconcile-spot-journal'])
        items = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        self.assertEqual(items[0]['status'], 'auto_resolved')
        self.assertEqual(items[0]['auto_resolution_reason'], 'journal_verified')

    def test_expire_snoozed_items_reopens_old_snoozes(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'old-snooze', 'status': 'snoozed', 'snoozed_at': '2026-03-17T00:00:00+00:00'}),
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'recent-snooze', 'status': 'snoozed', 'snoozed_at': '2999-03-17T00:00:00+00:00'}),
        ]) + '\n')
        result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_queue.py', 'expire-snoozed', '--queue-file', str(queue_path), '--hours', '24'],
            check=True, capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload['reopened'], 1)
        items = [json.loads(line) for line in queue_path.read_text().splitlines() if line.strip()]
        old_item = next(item for item in items if item['decision_id'] == 'old-snooze')
        recent_item = next(item for item in items if item['decision_id'] == 'recent-snooze')
        self.assertEqual(old_item['status'], 'open')
        self.assertEqual(old_item['auto_reopen_reason'], 'snooze_expired')
        self.assertEqual(recent_item['status'], 'snoozed')

    def test_show_spot_review_command_parsing_supports_new_commands(self):
        from trading_system.telegram_trade_reply_bridge import parse_command
        self.assertEqual(parse_command('LIST_SPOT_REVIEWS HIGH')['severity'], 'high')
        self.assertEqual(parse_command('SHOW_SPOT_REVIEW 7')['queue_id'], 7)
        parsed = parse_command('ASSIGN_SPOT_REVIEW 3 alice take-it')
        self.assertEqual(parsed['action'], 'assign_spot_review')
        self.assertEqual(parsed['queue_id'], 3)
        self.assertEqual(parsed['assignee'], 'alice')
        self.assertEqual(parsed['note'], 'take-it')
        self.assertEqual(parse_command('LIST_SPOT_REVIEWS CRITICAL')['severity'], 'critical')
        self.assertEqual(parse_command('MY_SPOT_REVIEWS alice')['action'], 'my_spot_reviews')
        self.assertEqual(parse_command('UNASSIGNED_SPOT_REVIEWS')['action'], 'unassigned_spot_reviews')
        self.assertEqual(parse_command('ASSIGNED_SPOT_REVIEWS alice')['action'], 'assigned_spot_reviews')
        self.assertEqual(parse_command('NEEDS_REASSIGNMENT_SPOT_REVIEWS')['action'], 'needs_reassignment_spot_reviews')
        self.assertEqual(parse_command('STALE_ASSIGNED_SPOT_REVIEWS')['action'], 'stale_assigned_spot_reviews')
        self.assertEqual(parse_command('CLAIM_SPOT_REVIEW 4')['action'], 'claim_spot_review')
        self.assertEqual(parse_command('UNASSIGN_SPOT_REVIEW 4')['action'], 'unassign_spot_review')
        self.assertEqual(parse_command('MY_SPOT_REVIEWS')['action'], 'my_spot_reviews')
        self.assertEqual(parse_command('BULK_ASSIGN_SPOT_REVIEWS HIGH alice')['action'], 'bulk_assign_spot_reviews')
        self.assertEqual(parse_command('BULK_SNOOZE_SPOT_REVIEWS alice')['action'], 'bulk_snooze_spot_reviews')
        self.assertEqual(parse_command('ACK_SPOT_DIGEST')['action'], 'ack_spot_digest')
        self.assertEqual(parse_command('MUTE_SPOT_DIGEST 12')['action'], 'mute_spot_digest')
        self.assertEqual(parse_command('YES')['action'], 'approve_spot_simple')
        self.assertEqual(parse_command('NO')['action'], 'reject_spot_simple')

    def test_manual_review_alert_check_degrades_on_thresholds(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text('\n'.join([
            json.dumps({'review_type': 'stale_submitted', 'decision_id': 'one', 'status': 'open', 'severity': 'critical', 'assignee': None, 'queued_at': '2026-03-17T00:00:00+00:00'}),
            json.dumps({'review_type': 'external_partial_fill', 'decision_id': 'two', 'status': 'open', 'severity': 'high', 'assignee': 'alice', 'queued_at': '2026-03-17T00:00:00+00:00'}),
        ]) + '\n')
        result = subprocess.run(
            ['python', '/home/brimigs/trading_system/spot_manual_review_alert_check.py', '--queue-file', str(queue_path), '--max-unassigned-open', '0', '--max-sla-breached', '0'],
            capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': '/home/brimigs'}
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload['status'], 'degraded')
        self.assertIn('unassigned_open_threshold', payload['blockers'])
        self.assertIn('sla_breached_threshold', payload['blockers'])
        self.assertNotEqual(result.returncode, 0)

    def test_show_spot_review_detail_includes_history_and_reassignment_fields(self):
        queue_path = self.data_dir / 'spot_recovery_manual_review.jsonl'
        queue_path.write_text(json.dumps({
            'queue_id': 1,
            'review_type': 'stale_submitted',
            'decision_id': 'detail-one',
            'status': 'open',
            'severity': 'critical',
            'priority_score': 100,
            'owner': 'operator',
            'assignee': 'alice',
            'queued_at': '2026-03-17T00:00:00+00:00',
            'history': [
                {'ts': '2026-03-17T00:00:00+00:00', 'action': 'assign:alice', 'actor': 'operator', 'note': 'take-it'}
            ]
        }) + '\n')
        from trading_system.spot_manual_review_queue import load_items
        item = load_items(queue_path)[0]
        self.assertTrue(item['needs_reassignment'])
        self.assertEqual(item['history'][0]['action'], 'assign:alice')

    def test_record_helpers_insert_rows(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(
            self.db_path,
            {'ts': '2026-03-17T21:45:37.888Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'SOL': {'price': 95}}},
            ['test alert'],
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T22:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'sizeUsd': 1.5, 'strategyTag': 'mean_reversion_near_buy', 'amount': 1.0},
        )
        record_signal_candidate(
            self.db_path,
            {'ts': '2026-03-17T22:00:00.000Z', 'source': 'monitor.mjs', 'symbol': 'SOL', 'signal_type': 'near_buy', 'status': 'candidate', 'side': 'buy', 'price': 95.0},
        )
        record_whale_observation(
            self.db_path,
            {'ts': '2026-03-17T22:05:00.000Z', 'wallet_label': 'BONK whale A', 'wallet_address': 'abc', 'focus_symbol': 'BONK', 'recent_tx_count': 12, 'self_fee_payer_count': 3, 'summary': 'active'},
        )
        record_system_event(
            self.db_path,
            {'ts': '2026-03-17T22:06:00.000Z', 'event_type': 'kill_switch', 'severity': 'warning', 'message': 'daily trade cap reached'},
        )
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM price_snapshots').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM alerts').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM auto_trades').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM signal_candidates').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM whale_observations').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM system_events').fetchone()[0], 1)
        finally:
            conn.close()

    def test_trusted_trade_summary_flags_legacy_buy_amount_mismatch(self):
        init_db(self.db_path)
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:00:00.000Z',
                'symbol': 'BONK',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'amount': 1.5,
                'sizeUsd': 1.5,
                'outAmount': '223880',
                'strategyTag': 'mean_reversion_near_buy',
            },
        )
        summary = get_trusted_trade_summary(self.db_path)
        self.assertEqual(summary['trusted_trade_count'], 0)
        self.assertEqual(summary['untrusted_trade_count'], 1)
        self.assertEqual(summary['untrusted_rows'][0]['trust_reason'], 'legacy_buy_amount_output_mismatch')

    def test_trade_performance_split_groups_by_mode_and_realized_pnl(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 12.0, 'strategyTag': 'take_profit_exit'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T02:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'live', 'simulated': False, 'amount': 2.0, 'sizeUsd': 4.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T03:00:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'live', 'simulated': False, 'amount': 1.0, 'sizeUsd': 3.0, 'strategyTag': 'take_profit_exit'})
        summary = get_trade_performance_split(self.db_path)
        self.assertEqual(summary['paper']['trade_count'], 2)
        self.assertEqual(summary['paper']['notional_usd'], 22.0)
        self.assertEqual(summary['paper']['realized_pnl_usd'], 2.0)
        self.assertEqual(summary['live']['trade_count'], 2)
        self.assertEqual(summary['live']['realized_pnl_usd'], 1.0)

    def test_daily_analytics_trade_performance_respects_since_ts(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-16T23:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-16T23:30:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 12.0, 'strategyTag': 'take_profit_exit'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 4.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T02:00:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 9.0, 'strategyTag': 'take_profit_exit'})

        full_summary = get_trade_performance_split(self.db_path)
        filtered_summary = get_trade_performance_split(self.db_path, '2026-03-17T00:00:00.000Z')
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')

        self.assertEqual(full_summary['paper']['trade_count'], 4)
        self.assertEqual(full_summary['paper']['notional_usd'], 35.0)
        self.assertEqual(full_summary['paper']['realized_pnl_usd'], 7.0)

        self.assertEqual(filtered_summary['paper']['trade_count'], 2)
        self.assertEqual(filtered_summary['paper']['notional_usd'], 13.0)
        self.assertEqual(filtered_summary['paper']['realized_pnl_usd'], 5.0)

        self.assertEqual(report['trade_performance']['paper']['trade_count'], 2)
        self.assertEqual(report['trade_performance']['paper']['notional_usd'], 13.0)
        self.assertEqual(report['trade_performance']['paper']['realized_pnl_usd'], 5.0)

    def test_record_auto_trade_persists_snake_case_inputs(self):
        init_db(self.db_path)
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:00:00.000Z',
                'symbol': 'WIF',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'amount': 0.0,
                'size_usd': 5.0,
                'out_amount': '2.5',
                'expected_out_amount': '2.6',
                'quote_price_impact': 0.15,
                'strategy_family': 'mean_reversion',
                'decision_id': 'dec-123',
            },
        )
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT size_usd, out_amount, expected_out_amount, quote_price_impact, strategy_family, decision_id FROM auto_trades WHERE symbol='WIF'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 5.0)
        self.assertEqual(row[1], '2.5')
        self.assertEqual(row[2], '2.6')
        self.assertEqual(row[3], 0.15)
        self.assertEqual(row[4], 'mean_reversion')
        self.assertEqual(row[5], 'dec-123')

    def test_record_auto_trade_persists_explicit_realized_pnl_and_cost_basis(self):
        init_db(self.db_path)
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:10:00.000Z',
                'symbol': 'SOL',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'product_type': 'perps',
                'strategy_family': 'tiny_live_pilot',
                'amount': 0.05,
                'size_usd': 4.2,
                'realized_pnl_usd': 0.7,
                'cost_basis_usd': 3.5,
                'signature': 'paper-perp-close-1',
            },
        )
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT product_type, strategy_family, realized_pnl_usd, cost_basis_usd FROM auto_trades WHERE signature='paper-perp-close-1'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 'perps')
        self.assertEqual(row[1], 'tiny_live_pilot')
        self.assertEqual(row[2], 0.7)
        self.assertEqual(row[3], 3.5)

    def test_buy_trade_shape_uses_output_quantity_for_inventory_and_open_positions(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(
            self.db_path,
            {'ts': '2026-03-17T00:10:00.000Z', 'wallet': {'sol': 1, 'usdc': 20, 'totalUsd': 30}, 'prices': {'BONK': {'price': 0.01}}},
            [],
        )
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:00:00.000Z',
                'symbol': 'BONK',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'amount': 150.0,
                'sizeUsd': 1.5,
                'outAmount': '150',
                'expectedOutAmount': '155',
                'signature': 'paper-buy-bonk',
                'strategyTag': 'mean_reversion_near_buy',
            },
        )
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT amount, size_usd, out_amount, expected_out_amount FROM auto_trades WHERE signature='paper-buy-bonk'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 150.0)
        self.assertEqual(row[1], 1.5)
        self.assertEqual(row[2], '150')
        self.assertEqual(row[3], '155')
        positions = get_open_positions(self.db_path)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]['symbol'], 'BONK')
        self.assertEqual(positions[0]['remaining_amount'], 150.0)
        self.assertEqual(positions[0]['cost_basis_usd'], 1.5)
        self.assertEqual(positions[0]['market_value_usd'], 1.5)
        self.assertEqual(positions[0]['unrealized_pnl_usd'], 0.0)

    def test_sell_trade_shape_persists_token_amount_and_usd_proceeds_for_fifo_pnl(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(
            self.db_path,
            {'ts': '2026-03-17T00:20:00.000Z', 'wallet': {'sol': 1, 'usdc': 20, 'totalUsd': 30}, 'prices': {'BONK': {'price': 0.12}}},
            [],
        )
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:00:00.000Z',
                'symbol': 'BONK',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'amount': 1.0,
                'sizeUsd': 10.0,
                'outAmount': '100',
                'expectedOutAmount': '102',
                'signature': 'paper-buy-bonk-2',
                'strategyTag': 'mean_reversion_near_buy',
            },
        )
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T00:05:00.000Z',
                'symbol': 'BONK',
                'side': 'sell',
                'mode': 'paper',
                'simulated': True,
                'amount': 40.0,
                'sizeUsd': 5.0,
                'outAmount': '5.0',
                'expectedOutAmount': '5.2',
                'signature': 'paper-sell-bonk-2',
                'strategyTag': 'take_profit_exit',
            },
        )
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT amount, size_usd, out_amount, expected_out_amount, realized_pnl_usd, cost_basis_usd FROM auto_trades WHERE signature='paper-sell-bonk-2'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 40.0)
        self.assertEqual(row[1], 5.0)
        self.assertEqual(row[2], '5.0')
        self.assertEqual(row[3], '5.2')
        self.assertEqual(row[4], 1.0)
        self.assertEqual(row[5], 4.0)
        positions = get_open_positions(self.db_path)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]['remaining_amount'], 60.0)
        self.assertEqual(positions[0]['cost_basis_usd'], 6.0)
        self.assertAlmostEqual(positions[0]['market_value_usd'], 7.2)
        self.assertAlmostEqual(positions[0]['unrealized_pnl_usd'], 1.2)

    def test_oversize_sell_does_not_persist_valid_realized_pnl(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 30.0})
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT realized_pnl_usd, cost_basis_usd FROM auto_trades WHERE symbol='SOL' AND side='sell'").fetchone()
        finally:
            conn.close()
        self.assertIsNone(row[0])
        self.assertEqual(row[1], 10.0)
        self.assertEqual(get_open_positions(self.db_path), [])
        self.assertEqual(get_accounting_audit(self.db_path)['status'], 'failing')

    def test_spot_analytics_ignore_perp_rows(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'product_type': 'spot', 'amount': 1.0, 'sizeUsd': 10.0, 'strategyTag': 'mean_reversion_near_buy', 'price': 10.0, 'expectedOutAmount': 1.01, 'outAmount': 1.0})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'product_type': 'spot', 'amount': 1.0, 'sizeUsd': 12.0, 'strategyTag': 'take_profit_exit', 'price': 12.0})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T02:00:00.000Z', 'symbol': 'SOL-PERP', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'product_type': 'perps', 'amount': 5.0, 'sizeUsd': 500.0, 'strategyTag': 'perp_momentum', 'price': 100.0, 'expectedOutAmount': 5.0, 'outAmount': 4.0})
        conn = sqlite3.connect(self.db_path)
        try:
            spot_sell = conn.execute("SELECT realized_pnl_usd FROM auto_trades WHERE symbol='SOL' AND side='sell' AND COALESCE(product_type, 'spot')='spot'").fetchone()
            perp_sell = conn.execute("SELECT realized_pnl_usd FROM auto_trades WHERE symbol='SOL-PERP' AND side='sell' AND product_type='perps'").fetchone()
        finally:
            conn.close()
        self.assertEqual(spot_sell[0], 2.0)
        self.assertIsNone(perp_sell[0])
        self.assertEqual(get_open_positions(self.db_path), [])
        self.assertEqual(get_accounting_audit(self.db_path)['status'], 'clean')

        trade_split = get_trade_performance_split(self.db_path)
        self.assertEqual(trade_split['paper']['trade_count'], 2)
        self.assertEqual(trade_split['paper']['notional_usd'], 22.0)
        self.assertEqual(trade_split['paper']['realized_pnl_usd'], 2.0)

        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(report['trade_performance']['paper']['trade_count'], 2)
        self.assertEqual(report['trade_performance']['paper']['notional_usd'], 22.0)
        self.assertEqual(report['trade_performance']['paper']['realized_pnl_usd'], 2.0)
        self.assertEqual({row['strategy_tag'] for row in report['strategy_breakdown']}, {'mean_reversion_near_buy', 'take_profit_exit'})
        self.assertEqual({row['symbol'] for row in report['symbol_breakdown']}, {'SOL'})
        self.assertEqual(len(report['execution_quality']), 1)
        self.assertEqual(report['execution_quality'][0]['mode'], 'paper')
        self.assertEqual(report['execution_quality'][0]['trade_count'], 2)

    def test_daily_analytics_includes_symbol_breakdown_unrealized_deduped_whales_open_positions_health_order_flow_exec_quality_and_recommendations(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(
            self.db_path,
            {
                'ts': '2026-03-17T21:45:37.888Z',
                'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3},
                'prices': {
                    'SOL': {'price': 95},
                    'BONK': {'price': 2.0, 'holdingAmount': 1.0, 'holdingValue': 2.0}
                },
            },
            ['test alert'],
        )
        record_auto_trade(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 1.0, 'strategyTag': 'mean_reversion_near_buy', 'expectedOutAmount': 1.1, 'outAmount': 1.0})
        record_whale_observation(self.db_path, {'ts': '2026-03-17T22:01:00.000Z', 'wallet_label': 'BONK whale A', 'wallet_address': 'abc', 'focus_symbol': 'BONK', 'recent_tx_count': 10, 'self_fee_payer_count': 2, 'summary': 'older'})
        record_whale_observation(self.db_path, {'ts': '2026-03-17T22:02:00.000Z', 'wallet_label': 'BONK whale A', 'wallet_address': 'abc', 'focus_symbol': 'BONK', 'recent_tx_count': 20, 'self_fee_payer_count': 3, 'summary': 'newer'})
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertIn('strategy_breakdown', report)
        self.assertTrue(any(row['strategy_tag'] == 'mean_reversion_near_buy' for row in report['strategy_breakdown']))
        self.assertEqual(len(report['top_whales']), 1)
        self.assertEqual(report['top_whales'][0]['summary'], 'newer')
        self.assertAlmostEqual(report['portfolio_pnl']['unrealized_pnl_usd'], 1.0)
        self.assertTrue(any(row['symbol'] == 'BONK' for row in report['open_positions']))
        self.assertTrue(any(row['symbol'] == 'BONK' for row in report['symbol_breakdown']))
        self.assertTrue(any(row['strategy_tag'] == 'mean_reversion_near_buy' for row in report['strategy_health']))
        self.assertTrue(any(row['symbol'] == 'BONK' for row in report['order_flow_signals']))
        self.assertTrue(any(row['mode'] == 'paper' for row in report['execution_quality']))
        self.assertIn('accounting_audit', report)
        self.assertEqual(report['accounting_audit']['status'], 'clean')
        self.assertTrue(any('open positions' in r.lower() for r in report['recommendations']))

    def test_strategy_risk_controls_pause_losing_strategy(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'live', 'simulated': False, 'amount': 1.0, 'sizeUsd': 5.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'live', 'simulated': False, 'amount': 1.0, 'sizeUsd': 3.0, 'strategyTag': 'take_profit_exit'})
        controls = get_strategy_risk_controls(self.db_path, min_trades=2, min_realized_pnl_usd=0.0)
        paused = {row['strategy_tag'] for row in controls['paused_strategies']}
        self.assertIn('mean_reversion_near_buy', paused)

    def test_strategy_execution_policy_respects_threshold_arguments(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 1.0}}}, [])
        record_signal_candidate(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'source': 'monitor.mjs', 'symbol': 'BONK', 'signal_type': 'near_buy', 'strategy_tag': 'mean_reversion_near_buy', 'side': 'buy', 'price': 1.0, 'status': 'candidate', 'metadata': {'planned_size_usd': 1.0}})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'live', 'simulated': False, 'amount': 1.0, 'sizeUsd': 5.0, 'strategyTag': 'mean_reversion_near_buy'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'live', 'simulated': False, 'amount': 1.0, 'sizeUsd': 3.0, 'strategyTag': 'take_profit_exit'})
        policy_loose = get_strategy_execution_policy(self.db_path, min_trades=2, min_realized_pnl_usd=-3.0)
        policy_strict = get_strategy_execution_policy(self.db_path, min_trades=2, min_realized_pnl_usd=0.0)
        paused_loose = {row['strategy_tag'] for row in policy_loose['paused_strategies']}
        paused_strict = {row['strategy_tag'] for row in policy_strict['paused_strategies']}
        self.assertNotIn('mean_reversion_near_buy', paused_loose)
        self.assertIn('mean_reversion_near_buy', paused_strict)

    def test_strategy_execution_policy_can_pause_paper_near_buy_probe(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 100.0, 'sizeUsd': 5.0, 'strategyTag': 'paper_near_buy_probe'})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 100.0, 'sizeUsd': 3.0, 'strategyTag': 'paper_stop_loss_exit'})
        policy = get_strategy_execution_policy(self.db_path, min_trades=2, min_realized_pnl_usd=0.0)
        paused = {row['strategy_tag'] for row in policy['paused_strategies']}
        self.assertIn('paper_near_buy_probe', paused)

    def test_open_positions_helper(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T21:45:37.888Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 2.0}}}, [])
        record_auto_trade(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 1.0, 'strategyTag': 'mean_reversion_near_buy'})
        positions = get_open_positions(self.db_path)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]['symbol'], 'BONK')
        self.assertEqual(positions[0]['product_type'], 'spot')

    def test_zero_notional_buy_does_not_persist_open_position(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(
            self.db_path,
            {'ts': '2026-03-17T21:45:37.888Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'JTO': {'price': 2.0}}},
            [],
        )
        record_auto_trade(
            self.db_path,
            {
                'ts': '2026-03-17T22:00:00.000Z',
                'symbol': 'JTO',
                'side': 'buy',
                'mode': 'paper',
                'simulated': True,
                'amount': 3.0,
                'sizeUsd': 0.0,
                'strategyTag': 'mean_reversion_near_buy',
            },
        )
        self.assertEqual(get_open_positions(self.db_path), [])
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM open_positions WHERE symbol='JTO'").fetchone()[0], 0)
            row = conn.execute(
                "SELECT realized_pnl_usd, cost_basis_usd FROM auto_trades WHERE symbol='JTO' AND side='buy'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], 0.0)
        self.assertIsNone(row[1])

    def test_perp_helpers_and_report(self):
        init_db(self.db_path)
        record_perp_market_snapshot(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': 2.5, 'highUsd24h': 101.0, 'lowUsd24h': 95.0, 'volumeUsd24h': 12345.0})
        record_perp_account_snapshot(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'wallet_address': 'wallet', 'open_position_count': 1, 'open_notional_usd': 20.0, 'unrealized_pnl_usd': 1.5, 'margin_used_usd': 10.0, 'equity_estimate_usd': 100.0})
        upsert_perp_position(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'position_key': 'pos-1', 'status': 'open', 'asset': 'SOL', 'side': 'long', 'size_usd': 20.0, 'notional_usd': 20.0, 'margin_used_usd': 10.0, 'leverage': 2.0, 'entry_price_usd': 100.0, 'mark_price_usd': 101.0, 'liq_price_usd': 80.0, 'unrealized_pnl_usd': 1.5, 'mode': 'paper'})
        record_perp_order(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'order_key': 'ord-1', 'position_key': 'pos-1', 'asset': 'SOL', 'side': 'long', 'order_type': 'market', 'status': 'filled', 'size_usd': 20.0, 'mode': 'paper'})
        record_perp_fill(self.db_path, {'ts': '2026-03-17T22:01:00.000Z', 'position_key': 'pos-1', 'order_key': 'ord-1', 'asset': 'SOL', 'side': 'long', 'action': 'open', 'price_usd': 100.0, 'size_usd': 20.0, 'fees_usd': 0.1, 'funding_usd': 0.0, 'realized_pnl_usd': 0.0, 'mode': 'paper'})
        record_risk_event(self.db_path, {'ts': '2026-03-17T22:01:30.000Z', 'product_type': 'perps', 'severity': 'warning', 'event_type': 'liq_buffer_check', 'message': 'buffer healthy'})
        perp_positions = get_perp_open_positions(self.db_path)
        self.assertEqual(len(perp_positions), 1)
        self.assertEqual(perp_positions[0]['asset'], 'SOL')
        perp_summary = get_perp_summary(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(len(perp_summary['markets']), 1)
        self.assertEqual(perp_summary['risk_summary']['open_position_count'], 1)
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertIn('perp_market_overview', report)
        self.assertIn('perp_position_summary', report)
        self.assertIn('perp_trade_performance', report)
        self.assertIn('perp_risk_summary', report)
        self.assertIn('combined_exposure_summary', report)
        self.assertTrue(any(row['asset'] == 'SOL' for row in report['perp_market_overview']))

    def test_record_perp_fill_is_idempotent_for_duplicate_payload_retries(self):
        init_db(self.db_path)
        payload = {
            'ts': '2026-03-17T22:01:00.000Z',
            'position_key': 'pos-1',
            'order_key': 'ord-1',
            'asset': 'SOL',
            'side': 'long',
            'action': 'open',
            'price_usd': 100.0,
            'size_usd': 20.0,
            'fees_usd': 0.1,
            'funding_usd': 0.0,
            'realized_pnl_usd': 0.0,
            'mode': 'paper',
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'decision_id': 'decision-1',
        }

        record_perp_fill(self.db_path, payload)
        record_perp_fill(self.db_path, dict(payload))

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                'SELECT COUNT(*) AS row_count, COUNT(fill_key) AS keyed_count, MIN(fill_key), MAX(fill_key) FROM perp_fills'
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], row[3])

        perp_summary = get_perp_summary(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(perp_summary['trade_performance'][0]['fill_count'], 1)
        self.assertEqual(perp_summary['trade_performance'][0]['notional_usd'], 20.0)
        self.assertEqual(perp_summary['trade_performance'][0]['fees_usd'], 0.1)

    def test_record_perp_fill_prefers_explicit_fill_key_for_deduping(self):
        init_db(self.db_path)
        record_perp_fill(
            self.db_path,
            {
                'fill_key': 'fill-123',
                'ts': '2026-03-17T22:01:00.000Z',
                'position_key': 'pos-explicit-1',
                'order_key': 'ord-explicit-1',
                'asset': 'ETH',
                'side': 'long',
                'action': 'open',
                'price_usd': 2100.0,
                'size_usd': 12.0,
                'fees_usd': 0.08,
                'funding_usd': 0.0,
                'realized_pnl_usd': 0.0,
                'mode': 'paper',
            },
        )
        record_perp_fill(
            self.db_path,
            {
                'fill_key': 'fill-123',
                'ts': '2026-03-17T22:01:05.000Z',
                'position_key': 'pos-explicit-1',
                'order_key': 'ord-explicit-retry',
                'asset': 'ETH',
                'side': 'long',
                'action': 'open',
                'price_usd': 2100.0,
                'size_usd': 999.0,
                'fees_usd': 9.99,
                'funding_usd': 0.0,
                'realized_pnl_usd': 0.0,
                'mode': 'paper',
            },
        )

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute('SELECT COUNT(*), MIN(fill_key), MAX(size_usd), MAX(fees_usd) FROM perp_fills').fetchone()
        finally:
            conn.close()

        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 'fill-123')
        self.assertEqual(row[2], 12.0)
        self.assertEqual(row[3], 0.08)

    def test_record_perp_fill_dedupes_when_strategy_metadata_changes(self):
        init_db(self.db_path)
        payload = {
            'ts': '2026-03-17T22:01:00.000Z',
            'position_key': 'pos-1',
            'order_key': 'ord-1',
            'asset': 'SOL',
            'side': 'short',
            'action': 'open',
            'price_usd': 100.0,
            'size_usd': 20.0,
            'fees_usd': 0.1,
            'funding_usd': 0.0,
            'realized_pnl_usd': 0.0,
            'mode': 'paper',
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'decision_id': 'decision-1',
        }

        record_perp_fill(self.db_path, payload)
        record_perp_fill(
            self.db_path,
            {
                **payload,
                'strategy_tag': 'tiny_live_pilot_perp_short_failed_bounce',
                'decision_id': 'decision-1-retry-with-enriched-metadata',
            },
        )

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute('SELECT COUNT(*), MIN(fill_key), MAX(fill_key) FROM perp_fills').fetchone()
        finally:
            conn.close()

        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], row[2])

    def test_record_perp_fill_backfills_legacy_table_and_dedupes_retry(self):
        init_db(self.db_path)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                '''
                ALTER TABLE perp_fills RENAME TO perp_fills_with_fill_key;
                CREATE TABLE perp_fills (
                    id INTEGER PRIMARY KEY,
                    ts TEXT NOT NULL,
                    position_key TEXT,
                    order_key TEXT,
                    asset TEXT NOT NULL,
                    side TEXT NOT NULL,
                    action TEXT NOT NULL,
                    price_usd REAL,
                    size_usd REAL,
                    fees_usd REAL,
                    funding_usd REAL,
                    realized_pnl_usd REAL,
                    mode TEXT NOT NULL,
                    strategy_tag TEXT,
                    decision_id TEXT,
                    raw_json TEXT
                );
                DROP TABLE perp_fills_with_fill_key;
                '''
            )
            conn.commit()
            cols = [row[1] for row in conn.execute('PRAGMA table_info(perp_fills)').fetchall()]
        finally:
            conn.close()
        self.assertNotIn('fill_key', cols)

        payload = {
            'ts': '2026-03-17T22:01:00.000Z',
            'position_key': 'legacy-pos-1',
            'order_key': 'legacy-ord-1',
            'asset': 'BTC',
            'side': 'short',
            'action': 'close',
            'price_usd': 65000.0,
            'size_usd': 15.0,
            'fees_usd': 0.25,
            'funding_usd': -0.05,
            'realized_pnl_usd': 1.5,
            'mode': 'paper',
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'decision_id': 'decision-legacy-1',
        }

        record_perp_fill(self.db_path, payload)
        record_perp_fill(self.db_path, dict(payload))

        conn = sqlite3.connect(self.db_path)
        try:
            cols = [row[1] for row in conn.execute('PRAGMA table_info(perp_fills)').fetchall()]
            row = conn.execute(
                'SELECT COUNT(*) AS row_count, COUNT(fill_key) AS keyed_count, MIN(fill_key), MAX(fill_key) FROM perp_fills'
            ).fetchone()
        finally:
            conn.close()

        self.assertIn('fill_key', cols)
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], row[3])

        perp_summary = get_perp_summary(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(perp_summary['trade_performance'][0]['fill_count'], 1)
        self.assertEqual(perp_summary['trade_performance'][0]['realized_pnl_usd'], 1.5)
        self.assertEqual(perp_summary['trade_performance'][0]['funding_usd'], -0.05)

    def test_init_db_reconciles_duplicate_fill_keys_before_creating_unique_index(self):
        init_db(self.db_path)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                '''
                DROP INDEX IF EXISTS idx_perp_fills_fill_key;
                INSERT INTO perp_fills (
                    fill_key, ts, position_key, order_key, asset, side, action, price_usd, size_usd,
                    fees_usd, funding_usd, realized_pnl_usd, mode, strategy_tag, decision_id, raw_json
                ) VALUES
                    ('dup-fill', '2026-03-17T22:01:00.000Z', 'p1', 'o1', 'SOL', 'short', 'open', 100.0, 5.0, 0.01, 0.0, 0.0, 'paper', 's1', 'd1', '{}'),
                    ('dup-fill', '2026-03-17T22:02:00.000Z', 'p2', 'o2', 'ETH', 'short', 'open', 2000.0, 5.0, 0.01, 0.0, 0.0, 'paper', 's2', 'd2', '{}');
                '''
            )
            conn.commit()
        finally:
            conn.close()

        init_db(self.db_path)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT id, fill_key FROM perp_fills WHERE id IN (SELECT id FROM perp_fills ORDER BY id ASC LIMIT 2) ORDER BY id ASC"
            ).fetchall()
            indexes = conn.execute("PRAGMA index_list('perp_fills')").fetchall()
        finally:
            conn.close()

        self.assertEqual(rows[0][1], 'dup-fill')
        self.assertIsNone(rows[1][1])
        self.assertTrue(any(row[1] == 'idx_perp_fills_fill_key' and row[2] for row in indexes))

    def test_daily_analytics_recommends_when_accounting_not_clean(self):
        init_db(self.db_path)
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 5.0})
        record_auto_trade(self.db_path, {'ts': '2026-03-17T00:05:00.000Z', 'symbol': 'BONK', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 0.0})
        audit = get_accounting_audit(self.db_path)
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        self.assertEqual(audit['status'], 'failing')
        self.assertEqual(report['accounting_audit']['status'], 'failing')
        self.assertTrue(any('accounting audit is failing' in rec.lower() for rec in report['recommendations']))

    def test_signal_candidates_multi_horizon_and_comparison(self):
        init_db(self.db_path)
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 1.0}}}, [])
        record_signal_candidate(self.db_path, {'ts': '2026-03-17T22:00:00.000Z', 'source': 'monitor.mjs', 'symbol': 'BONK', 'signal_type': 'near_buy', 'strategy_tag': 'mean_reversion_near_buy', 'side': 'buy', 'price': 1.0, 'status': 'candidate', 'metadata': {'planned_size_usd': 1.0}})
        record_signal_candidate(self.db_path, {'ts': '2026-03-17T22:05:00.000Z', 'source': 'auto-trade.mjs', 'symbol': 'BONK', 'signal_type': 'near_buy', 'strategy_tag': 'paper_near_buy_probe', 'side': 'buy', 'price': 1.0, 'status': 'executed', 'metadata': {'planned_size_usd': 0.5}})
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T22:08:00.000Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 1.01}}}, [])
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T22:20:00.000Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 1.02}}}, [])
        record_snapshot_and_alerts(self.db_path, {'ts': '2026-03-17T23:00:00.000Z', 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {'BONK': {'price': 1.10}}}, [])
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        policy = get_strategy_execution_policy(self.db_path)
        self.assertTrue(any(row['horizon_minutes'] == 10 for row in report['candidate_outcomes_multi']))
        self.assertTrue(any(row['horizon_minutes'] == 60 for row in report['candidate_outcomes_multi']))
        self.assertTrue(any(row['signal_type'] == 'near_buy' for row in report['candidate_status_comparison']))
        self.assertTrue(any(row['signal_type'] == 'near_buy' and row['conversion_rate'] > 0 for row in report['candidate_conversion']))
        self.assertTrue(any(row['strategy_tag'] == 'mean_reversion_near_buy' for row in report['candidate_outcomes_by_strategy']))
        self.assertTrue(any(row['symbol'] == 'BONK' for row in report['candidate_outcomes_by_symbol']))
        self.assertTrue(any(row['signal_type'] == 'near_buy' and row['status'] == 'candidate' for row in report['missed_opportunities']))
        self.assertIn('promotion_rules', report)
        self.assertIn('symbol_promotion_rules', report)
        self.assertIn('execution_scores', report)
        self.assertIn('candidate_volume_by_symbol', report)
        self.assertIn('candidate_volume_by_symbol_side', report)
        self.assertTrue(any('sample' in row['decision_reason'].lower() for row in report['promotion_rules']))
        self.assertTrue(any(row['decision'] in {'monitor', 'insufficient_sample', 'promote'} for row in report['promotion_rules']))
        self.assertTrue(any(row['name'] == 'BONK' for row in report['symbol_promotion_rules']))
        self.assertTrue(any(row['symbol'] == 'BONK' and row['score'] >= 0 for row in report['execution_scores']))
        bonk_volume = next(row for row in report['candidate_volume_by_symbol'] if row['symbol'] == 'BONK')
        bonk_buy_volume = next(row for row in report['candidate_volume_by_symbol_side'] if row['symbol'] == 'BONK' and row['side'] == 'buy')
        self.assertEqual(bonk_volume['candidate_count'], 2)
        self.assertEqual(bonk_volume['executed_count'], 1)
        self.assertAlmostEqual(bonk_volume['candidate_notional_usd'], 1.5)
        self.assertEqual(bonk_buy_volume['recent_candidate_count'], 2)
        self.assertTrue(any(row['symbol'] == 'BONK' for row in policy['candidate_volume_by_symbol']))
        self.assertTrue(any(row['symbol'] == 'BONK' and row['side'] == 'buy' for row in policy['candidate_volume_by_symbol_side']))

    def test_phase3_gate_rules_emit_explicit_fields_and_report_sections(self):
        init_db(self.db_path)

        def add_candidate(symbol, strategy_tag, ts, future_ts, entry_price, future_price, tier):
            record_signal_candidate(
                self.db_path,
                {
                    'ts': ts,
                    'source': 'monitor.mjs',
                    'symbol': symbol,
                    'signal_type': 'near_buy',
                    'strategy_tag': strategy_tag,
                    'side': 'buy',
                    'price': entry_price,
                    'status': 'candidate',
                    'metadata': {'planned_size_usd': 1.0, 'tier': tier},
                },
            )
            record_snapshot_and_alerts(
                self.db_path,
                {'ts': future_ts, 'wallet': {'sol': 1, 'usdc': 2, 'totalUsd': 3}, 'prices': {symbol: {'price': future_price}}},
                [],
            )

        for i in range(5):
            add_candidate('JUP', 'quality_strategy', f'2026-03-17T00:0{i}:00.000Z', f'2026-03-17T00:3{i}:00.000Z', 1.0, 1.03, 1)
            add_candidate('BONK', 'bad_strategy', f'2026-03-17T01:0{i}:00.000Z', f'2026-03-17T01:3{i}:00.000Z', 1.0, 0.97, 3)
        for i in range(2):
            add_candidate('RAY', 'new_strategy', f'2026-03-17T02:0{i}:00.000Z', f'2026-03-17T02:3{i}:00.000Z', 1.0, 1.01, 1)

        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        policy = get_strategy_execution_policy(self.db_path)

        quality_rule = next(row for row in report['promotion_rules'] if row['name'] == 'quality_strategy')
        bad_rule = next(row for row in report['promotion_rules'] if row['name'] == 'bad_strategy')
        new_rule = next(row for row in report['promotion_rules'] if row['name'] == 'new_strategy')
        bonk_symbol_rule = next(row for row in report['symbol_promotion_rules'] if row['name'] == 'BONK')

        self.assertEqual(quality_rule['decision'], 'promote')
        self.assertEqual(bad_rule['decision'], 'demote')
        self.assertEqual(new_rule['decision'], 'insufficient_sample')
        self.assertIn('execution_score', quality_rule)
        self.assertIn('candidate_count', quality_rule)
        self.assertIn('sample_sufficient', quality_rule)
        self.assertGreaterEqual(quality_rule['execution_score'], quality_rule['execution_score_threshold'])
        self.assertLess(bad_rule['execution_score'], quality_rule['execution_score'])
        self.assertEqual(bonk_symbol_rule['decision'], 'demote')
        self.assertGreater(bonk_symbol_rule['symbol_tier_penalty'], 0)
        self.assertTrue(any(row['name'] == 'quality_strategy' for row in report['promoted_strategies']))
        self.assertTrue(any(row['name'] == 'bad_strategy' for row in report['demoted_strategies']))
        self.assertTrue(any(row['name'] == 'BONK' for row in report['demoted_symbols']))
        self.assertTrue(any(row['name'] == 'new_strategy' for row in report['insufficient_sample_strategies']))
        self.assertEqual(report['deployment_mode_recommendation'], 'paper_ranked_only')
        self.assertTrue(any(row['name'] == 'quality_strategy' for row in policy['promoted_strategies']))
        self.assertEqual(policy['deployment_mode_recommendation'], 'paper_ranked_only')

    def test_perp_executor_state_groups_recent_decisions_and_daily_metrics(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 104.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        for signal_type, score in [('perp_short_continuation', 82), ('perp_short_failed_bounce', 70), ('perp_no_trade', 40)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-sol-1',
                'candidate_key': f'decision-sol-1:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}},
            })
        record_perp_fill(self.db_path, {'ts': '2099-03-17T22:05:00.000Z', 'position_key': 'p1', 'order_key': 'o1', 'asset': 'SOL', 'side': 'sell', 'action': 'open', 'price_usd': 100.0, 'size_usd': 3.0, 'fees_usd': 0.01, 'funding_usd': 0.0, 'realized_pnl_usd': 0.0, 'mode': 'paper', 'strategy_tag': 'tiny_live_pilot_perp_short_continuation', 'decision_id': 'decision-sol-1'})
        record_auto_trade(self.db_path, {'ts': '2099-03-17T22:05:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'product_type': 'perps', 'strategy_family': 'tiny_live_pilot', 'sizeUsd': 3.0, 'amount': 0.03, 'strategyTag': 'tiny_live_pilot_perp_short_continuation'})
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        self.assertEqual(state['anchor_ts'], '2099-03-17T23:00:00Z')
        self.assertEqual(len(state['decisions']), 1)
        self.assertEqual(state['decisions'][0]['decision_id'], 'decision-sol-1')
        self.assertEqual(state['decisions'][0]['best_short_lane']['signal_type'], 'perp_short_continuation')
        self.assertEqual(state['daily_paper_metrics']['trade_count'], 1)
        self.assertEqual(state['daily_paper_metrics']['fill_count'], 1)

    def test_perp_executor_state_summarizes_trade_ready_blocked_and_no_trade_decisions(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:30:00.000Z'
        decision_specs = [
            ('decision-ready', [('perp_short_continuation', 82), ('perp_short_failed_bounce', 70), ('perp_no_trade', 40)]),
            ('decision-no-trade', [('perp_short_continuation', 74), ('perp_short_failed_bounce', 68), ('perp_no_trade', 92)]),
            ('decision-blocked', [('perp_short_continuation', 77), ('perp_short_failed_bounce', 63), ('perp_no_trade', 55)]),
        ]
        for idx, (decision_id, lanes) in enumerate(decision_specs):
            decision_ts = f'2099-03-17T22:3{idx}:00.000Z'
            for signal_type, score in lanes:
                record_signal_candidate(self.db_path, {
                    'ts': decision_ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'score_components': {'edge': score}},
                })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:35:00.000Z',
            {
                'ts': '2099-03-17T22:35:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'decision-blocked',
                    'symbol': 'SOL',
                    'selected_signal_type': 'perp_short_continuation',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 77,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 55,
                    'score_gap_vs_no_trade': 22,
                    'blocked_trade': True,
                    'block_reason': 'market_stale',
                },
                'entry_result': {'action': 'skip', 'decision_id': 'decision-blocked', 'reason': 'market_stale'},
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        readiness = state['decision_readiness']
        by_id = {row['decision_id']: row for row in state['decisions']}
        self.assertEqual(readiness['pending_decision_count'], 3)
        self.assertEqual(readiness['trade_ready_count'], 1)
        self.assertEqual(readiness['blocked_count'], 1)
        self.assertEqual(readiness['no_trade_selected_count'], 1)
        self.assertEqual(by_id['decision-ready']['operator_status'], 'trade_ready')
        self.assertEqual(by_id['decision-ready']['score_gap_vs_no_trade'], 42.0)
        self.assertEqual(by_id['decision-no-trade']['operator_status'], 'no_trade_selected')
        self.assertIn('no_trade_outscored_best_short', by_id['decision-no-trade']['operator_blockers'])
        self.assertEqual(by_id['decision-blocked']['operator_status'], 'blocked')
        self.assertIn('market_stale', by_id['decision-blocked']['operator_blockers'])
        self.assertEqual(readiness['top_blockers'][0]['blocker'], 'market_stale')
        self.assertTrue(any(row['decision_id'] == 'decision-ready' for row in readiness['top_opportunities']))

    def test_perp_executor_state_classifies_blocked_no_trade_fallback_as_blocked(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:00:00.000Z'
        for signal_type, score in [('perp_short_continuation', 84), ('perp_short_failed_bounce', 67), ('perp_no_trade', 41)]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-blocked-no-trade',
                'candidate_key': f'decision-blocked-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 6},
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:01:00.000Z',
            {
                'ts': '2099-03-17T22:01:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'decision-blocked-no-trade',
                    'symbol': 'SOL',
                    'selected_signal_type': 'perp_no_trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 84,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 41,
                    'score_gap_vs_no_trade': 43,
                    'blocked_trade': True,
                    'block_reason': 'market_stale',
                    'candidate_strategy': 'perp_short_continuation',
                },
                'entry_result': {
                    'action': 'no_trade',
                    'decision_id': 'decision-blocked-no-trade',
                    'symbol': 'SOL',
                    'signal_type': 'perp_no_trade',
                    'reason': 'paper_no_trade_due_to_market_stale',
                    'score': 41,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        readiness = state['decision_readiness']
        decision = next(row for row in state['decisions'] if row['decision_id'] == 'decision-blocked-no-trade')

        self.assertEqual(readiness['pending_decision_count'], 1)
        self.assertEqual(readiness['trade_ready_count'], 0)
        self.assertEqual(readiness['blocked_count'], 1)
        self.assertEqual(readiness['no_trade_selected_count'], 0)
        self.assertEqual(decision['operator_status'], 'blocked')
        self.assertEqual(decision['selected_signal_type'], 'perp_no_trade')
        self.assertIn('market_stale', decision['operator_blockers'])
        self.assertEqual(state['latest_candidate_choice']['candidate_strategy'], 'perp_short_continuation')

    def test_perp_executor_state_best_current_opportunity_prefers_trade_ready_over_newer_no_trade_cycle(self):
        init_db(self.db_path)
        sol_ts = '2099-03-17T22:30:00.000Z'
        btc_ts = '2099-03-17T22:40:00.000Z'

        for signal_type, score, metadata in [
            ('perp_short_continuation', 88, {
                'invalidation_price': 176.5,
                'stop_loss_price': 175.8,
                'take_profit_price': 168.2,
                'planned_size_usd': 125.0,
                'paper_notional_usd': 125.0,
                'market_age_minutes': 4,
            }),
            ('perp_short_failed_bounce', 74, {}),
            ('perp_no_trade', 41, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': sol_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 172.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-sol-ready',
                'candidate_key': f'decision-sol-ready:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })

        for signal_type, score in [('perp_short_continuation', 63), ('perp_short_failed_bounce', 58), ('perp_no_trade', 91)]:
            record_signal_candidate(self.db_path, {
                'ts': btc_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'chop',
                'decision_id': 'decision-btc-no-trade',
                'candidate_key': f'decision-btc-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 1},
            })

        self._record_perp_executor_cycle_event(
            '2099-03-17T22:41:00.000Z',
            {
                'ts': '2099-03-17T22:41:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'decision-btc-no-trade',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_no_trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 63,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 91,
                    'score_gap_vs_no_trade': -28,
                    'blocked_trade': False,
                },
                'entry_result': {
                    'action': 'no_trade',
                    'decision_id': 'decision-btc-no-trade',
                    'symbol': 'BTC',
                    'signal_type': 'perp_no_trade',
                    'reason': 'no_trade',
                    'score': 91,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')

        best = state['best_current_opportunity']
        self.assertEqual(best['decision_id'], 'decision-sol-ready')
        self.assertEqual(best['symbol'], 'SOL')
        self.assertEqual(best['selected_signal_type'], 'perp_short_continuation')
        self.assertEqual(best['operator_status'], 'trade_ready')
        self.assertEqual(best['score_gap_vs_no_trade'], 47.0)
        self.assertEqual(best['age_minutes'], 30.0)
        self.assertEqual(best['invalidation_price'], 176.5)
        self.assertEqual(best['stop_loss_price'], 175.8)
        self.assertEqual(best['take_profit_price'], 168.2)
        self.assertEqual(best['planned_size_usd'], 125.0)
        self.assertEqual(best['paper_notional_usd'], 125.0)
        self.assertEqual(best['market_age_minutes'], 4.0)
        self.assertEqual(state['latest_candidate_choice']['decision_id'], 'decision-btc-no-trade')
        self.assertEqual(state['latest_candidate_choice']['selected_signal_type'], 'perp_no_trade')
        self.assertEqual(state['decision_readiness']['best_current_opportunity']['decision_id'], 'decision-sol-ready')
        self.assertEqual(state['decision_readiness']['signal_age_limit_minutes'], 40)

    def test_perp_executor_state_blocks_stale_trade_ready_decisions_from_best_current_opportunity(self):
        init_db(self.db_path)
        stale_ts = '2099-03-17T21:50:00.000Z'
        fresh_no_trade_ts = '2099-03-17T22:40:00.000Z'

        for signal_type, score, metadata in [
            ('perp_short_continuation', 88, {'planned_size_usd': 125.0, 'paper_notional_usd': 125.0, 'market_age_minutes': 2}),
            ('perp_short_failed_bounce', 74, {}),
            ('perp_no_trade', 41, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': stale_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 172.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-sol-stale',
                'candidate_key': f'decision-sol-stale:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })

        for signal_type, score in [('perp_short_continuation', 63), ('perp_short_failed_bounce', 58), ('perp_no_trade', 91)]:
            record_signal_candidate(self.db_path, {
                'ts': fresh_no_trade_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'chop',
                'decision_id': 'decision-btc-fresh-no-trade',
                'candidate_key': f'decision-btc-fresh-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 1},
            })

        self._record_perp_executor_cycle_event(
            '2099-03-17T22:41:00.000Z',
            {
                'ts': '2099-03-17T22:41:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'decision-btc-fresh-no-trade',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_no_trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 63,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 91,
                    'score_gap_vs_no_trade': -28,
                    'blocked_trade': False,
                },
                'entry_result': {
                    'action': 'no_trade',
                    'decision_id': 'decision-btc-fresh-no-trade',
                    'symbol': 'BTC',
                    'signal_type': 'perp_no_trade',
                    'reason': 'no_trade',
                    'score': 91,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')

        stale_summary = next(row for row in state['decision_readiness']['decisions'] if row['decision_id'] == 'decision-sol-stale')
        self.assertEqual(stale_summary['operator_status'], 'blocked')
        self.assertIn('stale_signal', stale_summary['blockers'])
        self.assertEqual(state['best_current_opportunity']['decision_id'], 'decision-btc-fresh-no-trade')
        self.assertEqual(state['best_current_opportunity']['operator_status'], 'no_trade_selected')
        self.assertEqual(state['decision_readiness']['blocked_count'], 1)
        self.assertIn({'blocker': 'stale_signal', 'count': 1}, state['decision_readiness']['top_blockers'])

    def test_perp_executor_state_does_not_rank_market_stale_block_above_fresh_no_trade(self):
        init_db(self.db_path)
        stale_trade_ts = '2099-03-17T22:30:00.000Z'
        fresh_no_trade_ts = '2099-03-17T22:40:00.000Z'

        for signal_type, score in [('perp_short_continuation', 84), ('perp_short_failed_bounce', 67), ('perp_no_trade', 41)]:
            record_signal_candidate(self.db_path, {
                'ts': stale_trade_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-market-stale',
                'candidate_key': f'decision-market-stale:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 6},
            })

        for signal_type, score in [('perp_short_continuation', 63), ('perp_short_failed_bounce', 58), ('perp_no_trade', 91)]:
            record_signal_candidate(self.db_path, {
                'ts': fresh_no_trade_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'chop',
                'decision_id': 'decision-fresh-no-trade',
                'candidate_key': f'decision-fresh-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 1},
            })

        self._record_perp_executor_cycle_event(
            '2099-03-17T22:31:00.000Z',
            {
                'ts': '2099-03-17T22:31:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'decision-market-stale',
                    'symbol': 'SOL',
                    'selected_signal_type': 'perp_no_trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 84,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 41,
                    'score_gap_vs_no_trade': 43,
                    'blocked_trade': True,
                    'block_reason': 'market_stale',
                    'candidate_strategy': 'perp_short_continuation',
                },
                'entry_result': {
                    'action': 'skip',
                    'decision_id': 'decision-market-stale',
                    'symbol': 'SOL',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'market_stale',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:41:00.000Z',
            {
                'ts': '2099-03-17T22:41:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'decision-fresh-no-trade',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_no_trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 63,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 91,
                    'score_gap_vs_no_trade': -28,
                    'blocked_trade': False,
                },
                'entry_result': {
                    'action': 'no_trade',
                    'decision_id': 'decision-fresh-no-trade',
                    'symbol': 'BTC',
                    'signal_type': 'perp_no_trade',
                    'reason': 'no_trade',
                    'score': 91,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')

        stale_summary = next(row for row in state['decision_readiness']['decisions'] if row['decision_id'] == 'decision-market-stale')
        self.assertEqual(stale_summary['operator_status'], 'blocked')
        self.assertIn('market_stale', stale_summary['blockers'])
        self.assertEqual(state['best_current_opportunity']['decision_id'], 'decision-fresh-no-trade')
        self.assertEqual(state['best_current_opportunity']['operator_status'], 'no_trade_selected')
        self.assertFalse(any(row['decision_id'] == 'decision-market-stale' for row in state['decision_readiness']['top_opportunities']))

    def test_perp_executor_state_best_current_opportunity_falls_back_to_best_no_trade_decision(self):
        init_db(self.db_path)
        scenarios = [
            ('decision-no-trade-best', '2099-03-17T22:40:00.000Z', 'BTC', 64, 90),
            ('decision-no-trade-worse', '2099-03-17T22:50:00.000Z', 'ETH', 58, 96),
        ]

        for decision_id, decision_ts, symbol, best_short_score, no_trade_score in scenarios:
            for signal_type, score in [
                ('perp_short_continuation', best_short_score),
                ('perp_short_failed_bounce', best_short_score - 7),
                ('perp_no_trade', no_trade_score),
            ]:
                record_signal_candidate(self.db_path, {
                    'ts': decision_ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': symbol,
                    'market': symbol,
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'score_components': {'edge': score}},
                })
            self._record_perp_executor_cycle_event(
                decision_ts,
                {
                    'ts': decision_ts,
                    'requested_mode': 'paper',
                    'active_mode': 'paper',
                    'candidate_choice': {
                        'type': 'no_trade',
                        'decision_id': decision_id,
                        'symbol': symbol,
                        'selected_signal_type': 'perp_no_trade',
                        'best_short_signal_type': 'perp_short_continuation',
                        'best_short_score': best_short_score,
                        'no_trade_signal_type': 'perp_no_trade',
                        'no_trade_score': no_trade_score,
                        'score_gap_vs_no_trade': best_short_score - no_trade_score,
                        'blocked_trade': False,
                    },
                    'entry_result': {
                        'action': 'no_trade',
                        'decision_id': decision_id,
                        'symbol': symbol,
                        'signal_type': 'perp_no_trade',
                        'reason': 'no_trade',
                        'score': no_trade_score,
                    },
                    'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
                },
            )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        readiness = state['decision_readiness']

        self.assertEqual(readiness['trade_ready_count'], 0)
        self.assertEqual(readiness['blocked_count'], 0)
        self.assertEqual(readiness['no_trade_selected_count'], 2)
        self.assertEqual(readiness['top_opportunities'], [])
        self.assertEqual(readiness['best_current_opportunity']['decision_id'], 'decision-no-trade-best')
        self.assertEqual(readiness['best_current_opportunity']['operator_status'], 'no_trade_selected')
        self.assertEqual(readiness['best_current_opportunity']['selected_signal_type'], 'perp_no_trade')
        self.assertEqual(readiness['best_current_opportunity']['score_gap_vs_no_trade'], -26.0)
        self.assertEqual(state['best_current_opportunity']['decision_id'], 'decision-no-trade-best')
        self.assertNotEqual(state['best_current_opportunity'], {})

    def test_perp_executor_state_classifies_blocked_decisions_beyond_recent_cycle_display_limit(self):
        init_db(self.db_path)
        for idx in range(6):
            decision_id = f'decision-{idx}'
            decision_ts = f'2099-03-17T22:0{idx}:00.000Z'
            for signal_type, score in [('perp_short_continuation', 80 - idx), ('perp_short_failed_bounce', 60 - idx), ('perp_no_trade', 40 - idx)]:
                record_signal_candidate(self.db_path, {
                    'ts': decision_ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'score_components': {'edge': score}},
                })
            self._record_perp_executor_cycle_event(
                f'2099-03-17T22:1{idx}:00.000Z',
                {
                    'ts': f'2099-03-17T22:1{idx}:00.000Z',
                    'requested_mode': 'paper',
                    'active_mode': 'paper',
                    'candidate_choice': {
                        'type': 'trade',
                        'decision_id': decision_id,
                        'symbol': 'SOL',
                        'selected_signal_type': 'perp_short_continuation',
                        'best_short_signal_type': 'perp_short_continuation',
                        'best_short_score': 80 - idx,
                        'no_trade_signal_type': 'perp_no_trade',
                        'no_trade_score': 40 - idx,
                        'score_gap_vs_no_trade': 40,
                        'blocked_trade': True,
                        'block_reason': 'global_cooldown',
                    },
                    'entry_result': {'action': 'skip', 'decision_id': decision_id, 'reason': 'global_cooldown'},
                    'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
                },
            )
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:30:00.000Z')
        readiness = state['decision_readiness']
        by_id = {row['decision_id']: row for row in state['decisions']}
        self.assertEqual(len(state['recent_cycles']), 5)
        self.assertEqual(readiness['pending_decision_count'], 6)
        self.assertEqual(readiness['blocked_count'], 6)
        self.assertEqual(readiness['trade_ready_count'], 0)
        self.assertEqual(readiness['no_trade_selected_count'], 0)
        self.assertEqual(by_id['decision-5']['operator_status'], 'blocked')
        self.assertIn('global_cooldown', by_id['decision-5']['operator_blockers'])
        self.assertEqual(readiness['top_blockers'][0], {'blocker': 'global_cooldown', 'count': 6})

    def test_perp_executor_state_classifies_blocked_decisions_with_more_than_25_cycles_in_lookback(self):
        init_db(self.db_path)
        oldest_decision_id = 'decision-00'
        for idx in range(30):
            decision_id = f'decision-{idx:02d}'
            decision_ts = f'2099-03-17T22:{idx:02d}:00.000Z'
            for signal_type, score in [('perp_short_continuation', 90 - idx), ('perp_short_failed_bounce', 70 - idx), ('perp_no_trade', 40 - idx)]:
                record_signal_candidate(self.db_path, {
                    'ts': decision_ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'score_components': {'edge': score}},
                })
            self._record_perp_executor_cycle_event(
                f'2099-03-17T23:{idx:02d}:00.000Z',
                {
                    'ts': f'2099-03-17T23:{idx:02d}:00.000Z',
                    'requested_mode': 'paper',
                    'active_mode': 'paper',
                    'candidate_choice': {
                        'type': 'trade',
                        'decision_id': decision_id,
                        'symbol': 'SOL',
                        'selected_signal_type': 'perp_short_continuation',
                        'best_short_signal_type': 'perp_short_continuation',
                        'best_short_score': 90 - idx,
                        'no_trade_signal_type': 'perp_no_trade',
                        'no_trade_score': 40 - idx,
                        'score_gap_vs_no_trade': 50,
                        'blocked_trade': True,
                        'block_reason': 'market_stale',
                    },
                    'entry_result': {'action': 'skip', 'decision_id': decision_id, 'reason': 'market_stale'},
                    'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
                },
            )
        state = get_perp_executor_state(self.db_path, lookback_minutes=180, analytics_lookback_hours=24, anchor_ts='2099-03-18T00:00:00.000Z')
        readiness = state['decision_readiness']
        by_id = {row['decision_id']: row for row in state['decisions']}
        self.assertEqual(len(state['recent_cycles']), 5)
        self.assertEqual(readiness['pending_decision_count'], 30)
        self.assertEqual(readiness['blocked_count'], 30)
        self.assertEqual(by_id[oldest_decision_id]['operator_status'], 'blocked')
        self.assertIn('market_stale', by_id[oldest_decision_id]['operator_blockers'])
        self.assertEqual(readiness['top_blockers'][0], {'blocker': 'market_stale', 'count': 30})

    def test_perp_executor_state_infers_blocked_decision_from_entry_reason_alone(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:00:00.000Z'
        for signal_type, score in [('perp_short_continuation', 81), ('perp_short_failed_bounce', 68), ('perp_no_trade', 43)]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'ETH',
                'market': 'ETH',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 2200.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-entry-reason-blocked',
                'candidate_key': f'decision-entry-reason-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}},
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:01:00.000Z',
            {
                'ts': '2099-03-17T22:01:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'decision-entry-reason-blocked',
                    'symbol': 'ETH',
                    'selected_signal_type': 'perp_short_continuation',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 81,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 43,
                    'score_gap_vs_no_trade': 38,
                },
                'entry_result': {
                    'action': 'skip',
                    'decision_id': 'decision-entry-reason-blocked',
                    'symbol': 'ETH',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'market_stale',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        by_id = {row['decision_id']: row for row in state['decisions']}
        self.assertEqual(by_id['decision-entry-reason-blocked']['operator_status'], 'blocked')
        self.assertIn('market_stale', by_id['decision-entry-reason-blocked']['operator_blockers'])
        self.assertEqual(state['decision_readiness']['top_blockers'][0]['blocker'], 'market_stale')

    def test_perp_executor_state_planned_size_usd_falls_back_to_candidate_choice(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:30:00.000Z'
        for signal_type, score, metadata in [
            ('perp_short_continuation', 87, {'market_age_minutes': 3}),
            ('perp_short_failed_bounce', 74, {}),
            ('perp_no_trade', 46, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'decision-plan-fallback',
                'candidate_key': f'decision-plan-fallback:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:31:00.000Z',
            {
                'ts': '2099-03-17T22:31:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'decision-plan-fallback',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_short_continuation',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 87,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 46,
                    'score_gap_vs_no_trade': 41,
                    'planned_size_usd': 88.5,
                },
                'entry_result': {
                    'action': 'queued',
                    'decision_id': 'decision-plan-fallback',
                    'symbol': 'BTC',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'submitted',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        self.assertEqual(state['best_current_opportunity']['decision_id'], 'decision-plan-fallback')
        self.assertEqual(state['best_current_opportunity']['planned_size_usd'], 88.5)
        self.assertEqual(state['decision_readiness']['top_opportunities'][0]['planned_size_usd'], 88.5)

    def test_perp_executor_state_exposes_recent_cycle_summaries_and_reason_counts(self):
        init_db(self.db_path)
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:00:00.000Z',
            {
                'ts': '2099-03-17T22:00:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 0,
                'entry_result': {
                    'action': 'no_trade',
                    'symbol': 'SOL',
                    'signal_type': 'perp_no_trade',
                    'decision_id': 'cycle-1',
                    'reason': 'no_trade',
                    'score': 91,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:05:00.000Z',
            {
                'ts': '2099-03-17T22:05:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 0,
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'cycle-2',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'selected_signal_type': 'perp_short_continuation',
                    'selected_score': 77,
                    'selected_reason': 'short setup',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 77,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 63,
                    'score_gap_vs_no_trade': 14,
                    'blocked_trade': False,
                    'block_reason': None,
                    'market_age_minutes': 2,
                    'invalidation_price': 101.0,
                    'stop_loss_price': 100.9,
                    'take_profit_price': 98.2,
                    'paper_notional_usd': 3.0,
                    'candidate_strategy': 'perp_short_continuation',
                },
                'entry_result': {
                    'action': 'skip',
                    'symbol': 'SOL',
                    'signal_type': 'perp_short_continuation',
                    'decision_id': 'cycle-2',
                    'reason': 'global_cooldown',
                },
                'position_result': {'action': 'hold', 'results': [{'action': 'hold', 'symbol': 'SOL'}], 'closed_count': 0},
            },
        )
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:10:00.000Z',
            {
                'ts': '2099-03-17T22:10:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 1,
                'entry_result': {
                    'action': 'skip',
                    'reason': 'active_position_exists',
                    'open_position_count': 1,
                },
                'position_result': {
                    'action': 'managed',
                    'results': [
                        {'action': 'closed', 'symbol': 'SOL', 'reason': 'take_profit_hit', 'realized_pnl_usd': 1.25},
                    ],
                    'closed_count': 1,
                },
            },
        )
        record_system_event(
            self.db_path,
            {
                'ts': '2099-03-17T22:15:00.000Z',
                'event_type': 'perp_executor_cycle',
                'severity': 'info',
                'message': 'Malformed summary should not break analytics',
                'source': 'perps-auto-trade.mjs',
                'metadata': {'summary': 'not-a-dict'},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')

        self.assertIn('recent_cycles', state)
        self.assertGreaterEqual(len(state['recent_cycles']), 4)
        self.assertEqual(state['recent_cycles'][0]['event_ts'], '2099-03-17T22:15:00.000Z')
        self.assertEqual(state['recent_cycles'][0]['entry_outcome'], 'unknown')
        self.assertEqual(state['recent_cycles'][1]['entry_outcome'], 'active_position_exists')
        self.assertEqual(state['recent_cycles'][1]['close_reasons'], ['take_profit_hit'])
        self.assertEqual(state['recent_cycles'][2]['entry_outcome'], 'global_cooldown')
        self.assertEqual(state['recent_cycles'][2]['candidate_choice']['selected_signal_type'], 'perp_short_continuation')
        self.assertEqual(state['recent_cycles'][2]['candidate_choice']['score_gap_vs_no_trade'], 14.0)
        self.assertEqual(state['latest_candidate_choice']['selected_signal_type'], 'perp_short_continuation')
        self.assertEqual(state['latest_candidate_choice']['candidate_strategy'], 'perp_short_continuation')
        self.assertEqual(state['recent_cycles'][3]['entry_outcome'], 'no_trade')
        self.assertEqual(state['recent_entry_reason_counts']['active_position_exists'], 1)
        self.assertEqual(state['recent_entry_reason_counts']['global_cooldown'], 1)
        self.assertEqual(state['recent_entry_reason_counts']['no_trade'], 1)
        self.assertEqual(state['recent_close_reason_counts']['take_profit_hit'], 1)

    def test_perp_executor_state_exposes_latest_pilot_policy_context(self):
        init_db(self.db_path)
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:10:00.000Z',
            {
                'ts': '2099-03-17T22:10:00.000Z',
                'requested_mode': 'live',
                'active_mode': 'paper',
                'pilot_policy': {
                    'requested_mode': 'live',
                    'env_live_allowed': True,
                    'env_live_enabled': True,
                    'active_mode': 'paper',
                    'strategy_family': 'tiny_live_pilot',
                    'candidate_strategy': 'perp_short_continuation',
                    'candidate_symbol': 'SOL',
                    'live_eligible_for_executor': True,
                    'policy_status': 'approved_for_executor',
                    'denial_reason': None,
                    'tiny_live_pilot_decision': {
                        'approved': True,
                        'mode': 'tiny_live_pilot',
                        'product_type': 'perps',
                        'strategy': 'perp_short_continuation',
                        'symbol': 'SOL',
                        'reason': 'approved by analytics',
                    },
                },
                'entry_result': {'action': 'skip', 'reason': 'global_cooldown'},
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')

        self.assertEqual(state['latest_pilot_policy']['policy_status'], 'approved_for_executor')
        self.assertIsNone(state['latest_pilot_policy']['denial_reason'])
        self.assertTrue(state['latest_pilot_policy']['live_eligible_for_executor'])
        self.assertEqual(state['latest_pilot_policy']['candidate_strategy'], 'perp_short_continuation')
        self.assertEqual(state['latest_pilot_policy']['candidate_symbol'], 'SOL')
        self.assertEqual(state['latest_pilot_policy']['tiny_live_pilot_decision']['strategy'], 'perp_short_continuation')
        self.assertEqual(state['recent_cycles'][0]['pilot_policy']['policy_status'], 'approved_for_executor')

    def test_candidate_analytics_tolerate_malformed_or_non_object_metadata(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:00:00.000Z'
        future_ts = '2099-03-17T23:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 95.0, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_signal_candidate(self.db_path, {
            'ts': ts,
            'source': 'perps-monitor.mjs',
            'symbol': 'SOL',
            'market': 'SOL',
            'signal_type': 'perp_short_continuation',
            'strategy_tag': 'perp_short_continuation',
            'side': 'sell',
            'product_type': 'perps',
            'price': 100.0,
            'score': 78,
            'regime_tag': 'trend_down',
            'decision_id': 'bad-meta',
            'candidate_key': 'bad-meta:short',
            'status': 'candidate',
            'reason': 'bad metadata row',
            'metadata': 'not-json',
        })
        record_signal_candidate(self.db_path, {
            'ts': ts,
            'source': 'perps-monitor.mjs',
            'symbol': 'SOL',
            'market': 'SOL',
            'signal_type': 'perp_no_trade',
            'strategy_tag': 'perp_no_trade',
            'side': 'flat',
            'product_type': 'perps',
            'price': 100.0,
            'score': 55,
            'regime_tag': 'trend_down',
            'decision_id': 'bad-meta',
            'candidate_key': 'bad-meta:no-trade',
            'status': 'candidate',
            'reason': 'array metadata row',
            'metadata': '[]',
        })

        scored = get_daily_analytics(self.db_path, '2099-03-17T00:00:00.000Z')['perp_recent_scored_candidates']
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts='2099-03-17T23:00:00.000Z')
        competition = get_perp_candidate_competition(self.db_path, since_ts='2099-03-17T00:00:00.000Z', horizons=[60])

        self.assertTrue(scored)
        self.assertTrue(any(row['signal_type'] == 'perp_short_continuation' and row['threshold_distance'] is None for row in scored))
        self.assertTrue(state['decisions'])
        self.assertTrue(competition)

    def test_perp_candidate_competition_uses_strictest_threshold_when_lane_metadata_disagrees(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:00:00.000Z'
        future_ts = '2099-03-17T23:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 99.7, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        lane_thresholds = {
            'perp_short_continuation': 0.4,
            'perp_short_failed_bounce': 0.2,
            'perp_no_trade': 0.1,
        }
        for signal_type, score in [('perp_short_continuation', 81), ('perp_short_failed_bounce', 70), ('perp_no_trade', 55)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'strict-threshold',
                'candidate_key': f'strict-threshold:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'no_trade_min_short_edge_pct': lane_thresholds[signal_type]},
            })

        competition = get_perp_candidate_competition(self.db_path, since_ts='2099-03-17T00:00:00.000Z', horizons=[60])
        symbol_rows = {
            row['signal_type']: row
            for row in competition
            if row['competition_scope'] == 'symbol' and row['symbol'] == 'SOL' and row['horizon_minutes'] == 60
        }

        self.assertEqual(symbol_rows['perp_no_trade']['decision_count'], 1)
        self.assertEqual(symbol_rows['perp_no_trade']['win_count'], 1)
        self.assertEqual(symbol_rows['perp_short_continuation']['win_count'], 0)
        self.assertEqual(symbol_rows['perp_short_failed_bounce']['win_count'], 0)

    def test_perp_candidate_competition_uses_strictest_threshold_across_duplicate_lane_rows(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:00:00.000Z'
        future_ts = '2099-03-17T23:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'ETH', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'ETH', 'priceUsd': 99.7, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_signal_candidate(self.db_path, {
            'ts': ts,
            'source': 'perps-monitor.mjs',
            'symbol': 'ETH',
            'market': 'ETH',
            'signal_type': 'perp_short_continuation',
            'strategy_tag': 'perp_short_continuation',
            'side': 'sell',
            'product_type': 'perps',
            'price': 100.0,
            'score': 82,
            'regime_tag': 'trend_down',
            'decision_id': 'duplicate-threshold',
            'candidate_key': 'duplicate-threshold:monitor:continuation',
            'status': 'candidate',
            'reason': 'monitor lane',
            'metadata': {'no_trade_min_short_edge_pct': 0.4},
        })
        record_signal_candidate(self.db_path, {
            'ts': ts,
            'source': 'perps-auto-trade.mjs',
            'symbol': 'ETH',
            'market': 'ETH',
            'signal_type': 'perp_short_continuation',
            'strategy_tag': 'perp_short_continuation',
            'side': 'sell',
            'product_type': 'perps',
            'price': 100.0,
            'score': 82,
            'regime_tag': 'trend_down',
            'decision_id': 'duplicate-threshold',
            'candidate_key': 'duplicate-threshold:executor:continuation',
            'status': 'skipped',
            'reason': 'executor follow-up',
            'metadata': {'no_trade_min_short_edge_pct': 0.1},
        })
        for signal_type, score in [('perp_short_failed_bounce', 70), ('perp_no_trade', 55)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'ETH',
                'market': 'ETH',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'duplicate-threshold',
                'candidate_key': f'duplicate-threshold:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'no_trade_min_short_edge_pct': 0.1},
            })

        competition = get_perp_candidate_competition(self.db_path, since_ts='2099-03-17T00:00:00.000Z', horizons=[60])
        symbol_rows = {
            row['signal_type']: row
            for row in competition
            if row['competition_scope'] == 'symbol' and row['symbol'] == 'ETH' and row['horizon_minutes'] == 60
        }

        self.assertEqual(symbol_rows['perp_no_trade']['win_count'], 1)
        self.assertEqual(symbol_rows['perp_short_continuation']['win_count'], 0)

    def test_daily_analytics_emits_machine_readable_tiny_live_pilot_decision(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='SOL')
        report = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')
        policy = get_strategy_execution_policy(self.db_path)
        pilot = report['tiny_live_pilot_decision']
        self.assertIn('tiny_live_pilot_decision', report)
        self.assertIn('best_candidates', pilot)
        self.assertIn('perp_executor_state', report)
        self.assertEqual(policy['tiny_live_pilot_decision'], pilot)
        self.assertEqual(pilot['product_type'], 'perps')
        self.assertEqual(pilot['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['symbol'], 'SOL')
        self.assertTrue(pilot['approved'])
        self.assertEqual(pilot['evaluation_horizon_minutes'], 60)
        self.assertEqual(pilot['blockers'], [])
        self.assertEqual(pilot['approval_requirements']['min_candidate_count'], 8)
        self.assertEqual(pilot['evidence']['best_perp_candidate']['signal_type'], 'perp_short_continuation')
        self.assertEqual(pilot['evidence']['best_perp_candidate']['symbol'], 'SOL')
        self.assertEqual(sorted(pilot['evidence']['perp_available_horizons']), [60, 240])
        self.assertIn('perp_symbol_available_horizons', pilot['evidence'])
        self.assertEqual(pilot['evidence']['perp_symbol_available_horizons']['SOL'], [60, 240])
        self.assertEqual(pilot['actionable_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['actionable_candidate']['product_type'], 'perps')
        self.assertEqual(pilot['actionable_candidate']['symbol'], 'SOL')
        self.assertEqual(pilot['recommended_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['recommended_candidate']['product_type'], 'perps')
        self.assertEqual(pilot['recommended_candidate']['symbol'], 'SOL')
        self.assertEqual(pilot['no_trade_benchmark']['strategy'], 'perp_no_trade')
        self.assertEqual(pilot['no_trade_benchmark']['product_type'], 'perps')
        self.assertEqual(pilot['no_trade_benchmark']['symbol'], 'SOL')
        self.assertIsNone(pilot['blocked_candidate'])

    def test_daily_analytics_tiny_live_pilot_exposes_blocked_candidate_context_when_not_approved(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:00:00.000Z'
        future_ts = '2099-03-17T22:45:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 95.0, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        for signal_type, score in [('perp_short_continuation', 82), ('perp_short_failed_bounce', 61), ('perp_no_trade', 35)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'blocked-pilot',
                'candidate_key': f'blocked-pilot:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'no_trade_min_short_edge_pct': 0.25},
            })
        pilot = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')['tiny_live_pilot_decision']
        self.assertFalse(pilot['approved'])
        self.assertIsNone(pilot['recommended_candidate'])
        self.assertEqual(pilot['product_type'], None)
        self.assertEqual(pilot['strategy'], None)
        self.assertEqual(pilot['symbol'], None)
        self.assertIn('insufficient_perp_sample_size', pilot['blockers'])
        self.assertEqual(pilot['actionable_candidate']['product_type'], 'perps')
        self.assertEqual(pilot['actionable_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['blocked_candidate']['product_type'], 'perps')
        self.assertEqual(pilot['blocked_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['blocked_candidate']['symbol'], 'SOL')
        self.assertEqual(pilot['blocked_candidate']['metrics']['candidate_count'], 1)
        self.assertEqual(pilot['blocked_candidate']['comparison_vs_no_trade']['signal_type'], 'perp_no_trade')
        self.assertEqual(pilot['no_trade_benchmark']['strategy'], 'perp_no_trade')
        self.assertEqual(pilot['no_trade_benchmark']['symbol'], 'SOL')

    def test_daily_analytics_tiny_live_pilot_uses_real_spot_pair_instead_of_synthetic_strategy_symbol_combo(self):
        init_db(self.db_path)
        base_ts = '2099-03-17T00:00:00.000Z'
        for i in range(8):
            hour = i * 2
            ts = f'2099-03-17T{hour:02d}:00:00.000Z'
            future_ts = f'2099-03-17T{hour:02d}:45:00.000Z'
            record_snapshot_and_alerts(
                self.db_path,
                {
                    'ts': future_ts,
                    'wallet': {'sol': 1.0, 'usdc': 50.0, 'totalUsd': 150.0},
                    'prices': {
                        'BONK': {'price': 1.006},
                        'WIF': {'price': 1.007},
                    },
                },
                [],
            )
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'monitor.mjs',
                'symbol': 'BONK',
                'signal_type': 'near_buy',
                'strategy_tag': 'strategy_alpha',
                'side': 'buy',
                'product_type': 'spot',
                'price': 1.0,
                'status': 'candidate',
                'reason': 'alpha-bonk',
            })
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'monitor.mjs',
                'symbol': 'WIF',
                'signal_type': 'near_buy',
                'strategy_tag': 'strategy_beta',
                'side': 'buy',
                'product_type': 'spot',
                'price': 1.0,
                'status': 'candidate',
                'reason': 'beta-wif',
            })

        report = get_daily_analytics(self.db_path, base_ts)
        pilot = report['tiny_live_pilot_decision']

        self.assertTrue(pilot['approved'])
        self.assertEqual(pilot['product_type'], 'spot')
        self.assertEqual(pilot['strategy'], 'strategy_beta')
        self.assertEqual(pilot['symbol'], 'WIF')
        self.assertEqual(pilot['actionable_candidate']['strategy'], 'strategy_beta')
        self.assertEqual(pilot['actionable_candidate']['symbol'], 'WIF')
        self.assertEqual(pilot['recommended_candidate']['strategy'], 'strategy_beta')
        self.assertEqual(pilot['recommended_candidate']['symbol'], 'WIF')
        self.assertEqual(pilot['actionable_candidate']['metrics']['candidate_count'], 8)
        self.assertGreater(pilot['actionable_candidate']['metrics']['avg_forward_return_pct'], 0.6)
        self.assertIsNone(pilot['actionable_candidate']['comparison_vs_no_trade'])

    def test_daily_analytics_tiny_live_pilot_keeps_no_trade_as_separate_benchmark_when_it_blocks_promotion(self):
        init_db(self.db_path)
        for i in range(8):
            hour = i * 2
            ts = f'2099-03-17T{hour:02d}:00:00.000Z'
            future_ts = f'2099-03-17T{hour:02d}:45:00.000Z'
            decision_id = f'blocked-no-trade-{i}'
            record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -1.0, 'highUsd24h': 101.0, 'lowUsd24h': 99.0, 'volumeUsd24h': 50000000.0})
            record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 103.0, 'changePct24h': 1.5, 'highUsd24h': 104.0, 'lowUsd24h': 99.0, 'volumeUsd24h': 50000000.0})
            for signal_type, score in [('perp_short_continuation', 82), ('perp_short_failed_bounce', 70), ('perp_no_trade', 95)]:
                record_signal_candidate(self.db_path, {
                    'ts': ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_up',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'no_trade_min_short_edge_pct': 0.25},
                })

        pilot = get_daily_analytics(self.db_path, '2026-03-17T00:00:00.000Z')['tiny_live_pilot_decision']

        self.assertFalse(pilot['approved'])
        self.assertIn('perp_short_lane_not_beating_no_trade_win_rate', pilot['blockers'])
        self.assertEqual(pilot['actionable_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['blocked_candidate']['strategy'], 'perp_short_continuation')
        self.assertEqual(pilot['no_trade_benchmark']['strategy'], 'perp_no_trade')
        self.assertLess(
            pilot['actionable_candidate']['comparison_vs_no_trade']['win_rate_gap'],
            0,
        )

    def test_daily_analytics_report_calls_out_no_trade_suppression_separately(self):
        init_db(self.db_path)
        for i in range(8):
            hour = i * 2
            ts = f'2099-03-17T{hour:02d}:00:00.000Z'
            future_ts = f'2099-03-17T{hour:02d}:45:00.000Z'
            decision_id = f'report-blocked-no-trade-{i}'
            record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -1.0, 'highUsd24h': 101.0, 'lowUsd24h': 99.0, 'volumeUsd24h': 50000000.0})
            record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 103.0, 'changePct24h': 1.5, 'highUsd24h': 104.0, 'lowUsd24h': 99.0, 'volumeUsd24h': 50000000.0})
            for signal_type, score in [('perp_short_continuation', 82), ('perp_short_failed_bounce', 70), ('perp_no_trade', 95)]:
                record_signal_candidate(self.db_path, {
                    'ts': ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': 100.0,
                    'score': score,
                    'regime_tag': 'trend_up',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'no_trade_min_short_edge_pct': 0.25},
                })

        stdout = self._run_daily_analytics_report()

        self.assertIn('actionable_candidate: perps perp_short_continuation / SOL @60m', stdout)
        self.assertIn('no_trade_benchmark: perps perp_no_trade / SOL @60m', stdout)
        self.assertIn('promotion_suppressed_by_no_trade: yes', stdout)

    def test_daily_analytics_report_prints_perp_operator_sections(self):
        init_db(self.db_path)
        ts = '2099-03-17T22:25:00.000Z'
        future_ts = '2099-03-17T22:45:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -4.0, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'SOL', 'priceUsd': 95.0, 'changePct24h': -4.2, 'highUsd24h': 103.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 50000000.0})
        for signal_type, score, metadata in [
            ('perp_short_continuation', 82, {'invalidation_price': 101.0, 'stop_loss_price': 100.9, 'take_profit_price': 98.2, 'planned_size_usd': 3.0, 'paper_notional_usd': 3.0, 'market_age_minutes': 2}),
            ('perp_short_failed_bounce', 61, {}),
            ('perp_no_trade', 35, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'report-perp-decision',
                'candidate_key': f'report-perp-decision:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'no_trade_min_short_edge_pct': 0.25, **metadata},
            })
        for signal_type, score in [('perp_short_continuation', 54), ('perp_short_failed_bounce', 48), ('perp_no_trade', 89)]:
            record_signal_candidate(self.db_path, {
                'ts': '2099-03-17T22:30:00.000Z',
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'sideways',
                'decision_id': 'report-btc-no-trade',
                'candidate_key': f'report-btc-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 1},
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:05:00.000Z',
            {
                'ts': '2099-03-17T22:05:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 0,
                'entry_result': {
                    'action': 'no_trade',
                    'symbol': 'SOL',
                    'signal_type': 'perp_no_trade',
                    'decision_id': 'report-cycle-1',
                    'reason': 'no_trade',
                    'score': 87,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:40:00.000Z',
            {
                'ts': '2099-03-17T22:40:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 0,
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'report-btc-no-trade',
                    'symbol': 'BTC',
                    'market': 'BTC',
                    'selected_signal_type': 'perp_no_trade',
                    'selected_score': 89,
                    'selected_reason': 'no trade',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 54,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 89,
                    'score_gap_vs_no_trade': -35,
                    'blocked_trade': False,
                    'block_reason': None,
                    'market_age_minutes': 1,
                    'candidate_strategy': 'perp_no_trade',
                },
                'entry_result': {
                    'action': 'no_trade',
                    'symbol': 'BTC',
                    'signal_type': 'perp_no_trade',
                    'decision_id': 'report-btc-no-trade',
                    'reason': 'no_trade',
                    'score': 89,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )
        result = subprocess.run(
            ['python', '/home/brimigs/trading_system/daily_analytics_report.py', '--db', str(self.db_path), '--since-ts', '2026-03-17T00:00:00.000Z'],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, 'PYTHONPATH': '/home/brimigs'},
        )
        stdout = result.stdout
        self.assertIn('Perp candidate volume by symbol:', stdout)
        self.assertIn('Perp executor paper state:', stdout)
        self.assertIn('readiness: trade_ready=1 blocked=0 no_trade_selected=1', stdout)
        self.assertIn('actionability_guardrails: max_signal_age_min=40', stdout)
        self.assertIn('best_current_opportunity SOL / report-perp-decision: status=trade_ready selected=perp_short_continuation', stdout)
        self.assertIn('best_current_plan: planned_size=$3.00 notional=$3.00 market_age_min=2.0 stop=$100.9000 target=$98.2000 invalidate=$101.0000', stdout)
        self.assertIn('opportunity SOL / report-perp-decision: status=trade_ready selected=perp_short_continuation', stdout)
        self.assertIn('current_choice BTC / report-btc-no-trade: selected=perp_no_trade type=no_trade', stdout)
        self.assertIn('Recent executor cycles:', stdout)
        self.assertIn('Entry outcomes:', stdout)
        self.assertIn('Close reasons:', stdout)
        self.assertIn('no_trade', stdout)
        self.assertIn('Tiny live pilot decision:', stdout)
        self.assertIn('best_perp_candidate:', stdout)
        self.assertIn('actionable_candidate: perps perp_short_continuation / SOL @60m', stdout)
        self.assertIn('actionable_candidate_vs_no_trade: signal=perp_no_trade', stdout)
        self.assertIn('no_trade_benchmark: perps perp_no_trade / SOL @60m', stdout)

    def test_daily_analytics_report_classifies_blocked_no_trade_fallback_as_blocked(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:00:00.000Z'
        for signal_type, score in [('perp_short_continuation', 84), ('perp_short_failed_bounce', 67), ('perp_no_trade', 41)]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'report-blocked-no-trade',
                'candidate_key': f'report-blocked-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 6},
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:01:00.000Z',
            {
                'ts': '2099-03-17T22:01:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'open_positions_before': 0,
                'candidate_choice': {
                    'type': 'no_trade',
                    'decision_id': 'report-blocked-no-trade',
                    'symbol': 'SOL',
                    'market': 'SOL',
                    'selected_signal_type': 'perp_no_trade',
                    'selected_score': 41,
                    'selected_reason': 'paper no trade due to market stale',
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 84,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 41,
                    'score_gap_vs_no_trade': 43,
                    'blocked_trade': True,
                    'block_reason': 'market_stale',
                    'market_age_minutes': 6,
                    'candidate_strategy': 'perp_short_continuation',
                },
                'entry_result': {
                    'action': 'no_trade',
                    'symbol': 'SOL',
                    'signal_type': 'perp_no_trade',
                    'decision_id': 'report-blocked-no-trade',
                    'reason': 'paper_no_trade_due_to_market_stale',
                    'score': 41,
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        stdout = self._run_daily_analytics_report()

        self.assertIn('readiness: trade_ready=0 blocked=1 no_trade_selected=0', stdout)
        self.assertIn('current_choice SOL / report-blocked-no-trade: selected=perp_no_trade type=no_trade', stdout)
        self.assertIn('current_choice_blocked: reason=market_stale market_age_min=6.0', stdout)

    def test_daily_analytics_report_prints_latest_pilot_policy_context(self):
        init_db(self.db_path)
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:20:00.000Z',
            {
                'ts': '2099-03-17T22:20:00.000Z',
                'requested_mode': 'live',
                'active_mode': 'paper',
                'pilot_policy': {
                    'requested_mode': 'live',
                    'env_live_allowed': True,
                    'env_live_enabled': True,
                    'active_mode': 'paper',
                    'strategy_family': 'tiny_live_pilot',
                    'candidate_strategy': 'perp_short_failed_bounce',
                    'candidate_symbol': 'SOL',
                    'live_eligible_for_executor': False,
                    'policy_status': 'paper_only',
                    'denial_reason': 'pilot_strategy_mismatch',
                    'tiny_live_pilot_decision': {
                        'approved': True,
                        'mode': 'tiny_live_pilot',
                        'product_type': 'perps',
                        'strategy': 'perp_short_continuation',
                        'symbol': 'SOL',
                        'reason': 'approved by analytics',
                    },
                },
                'entry_result': {'action': 'skip', 'reason': 'policy_denied'},
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        stdout = self._run_daily_analytics_report()

        self.assertIn('pilot_policy: status=paper_only live_eligible=False denial_reason=pilot_strategy_mismatch candidate=perp_short_failed_bounce / SOL', stdout)
        self.assertIn('pilot_approval: approved=True mode=tiny_live_pilot approved_candidate=perps perp_short_continuation / SOL', stdout)

    def test_daily_analytics_report_current_choice_uses_inferred_blocked_context(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:00:00.000Z'
        for signal_type, score in [('perp_short_continuation', 81), ('perp_short_failed_bounce', 70), ('perp_no_trade', 43)]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'ETH',
                'market': 'ETH',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 2200.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'report-current-choice-blocked',
                'candidate_key': f'report-current-choice-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'market_age_minutes': 7},
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:01:00.000Z',
            {
                'ts': '2099-03-17T22:01:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'report-current-choice-blocked',
                    'symbol': 'ETH',
                    'selected_signal_type': 'perp_short_continuation',
                    'selected_score': 81,
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 81,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 43,
                    'score_gap_vs_no_trade': 38,
                },
                'entry_result': {
                    'action': 'skip',
                    'decision_id': 'report-current-choice-blocked',
                    'symbol': 'ETH',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'market_stale',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        stdout = self._run_daily_analytics_report()

        self.assertIn('current_choice ETH / report-current-choice-blocked: selected=perp_short_continuation type=trade', stdout)
        self.assertIn('current_choice_blocked: reason=market_stale market_age_min=7.0', stdout)

    def test_daily_analytics_report_current_trade_plan_uses_planned_size_fallback(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:10:00.000Z'
        for signal_type, score, metadata in [
            ('perp_short_continuation', 87, {'market_age_minutes': 3}),
            ('perp_short_failed_bounce', 74, {}),
            ('perp_no_trade', 46, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'report-plan-fallback',
                'candidate_key': f'report-plan-fallback:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:11:00.000Z',
            {
                'ts': '2099-03-17T22:11:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'report-plan-fallback',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_short_continuation',
                    'selected_score': 87,
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 87,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 46,
                    'score_gap_vs_no_trade': 41,
                    'planned_size_usd': 88.5,
                },
                'entry_result': {
                    'action': 'queued',
                    'decision_id': 'report-plan-fallback',
                    'symbol': 'BTC',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'submitted',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        stdout = self._run_daily_analytics_report()

        self.assertIn('current_choice BTC / report-plan-fallback: selected=perp_short_continuation type=trade', stdout)
        self.assertIn('current_trade_plan: planned_size=$88.50 notional=$88.50', stdout)

    def test_daily_analytics_report_best_current_plan_uses_planned_size_fallback(self):
        init_db(self.db_path)
        decision_ts = '2099-03-17T22:20:00.000Z'
        for signal_type, score, metadata in [
            ('perp_short_continuation', 91, {
                'market_age_minutes': 5,
                'stop_loss_price': 65250.0,
                'take_profit_price': 64000.0,
                'invalidation_price': 65300.0,
            }),
            ('perp_short_failed_bounce', 73, {}),
            ('perp_no_trade', 40, {}),
        ]:
            record_signal_candidate(self.db_path, {
                'ts': decision_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 64800.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'report-best-current-plan-fallback',
                'candidate_key': f'report-best-current-plan-fallback:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })
        self._record_perp_executor_cycle_event(
            '2099-03-17T22:21:00.000Z',
            {
                'ts': '2099-03-17T22:21:00.000Z',
                'requested_mode': 'paper',
                'active_mode': 'paper',
                'candidate_choice': {
                    'type': 'trade',
                    'decision_id': 'report-best-current-plan-fallback',
                    'symbol': 'BTC',
                    'selected_signal_type': 'perp_short_continuation',
                    'selected_score': 91,
                    'best_short_signal_type': 'perp_short_continuation',
                    'best_short_score': 91,
                    'no_trade_signal_type': 'perp_no_trade',
                    'no_trade_score': 40,
                    'score_gap_vs_no_trade': 51,
                    'planned_size_usd': 88.5,
                    'market_age_minutes': 5,
                },
                'entry_result': {
                    'action': 'queued',
                    'decision_id': 'report-best-current-plan-fallback',
                    'symbol': 'BTC',
                    'signal_type': 'perp_short_continuation',
                    'reason': 'submitted',
                },
                'position_result': {'action': 'none', 'results': [], 'closed_count': 0},
            },
        )

        stdout = self._run_daily_analytics_report()

        self.assertIn('best_current_opportunity BTC / report-best-current-plan-fallback: status=trade_ready selected=perp_short_continuation', stdout)
        self.assertIn('best_current_plan: planned_size=$88.50 notional=$88.50 market_age_min=5.0 stop=$65250.0000 target=$64000.0000 invalidate=$65300.0000', stdout)

    def test_perps_auto_trade_live_request_denies_missing_pilot_decision(self):
        init_db(self.db_path)
        shim_path = self._write_strategy_controls_shim([
            "data.pop('tiny_live_pilot_decision', None)",
        ])
        result, payload = self._run_perps_auto_trade(
            extra_env={
                'PERPS_AUTO_TRADE_ALLOW_LIVE': '1',
                'PERPS_AUTO_TRADE_DB_CLI': str(shim_path),
            },
            args=['--live'],
        )
        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'missing_tiny_live_pilot_decision')
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])
        self.assertIn('pilot_policy', payload)
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT event_type, metadata_json FROM risk_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            system_row = conn.execute(
                "SELECT event_type, metadata_json FROM system_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(risk_row[0], 'perp_live_policy_denied')
        self.assertEqual(system_row[0], 'perp_live_policy_denied')
        self.assertEqual(json.loads(risk_row[1])['denial_reason'], 'missing_tiny_live_pilot_decision')
        self.assertEqual(json.loads(system_row[1])['denial_reason'], 'missing_tiny_live_pilot_decision')
        self.assertIn('missing_tiny_live_pilot_decision', result.stdout)

    def test_perps_auto_trade_live_request_denies_unapproved_perp_policy(self):
        init_db(self.db_path)
        shim_path = self._write_strategy_controls_shim([
            "data['tiny_live_pilot_decision'] = {'approved': False, 'mode': 'paper_only', 'product_type': 'perps', 'strategy': 'perp_short_continuation', 'symbol': 'SOL/BTC/ETH basket', 'reason': 'not enough evidence', 'blockers': ['perp_win_rate_below_threshold']}"
        ])
        _, payload = self._run_perps_auto_trade(
            extra_env={
                'PERPS_AUTO_TRADE_ALLOW_LIVE': '1',
                'PERPS_AUTO_TRADE_DB_CLI': str(shim_path),
            },
            args=['--live'],
        )
        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'tiny_live_pilot_not_approved')
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])
        self.assertFalse(payload['pilot_policy']['tiny_live_pilot_decision']['approved'])
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(json.loads(risk_row[0])['denial_reason'], 'tiny_live_pilot_not_approved')

    def test_perps_auto_trade_live_request_denies_product_type_mismatch(self):
        init_db(self.db_path)
        shim_path = self._write_strategy_controls_shim([
            "data['tiny_live_pilot_decision'] = {'approved': True, 'mode': 'tiny_live_pilot', 'product_type': 'spot', 'strategy': 'mean_reversion_near_buy', 'symbol': 'WIF', 'reason': 'spot approved'}"
        ])
        _, payload = self._run_perps_auto_trade(
            extra_env={
                'PERPS_AUTO_TRADE_ALLOW_LIVE': '1',
                'PERPS_AUTO_TRADE_DB_CLI': str(shim_path),
            },
            args=['--live'],
        )
        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'pilot_product_type_mismatch')
        self.assertEqual(payload['pilot_policy']['tiny_live_pilot_decision']['product_type'], 'spot')
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(json.loads(risk_row[0])['denial_reason'], 'pilot_product_type_mismatch')

    def test_perps_auto_trade_live_request_denies_non_actionable_pilot_strategy(self):
        init_db(self.db_path)
        shim_path = self._write_strategy_controls_shim([
            "data['tiny_live_pilot_decision'] = {'approved': True, 'mode': 'tiny_live_pilot', 'product_type': 'perps', 'strategy': 'perp_no_trade', 'symbol': 'SOL/BTC/ETH basket', 'reason': 'bad policy object'}"
        ])
        _, payload = self._run_perps_auto_trade(
            extra_env={
                'PERPS_AUTO_TRADE_ALLOW_LIVE': '1',
                'PERPS_AUTO_TRADE_DB_CLI': str(shim_path),
            },
            args=['--live'],
        )
        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'pilot_strategy_not_actionable')
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])

    def test_perps_auto_trade_live_request_treats_no_trade_as_no_live_candidate(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history()
        ts = '2099-03-18T00:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -1.0, 'highUsd24h': 101.0, 'lowUsd24h': 99.0, 'volumeUsd24h': 60000000.0})
        for signal_type, score in [('perp_short_continuation', 74), ('perp_short_failed_bounce', 68), ('perp_no_trade', 95)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'stable',
                'decision_id': 'approved-no-trade',
                'candidate_key': f'approved-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}, 'metrics': {'highRecent': 101.0, 'change24h': -1.0, 'volume24h': 60000000.0}},
            })
        _, payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1'},
            args=['--live'],
        )
        self.assertEqual(payload['entry_result']['action'], 'no_trade')
        self.assertEqual(payload['pilot_policy']['policy_status'], 'approved_but_no_live_candidate')
        self.assertIsNone(payload['pilot_policy']['denial_reason'])
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_mode_stubbed' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(json.loads(risk_row[0])['policy_status'], 'approved_but_no_live_candidate')

    def test_perps_auto_trade_live_request_preserves_blocked_no_trade_candidate_strategy_context(self):
        init_db(self.db_path)
        shim_path = self._write_strategy_controls_shim([
            "data['tiny_live_pilot_decision'] = {'approved': True, 'mode': 'tiny_live_pilot', 'product_type': 'perps', 'strategy': 'perp_short_continuation', 'symbol': 'BONK', 'reason': 'explicit test approval'}"
        ])
        ts = '2099-03-18T00:00:00.000Z'
        for signal_type, score in [('perp_short_continuation', 93), ('perp_short_failed_bounce', 71), ('perp_no_trade', 22)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BONK',
                'market': 'BONK',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 0.00002,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'blocked-no-trade-live-request',
                'candidate_key': f'blocked-no-trade-live-request:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}, 'metrics': {'highRecent': 0.000021, 'change24h': -4.0, 'volume24h': 60000000.0}},
            })

        _, payload = self._run_perps_auto_trade(
            extra_env={
                'PERPS_AUTO_TRADE_ALLOW_LIVE': '1',
                'PERPS_AUTO_TRADE_DB_CLI': str(shim_path),
            },
            args=['--live'],
        )

        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['entry_result']['action'], 'no_trade')
        self.assertEqual(payload['candidate_choice']['type'], 'no_trade')
        self.assertTrue(payload['candidate_choice']['blocked_trade'])
        self.assertEqual(payload['candidate_choice']['block_reason'], 'symbol_not_allowlisted')
        self.assertEqual(payload['candidate_choice']['selected_signal_type'], 'perp_no_trade')
        self.assertEqual(payload['candidate_choice']['candidate_strategy'], 'perp_short_continuation')
        self.assertEqual(payload['pilot_policy']['candidate_strategy'], 'perp_short_continuation')
        self.assertEqual(payload['pilot_policy']['candidate_symbol'], 'BONK')
        self.assertEqual(payload['pilot_policy']['policy_status'], 'approved_for_executor')
        self.assertIsNone(payload['pilot_policy']['denial_reason'])
        self.assertTrue(payload['pilot_policy']['live_eligible_for_executor'])

        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_mode_stubbed' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            cycle_row = conn.execute(
                "SELECT metadata_json FROM system_events WHERE event_type='perp_executor_cycle' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        risk_meta = json.loads(risk_row[0])
        cycle_meta = json.loads(cycle_row[0])
        self.assertEqual(risk_meta['candidate_strategy'], 'perp_short_continuation')
        self.assertEqual(risk_meta['candidate_symbol'], 'BONK')
        self.assertEqual(risk_meta['policy_status'], 'approved_for_executor')
        self.assertEqual(cycle_meta['summary']['candidate_choice']['candidate_strategy'], 'perp_short_continuation')

    def test_perps_auto_trade_live_request_denies_strategy_mismatch_and_keeps_paper_execution(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history()
        ts = '2099-03-18T00:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 100.0, 'changePct24h': -3.0, 'highUsd24h': 102.0, 'lowUsd24h': 98.0, 'volumeUsd24h': 60000000.0})
        for signal_type, score in [('perp_short_continuation', 74), ('perp_short_failed_bounce', 91), ('perp_no_trade', 15)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 100.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'current-live-request',
                'candidate_key': f'current-live-request:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}, 'metrics': {'highRecent': 101.0, 'change24h': -3.0, 'volume24h': 60000000.0}},
            })
        _, payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1'},
            args=['--live'],
        )
        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(payload['entry_result']['signal_type'], 'perp_short_failed_bounce')
        self.assertEqual(payload['pilot_policy']['policy_status'], 'paper_only')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'pilot_strategy_mismatch')
        self.assertTrue(payload['pilot_policy']['tiny_live_pilot_decision']['approved'])
        self.assertEqual(payload['pilot_policy']['tiny_live_pilot_decision']['product_type'], 'perps')
        self.assertEqual(payload['pilot_policy']['tiny_live_pilot_decision']['strategy'], 'perp_short_continuation')
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            trade_row = conn.execute(
                "SELECT mode, product_type, strategy_tag FROM auto_trades WHERE decision_id='current-live-request' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        risk_meta = json.loads(risk_row[0])
        self.assertEqual(risk_meta['denial_reason'], 'pilot_strategy_mismatch')
        self.assertEqual(risk_meta['candidate_strategy'], 'perp_short_failed_bounce')
        self.assertEqual(risk_meta['tiny_live_pilot_decision']['strategy'], 'perp_short_continuation')
        self.assertEqual(trade_row[0], 'paper')
        self.assertEqual(trade_row[1], 'perps')
        self.assertEqual(trade_row[2], 'tiny_live_pilot_perp_short_failed_bounce')

    def test_perps_auto_trade_live_request_denies_symbol_mismatch_and_keeps_paper_execution(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='SOL')
        ts = '2099-03-18T00:00:00.000Z'
        future_ts = '2099-03-18T00:45:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.0, 'highUsd24h': 66000.0, 'lowUsd24h': 64000.0, 'volumeUsd24h': 80000000.0})
        record_perp_market_snapshot(self.db_path, {'ts': future_ts, 'asset': 'BTC', 'priceUsd': 64000.0, 'changePct24h': -2.5, 'highUsd24h': 66000.0, 'lowUsd24h': 63500.0, 'volumeUsd24h': 82000000.0})
        for signal_type, score in [('perp_short_continuation', 93), ('perp_short_failed_bounce', 71), ('perp_no_trade', 22)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'btc-live-request',
                'candidate_key': f'btc-live-request:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'score_components': {'edge': score}, 'metrics': {'highRecent': 65100.0, 'change24h': -2.0, 'volume24h': 80000000.0}},
            })

        _, payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1'},
            args=['--live'],
        )

        self.assertEqual(payload['requested_mode'], 'live')
        self.assertEqual(payload['active_mode'], 'paper')
        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(payload['entry_result']['symbol'], 'BTC')
        self.assertEqual(payload['pilot_policy']['candidate_symbol'], 'BTC')
        self.assertEqual(payload['pilot_policy']['tiny_live_pilot_decision']['symbol'], 'SOL')
        self.assertEqual(payload['pilot_policy']['denial_reason'], 'pilot_symbol_mismatch')
        self.assertFalse(payload['pilot_policy']['live_eligible_for_executor'])

        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_live_policy_denied' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            trade_row = conn.execute(
                "SELECT mode, product_type, strategy_tag, symbol FROM auto_trades WHERE decision_id='btc-live-request' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        risk_meta = json.loads(risk_row[0])
        self.assertEqual(risk_meta['denial_reason'], 'pilot_symbol_mismatch')
        self.assertEqual(risk_meta['candidate_symbol'], 'BTC')
        self.assertEqual(risk_meta['tiny_live_pilot_decision']['symbol'], 'SOL')
        self.assertEqual(trade_row[0], 'paper')
        self.assertEqual(trade_row[1], 'perps')
        self.assertEqual(trade_row[2], 'tiny_live_pilot_perp_short_continuation')
        self.assertEqual(trade_row[3], 'BTC')

    def test_perps_auto_trade_live_request_creates_pending_telegram_approval(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='BTC', future_price=90.0)
        ts = '2099-03-18T00:30:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.5, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        for signal_type, score in [('perp_short_continuation', 88), ('perp_short_failed_bounce', 70), ('perp_no_trade', 20)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'btc-live-approval-request',
                'candidate_key': f'btc-live-approval-request:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 65100.0, 'change24h': -2.5, 'volume24h': 90000000.0}},
            })

        _, payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1', 'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0'},
            args=['--live'],
        )
        approval = self._read_perps_live_approval()
        conn = sqlite3.connect(self.db_path)
        try:
            event_row = conn.execute(
                "SELECT metadata_json FROM system_events WHERE event_type='perp_live_approval_requested' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(payload['entry_result']['action'], 'awaiting_approval')
        self.assertEqual(payload['entry_result']['reason'], 'awaiting_telegram_approval')
        self.assertEqual(approval['status'], 'pending')
        self.assertEqual(approval['decision_id'], 'btc-live-approval-request')
        self.assertIn('APPROVE_PERP', approval['commands']['approve'])
        self.assertEqual(json.loads(event_row[0])['approval_intent']['decision_id'], 'btc-live-approval-request')

    def test_perps_auto_trade_live_request_executes_live_stub_after_approval(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='BTC', future_price=90.0)
        ts = '2099-03-18T00:35:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.5, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        for signal_type, score in [('perp_short_continuation', 88), ('perp_short_failed_bounce', 70), ('perp_no_trade', 20)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'btc-live-approved',
                'candidate_key': f'btc-live-approved:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 65100.0, 'change24h': -2.5, 'volume24h': 90000000.0}},
            })
        _, first_payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1', 'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0', 'PERPS_AUTO_TRADE_LIVE_STUB_NOTIONAL_USD': '1'},
            args=['--live'],
        )
        approval = self._read_perps_live_approval()
        approval['status'] = 'approved'
        approval['approved_at'] = self._iso_ts(datetime.now(timezone.utc))
        self._write_perps_live_approval(approval)
        _, second_payload = self._run_perps_auto_trade(
            extra_env={'PERPS_AUTO_TRADE_ALLOW_LIVE': '1', 'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0', 'PERPS_AUTO_TRADE_LIVE_STUB_NOTIONAL_USD': '1'},
            args=['--live'],
        )
        final_approval = self._read_perps_live_approval()
        conn = sqlite3.connect(self.db_path)
        try:
            event_row = conn.execute(
                "SELECT metadata_json FROM system_events WHERE event_type='perp_live_stub_entry_submitted' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            trade_row = conn.execute(
                "SELECT validation_mode, approval_status FROM auto_trades WHERE decision_id='btc-live-approved' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(first_payload['entry_result']['action'], 'awaiting_approval')
        self.assertEqual(second_payload['entry_result']['execution_mode'], 'live_stub')
        self.assertEqual(second_payload['entry_result']['approval_status'], 'telegram_approved')
        self.assertEqual(final_approval['status'], 'executed')
        self.assertEqual(json.loads(event_row[0])['decision_id'], 'btc-live-approved')
        self.assertEqual(trade_row[0], 'tiny_live_stub')
        self.assertEqual(trade_row[1], 'telegram_approved')

    def test_perps_auto_trade_processes_flatten_all_command(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.0, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        upsert_perp_position(self.db_path, {
            'position_key': 'paper-perp:live-flat',
            'opened_ts': ts,
            'updated_ts': ts,
            'status': 'open',
            'asset': 'BTC',
            'side': 'sell',
            'collateral_token': 'USDC',
            'entry_price_usd': 65000.0,
            'mark_price_usd': 65000.0,
            'liq_price_usd': 130000.0,
            'size_usd': 1.0,
            'notional_usd': 1.0,
            'margin_used_usd': 1.0,
            'leverage': 1.0,
            'take_profit_price': 64000.0,
            'stop_loss_price': 65200.0,
            'unrealized_pnl_usd': 0.0,
            'realized_pnl_usd': 0.0,
            'fees_usd': 0.0,
            'funding_usd': 0.0,
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'mode': 'paper',
            'decision_id': 'live-flat',
            'source': 'perps-auto-trade.mjs',
            'raw': {'strategy_family': 'tiny_live_pilot', 'live_stub': True, 'live_approval': {'status': 'telegram_approved'}},
        })
        self._write_perps_live_command({'version': 1, 'command': {'status': 'pending', 'requested_at': ts, 'command_type': 'flatten_all', 'source': 'telegram'}})

        _, payload = self._run_perps_auto_trade(extra_env={'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0'})
        conn = sqlite3.connect(self.db_path)
        try:
            status = conn.execute("SELECT status FROM perp_positions WHERE position_key='paper-perp:live-flat'").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(payload['command_result']['action'], 'flatten_all')
        self.assertEqual(status, 'closed')

    def test_perps_auto_trade_processes_reduce_command(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'ETH', 'priceUsd': 3200.0, 'changePct24h': -2.0, 'highUsd24h': 3210.0, 'lowUsd24h': 3180.0, 'volumeUsd24h': 85000000.0})
        upsert_perp_position(self.db_path, {
            'position_key': 'paper-perp:live-reduce',
            'opened_ts': ts,
            'updated_ts': ts,
            'status': 'open',
            'asset': 'ETH',
            'side': 'sell',
            'collateral_token': 'USDC',
            'entry_price_usd': 3200.0,
            'mark_price_usd': 3200.0,
            'liq_price_usd': 6400.0,
            'size_usd': 2.0,
            'notional_usd': 2.0,
            'margin_used_usd': 2.0,
            'leverage': 1.0,
            'take_profit_price': 3140.0,
            'stop_loss_price': 3225.0,
            'unrealized_pnl_usd': 0.0,
            'realized_pnl_usd': 0.0,
            'fees_usd': 0.0,
            'funding_usd': 0.0,
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'mode': 'paper',
            'decision_id': 'live-reduce',
            'source': 'perps-auto-trade.mjs',
            'raw': {'strategy_family': 'tiny_live_pilot', 'quantity': 2.0 / 3200.0, 'live_stub': True, 'live_approval': {'status': 'telegram_approved'}},
        })
        self._write_perps_live_command({'version': 1, 'command': {'status': 'pending', 'requested_at': ts, 'command_type': 'reduce_position', 'target_ref': 'paper-perp:live-reduce', 'reduction_fraction': 0.5, 'source': 'telegram'}})

        _, payload = self._run_perps_auto_trade(extra_env={'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0'})
        conn = sqlite3.connect(self.db_path)
        try:
            notional = conn.execute("SELECT notional_usd FROM perp_positions WHERE position_key='paper-perp:live-reduce'").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(payload['command_result']['action'], 'reduce_position')
        self.assertAlmostEqual(notional, 1.0, places=6)

    def test_perps_auto_trade_prefers_symbol_specific_competition_bonus(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='SOL', future_price=99.8)
        self._seed_approved_perp_pilot_history(symbol='BTC', future_price=90.0)

        current_rows = [
            ('2099-03-18T00:00:00.000Z', 'BTC', 'btc-current'),
            ('2099-03-18T00:05:00.000Z', 'SOL', 'sol-current'),
        ]
        for ts, symbol, decision_id in current_rows:
            price = 65000.0 if symbol == 'BTC' else 100.0
            high_recent = 65100.0 if symbol == 'BTC' else 101.0
            volume = 80000000.0 if symbol == 'BTC' else 60000000.0
            record_perp_market_snapshot(self.db_path, {
                'ts': ts,
                'asset': symbol,
                'priceUsd': price,
                'changePct24h': -2.5,
                'highUsd24h': high_recent,
                'lowUsd24h': price * 0.98,
                'volumeUsd24h': volume,
            })
            for signal_type, score in [('perp_short_continuation', 74), ('perp_short_failed_bounce', 60), ('perp_no_trade', 20)]:
                record_signal_candidate(self.db_path, {
                    'ts': ts,
                    'source': 'perps-monitor.mjs',
                    'symbol': symbol,
                    'market': symbol,
                    'signal_type': signal_type,
                    'strategy_tag': signal_type,
                    'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                    'product_type': 'perps',
                    'price': price,
                    'score': score,
                    'regime_tag': 'trend_down',
                    'decision_id': decision_id,
                    'candidate_key': f'{decision_id}:{signal_type}',
                    'status': 'candidate',
                    'reason': signal_type,
                    'metadata': {'score_components': {'edge': score}, 'metrics': {'highRecent': high_recent, 'change24h': -2.5, 'volume24h': volume}},
                })

        _, payload = self._run_perps_auto_trade()

        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(payload['entry_result']['symbol'], 'BTC')
        self.assertEqual(payload['candidate_choice']['symbol'], 'BTC')
        self.assertEqual(payload['candidate_choice']['selected_signal_type'], 'perp_short_continuation')

    def test_perps_auto_trade_does_not_let_blocked_fallback_outrank_trade_ready_symbol(self):
        init_db(self.db_path)
        now = datetime.now(timezone.utc)
        signal_ts = self._iso_ts(now)
        stale_market_ts = self._iso_ts(now - timedelta(minutes=30))

        market_specs = {
            'SOL': {'price': 172.0, 'high_recent': 176.0, 'volume': 65000000.0, 'ts': stale_market_ts},
            'BTC': {'price': 65000.0, 'high_recent': 65150.0, 'volume': 90000000.0, 'ts': signal_ts},
        }
        for symbol, market in market_specs.items():
            record_perp_market_snapshot(self.db_path, {
                'ts': market['ts'],
                'asset': symbol,
                'priceUsd': market['price'],
                'changePct24h': -3.0,
                'highUsd24h': market['high_recent'],
                'lowUsd24h': market['price'] * 0.98,
                'volumeUsd24h': market['volume'],
            })

        for signal_type, score in [('perp_short_continuation', 96), ('perp_short_failed_bounce', 81)]:
            record_signal_candidate(self.db_path, {
                'ts': signal_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell',
                'product_type': 'perps',
                'price': market_specs['SOL']['price'],
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'sol-blocked-fallback',
                'candidate_key': f'sol-blocked-fallback:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {
                    'score_components': {'edge': score},
                    'metrics': {
                        'highRecent': market_specs['SOL']['high_recent'],
                        'change24h': -3.0,
                        'volume24h': market_specs['SOL']['volume'],
                    },
                },
            })

        for signal_type, score in [('perp_short_continuation', 78), ('perp_short_failed_bounce', 66), ('perp_no_trade', 18)]:
            record_signal_candidate(self.db_path, {
                'ts': signal_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': market_specs['BTC']['price'],
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'btc-trade-ready',
                'candidate_key': f'btc-trade-ready:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {
                    'score_components': {'edge': score},
                    'metrics': {
                        'highRecent': market_specs['BTC']['high_recent'],
                        'change24h': -3.0,
                        'volume24h': market_specs['BTC']['volume'],
                    },
                },
            })

        _, payload = self._run_perps_auto_trade(extra_env={'PERPS_AUTO_TRADE_MAX_MARKET_STALENESS_MINUTES': '20'})

        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(payload['entry_result']['symbol'], 'BTC')
        self.assertEqual(payload['candidate_choice']['decision_id'], 'btc-trade-ready')
        self.assertEqual(payload['candidate_choice']['selected_signal_type'], 'perp_short_continuation')
        self.assertFalse(payload['candidate_choice']['blocked_trade'])

    def test_perps_auto_trade_keeps_candidate_strategy_null_for_genuine_no_trade_cycle(self):
        init_db(self.db_path)
        now = datetime.now(timezone.utc)
        signal_ts = self._iso_ts(now)
        price = 65000.0
        high_recent = 65100.0
        volume = 85000000.0

        record_perp_market_snapshot(self.db_path, {
            'ts': signal_ts,
            'asset': 'BTC',
            'priceUsd': price,
            'changePct24h': -1.5,
            'highUsd24h': high_recent,
            'lowUsd24h': price * 0.985,
            'volumeUsd24h': volume,
        })
        for signal_type, score in [('perp_short_continuation', 74), ('perp_short_failed_bounce', 68), ('perp_no_trade', 92)]:
            record_signal_candidate(self.db_path, {
                'ts': signal_ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': price,
                'score': score,
                'regime_tag': 'chop',
                'decision_id': 'btc-genuine-no-trade',
                'candidate_key': f'btc-genuine-no-trade:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {
                    'score_components': {'edge': score},
                    'metrics': {'highRecent': high_recent, 'change24h': -1.5, 'volume24h': volume},
                },
            })

        _, payload = self._run_perps_auto_trade(extra_env={'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0'})
        state = get_perp_executor_state(self.db_path, lookback_minutes=1440, analytics_lookback_hours=24, anchor_ts=self._iso_ts(now + timedelta(minutes=1)))

        self.assertEqual(payload['entry_result']['action'], 'no_trade')
        self.assertEqual(payload['candidate_choice']['decision_id'], 'btc-genuine-no-trade')
        self.assertFalse(payload['candidate_choice']['blocked_trade'])
        self.assertIsNone(payload['candidate_choice'].get('candidate_strategy'))
        self.assertIsNone(state['latest_candidate_choice'].get('candidate_strategy'))

    def test_perps_auto_trade_recovers_submit_timeout_without_duplicate_fill(self):
        init_db(self.db_path)
        ts = '2099-03-18T00:00:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.0, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        for signal_type, score in [('perp_short_continuation', 89), ('perp_short_failed_bounce', 70), ('perp_no_trade', 25)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'timeout-recovery-open',
                'candidate_key': f'timeout-recovery-open:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 65100.0, 'change24h': -2.0, 'volume24h': 90000000.0}},
            })
        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_SIMULATED_SUBMIT_TIMEOUT_MODE': 'filled',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
        })
        journal = self._read_perps_journal()
        action = journal['actions']['open:timeout-recovery-open']
        conn = sqlite3.connect(self.db_path)
        try:
            fill_count = conn.execute("SELECT COUNT(*) FROM perp_fills WHERE decision_id='timeout-recovery-open'").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(payload['entry_result']['reason'], 'submit_timeout_after_possible_fill')
        self.assertEqual(payload['recovery']['final']['recovered_timeout_actions'], 1)
        self.assertEqual(fill_count, 1)
        self.assertEqual(action['status'], 'open_position_active')
        self.assertEqual(action['last_risk_decision'], 'timeout_recovered_via_reconciliation')
        self.assertEqual(action['submitted_order_ids'], ['open:timeout-recovery-open:order:1'])

    def test_perps_auto_trade_handles_partial_fill_with_cancel_replace_recovery(self):
        init_db(self.db_path)
        ts = '2099-03-18T00:05:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'ETH', 'priceUsd': 3200.0, 'changePct24h': -3.0, 'highUsd24h': 3215.0, 'lowUsd24h': 3170.0, 'volumeUsd24h': 85000000.0})
        for signal_type, score in [('perp_short_continuation', 88), ('perp_short_failed_bounce', 69), ('perp_no_trade', 15)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'ETH',
                'market': 'ETH',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 3200.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'partial-fill-recovery',
                'candidate_key': f'partial-fill-recovery:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 3215.0, 'change24h': -3.0, 'volume24h': 85000000.0}},
            })
        _, first_payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_SIMULATED_PARTIAL_FILL_PCT': '50',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
        })
        _, second_payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_SIMULATED_PARTIAL_FILL_PCT': '50',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
        })
        journal = self._read_perps_journal()
        action = journal['actions']['open:partial-fill-recovery']
        conn = sqlite3.connect(self.db_path)
        try:
            order_statuses = [row[0] for row in conn.execute("SELECT status FROM perp_orders WHERE decision_id='partial-fill-recovery' ORDER BY id")]
            fill_count = conn.execute("SELECT COUNT(*) FROM perp_fills WHERE decision_id='partial-fill-recovery'").fetchone()[0]
            position_notional = conn.execute("SELECT notional_usd FROM perp_positions WHERE position_key='paper-perp:partial-fill-recovery'").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(first_payload['entry_result']['action'], 'partial_fill')
        self.assertEqual(second_payload['entry_result']['action'], 'skip')
        self.assertEqual(second_payload['entry_result']['reason'], 'active_position_exists')
        self.assertEqual(fill_count, 2)
        self.assertIn('partially_filled', order_statuses)
        self.assertIn('cancelled', order_statuses)
        self.assertIn('filled', order_statuses)
        self.assertAlmostEqual(position_notional, 3.0, places=6)
        self.assertEqual(action['status'], 'open_position_active')
        self.assertEqual(action['replacement_count'], 1)
        self.assertEqual(action['last_risk_decision'], 'partial_fill_cancel_replace_completed')

    def test_perps_auto_trade_records_response_drift_in_recovery_journal(self):
        init_db(self.db_path)
        ts = '2099-03-18T00:10:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 180.0, 'changePct24h': -2.5, 'highUsd24h': 181.0, 'lowUsd24h': 177.0, 'volumeUsd24h': 70000000.0})
        for signal_type, score in [('perp_short_continuation', 86), ('perp_short_failed_bounce', 68), ('perp_no_trade', 20)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 180.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'response-drift-open',
                'candidate_key': f'response-drift-open:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 181.0, 'change24h': -2.5, 'volume24h': 70000000.0}},
            })
        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_SIMULATED_RESPONSE_DRIFT_BPS': '120',
            'PERPS_AUTO_TRADE_MAX_QUOTE_DRIFT_BPS': '50',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
        })
        journal = self._read_perps_journal()
        action = journal['actions']['open:response-drift-open']
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_execution_response_drift' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        risk_meta = json.loads(risk_row[0])
        self.assertEqual(payload['entry_result']['action'], 'opened')
        self.assertEqual(action['last_risk_decision'], 'response_drift_detected')
        self.assertIn('response_drift_detected', action['recovery_notes'])
        self.assertGreater(abs(risk_meta['drift_bps']), 50)

    def test_perps_auto_trade_recovers_orphaned_open_position_after_restart(self):
        init_db(self.db_path)
        ts = '2099-03-18T00:15:00.000Z'
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 180.0, 'changePct24h': -1.0, 'highUsd24h': 181.0, 'lowUsd24h': 179.0, 'volumeUsd24h': 72000000.0})
        upsert_perp_position(self.db_path, {
            'position_key': 'paper-perp:orphaned-sol',
            'opened_ts': ts,
            'updated_ts': ts,
            'status': 'open',
            'asset': 'SOL',
            'side': 'sell',
            'collateral_token': 'USDC',
            'entry_price_usd': 180.0,
            'mark_price_usd': 180.0,
            'liq_price_usd': 360.0,
            'size_usd': 3.0,
            'notional_usd': 3.0,
            'margin_used_usd': 3.0,
            'leverage': 1.0,
            'take_profit_price': 176.0,
            'stop_loss_price': 182.0,
            'unrealized_pnl_usd': 0.0,
            'realized_pnl_usd': 0.0,
            'fees_usd': 0.0,
            'funding_usd': 0.0,
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'mode': 'paper',
            'decision_id': 'orphaned-sol',
            'source': 'perps-auto-trade.mjs',
            'raw': {'strategy_family': 'tiny_live_pilot'},
        })
        _, payload = self._run_perps_auto_trade(extra_env={'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0'})
        journal = self._read_perps_journal()
        recovered = journal['actions']['recovered:paper-perp:orphaned-sol']
        conn = sqlite3.connect(self.db_path)
        try:
            risk_row = conn.execute(
                "SELECT metadata_json FROM risk_events WHERE event_type='perp_orphan_position_recovered' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(payload['recovery']['initial']['recovered_orphan_positions'], 1)
        self.assertEqual(recovered['status'], 'open_position_active')
        self.assertEqual(recovered['last_risk_decision'], 'orphaned_open_position_recovered')
        self.assertEqual(json.loads(risk_row[0])['position_key'], 'paper-perp:orphaned-sol')

    def test_perps_auto_trade_blocks_entry_when_drawdown_limit_hit(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.0, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        record_perp_fill(self.db_path, {
            'ts': ts,
            'position_key': 'closed-dd-1',
            'order_key': 'closed-dd-order-1',
            'asset': 'SOL',
            'side': 'buy',
            'action': 'close',
            'price_usd': 180.0,
            'size_usd': 3.0,
            'fees_usd': 0.01,
            'funding_usd': 0.0,
            'realized_pnl_usd': -1.0,
            'mode': 'paper',
            'strategy_tag': 'tiny_live_pilot_perp_short_continuation',
            'decision_id': 'closed-dd-1',
        })
        for signal_type, score in [('perp_short_continuation', 84), ('perp_short_failed_bounce', 70), ('perp_no_trade', 15)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'drawdown-blocked',
                'candidate_key': f'drawdown-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 65100.0, 'change24h': -2.0, 'volume24h': 90000000.0}},
            })
        journal_path = self.data_dir / 'perps_auto_trade_journal.json'
        journal_path.write_text(json.dumps({'version': 2, 'actions': {}, 'state': {'equity': {'baseline_equity_usd': 10.0, 'peak_equity_usd': 10.0, 'last_equity_usd': 10.0, 'current_drawdown_pct': 0.0, 'max_drawdown_pct': 0.0}}}))

        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_PAPER_EQUITY_USD': '10',
            'PERPS_AUTO_TRADE_MAX_ACCOUNT_DRAWDOWN_PCT': '5',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
            'PERPS_AUTO_TRADE_SYMBOL_COOLDOWN_MINUTES': '0',
        })

        self.assertEqual(payload['entry_result']['action'], 'skip')
        self.assertEqual(payload['entry_result']['reason'], 'account_drawdown_cap_hit')
        self.assertGreater(payload['entry_result']['risk_guard']['equity']['current_drawdown_pct'], 5)

    def test_perps_auto_trade_blocks_entry_when_notional_to_equity_limit_hit(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'BTC', 'priceUsd': 65000.0, 'changePct24h': -2.0, 'highUsd24h': 65100.0, 'lowUsd24h': 64500.0, 'volumeUsd24h': 90000000.0})
        for signal_type, score in [('perp_short_continuation', 84), ('perp_short_failed_bounce', 70), ('perp_no_trade', 15)]:
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'BTC',
                'market': 'BTC',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 65000.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'equity-ratio-blocked',
                'candidate_key': f'equity-ratio-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': {'metrics': {'highRecent': 65100.0, 'change24h': -2.0, 'volume24h': 90000000.0}},
            })

        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_PAPER_EQUITY_USD': '5',
            'PERPS_AUTO_TRADE_PAPER_NOTIONAL_USD': '3',
            'PERPS_AUTO_TRADE_MAX_NOTIONAL_TO_EQUITY_PCT': '40',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
            'PERPS_AUTO_TRADE_SYMBOL_COOLDOWN_MINUTES': '0',
        })

        self.assertEqual(payload['entry_result']['action'], 'skip')
        self.assertEqual(payload['entry_result']['reason'], 'notional_to_equity_cap_hit')
        self.assertGreater(payload['entry_result']['risk_guard']['notional_to_equity_pct'], 40)

    def test_perps_auto_trade_blocks_entry_when_estimated_spread_is_too_wide(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'ETH', 'priceUsd': 3200.0, 'changePct24h': -3.0, 'highUsd24h': 3215.0, 'lowUsd24h': 3170.0, 'volumeUsd24h': 85000000.0})
        for signal_type, score in [('perp_short_continuation', 88), ('perp_short_failed_bounce', 69), ('perp_no_trade', 15)]:
            metadata = {'metrics': {'highRecent': 3215.0, 'change24h': -3.0, 'volume24h': 85000000.0}}
            if signal_type != 'perp_no_trade':
                metadata['execution'] = {'estimated_spread_bps': 55, 'estimated_slippage_bps': 20}
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'ETH',
                'market': 'ETH',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 3200.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'spread-blocked',
                'candidate_key': f'spread-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })

        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_MAX_ESTIMATED_SPREAD_BPS': '25',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
            'PERPS_AUTO_TRADE_SYMBOL_COOLDOWN_MINUTES': '0',
        })

        self.assertEqual(payload['entry_result']['action'], 'skip')
        self.assertEqual(payload['entry_result']['reason'], 'estimated_spread_too_wide')
        self.assertEqual(payload['entry_result']['risk_guard']['execution_estimates']['spread_bps'], 55)

    def test_perps_auto_trade_blocks_entry_when_estimated_slippage_is_too_high(self):
        init_db(self.db_path)
        ts = self._iso_ts(datetime.now(timezone.utc))
        record_perp_market_snapshot(self.db_path, {'ts': ts, 'asset': 'SOL', 'priceUsd': 180.0, 'changePct24h': -2.5, 'highUsd24h': 181.0, 'lowUsd24h': 177.0, 'volumeUsd24h': 70000000.0})
        for signal_type, score in [('perp_short_continuation', 86), ('perp_short_failed_bounce', 68), ('perp_no_trade', 20)]:
            metadata = {'metrics': {'highRecent': 181.0, 'change24h': -2.5, 'volume24h': 70000000.0}}
            if signal_type != 'perp_no_trade':
                metadata['execution'] = {'estimated_spread_bps': 10, 'estimated_slippage_bps': 70}
            record_signal_candidate(self.db_path, {
                'ts': ts,
                'source': 'perps-monitor.mjs',
                'symbol': 'SOL',
                'market': 'SOL',
                'signal_type': signal_type,
                'strategy_tag': signal_type,
                'side': 'sell' if signal_type != 'perp_no_trade' else 'flat',
                'product_type': 'perps',
                'price': 180.0,
                'score': score,
                'regime_tag': 'trend_down',
                'decision_id': 'slippage-blocked',
                'candidate_key': f'slippage-blocked:{signal_type}',
                'status': 'candidate',
                'reason': signal_type,
                'metadata': metadata,
            })

        _, payload = self._run_perps_auto_trade(extra_env={
            'PERPS_AUTO_TRADE_MAX_ESTIMATED_SLIPPAGE_BPS': '25',
            'PERPS_AUTO_TRADE_GLOBAL_COOLDOWN_MINUTES': '0',
            'PERPS_AUTO_TRADE_SYMBOL_COOLDOWN_MINUTES': '0',
        })

        self.assertEqual(payload['entry_result']['action'], 'skip')
        self.assertEqual(payload['entry_result']['reason'], 'estimated_slippage_too_high')
        self.assertEqual(payload['entry_result']['risk_guard']['execution_estimates']['slippage_bps'], 70)

    def test_daily_analytics_report_prints_symbol_aware_perp_pilot_details(self):
        init_db(self.db_path)
        self._seed_approved_perp_pilot_history(symbol='SOL')

        stdout = self._run_daily_analytics_report(since_ts='2099-03-17T00:00:00.000Z')

        self.assertIn('perp_short_continuation SOL @60m', stdout)
        self.assertIn('best_perp_candidate: perp_short_continuation / SOL @60m', stdout)
        self.assertIn('actionable_candidate: perps perp_short_continuation / SOL @60m', stdout)
        self.assertIn('no_trade_benchmark: perps perp_no_trade / SOL @60m', stdout)
