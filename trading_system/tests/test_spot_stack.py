import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

from trading_system.trading_db import init_db, record_system_event, sync_from_files


class SpotStackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.repo_root = Path(__file__).resolve().parents[2]
        self.db_path = self.base / 'trading.db'
        self.data_dir = self.base / 'data'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.queue_file = self.data_dir / 'spot_recovery_manual_review.jsonl'
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def _python_env(self, extra=None):
        env = {
            **os.environ,
            'PYTHONPATH': str(self.repo_root),
            'SPOT_BOT_REPO_ROOT': str(self.repo_root),
            'AUTO_TRADER_DATA_DIR': str(self.data_dir),
            'AUTO_TRADER_DB_PATH': str(self.db_path),
        }
        if extra:
            env.update(extra)
        return env

    def _run_python(self, rel_path, *args, extra_env=None):
        return subprocess.run(
            ['python', str(self.repo_root / rel_path), *args],
            check=True,
            capture_output=True,
            text=True,
            env=self._python_env(extra_env),
        )

    def _write_fixture_files(self):
        (self.data_dir / 'price_history.json').write_text(
            json.dumps(
                {
                    'snapshots': [
                        {
                            'ts': '2026-03-17T21:45:37.888Z',
                            'prices': {
                                'SOL': {
                                    'price': 95.33,
                                    'change24h': -1.2,
                                    'liquidity': 722505892.77,
                                    'holdingAmount': 0.84,
                                    'holdingValue': 80.82,
                                    'alertBelow': 92,
                                    'buyBelow': None,
                                    'sellAbove': 110,
                                },
                                'PYTH': {
                                    'price': 0.0412,
                                    'change24h': 1.5,
                                    'liquidity': 125000.0,
                                    'holdingAmount': 0,
                                    'holdingValue': 0,
                                    'alertBelow': 0.042,
                                    'buyBelow': 0.0412,
                                    'sellAbove': 0.055,
                                },
                            },
                            'wallet': {'sol': 0.84, 'usdc': 19.2, 'totalUsd': 100.01},
                        }
                    ]
                }
            )
        )
        (self.data_dir / 'auto_trades.json').write_text(
            json.dumps(
                [
                    {
                        'ts': '2026-03-17T21:00:00.000Z',
                        'symbol': 'PYTH',
                        'side': 'buy',
                        'mode': 'paper',
                        'simulated': True,
                        'price': 0.0408,
                        'amount': 10.0,
                        'sizeUsd': 0.408,
                        'outAmount': '10',
                        'signature': 'paper-123',
                        'quotePriceImpact': 0.05,
                        'reason': 'Price <= target',
                        'strategyTag': 'mean_reversion_near_buy',
                    }
                ]
            )
        )
        (self.data_dir / 'alerts.log').write_text(
            '[2026-03-17T21:45:37.888Z] 👀 NEAR-BUY: PYTH at $0.041200 is within 0.00% of auto-buy $0.041200\n'
        )

    def _write_queue_items(self):
        rows = [
            {
                'decision_id': 'one',
                'status': 'open',
                'review_type': 'stale_submitted',
                'severity': 'high',
                'priority_score': 100,
                'queued_at': '2026-03-17T00:00:00+00:00',
            },
            {
                'decision_id': 'two',
                'status': 'open',
                'review_type': 'external_partial_fill',
                'severity': 'medium',
                'priority_score': 80,
                'queued_at': '2026-03-17T01:00:00+00:00',
            },
        ]
        self.queue_file.write_text(''.join(json.dumps(r) + '\n' for r in rows))

    def test_sync_from_files_ingests_price_and_trade_data(self):
        self._write_fixture_files()
        counts = sync_from_files(self.db_path, self.data_dir)
        self.assertGreaterEqual(counts['price_points'], 2)
        self.assertGreaterEqual(counts['auto_trades'], 1)

        conn = sqlite3.connect(self.db_path)
        try:
            symbols = [row[0] for row in conn.execute('SELECT symbol FROM price_points ORDER BY symbol')]
            self.assertIn('PYTH', symbols)
            trade = conn.execute('SELECT symbol, mode FROM auto_trades').fetchone()
            self.assertEqual(trade, ('PYTH', 'paper'))
        finally:
            conn.close()

    def test_daily_analytics_report_runs_with_repo_local_paths(self):
        record_system_event(
            self.db_path,
            {
                'ts': '2026-03-17T21:45:37.888Z',
                'event_type': 'spot_live_candidate_summary',
                'severity': 'info',
                'message': 'summary available',
                'source': 'spot_live_candidate_summary.py',
                'metadata': {'ranked_candidates': [{'symbol': 'PYTH', 'status': 'near_buy', 'blocker': 'no_recent_blocker'}]},
            },
        )
        result = self._run_python('trading_system/daily_analytics_report.py', '--db', str(self.db_path), '--since-ts', '2026-03-17T00:00:00.000Z')
        self.assertIn('Daily trading analytics', result.stdout)

    def test_manual_review_queue_cli_uses_repo_local_paths(self):
        self._write_queue_items()
        result = self._run_python('trading_system/spot_manual_review_queue.py', 'summary', '--queue-file', str(self.queue_file), '--status', 'open')
        payload = json.loads(result.stdout)
        self.assertEqual(payload['count'], 2)
        self.assertEqual(payload['by_severity']['high'], 1)

        assign = self._run_python('trading_system/spot_manual_review_queue.py', 'assign', '--queue-file', str(self.queue_file), '--decision-id', 'two', '--assignee', 'alice', '--note', 'take-it')
        assign_payload = json.loads(assign.stdout)
        self.assertEqual(assign_payload['updated'], 1)

        snooze = self._run_python('trading_system/spot_manual_review_queue.py', 'snooze', '--queue-file', str(self.queue_file), '--decision-id', 'two', '--note', 'later')
        snooze_payload = json.loads(snooze.stdout)
        self.assertEqual(snooze_payload['updated'], 1)

        expire = self._run_python('trading_system/spot_manual_review_queue.py', 'expire-snoozed', '--queue-file', str(self.queue_file), '--hours', '0')
        expire_payload = json.loads(expire.stdout)
        self.assertIn('reopened', expire_payload)

    def test_manual_review_alert_check_runs_with_repo_local_paths(self):
        self._write_queue_items()
        result = subprocess.run(
            [
                'python',
                str(self.repo_root / 'trading_system/spot_manual_review_alert_check.py'),
                '--queue-file', str(self.queue_file),
                '--max-unassigned-open', '0',
                '--max-sla-breached', '99',
            ],
            capture_output=True,
            text=True,
            env=self._python_env(),
        )
        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload['status'], 'degraded')
        self.assertIn('unassigned_open_threshold', payload['blockers'])


if __name__ == '__main__':
    unittest.main()
