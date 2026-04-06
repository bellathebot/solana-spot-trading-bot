import sqlite3
import tempfile
import unittest
from pathlib import Path

from trading_system.accounting_audit import get_accounting_audit
from trading_system.trading_db import init_db, record_auto_trade


class AccountingAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / 'trading.db'
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def _inventory_row(self, audit: dict, symbol: str, mode: str = 'paper') -> dict:
        for row in audit['inventory']:
            if row['symbol'] == symbol and row['mode'] == mode:
                return row
        raise AssertionError(f'missing inventory row for {mode} {symbol}')

    def test_paper_buy_opens_inventory(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'BONK', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 10.0},
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'BONK')
        self.assertEqual(audit['status'], 'clean')
        self.assertEqual(row['remaining_amount'], 2.0)
        self.assertEqual(row['cost_basis_usd'], 10.0)
        self.assertEqual(row['avg_cost_per_unit'], 5.0)

    def test_partial_exit_reduces_remaining_qty(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'SOL', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 20.0},
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'SOL', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 0.5, 'sizeUsd': 7.0},
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'SOL')
        self.assertEqual(audit['status'], 'clean')
        self.assertAlmostEqual(row['remaining_amount'], 1.5)
        self.assertAlmostEqual(row['cost_basis_usd'], 15.0)
        self.assertAlmostEqual(row['avg_cost_per_unit'], 10.0)

    def test_buy_inventory_reconstruction_falls_back_to_out_amount(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'WIF', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 0.0, 'sizeUsd': 6.0, 'out_amount': '3.0'},
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'WIF')
        self.assertEqual(audit['status'], 'clean')
        self.assertEqual(row['remaining_amount'], 3.0)
        self.assertEqual(row['cost_basis_usd'], 6.0)
        self.assertEqual(row['avg_cost_per_unit'], 2.0)

    def test_buy_inventory_reconstruction_falls_back_to_expected_out_amount(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'JTO', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 0.0, 'sizeUsd': 9.0, 'expected_out_amount': '4.5'},
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'JTO')
        self.assertEqual(audit['status'], 'clean')
        self.assertEqual(row['remaining_amount'], 4.5)
        self.assertEqual(row['cost_basis_usd'], 9.0)
        self.assertEqual(row['avg_cost_per_unit'], 2.0)

    def test_buy_inventory_prefers_out_amount_over_legacy_amount_field(self):
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
                'out_amount': '225000',
            },
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'BONK')
        self.assertEqual(audit['status'], 'clean')
        self.assertEqual(row['remaining_amount'], 225000.0)
        self.assertEqual(row['cost_basis_usd'], 1.5)

    def test_zero_notional_buy_is_flagged_and_not_counted_as_clean_inventory(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'JTO', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 2.0, 'sizeUsd': 0.0},
        )
        audit = get_accounting_audit(self.db_path)
        self.assertEqual(audit['status'], 'warning')
        self.assertEqual(audit['summary']['issue_counts']['missing_buy_notional'], 1)
        row = self._inventory_row(audit, 'JTO')
        self.assertEqual(row['remaining_amount'], 0.0)
        self.assertEqual(row['cost_basis_usd'], 0.0)

    def test_stop_loss_exit_pnl_semantics(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'RAY', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0},
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T01:00:00.000Z', 'symbol': 'RAY', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 8.0, 'reason': 'stop-loss exit', 'strategyTag': 'stop_loss_exit'},
        )
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT realized_pnl_usd, cost_basis_usd FROM auto_trades WHERE symbol = 'RAY' AND side = 'sell'").fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], -2.0)
        self.assertEqual(row[1], 10.0)
        self.assertEqual(get_accounting_audit(self.db_path)['status'], 'clean')

    def test_avg_cost_after_multiple_entries(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'JUP', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0},
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:10:00.000Z', 'symbol': 'JUP', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 3.0, 'sizeUsd': 18.0},
        )
        audit = get_accounting_audit(self.db_path)
        row = self._inventory_row(audit, 'JUP')
        self.assertEqual(audit['status'], 'clean')
        self.assertEqual(row['remaining_amount'], 4.0)
        self.assertEqual(row['cost_basis_usd'], 28.0)
        self.assertEqual(row['avg_cost_per_unit'], 7.0)

    def test_malformed_and_zero_notional_rows_surface_as_audit_failures(self):
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:00:00.000Z', 'symbol': 'PYTH', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 10.0},
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:05:00.000Z', 'symbol': 'PYTH', 'side': 'buy', 'mode': 'paper', 'simulated': True, 'amount': 0.0, 'sizeUsd': 5.0},
        )
        record_auto_trade(
            self.db_path,
            {'ts': '2026-03-17T00:10:00.000Z', 'symbol': 'PYTH', 'side': 'sell', 'mode': 'paper', 'simulated': True, 'amount': 1.0, 'sizeUsd': 0.0},
        )
        audit = get_accounting_audit(self.db_path)
        issue_types = {finding['issue_type'] for finding in audit['findings']}
        self.assertEqual(audit['status'], 'failing')
        self.assertIn('missing_buy_token_quantity', issue_types)
        self.assertIn('missing_sell_proceeds', issue_types)
        self.assertIn('zero_notional_exit', issue_types)


if __name__ == '__main__':
    unittest.main()
