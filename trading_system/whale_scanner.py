#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, '/home/brimigs')

from trading_system.trading_db import record_whale_observation

HELIUS_BIN = '/home/brimigs/.hermes/node/bin/helius'
PATH_ENV = f"/home/brimigs/.hermes/node/bin:/home/brimigs/.cargo/bin:{os.environ.get('PATH', '')}"
DEFAULT_DB = '/home/brimigs/.trading-data/trading.db'

WALLETS = [
    ('BONK whale A', 'H8F1aRaxpf7BHwAYWHvryDeCnmGZkS7MUHWptsFzfVzv', 'BONK'),
    ('BONK whale B', 'Hz9UDbrf4TBVRuauxLhLswymsxkbbMTGL7AjQ72djbPC', 'BONK'),
    ('BONK whale C', '3uQkfoxj3WK8cGTkkWDfpWExSJGWAKy4CVcZuS8Kp8ft', 'BONK'),
    ('BONK whale D', '3yrhz96ZbV5PBD4C2JhfRc93ArkfUwwnwVHN3PMFDQjY', 'BONK'),
    ('WIF whale A', 'BAd1gxwsNHdKgprCZ34Ruth1tzhCo3dfs46w8XW8BEoy', 'WIF'),
    ('JTO whale A', 'AM4iWGYTFLiNHVsrWp55CrFPgwtdCytkntJo5NhFUc12', 'JTO'),
    ('JTO whale B', 'FveZuhYEEuf7PYmUmoux9Zxo36NazwimAE4drYExEFQz', 'JTO'),
    ('RAY whale A', 'tDzMQZx2kHjT9Uoz4oBPD4q49SA7VntJU1guUVRUEBV', 'RAY'),
]

TRACKED_SYMBOLS = {'SOL', 'USDC', 'BONK', 'WIF', 'JTO', 'RAY', 'JUP', 'PYTH'}


def run_json(command: list[str]):
    res = subprocess.run(command, capture_output=True, text=True, env={**os.environ, 'PATH': PATH_ENV}, timeout=60)
    if res.stdout.strip():
        try:
            return json.loads(res.stdout)
        except Exception:
            pass
    return None


def get_balances(address: str):
    return run_json([HELIUS_BIN, 'wallet', 'balances', address, '--json']) or {}


def get_history(address: str):
    return run_json([HELIUS_BIN, 'wallet', 'history', address, '--json']) or {}


def summarize_wallet(label: str, address: str, focus_symbol: str):
    ts = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    balances = get_balances(address)
    history = get_history(address)
    txs = history.get('data', []) or []
    exposures = {}
    for item in balances.get('balances', []) or []:
        sym = item.get('symbol')
        if sym in TRACKED_SYMBOLS:
            exposures[sym] = {
                'balance': item.get('balance'),
                'usd_value': item.get('usdValue'),
            }
    recent_tx_count = len(txs)
    self_fee_payer_count = sum(1 for tx in txs if tx.get('feePayer') == address)
    tags = []
    if recent_tx_count >= 10:
        tags.append(f'{recent_tx_count} recent txs')
    if self_fee_payer_count >= 5:
        tags.append(f'{self_fee_payer_count} self-paid txs')
    if focus_symbol in exposures:
        tags.append(f'holds {focus_symbol}')
    if not tags:
        tags.append('no strong signal')
    summary = ', '.join(tags)
    return {
        'ts': ts,
        'wallet_label': label,
        'wallet_address': address,
        'focus_symbol': focus_symbol,
        'recent_tx_count': recent_tx_count,
        'self_fee_payer_count': self_fee_payer_count,
        'token_exposure': exposures,
        'summary': summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Scan whale wallets and persist observations to SQLite')
    parser.add_argument('--db', default=DEFAULT_DB)
    args = parser.parse_args()

    db_path = Path(args.db)
    rows = [summarize_wallet(label, address, focus) for label, address, focus in WALLETS]
    for row in rows:
        record_whale_observation(db_path, row)

    ranked = sorted(rows, key=lambda r: (r['recent_tx_count'] or 0, r['self_fee_payer_count'] or 0), reverse=True)
    print('Whale scanner summary:')
    for row in ranked[:3]:
        exposure = ', '.join(sorted(row['token_exposure'].keys())) if row['token_exposure'] else 'none'
        print(f"- {row['wallet_label']}: {row['summary']} | exposure: {exposure}")
    notable = [r for r in ranked if (r['recent_tx_count'] or 0) >= 10 or (r['self_fee_payer_count'] or 0) >= 5]
    if notable:
        print('Actionable follow-up:')
        for row in notable[:5]:
            print(f"- watch {row['wallet_label']} ({row['wallet_address']})")
    else:
        print('No notable whale activity this run.')


if __name__ == '__main__':
    main()
