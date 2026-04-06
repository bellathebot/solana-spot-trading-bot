#!/usr/bin/env python3
import json
import math
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_system.runtime_config import BRIDGE_DIR, JUP_BIN, MONITOR_PATH, TELEGRAM_TOKEN_FILE, TELEGRAM_TRADE_CHAT_ID, build_path_env

PENDING_FILE = BRIDGE_DIR / 'pending_trade_alert.json'
ALERT_LOG = BRIDGE_DIR / 'alert_history.jsonl'
TOKEN_FILE = TELEGRAM_TOKEN_FILE
CHAT_ID = TELEGRAM_TRADE_CHAT_ID
MONITOR = MONITOR_PATH
JUP_KEY = 'trading'

WATCHLIST = {
    'JUP': {'mint': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', 'max_buy_usd': 2.0, 'sell_above': 0.22, 'tier': 1, 'eligible': True, 'observation_only': False},
    'PYTH': {'mint': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', 'max_buy_usd': 1.5, 'sell_above': 0.065, 'tier': 1, 'eligible': True, 'observation_only': False},
    'RAY': {'mint': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R', 'max_buy_usd': 2.0, 'sell_above': 0.8, 'tier': 1, 'eligible': True, 'observation_only': False},
    'JTO': {'mint': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL', 'max_buy_usd': 2.0, 'sell_above': 0.38, 'tier': 2, 'eligible': True, 'observation_only': False},
    'BONK': {'mint': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 'max_buy_usd': 1.5, 'sell_above': 0.00001, 'tier': 3, 'eligible': False, 'observation_only': True},
    'WIF': {'mint': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', 'max_buy_usd': 1.5, 'sell_above': 0.25, 'tier': 3, 'eligible': True, 'observation_only': False},
}


def run(cmd: str) -> str:
    env = {**os.environ, 'PATH': build_path_env()}
    res = subprocess.run(cmd, shell=True, text=True, capture_output=True, env=env, timeout=120)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout).strip())
    return res.stdout


def parse_monitor_json(raw: str):
    m = re.search(r'(\{[\s\S]*\})\s*$', raw)
    if not m:
        raise ValueError('Could not locate JSON payload in monitor output')
    return json.loads(m.group(1))


def get_monitor_snapshot():
    raw = run(f'node {MONITOR} --json')
    return parse_monitor_json(raw)


def get_perps_markets():
    raw = run(f'{JUP_BIN} perps markets')
    return json.loads(raw)


def get_portfolio():
    raw = run(f'{JUP_BIN} spot portfolio --key {JUP_KEY}')
    return json.loads(raw)


def shorten(text: str, limit: int = 3000) -> str:
    text = (text or '').strip()
    return text if len(text) <= limit else text[:limit] + ' ...[truncated]'


def telegram_send(message: str):
    token = TOKEN_FILE.read_text().strip()
    data = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': shorten(message)}).encode()
    req = urllib.request.urlopen(f'https://api.telegram.org/bot{token}/sendMessage', data=data, timeout=20)
    return json.loads(req.read().decode())


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def score_spot_candidate(signal, snapshot_prices):
    symbol = signal['symbol']
    cfg = WATCHLIST.get(symbol, {})
    px = snapshot_prices.get(symbol, {})
    monitor_score = float(signal.get('score', 0))
    liquidity = float(px.get('liquidity', 0) or 0)
    regime = signal.get('regime_tag') or 'unknown'
    signal_type = signal.get('signal_type') or 'unknown'

    score = monitor_score * 1.3
    if cfg.get('tier') == 1:
        score += 5
    elif cfg.get('tier') == 2:
        score += 2
    if liquidity >= 5_000_000:
        score += 5
    elif liquidity >= 1_000_000:
        score += 3
    elif liquidity >= 250_000:
        score += 1
    if regime == 'stable':
        score += 5
    elif regime == 'choppy':
        score += 2
    elif regime == 'trend_down':
        score -= 12
    elif regime == 'panic_selloff':
        score -= 25
    if signal_type == 'hard_buy':
        score += 5
    elif signal_type == 'near_buy':
        score -= 3
    if liquidity < 100_000:
        score -= 20
    if cfg.get('observation_only'):
        score -= 15
    if not cfg.get('eligible', True):
        score -= 10

    score = int(round(clamp(score, 0, 100)))
    return score


def build_spot_trade(signal, snapshot):
    symbol = signal['symbol']
    cfg = WATCHLIST[symbol]
    px = snapshot['snapshot']['prices'][symbol]
    amount = cfg['max_buy_usd']
    command = f"{JUP_BIN} -f json spot swap --from USDC --to {cfg['mint']} --amount {amount:.2f} --key {JUP_KEY}"
    return {
        'rank': None,
        'label': symbol,
        'instrument_type': 'spot',
        'instrument': symbol,
        'side': 'buy',
        'score': score_spot_candidate(signal, snapshot['snapshot']['prices']),
        'price': px.get('price'),
        'reason': f"{symbol} {signal.get('signal_type')} in {signal.get('regime_tag')} regime with liquidity ${float(px.get('liquidity',0)):.0f}",
        'command': command,
        'size_usd': amount,
    }


def score_sell_candidate(symbol, holding_amount, price, sell_above, liquidity):
    distance_pct = ((price - sell_above) / sell_above) * 100 if sell_above else -999
    score = 45
    if holding_amount > 0:
        score += 10
    if liquidity >= 5_000_000:
        score += 8
    elif liquidity >= 1_000_000:
        score += 5
    elif liquidity >= 250_000:
        score += 2
    if sell_above and price >= sell_above:
        score += 30
    elif sell_above and price >= sell_above * 0.97:
        score += 18
    elif sell_above and price >= sell_above * 0.93:
        score += 8
    else:
        score -= 10
    score += min(7, max(0, distance_pct * 2)) if distance_pct > 0 else 0
    return int(round(clamp(score, 0, 100)))


def build_sell_candidates(snapshot, portfolio):
    candidates = []
    holdings = {t.get('symbol'): t for t in portfolio.get('tokens', [])}
    for symbol, cfg in WATCHLIST.items():
        holding = holdings.get(symbol)
        if not holding:
            continue
        amount = float(holding.get('amount', 0) or 0)
        if amount <= 0:
            continue
        sell_above = cfg.get('sell_above')
        if not sell_above:
            continue
        px = snapshot['snapshot']['prices'].get(symbol, {})
        price = float(px.get('price', 0) or 0)
        liquidity = float(px.get('liquidity', 0) or 0)
        score = score_sell_candidate(symbol, amount, price, sell_above, liquidity)
        if score < 70:
            continue
        command = f"{JUP_BIN} -f json spot swap --from {cfg['mint']} --to USDC --amount {amount:.6f} --key {JUP_KEY}"
        if price >= sell_above:
            reason = f"{symbol} is at/above sell target {sell_above} with wallet holding {amount:.6f}"
        else:
            reason = f"{symbol} is within the take-profit watch zone below sell target {sell_above} with wallet holding {amount:.6f}"
        candidates.append({
            'rank': None,
            'label': symbol,
            'instrument_type': 'spot',
            'instrument': symbol,
            'side': 'sell',
            'score': score,
            'price': price,
            'reason': reason,
            'command': command,
            'size_usd': float(holding.get('value', 0) or 0),
        })
    return candidates


def evaluate_candidates():
    snapshot = get_monitor_snapshot()
    portfolio = get_portfolio()
    _ = get_perps_markets()  # reserved for future extension
    candidates = []
    for signal in snapshot.get('scored_signals', []):
        symbol = signal.get('symbol')
        if symbol not in WATCHLIST:
            continue
        candidates.append(build_spot_trade(signal, snapshot))
    candidates.extend(build_sell_candidates(snapshot, portfolio))
    candidates.sort(key=lambda c: c['score'], reverse=True)
    for i, c in enumerate(candidates, 1):
        c['rank'] = i
    return snapshot, candidates


def write_pending_alert(top_candidates):
    BRIDGE_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    alert_id = now.strftime('%Y%m%dT%H%M%SZ')
    payload = {
        'alert_id': alert_id,
        'created_at': now.isoformat(),
        'expires_at': (now + timedelta(hours=24)).isoformat(),
        'status': 'pending',
        'candidates': top_candidates,
    }
    PENDING_FILE.write_text(json.dumps(payload, indent=2))
    with ALERT_LOG.open('a') as f:
        f.write(json.dumps(payload) + '\n')
    return payload


def format_message(payload):
    lines = [
        f"Trade alert {payload['alert_id']} UTC",
        ""
    ]
    for c in payload['candidates'][:2]:
        lines.append(f"{c['rank']}) {c['instrument']} {c['side'].upper()} score {c['score']}")
        lines.append(f"   {c['reason']}")
    lines += [
        "",
        "Reply to the bot with one of:",
        f"EXECUTE {payload['alert_id']} TOP",
        f"EXECUTE {payload['alert_id']} 1",
        f"EXECUTE {payload['alert_id']} 2",
        f"SKIP {payload['alert_id']}",
    ]
    return '\n'.join(lines)


def main():
    snapshot, candidates = evaluate_candidates()
    strong = [c for c in candidates if c['score'] > 80]
    if not strong:
        print('NO_ALERT')
        return
    payload = write_pending_alert(strong[:2])
    msg = format_message(payload)
    result = telegram_send(msg)
    payload['telegram_message_id'] = result.get('result', {}).get('message_id')
    PENDING_FILE.write_text(json.dumps(payload, indent=2))
    print(json.dumps({'status': 'ALERT_SENT', 'alert_id': payload['alert_id'], 'telegram_message_id': payload['telegram_message_id'], 'top': strong[:2]}, indent=2))


if __name__ == '__main__':
    main()
