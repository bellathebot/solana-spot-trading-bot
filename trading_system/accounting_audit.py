import sqlite3
from collections import Counter
from pathlib import Path

EPSILON = 1e-12
IMPOSSIBLE_COST_RATIO = 100.0


def _connect(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _to_float(value):
    try:
        if value is None or value == '':
            return None
        return float(value)
    except Exception:
        return None


def _buy_quantity(trade: dict) -> float | None:
    out_amount = _to_float(trade.get('out_amount'))
    if out_amount is not None and out_amount > EPSILON:
        return out_amount
    expected_out_amount = _to_float(trade.get('expected_out_amount'))
    if expected_out_amount is not None and expected_out_amount > EPSILON:
        return expected_out_amount
    amount = _to_float(trade.get('amount'))
    if amount is not None and amount > EPSILON:
        return amount
    return amount


def load_spot_trades(db_path: Path | str) -> list[dict]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            '''
            SELECT id, ts, symbol, side, mode, amount, size_usd, out_amount,
                   expected_out_amount, cost_basis_usd, realized_pnl_usd
            FROM auto_trades
            WHERE COALESCE(product_type, 'spot') = 'spot'
            ORDER BY ts, id
            '''
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _load_latest_prices(conn: sqlite3.Connection) -> dict[tuple[str, str], float]:
    rows = conn.execute(
        '''
        SELECT op.mode, op.symbol, op.last_price, pp.price
        FROM open_positions op
        LEFT JOIN price_points pp
          ON pp.symbol = op.symbol
         AND pp.snapshot_id = (SELECT id FROM price_snapshots ORDER BY ts DESC LIMIT 1)
        '''
    ).fetchall()
    latest_prices = {}
    for row in rows:
        price = _to_float(row['last_price'])
        if price is None:
            price = _to_float(row['price'])
        if price is not None:
            latest_prices[(row['mode'], row['symbol'])] = price
    return latest_prices


def get_accounting_audit(db_path: Path | str) -> dict:
    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        trades = load_spot_trades(db_path)
        latest_prices = _load_latest_prices(conn)
    finally:
        conn.close()

    findings = []
    inventory = {}

    def add_finding(severity: str, issue_type: str, trade: dict | None, message: str, **details) -> None:
        finding = {
            'severity': severity,
            'issue_type': issue_type,
            'message': message,
            'symbol': details.pop('symbol', trade.get('symbol') if trade else None),
            'mode': details.pop('mode', trade.get('mode') if trade else None),
            'trade_id': trade.get('id') if trade else None,
            'ts': trade.get('ts') if trade else None,
            'side': trade.get('side') if trade else None,
            'details': details,
        }
        findings.append(finding)

    for trade in trades:
        key = (trade.get('mode') or 'unknown', trade['symbol'])
        state = inventory.setdefault(key, {'remaining_qty': 0.0, 'cost_basis_usd': 0.0})
        side = (trade.get('side') or '').lower()
        qty = _to_float(trade.get('amount'))
        size_usd = _to_float(trade.get('size_usd'))
        out_amount = _to_float(trade.get('out_amount'))
        expected_out_amount = _to_float(trade.get('expected_out_amount'))
        cost_basis_usd = _to_float(trade.get('cost_basis_usd'))
        realized_pnl_usd = _to_float(trade.get('realized_pnl_usd'))

        if side == 'buy':
            qty = _buy_quantity(trade)
            if qty is None or qty <= EPSILON:
                add_finding(
                    'warning',
                    'missing_buy_token_quantity',
                    trade,
                    'Buy trade is missing a usable token quantity.',
                    amount=trade.get('amount'),
                    out_amount=trade.get('out_amount'),
                    expected_out_amount=trade.get('expected_out_amount'),
                )
                continue
            if size_usd is None or size_usd <= EPSILON:
                add_finding('warning', 'missing_buy_notional', trade, 'Buy trade is missing a usable USD notional.', size_usd=trade.get('size_usd'))
                continue
            state['remaining_qty'] += qty
            state['cost_basis_usd'] += size_usd
        elif side == 'sell':
            if qty is None or qty <= EPSILON:
                add_finding('warning', 'missing_sell_quantity', trade, 'Sell trade is missing a usable exit quantity.', amount=trade.get('amount'))
                continue
            if (size_usd is None or size_usd <= EPSILON) and (out_amount is None or out_amount <= EPSILON):
                add_finding(
                    'warning',
                    'missing_sell_proceeds',
                    trade,
                    'Sell trade is missing usable proceeds.',
                    size_usd=trade.get('size_usd'),
                    out_amount=trade.get('out_amount'),
                    expected_out_amount=trade.get('expected_out_amount'),
                )
            if size_usd is None or size_usd <= EPSILON:
                add_finding('failing', 'zero_notional_exit', trade, 'Sell trade has zero or missing USD notional.', size_usd=trade.get('size_usd'))
            open_qty = state['remaining_qty']
            if qty - open_qty > EPSILON:
                add_finding(
                    'failing',
                    'sell_qty_exceeds_open_qty',
                    trade,
                    'Sell quantity exceeds reconstructed open quantity.',
                    sell_qty=qty,
                    open_qty=round(open_qty, 12),
                )
            matched_qty = min(qty, max(open_qty, 0.0))
            avg_cost = (state['cost_basis_usd'] / open_qty) if open_qty > EPSILON else 0.0
            expected_cost_basis = matched_qty * avg_cost if matched_qty > EPSILON else 0.0
            matched_cost_basis = cost_basis_usd if cost_basis_usd is not None else expected_cost_basis
            if size_usd is not None and size_usd > EPSILON and matched_cost_basis > size_usd * IMPOSSIBLE_COST_RATIO:
                add_finding(
                    'warning',
                    'impossible_cost_basis_ratio',
                    trade,
                    'Sell cost basis is implausibly large versus exit proceeds.',
                    cost_basis_usd=round(matched_cost_basis, 6),
                    proceeds_usd=round(size_usd, 6),
                    ratio=round(matched_cost_basis / size_usd, 6),
                )
            if cost_basis_usd is not None and size_usd is not None and realized_pnl_usd is not None:
                expected_realized = round(size_usd - cost_basis_usd, 6)
                if abs(expected_realized - realized_pnl_usd) > 1e-6:
                    add_finding(
                        'warning',
                        'realized_pnl_mismatch',
                        trade,
                        'Sell realized PnL does not match proceeds minus cost basis.',
                        proceeds_usd=round(size_usd, 6),
                        cost_basis_usd=round(cost_basis_usd, 6),
                        realized_pnl_usd=round(realized_pnl_usd, 6),
                        expected_realized_pnl_usd=expected_realized,
                    )
            consumed_cost = matched_cost_basis if matched_cost_basis is not None else expected_cost_basis
            state['remaining_qty'] -= qty
            state['cost_basis_usd'] -= consumed_cost
            if state['remaining_qty'] < -EPSILON:
                add_finding(
                    'failing',
                    'negative_remaining_inventory',
                    trade,
                    'Inventory went negative after applying sell trade.',
                    remaining_qty=round(state['remaining_qty'], 12),
                )
            if state['cost_basis_usd'] < -EPSILON:
                add_finding(
                    'warning',
                    'negative_remaining_cost_basis',
                    trade,
                    'Cost basis went negative after applying sell trade.',
                    remaining_cost_basis_usd=round(state['cost_basis_usd'], 6),
                )
        else:
            add_finding('warning', 'unknown_trade_side', trade, 'Trade side is not recognized by accounting audit.', side=trade.get('side'))

    inventory_rows = []
    for (mode, symbol), state in sorted(inventory.items()):
        remaining_qty = round(state['remaining_qty'], 12)
        cost_basis_usd = round(state['cost_basis_usd'], 6)
        market_value_usd = None
        price = latest_prices.get((mode, symbol))
        if price is not None:
            market_value_usd = round(remaining_qty * price, 6)
            if remaining_qty > EPSILON and market_value_usd > EPSILON and cost_basis_usd > market_value_usd * IMPOSSIBLE_COST_RATIO:
                add_finding(
                    'warning',
                    'impossible_cost_basis_ratio',
                    None,
                    'Open position cost basis is implausibly large versus market value.',
                    mode=mode,
                    symbol=symbol,
                    remaining_qty=remaining_qty,
                    cost_basis_usd=cost_basis_usd,
                    market_value_usd=market_value_usd,
                    ratio=round(cost_basis_usd / market_value_usd, 6),
                )
        inventory_rows.append(
            {
                'mode': mode,
                'symbol': symbol,
                'remaining_amount': remaining_qty,
                'cost_basis_usd': cost_basis_usd,
                'market_value_usd': market_value_usd,
                'avg_cost_per_unit': round((cost_basis_usd / remaining_qty), 6) if remaining_qty > EPSILON else 0.0,
            }
        )

    issue_counts = Counter(finding['issue_type'] for finding in findings)
    flagged_trade_ids = sorted({finding['trade_id'] for finding in findings if finding['trade_id'] is not None})
    if any(finding['severity'] == 'failing' for finding in findings):
        status = 'failing'
    elif findings:
        status = 'warning'
    else:
        status = 'clean'

    return {
        'status': status,
        'summary': {
            'trade_count': len(trades),
            'finding_count': len(findings),
            'flagged_trade_count': len(flagged_trade_ids),
            'issue_counts': dict(sorted(issue_counts.items())),
            'top_issues': [
                {'issue_type': issue_type, 'count': count}
                for issue_type, count in issue_counts.most_common(3)
            ],
        },
        'flagged_trade_ids': flagged_trade_ids,
        'findings': findings,
        'inventory': inventory_rows,
    }
