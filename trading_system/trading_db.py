import hashlib
import json
import os
import sqlite3
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable

from trading_system.accounting_audit import get_accounting_audit as run_accounting_audit

# `open_positions` and FIFO realized-PnL logic are spot-inventory models.
# Perpetuals are tracked separately in dedicated perp_* tables and must not reuse
# spot inventory accounting semantics.
# Easter egg tag carried through planning docs and safe metadata ideas: ethan was here

SCHEMA_VERSION = 8
EASTER_EGG_TAG = 'ethan was here'

GATING_MIN_SAMPLE_SIZE = 5
PROMOTION_AVG_FORWARD_RETURN_PCT = 0.15
DEMOTION_AVG_FORWARD_RETURN_PCT = 0.0
PROMOTION_FAVORABLE_RATE = 0.58
DEMOTION_FAVORABLE_RATE = 0.45
PROMOTION_EXECUTION_SCORE = 60
DEMOTION_EXECUTION_SCORE = 35


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
   raw = os.getenv(name)
   if raw in (None, ''):
       value = default
   else:
       try:
           value = int(raw)
       except (TypeError, ValueError):
           value = default
   if minimum is not None:
       value = max(minimum, value)
   return value


PERP_EXECUTOR_MAX_SIGNAL_AGE_MINUTES = _env_int('PERPS_AUTO_TRADE_MAX_SIGNAL_AGE_MINUTES', 40, minimum=1)


def connect(db_path: Path | str) -> sqlite3.Connection:
   conn = sqlite3.connect(str(db_path))
   conn.row_factory = sqlite3.Row
   conn.execute('PRAGMA foreign_keys = ON')
   return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
   cols = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}
   if column not in cols:
       conn.execute(f'ALTER TABLE {table} ADD COLUMN {ddl}')



def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
   return {row['name'] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}



def _ensure_perp_fill_identity_schema(conn: sqlite3.Connection) -> None:
   _ensure_column(conn, 'perp_fills', 'fill_key', 'fill_key TEXT')
   conn.execute(
       '''
       UPDATE perp_fills
       SET fill_key = NULL
       WHERE fill_key IS NOT NULL
         AND id NOT IN (
             SELECT MIN(id)
             FROM perp_fills
             WHERE fill_key IS NOT NULL
             GROUP BY fill_key
         )
       '''
   )
   conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_perp_fills_fill_key ON perp_fills(fill_key) WHERE fill_key IS NOT NULL')


def _ensure_auto_trade_identity_schema(conn: sqlite3.Connection) -> None:
   _ensure_column(conn, 'auto_trades', 'trade_key', 'trade_key TEXT')
   conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_auto_trades_trade_key ON auto_trades(trade_key) WHERE trade_key IS NOT NULL')


def init_db(db_path: Path | str) -> None:
   db_path = Path(db_path)
   db_path.parent.mkdir(parents=True, exist_ok=True)
   conn = connect(db_path)
   try:
       conn.executescript(
           '''
           CREATE TABLE IF NOT EXISTS schema_meta (
               key TEXT PRIMARY KEY,
               value TEXT NOT NULL
           );

           CREATE TABLE IF NOT EXISTS price_snapshots (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL UNIQUE,
               wallet_sol REAL,
               wallet_usdc REAL,
               wallet_total_usd REAL
           );

           CREATE TABLE IF NOT EXISTS price_points (
               id INTEGER PRIMARY KEY,
               snapshot_id INTEGER NOT NULL REFERENCES price_snapshots(id) ON DELETE CASCADE,
               symbol TEXT NOT NULL,
               price REAL,
               change24h REAL,
               liquidity REAL,
               holding_amount REAL,
               holding_value REAL,
               alert_below REAL,
               buy_below REAL,
               sell_above REAL,
               UNIQUE(snapshot_id, symbol)
           );

           CREATE TABLE IF NOT EXISTS alerts (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               message TEXT NOT NULL,
               UNIQUE(ts, message)
           );

           CREATE TABLE IF NOT EXISTS auto_trades (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               symbol TEXT NOT NULL,
               side TEXT NOT NULL,
               mode TEXT,
               simulated INTEGER NOT NULL DEFAULT 0,
               product_type TEXT NOT NULL DEFAULT 'spot',
               venue TEXT DEFAULT 'jupiter',
               strategy_family TEXT,
               decision_id TEXT,
               market TEXT,
               price REAL,
               amount REAL,
               size_usd REAL,
               out_amount TEXT,
               expected_out_amount TEXT,
               signature TEXT,
               quote_price_impact REAL,
               slippage_bps REAL,
               reason TEXT,
               strategy_tag TEXT,
               realized_pnl_usd REAL,
               cost_basis_usd REAL,
               trade_key TEXT,
               UNIQUE(ts, symbol, side, signature)
           );

           CREATE TABLE IF NOT EXISTS whale_observations (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               wallet_label TEXT NOT NULL,
               wallet_address TEXT NOT NULL,
               focus_symbol TEXT,
               recent_tx_count INTEGER,
               self_fee_payer_count INTEGER,
               token_exposure_json TEXT,
               summary TEXT,
               raw_json TEXT,
               UNIQUE(ts, wallet_address)
           );

           CREATE TABLE IF NOT EXISTS system_events (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               event_type TEXT NOT NULL,
               severity TEXT NOT NULL,
               message TEXT NOT NULL,
               source TEXT,
               metadata_json TEXT,
               UNIQUE(ts, event_type, message)
           );

           CREATE TABLE IF NOT EXISTS signal_candidates (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               source TEXT NOT NULL,
               symbol TEXT NOT NULL,
               signal_type TEXT NOT NULL,
               strategy_tag TEXT,
               side TEXT,
               product_type TEXT NOT NULL DEFAULT 'spot',
               market TEXT,
               price REAL,
               reference_level REAL,
               distance_pct REAL,
               liquidity REAL,
               quote_price_impact REAL,
               score REAL,
               regime_tag TEXT,
               decision_id TEXT,
               status TEXT NOT NULL,
               reason TEXT,
               metadata_json TEXT,
               UNIQUE(ts, source, symbol, signal_type, status, reason)
           );

           CREATE TABLE IF NOT EXISTS open_positions (
               id INTEGER PRIMARY KEY,
               mode TEXT NOT NULL,
               symbol TEXT NOT NULL,
               product_type TEXT NOT NULL DEFAULT 'spot',
               remaining_amount REAL NOT NULL DEFAULT 0,
               cost_basis_usd REAL NOT NULL DEFAULT 0,
               market_value_usd REAL NOT NULL DEFAULT 0,
               unrealized_pnl_usd REAL NOT NULL DEFAULT 0,
               avg_cost_per_unit REAL,
               last_price REAL,
               opened_ts TEXT,
               updated_ts TEXT,
               UNIQUE(mode, symbol)
           );

           CREATE TABLE IF NOT EXISTS perp_market_snapshots (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               asset TEXT NOT NULL,
               price_usd REAL,
               change_pct_24h REAL,
               high_usd_24h REAL,
               low_usd_24h REAL,
               volume_usd_24h REAL,
               raw_json TEXT,
               UNIQUE(ts, asset)
           );

           CREATE TABLE IF NOT EXISTS perp_account_snapshots (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL UNIQUE,
               wallet_address TEXT NOT NULL,
               open_position_count INTEGER NOT NULL DEFAULT 0,
               open_notional_usd REAL NOT NULL DEFAULT 0,
               unrealized_pnl_usd REAL NOT NULL DEFAULT 0,
               realized_pnl_usd REAL,
               margin_used_usd REAL,
               equity_estimate_usd REAL,
               raw_json TEXT
           );

           CREATE TABLE IF NOT EXISTS perp_positions (
               id INTEGER PRIMARY KEY,
               position_key TEXT NOT NULL UNIQUE,
               opened_ts TEXT,
               updated_ts TEXT NOT NULL,
               closed_ts TEXT,
               status TEXT NOT NULL,
               asset TEXT NOT NULL,
               side TEXT NOT NULL,
               collateral_token TEXT,
               entry_price_usd REAL,
               mark_price_usd REAL,
               liq_price_usd REAL,
               size_usd REAL,
               notional_usd REAL,
               margin_used_usd REAL,
               leverage REAL,
               take_profit_price REAL,
               stop_loss_price REAL,
               unrealized_pnl_usd REAL,
               realized_pnl_usd REAL,
               fees_usd REAL,
               funding_usd REAL,
               strategy_tag TEXT,
               mode TEXT NOT NULL,
               decision_id TEXT,
               source TEXT,
               raw_json TEXT
           );

           CREATE TABLE IF NOT EXISTS perp_orders (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               order_key TEXT UNIQUE,
               position_key TEXT,
               asset TEXT NOT NULL,
               side TEXT NOT NULL,
               order_type TEXT NOT NULL,
               status TEXT NOT NULL,
               size_usd REAL,
               limit_price REAL,
               trigger_price REAL,
               slippage_bps REAL,
               mode TEXT NOT NULL,
               strategy_tag TEXT,
               decision_id TEXT,
               reason TEXT,
               signature TEXT,
               raw_json TEXT
           );

           CREATE TABLE IF NOT EXISTS perp_fills (
               id INTEGER PRIMARY KEY,
               fill_key TEXT,
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

           CREATE TABLE IF NOT EXISTS risk_limits (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               scope TEXT NOT NULL,
               scope_key TEXT NOT NULL,
               limit_type TEXT NOT NULL,
               limit_value REAL,
               status TEXT NOT NULL,
               reason TEXT,
               UNIQUE(ts, scope, scope_key, limit_type)
           );

           CREATE TABLE IF NOT EXISTS risk_events (
               id INTEGER PRIMARY KEY,
               ts TEXT NOT NULL,
               product_type TEXT NOT NULL,
               severity TEXT NOT NULL,
               event_type TEXT NOT NULL,
               scope TEXT,
               scope_key TEXT,
               message TEXT NOT NULL,
               metadata_json TEXT
           );

           CREATE INDEX IF NOT EXISTS idx_price_points_symbol ON price_points(symbol);
           CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);
           CREATE INDEX IF NOT EXISTS idx_auto_trades_ts ON auto_trades(ts);
           CREATE INDEX IF NOT EXISTS idx_signal_candidates_ts ON signal_candidates(ts);
           CREATE INDEX IF NOT EXISTS idx_signal_candidates_symbol ON signal_candidates(symbol);
           CREATE INDEX IF NOT EXISTS idx_whale_obs_ts ON whale_observations(ts);
           CREATE INDEX IF NOT EXISTS idx_whale_obs_wallet ON whale_observations(wallet_address);
           CREATE INDEX IF NOT EXISTS idx_system_events_ts ON system_events(ts);
           CREATE INDEX IF NOT EXISTS idx_system_events_type ON system_events(event_type);
           '''
       )
       _ensure_column(conn, 'auto_trades', 'strategy_tag', 'strategy_tag TEXT')
       _ensure_column(conn, 'auto_trades', 'realized_pnl_usd', 'realized_pnl_usd REAL')
       _ensure_column(conn, 'auto_trades', 'cost_basis_usd', 'cost_basis_usd REAL')
       _ensure_column(conn, 'auto_trades', 'expected_out_amount', 'expected_out_amount TEXT')
       _ensure_column(conn, 'auto_trades', 'slippage_bps', 'slippage_bps REAL')
       _ensure_column(conn, 'auto_trades', 'product_type', "product_type TEXT NOT NULL DEFAULT 'spot'")
       _ensure_column(conn, 'auto_trades', 'venue', "venue TEXT DEFAULT 'jupiter'")
       _ensure_column(conn, 'auto_trades', 'strategy_family', 'strategy_family TEXT')
       _ensure_column(conn, 'auto_trades', 'decision_id', 'decision_id TEXT')
       _ensure_column(conn, 'auto_trades', 'market', 'market TEXT')
       _ensure_column(conn, 'signal_candidates', 'product_type', "product_type TEXT NOT NULL DEFAULT 'spot'")
       _ensure_column(conn, 'signal_candidates', 'market', 'market TEXT')
       _ensure_column(conn, 'signal_candidates', 'score', 'score REAL')
       _ensure_column(conn, 'signal_candidates', 'regime_tag', 'regime_tag TEXT')
       _ensure_column(conn, 'signal_candidates', 'decision_id', 'decision_id TEXT')
       _ensure_column(conn, 'signal_candidates', 'candidate_key', 'candidate_key TEXT')
       _ensure_column(conn, 'open_positions', 'opened_ts', 'opened_ts TEXT')
       _ensure_column(conn, 'open_positions', 'product_type', "product_type TEXT NOT NULL DEFAULT 'spot'")
       conn.execute("UPDATE auto_trades SET product_type='spot' WHERE product_type IS NULL OR product_type = ''")
       conn.execute("UPDATE signal_candidates SET product_type='spot' WHERE product_type IS NULL OR product_type = ''")
       conn.execute("UPDATE open_positions SET product_type='spot' WHERE product_type IS NULL OR product_type = ''")
       conn.execute('CREATE INDEX IF NOT EXISTS idx_auto_trades_strategy ON auto_trades(strategy_tag)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_auto_trades_product_type ON auto_trades(product_type)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_signal_candidates_product_type ON signal_candidates(product_type)')
       conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_candidates_candidate_key ON signal_candidates(candidate_key)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_signal_candidates_decision_id ON signal_candidates(decision_id)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_signal_candidates_product_symbol_ts ON signal_candidates(product_type, symbol, ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_market_snapshots_ts ON perp_market_snapshots(ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_market_snapshots_asset ON perp_market_snapshots(asset)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_market_snapshots_asset_ts ON perp_market_snapshots(asset, ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_account_snapshots_ts ON perp_account_snapshots(ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_positions_status ON perp_positions(status)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_positions_asset ON perp_positions(asset)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_positions_mode ON perp_positions(mode)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_orders_ts ON perp_orders(ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_orders_status ON perp_orders(status)')
       _ensure_perp_fill_identity_schema(conn)
       _ensure_auto_trade_identity_schema(conn)
       _ensure_production_columns(conn)
       conn.execute('CREATE INDEX IF NOT EXISTS idx_perp_fills_ts ON perp_fills(ts)')
       conn.execute('CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON risk_events(ts)')
       conn.execute(
           "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
           "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
           (str(SCHEMA_VERSION),),
       )
       conn.commit()
   finally:
       conn.close()


def _load_json(path: Path, default):
   if not path.exists():
       return default
   text = path.read_text().strip()
   if not text:
       return default
   return json.loads(text)


def _iter_alerts(path: Path) -> Iterable[tuple[str, str]]:
   if not path.exists():
       return []
   rows = []
   for line in path.read_text().splitlines():
       line = line.strip()
       if not line:
           continue
       if line.startswith('[') and '] ' in line:
           ts, message = line[1:].split('] ', 1)
           rows.append((ts, message))
   return rows


def _normalize_json(value):
   if value is None:
       return None
   if isinstance(value, str):
       return value
   return json.dumps(value, sort_keys=True)


def _parse_json(value):
   if not value:
       return {}
   if isinstance(value, dict):
       return value
   try:
       return json.loads(value)
   except Exception:
       return {}


def _parse_json_object(value):
   parsed = _parse_json(value)
   return parsed if isinstance(parsed, dict) else {}


def _json_extract_path(value, *path):

   current = _parse_json_object(value)
   for key in path:
       if not isinstance(current, dict):
           return None
       current = current.get(key)
   return current


def _to_float(value):
   try:
       if value is None or value == '':
           return None
       return float(value)
   except Exception:
       return None



def _normalize_symbol(value) -> str:
   if value is None:
       return ''
   return str(value).strip().upper()



def _normalize_fill_identity_value(value):
   if value is None or value == '':
       return None
   if isinstance(value, bool):
       return value
   if isinstance(value, (int, float)):
       numeric_value = _to_float(value)
       return format(numeric_value, '.12g') if numeric_value is not None else value
   if isinstance(value, str):
       numeric_value = _to_float(value)
       if numeric_value is not None:
           return format(numeric_value, '.12g')
   return value



def _compute_perp_fill_key(payload: dict) -> str:
   explicit_fill_key = payload.get('fill_key') or payload.get('fillKey')
   if explicit_fill_key:
       return str(explicit_fill_key)

   identity_payload = {
       'ts': payload.get('ts'),
       'position_key': payload.get('position_key') or payload.get('positionKey'),
       'order_key': payload.get('order_key') or payload.get('orderKey'),
       'asset': payload.get('asset'),
       'side': payload.get('side'),
       'action': payload.get('action'),
       'price_usd': _normalize_fill_identity_value(payload.get('price_usd', payload.get('priceUsd'))),
       'size_usd': _normalize_fill_identity_value(payload.get('size_usd', payload.get('sizeUsd'))),
       'fees_usd': _normalize_fill_identity_value(payload.get('fees_usd', payload.get('feesUsd'))),
       'funding_usd': _normalize_fill_identity_value(payload.get('funding_usd', payload.get('fundingUsd'))),
       'realized_pnl_usd': _normalize_fill_identity_value(payload.get('realized_pnl_usd', payload.get('realizedPnlUsd'))),
       'mode': payload.get('mode', 'paper'),
   }
   identity_json = json.dumps(identity_payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
   return f"derived:{hashlib.sha256(identity_json.encode('utf-8')).hexdigest()}"



def _compute_auto_trade_key(payload: dict) -> str:
   explicit_trade_key = payload.get('trade_key') or payload.get('tradeKey')
   if explicit_trade_key:
       return str(explicit_trade_key)

   signature = payload.get('signature')
   if signature:
       return f"signature:{signature}"

   identity_payload = {
       'ts': payload.get('ts'),
       'symbol': _normalize_symbol(payload.get('symbol')),
       'side': payload.get('side'),
       'mode': payload.get('mode', 'unknown'),
       'product_type': payload.get('product_type', payload.get('productType', 'spot')),
       'price': _normalize_fill_identity_value(payload.get('price')),
       'amount': _normalize_fill_identity_value(payload.get('amount')),
       'size_usd': _normalize_fill_identity_value(payload.get('size_usd', payload.get('sizeUsd'))),
       'decision_id': payload.get('decision_id', payload.get('decisionId')),
       'strategy_tag': payload.get('strategyTag') or payload.get('strategy_tag') or payload.get('entryStrategyTag') or payload.get('entry_strategy_tag'),
       'reason': payload.get('reason'),
   }
   identity_json = json.dumps(identity_payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
   return f"derived:{hashlib.sha256(identity_json.encode('utf-8')).hexdigest()}"



def _load_json_file(path: Path, default, *, db_path: Path | str | None = None, source: str = 'sync_from_files'):
   if not path.exists():
       return default
   try:
       return json.loads(path.read_text())
   except Exception as exc:
       if db_path is not None:
           record_system_event(db_path, {
               'ts': datetime.now(timezone.utc).isoformat(),
               'event_type': 'malformed_json_ingest',
               'severity': 'warning',
               'message': f'Failed to parse {path.name}',
               'source': source,
               'metadata': {'path': str(path), 'error': str(exc)},
           })
       return default



def _find_existing_perp_fill_row_id(conn: sqlite3.Connection, payload: dict, fill_key: str) -> int | None:
   columns = _table_columns(conn, 'perp_fills')
   if 'fill_key' in columns:
       existing = conn.execute('SELECT id FROM perp_fills WHERE fill_key = ? LIMIT 1', (fill_key,)).fetchone()
       if existing:
           return int(existing['id'])

   existing = conn.execute(
       '''
       SELECT id
       FROM perp_fills
       WHERE ts = ?
         AND ((position_key IS NULL AND ? IS NULL) OR position_key = ?)
         AND ((order_key IS NULL AND ? IS NULL) OR order_key = ?)
         AND asset = ?
         AND side = ?
         AND action = ?
         AND ((price_usd IS NULL AND ? IS NULL) OR price_usd = ?)
         AND ((size_usd IS NULL AND ? IS NULL) OR size_usd = ?)
         AND ((fees_usd IS NULL AND ? IS NULL) OR fees_usd = ?)
         AND ((funding_usd IS NULL AND ? IS NULL) OR funding_usd = ?)
         AND ((realized_pnl_usd IS NULL AND ? IS NULL) OR realized_pnl_usd = ?)
         AND mode = ?
       ORDER BY id ASC
       LIMIT 1
       ''',
       (
           payload.get('ts'),
           payload.get('position_key') or payload.get('positionKey'),
           payload.get('position_key') or payload.get('positionKey'),
           payload.get('order_key') or payload.get('orderKey'),
           payload.get('order_key') or payload.get('orderKey'),
           payload.get('asset'),
           payload.get('side'),
           payload.get('action'),
           payload.get('price_usd', payload.get('priceUsd')),
           payload.get('price_usd', payload.get('priceUsd')),
           payload.get('size_usd', payload.get('sizeUsd')),
           payload.get('size_usd', payload.get('sizeUsd')),
           payload.get('fees_usd', payload.get('feesUsd')),
           payload.get('fees_usd', payload.get('feesUsd')),
           payload.get('funding_usd', payload.get('fundingUsd')),
           payload.get('funding_usd', payload.get('fundingUsd')),
           payload.get('realized_pnl_usd', payload.get('realizedPnlUsd')),
           payload.get('realized_pnl_usd', payload.get('realizedPnlUsd')),
           payload.get('mode', 'paper'),
       ),
   ).fetchone()
   if not existing:
       return None
   existing_id = int(existing['id'])
   if 'fill_key' in columns:
       conn.execute('UPDATE perp_fills SET fill_key = COALESCE(fill_key, ?) WHERE id = ?', (fill_key, existing_id))
   return existing_id


def _is_spot_product_type(product_type) -> bool:
   return (product_type or 'spot') == 'spot'


def _buy_quantity(row) -> float:
   out_amount = _to_float(row['out_amount']) if 'out_amount' in row.keys() else None
   if out_amount is not None and out_amount > 0:
       return float(out_amount)
   expected_out_amount = _to_float(row['expected_out_amount']) if 'expected_out_amount' in row.keys() else None
   if expected_out_amount is not None and expected_out_amount > 0:
       return float(expected_out_amount)
   amount = _to_float(row['amount'])
   if amount is not None and amount > 0:
       return float(amount)
   return float(amount or 0)


def _sell_quantity(row) -> float:
   amount = _to_float(row['amount'])
   return float(amount or 0)


def _has_usable_buy_notional(row) -> bool:
   size_usd = _to_float(row['size_usd'])
   return size_usd is not None and size_usd > 1e-12


def assess_trade_trust(row) -> dict:
   side = (row['side'] or '').lower()
   if side != 'buy':
       return {'trusted': True, 'reason': 'sell_or_nonbuy'}
   amount = _to_float(row['amount'])
   out_amount = _to_float(row['out_amount']) if 'out_amount' in row.keys() else None
   expected_out_amount = _to_float(row['expected_out_amount']) if 'expected_out_amount' in row.keys() else None
   comparison_qty = out_amount if out_amount is not None and out_amount > 0 else expected_out_amount
   if comparison_qty is None or comparison_qty <= 0:
       return {'trusted': True, 'reason': 'no_output_quantity_to_compare'}
   if amount is None or amount <= 0:
       return {'trusted': True, 'reason': 'amount_missing_fallback_to_output_quantity'}
   ratio = max(amount, comparison_qty) / max(min(amount, comparison_qty), 1e-12)
   if ratio > 50:
       return {'trusted': False, 'reason': 'legacy_buy_amount_output_mismatch', 'ratio': ratio}
   return {'trusted': True, 'reason': 'amount_matches_output_quantity', 'ratio': ratio}


def infer_strategy_tag(symbol: str | None, side: str | None, reason: str | None) -> str:
   reason_l = (reason or '').lower()
   if 'near-buy' in reason_l or 'below auto-buy' in reason_l or '<= target' in reason_l:
       return 'mean_reversion_near_buy' if side == 'buy' else 'signal_near_buy'
   if 'sell target' in reason_l or '>= target' in reason_l or 'take-profit' in reason_l:
       return 'take_profit_exit' if side == 'sell' else 'take_profit_watch'
   if 'momentum' in reason_l:
       return 'momentum'
   if 'whale' in reason_l:
       return 'whale_follow'
   return 'unclassified'


def compute_realized_pnl_rows(rows: list[sqlite3.Row]) -> tuple[dict[int, tuple[float | None, float | None]], dict[str, dict[str, float]]]:
   inventory = defaultdict(deque)
   result = {}
   strategy_perf = defaultdict(lambda: {'trade_count': 0, 'notional_usd': 0.0, 'realized_pnl_usd': 0.0})
   for row in rows:
       row_id = int(row['id'])
       symbol = row['symbol']
       side = row['side']
       size_usd = float(row['size_usd'] or 0)
       strategy_tag = row['strategy_tag'] or infer_strategy_tag(row['symbol'], row['side'], row['reason'])
       qty = _buy_quantity(row) if side == 'buy' else _sell_quantity(row)
       if qty <= 0:
           result[row_id] = (None, None)
           continue
       if side == 'buy':
           if not _has_usable_buy_notional(row):
               result[row_id] = (0.0, None)
               strategy_perf[strategy_tag]['trade_count'] += 1
               continue
           unit_cost = size_usd / qty if qty else 0.0
           inventory[(row['mode'], symbol)].append([qty, unit_cost, strategy_tag, row['ts']])
           result[row_id] = (0.0, size_usd)
           strategy_perf[strategy_tag]['trade_count'] += 1
           strategy_perf[strategy_tag]['notional_usd'] += size_usd
       elif side == 'sell':
           lots = inventory[(row['mode'], symbol)]
           remaining = qty
           matched_qty = 0.0
           matched_cost = 0.0
           matched_proceeds = 0.0
           matched_strategy_contrib = defaultdict(lambda: {'notional_usd': 0.0, 'realized_pnl_usd': 0.0})
           proceeds_per_unit = size_usd / qty if qty else 0.0
           while remaining > 1e-12 and lots:
               lot_qty, unit_cost, lot_strategy, lot_ts = lots[0]
               used = min(remaining, lot_qty)
               consumed_cost = used * unit_cost
               consumed_proceeds = used * proceeds_per_unit
               matched_qty += used
               matched_cost += consumed_cost
               matched_proceeds += consumed_proceeds
               matched_strategy_contrib[lot_strategy]['notional_usd'] += consumed_proceeds
               matched_strategy_contrib[lot_strategy]['realized_pnl_usd'] += consumed_proceeds - consumed_cost
               lot_qty -= used
               remaining -= used
               if lot_qty <= 1e-12:
                   lots.popleft()
               else:
                   lots[0][0] = lot_qty
           strategy_perf[strategy_tag]['trade_count'] += 1
           strategy_perf[strategy_tag]['notional_usd'] += matched_proceeds if remaining > 1e-12 else size_usd
           if remaining > 1e-12:
               result[row_id] = (None, matched_cost if matched_qty > 1e-12 else None)
               continue
           realized = matched_proceeds - matched_cost
           result[row_id] = (realized, matched_cost)
           for lot_strategy, contrib in matched_strategy_contrib.items():
               strategy_perf[lot_strategy]['trade_count'] += 1
               strategy_perf[lot_strategy]['notional_usd'] += contrib['notional_usd']
               strategy_perf[lot_strategy]['realized_pnl_usd'] += contrib['realized_pnl_usd']
           strategy_perf[strategy_tag]['realized_pnl_usd'] += realized
       else:
           result[row_id] = (None, None)
   return result, strategy_perf


def recompute_trade_analytics(db_path: Path | str) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       rows = conn.execute(
           '''
           SELECT id, ts, symbol, side, mode, amount, size_usd, reason, strategy_tag,
                  out_amount, expected_out_amount, product_type
           FROM auto_trades
           WHERE COALESCE(product_type, 'spot') = 'spot'
           ORDER BY ts, id
           '''
       ).fetchall()
       pnl_map, _strategy_perf = compute_realized_pnl_rows(rows)
       inventory = defaultdict(deque)
       for row in rows:
           strategy_tag = row['strategy_tag'] or infer_strategy_tag(row['symbol'], row['side'], row['reason'])
           realized_pnl_usd, cost_basis_usd = pnl_map[int(row['id'])]
           conn.execute(
               '''
               UPDATE auto_trades
               SET strategy_tag = ?, realized_pnl_usd = ?, cost_basis_usd = ?
               WHERE id = ?
               ''',
               (strategy_tag, realized_pnl_usd, cost_basis_usd, int(row['id'])),
           )

           size_usd = float(row['size_usd'] or 0)
           qty = _buy_quantity(row) if row['side'] == 'buy' else _sell_quantity(row)
           if qty <= 0:
               continue
           key = (row['mode'], row['symbol'])
           if row['side'] == 'buy':
               if not _has_usable_buy_notional(row):
                   continue
               unit_cost = size_usd / qty if qty else 0.0
               inventory[key].append([qty, unit_cost, row['ts']])
           elif row['side'] == 'sell':
               remaining = qty
               while remaining > 1e-12 and inventory[key]:
                   lot_qty, unit_cost, opened_ts = inventory[key][0]
                   used = min(remaining, lot_qty)
                   lot_qty -= used
                   remaining -= used
                   if lot_qty <= 1e-12:
                       inventory[key].popleft()
                   else:
                       inventory[key][0][0] = lot_qty

       latest_prices = {
           row['symbol']: {'price': row['price'], 'holding_value': row['holding_value'], 'updated_ts': row['ts']}
           for row in conn.execute(
               '''
               SELECT pp.symbol, pp.price, pp.holding_value, ps.ts
               FROM price_points pp
               JOIN price_snapshots ps ON ps.id = pp.snapshot_id
               WHERE pp.snapshot_id = (SELECT id FROM price_snapshots ORDER BY ts DESC LIMIT 1)
               '''
           ).fetchall()
       }

       conn.execute("DELETE FROM open_positions WHERE COALESCE(product_type, 'spot') = 'spot'")
       for (mode, symbol), lots in inventory.items():
           remaining_amount = sum(lot[0] for lot in lots)
           if remaining_amount <= 1e-12:
               continue
           cost_basis_usd = sum(lot[0] * lot[1] for lot in lots)
           avg_cost = cost_basis_usd / remaining_amount if remaining_amount else 0.0
           last_price = latest_prices.get(symbol, {}).get('price')
           market_value_usd = remaining_amount * last_price if last_price is not None else 0.0
           unrealized_pnl_usd = market_value_usd - cost_basis_usd if last_price is not None else 0.0
           opened_ts = min(lot[2] for lot in lots) if lots else None
           conn.execute(
               '''
              INSERT INTO open_positions(
                  mode, symbol, product_type, remaining_amount, cost_basis_usd, market_value_usd,
                  unrealized_pnl_usd, avg_cost_per_unit, last_price, opened_ts, updated_ts
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''',
               (
                  mode,
                  symbol,
                  'spot',
                  remaining_amount,
cost_basis_usd,
                   market_value_usd,
                   unrealized_pnl_usd,
                   avg_cost,
                   last_price,
                   opened_ts,
                   latest_prices.get(symbol, {}).get('updated_ts'),
               ),
           )
       conn.commit()
   finally:
       conn.close()


def record_snapshot_and_alerts(db_path: Path | str, snapshot: dict, alerts: list[str] | None = None) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       wallet = snapshot.get('wallet', {}) or {}
       ts = snapshot.get('ts')
       conn.execute(
           '''
           INSERT OR IGNORE INTO price_snapshots(ts, wallet_sol, wallet_usdc, wallet_total_usd)
           VALUES (?, ?, ?, ?)
           ''',
           (ts, wallet.get('sol'), wallet.get('usdc'), wallet.get('totalUsd')),
       )
       snapshot_id = conn.execute('SELECT id FROM price_snapshots WHERE ts = ?', (ts,)).fetchone()[0]
       for symbol, point in (snapshot.get('prices') or {}).items():
           conn.execute(
               '''
               INSERT OR IGNORE INTO price_points(
                   snapshot_id, symbol, price, change24h, liquidity,
                   holding_amount, holding_value, alert_below, buy_below, sell_above
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ''',
               (
                   snapshot_id,
                   symbol,
                   point.get('price'),
                   point.get('change24h'),
                   point.get('liquidity'),
                   point.get('holdingAmount'),
                   point.get('holdingValue'),
                   point.get('alertBelow'),
                   point.get('buyBelow'),
                   point.get('sellAbove'),
               ),
           )
       for message in alerts or []:
           conn.execute('INSERT OR IGNORE INTO alerts(ts, message) VALUES(?, ?)', (ts, message))
       conn.commit()
   finally:
       conn.close()


def record_auto_trade(db_path: Path | str, trade: dict) -> None:
   init_db(db_path)
   trade = dict(trade)
   trade.setdefault('entry_signal_type', trade.get('entrySignalType'))
   trade.setdefault('entry_regime_tag', trade.get('entryRegimeTag'))
   trade.setdefault('entry_strategy_tag', trade.get('entryStrategyTag') or trade.get('strategyTag') or trade.get('strategy_tag'))
   trade.setdefault('exit_reason', trade.get('exitReason'))
   trade.setdefault('validation_mode', trade.get('validationMode') or trade.get('mode'))
   trade.setdefault('approval_status', trade.get('approvalStatus'))
   strategy_tag = trade.get('strategyTag') or trade.get('strategy_tag') or infer_strategy_tag(trade.get('symbol'), trade.get('side'), trade.get('reason'))
   expected_out = _to_float(trade.get('expectedOutAmount') or trade.get('expected_out_amount'))
   actual_out = _to_float(trade.get('outAmount') or trade.get('out_amount'))
   size_usd = trade.get('sizeUsd', trade.get('size_usd'))
   product_type = trade.get('product_type', trade.get('productType', 'spot'))
   strategy_family = trade.get('strategy_family', trade.get('strategyFamily'))
   decision_id = trade.get('decision_id', trade.get('decisionId'))
   trade_key = _compute_auto_trade_key(trade)
   quote_price_impact = trade.get('quotePriceImpact', trade.get('quote_price_impact'))
   realized_pnl_usd = trade.get('realized_pnl_usd', trade.get('realizedPnlUsd'))
   cost_basis_usd = trade.get('cost_basis_usd', trade.get('costBasisUsd'))
   slippage_bps = None
   if expected_out and actual_out and expected_out > 0:
       slippage_bps = ((expected_out - actual_out) / expected_out) * 10000.0
   conn = connect(db_path)
   try:
       conn.execute(
           '''
          INSERT OR IGNORE INTO auto_trades(
              ts, symbol, side, mode, simulated, product_type, venue, strategy_family, decision_id, market,
              price, amount, size_usd, out_amount, expected_out_amount, signature, quote_price_impact, slippage_bps, reason, strategy_tag,
              realized_pnl_usd, cost_basis_usd, trade_key, entry_signal_type, entry_regime_tag, entry_strategy_tag, exit_reason, validation_mode, approval_status
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''',
           (
               trade.get('ts'),
               trade.get('symbol'),
               trade.get('side'),
               trade.get('mode'),
               1 if trade.get('simulated') else 0,
               product_type,
               trade.get('venue', 'jupiter'),
               strategy_family,
               decision_id,
               trade.get('market'),
               trade.get('price'),
               trade.get('amount'),
               size_usd,
               None if trade.get('outAmount') is None and trade.get('out_amount') is None else str(trade.get('outAmount') or trade.get('out_amount')),
               None if trade.get('expectedOutAmount') is None and trade.get('expected_out_amount') is None else str(trade.get('expectedOutAmount') or trade.get('expected_out_amount')),
               trade.get('signature'),
               quote_price_impact,
               slippage_bps,
               trade.get('reason'),
               strategy_tag,
               realized_pnl_usd,
               cost_basis_usd,
               trade_key,
               trade.get('entry_signal_type') or trade.get('entrySignalType'),
               trade.get('entry_regime_tag') or trade.get('entryRegimeTag'),
               trade.get('entry_strategy_tag') or trade.get('entryStrategyTag') or trade.get('strategyTag') or trade.get('strategy_tag'),
               trade.get('exit_reason') or trade.get('exitReason'),
               trade.get('validation_mode') or trade.get('validationMode') or trade.get('mode'),
               trade.get('approval_status') or trade.get('approvalStatus'),
           ),
       )
       conn.execute(
           '''
           UPDATE auto_trades
           SET trade_key = COALESCE(trade_key, ?),
               entry_signal_type = COALESCE(entry_signal_type, ?),
               entry_regime_tag = COALESCE(entry_regime_tag, ?),
               entry_strategy_tag = COALESCE(entry_strategy_tag, ?),
               exit_reason = COALESCE(exit_reason, ?),
               validation_mode = COALESCE(validation_mode, ?),
               approval_status = COALESCE(approval_status, ?)
           WHERE signature = ? OR trade_key = ?
           ''',
           (
               trade_key,
               trade.get('entry_signal_type') or trade.get('entrySignalType'),
               trade.get('entry_regime_tag') or trade.get('entryRegimeTag'),
               trade.get('entry_strategy_tag') or trade.get('entryStrategyTag') or trade.get('strategyTag') or trade.get('strategy_tag'),
               trade.get('exit_reason') or trade.get('exitReason'),
               trade.get('validation_mode') or trade.get('validationMode') or trade.get('mode'),
               trade.get('approval_status') or trade.get('approvalStatus'),
               trade.get('signature'),
               trade_key,
           ),
       )
       conn.commit()
   finally:
       conn.close()
   recompute_trade_analytics(db_path)


def record_whale_observation(db_path: Path | str, observation: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT OR IGNORE INTO whale_observations(
               ts, wallet_label, wallet_address, focus_symbol,
               recent_tx_count, self_fee_payer_count, token_exposure_json, summary, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               observation.get('ts'),
               observation.get('wallet_label'),
               observation.get('wallet_address'),
               observation.get('focus_symbol'),
               observation.get('recent_tx_count'),
               observation.get('self_fee_payer_count'),
               _normalize_json(observation.get('token_exposure')),
               observation.get('summary'),
               _normalize_json(observation),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def record_system_event(db_path: Path | str, event: dict) -> int | None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       cursor = conn.execute(
           '''
           INSERT OR IGNORE INTO system_events(
               ts, event_type, severity, message, source, metadata_json
           ) VALUES (?, ?, ?, ?, ?, ?)
           ''',
           (
               event.get('ts'),
               event.get('event_type'),
               event.get('severity', 'info'),
               event.get('message'),
               event.get('source'),
               _normalize_json(event.get('metadata')),
           ),
       )
       if cursor.lastrowid:
           event_id = int(cursor.lastrowid)
       else:
           row = conn.execute(
               '''
               SELECT id FROM system_events
               WHERE ts = ? AND event_type = ? AND message = ?
               ORDER BY id DESC LIMIT 1
               ''',
               (event.get('ts'), event.get('event_type'), event.get('message')),
           ).fetchone()
           event_id = int(row['id']) if row else None
       conn.commit()
       return event_id
   finally:
       conn.close()


def sync_from_files(db_path: Path | str, data_dir: Path | str) -> dict:
   init_db(db_path)
   data_dir = Path(data_dir)
   counts = {
       'price_snapshots': 0,
       'price_points': 0,
       'alerts': 0,
       'auto_trades': 0,
       'signal_candidates': 0,
   }
   price_history_path = data_dir / 'price_history.json'
   if price_history_path.exists():
       payload = _load_json_file(price_history_path, {}, db_path=db_path)
       snapshots = payload.get('snapshots', []) if isinstance(payload, dict) else []
       for snapshot in snapshots:
           record_snapshot_and_alerts(db_path, snapshot, [])
           counts['price_snapshots'] += 1
           counts['price_points'] += len((snapshot.get('prices') or {}).keys())
   alerts_path = data_dir / 'alerts.log'
   if alerts_path.exists():
       alert_lines = [line.strip() for line in alerts_path.read_text().splitlines() if line.strip()]
       grouped_alerts = {}
       for line in alert_lines:
           if line.startswith('[') and '] ' in line:
               ts, message = line[1:].split('] ', 1)
               grouped_alerts.setdefault(ts, []).append(message)
           else:
               grouped_alerts.setdefault(datetime.now(timezone.utc).isoformat(), []).append(line)
       for ts, messages in grouped_alerts.items():
           record_snapshot_and_alerts(db_path, {'ts': ts, 'wallet': {}, 'prices': {}}, messages)
           counts['alerts'] += len(messages)
   trades_path = data_dir / 'auto_trades.json'
   if trades_path.exists():
       trades = _load_json_file(trades_path, [], db_path=db_path)
       for trade in trades:
           record_auto_trade(db_path, trade)
           counts['auto_trades'] += 1
   return counts


def record_signal_candidate(db_path: Path | str, candidate: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       strategy_tag = candidate.get('strategy_tag', candidate.get('strategyTag'))
       product_type = candidate.get('product_type', candidate.get('productType', 'spot'))
       reference_level = candidate.get('reference_level', candidate.get('referenceLevel'))
       distance_pct = candidate.get('distance_pct', candidate.get('distancePct'))
       quote_price_impact = candidate.get('quote_price_impact', candidate.get('quotePriceImpact'))
       decision_id = candidate.get('decision_id', candidate.get('decisionId'))
       candidate_key = candidate.get('candidate_key', candidate.get('candidateKey'))
       metadata = candidate.get('metadata', candidate.get('metadata_json'))
       regime_tag = candidate.get('regime_tag', candidate.get('regimeTag'))
       params = (
           candidate.get('ts'),
           candidate.get('source'),
           candidate.get('symbol'),
           candidate.get('signal_type'),
           strategy_tag,
           candidate.get('side'),
           product_type,
           candidate.get('market'),
           candidate.get('price'),
           reference_level,
           distance_pct,
           candidate.get('liquidity'),
           quote_price_impact,
           candidate.get('score'),
           regime_tag,
           decision_id,
           candidate.get('status', 'observed'),
           candidate.get('reason'),
           _normalize_json(metadata),
       )
       if candidate_key:
           conn.execute(
               '''
               INSERT INTO signal_candidates(
                   ts, source, symbol, signal_type, strategy_tag, side, product_type, market, price,
                   reference_level, distance_pct, liquidity, quote_price_impact, score, regime_tag, decision_id,
                   candidate_key, status, reason, metadata_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(candidate_key) DO UPDATE SET
                   ts=excluded.ts,
                   source=excluded.source,
                   symbol=excluded.symbol,
                   signal_type=excluded.signal_type,
                   strategy_tag=excluded.strategy_tag,
                   side=excluded.side,
                   product_type=excluded.product_type,
                   market=excluded.market,
                   price=excluded.price,
                   reference_level=excluded.reference_level,
                   distance_pct=excluded.distance_pct,
                   liquidity=excluded.liquidity,
                   quote_price_impact=excluded.quote_price_impact,
                   score=excluded.score,
                   regime_tag=excluded.regime_tag,
                   decision_id=excluded.decision_id,
                   status=excluded.status,
                   reason=excluded.reason,
                   metadata_json=excluded.metadata_json
               ''',
               params[:16] + (candidate_key,) + params[16:],
           )
       else:
           conn.execute(
               '''
               INSERT OR IGNORE INTO signal_candidates(
                   ts, source, symbol, signal_type, strategy_tag, side, product_type, market, price,
                   reference_level, distance_pct, liquidity, quote_price_impact, score, regime_tag, decision_id,
                   status, reason, metadata_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ''',
               params,
           )
       conn.commit()
   finally:
       conn.close()


def record_perp_market_snapshot(db_path: Path | str, snapshot: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT OR IGNORE INTO perp_market_snapshots(
               ts, asset, price_usd, change_pct_24h, high_usd_24h, low_usd_24h, volume_usd_24h, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               snapshot.get('ts'),
               snapshot.get('asset'),
               snapshot.get('priceUsd', snapshot.get('price_usd')),
               snapshot.get('changePct24h', snapshot.get('change_pct_24h')),
               snapshot.get('highUsd24h', snapshot.get('high_usd_24h')),
               snapshot.get('lowUsd24h', snapshot.get('low_usd_24h')),
               snapshot.get('volumeUsd24h', snapshot.get('volume_usd_24h')),
               _normalize_json(snapshot),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def record_perp_account_snapshot(db_path: Path | str, payload: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT OR REPLACE INTO perp_account_snapshots(
               ts, wallet_address, open_position_count, open_notional_usd, unrealized_pnl_usd,
               realized_pnl_usd, margin_used_usd, equity_estimate_usd, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               payload.get('ts'),
               payload.get('wallet_address'),
               payload.get('open_position_count', 0),
               payload.get('open_notional_usd', 0),
               payload.get('unrealized_pnl_usd', 0),
               payload.get('realized_pnl_usd'),
               payload.get('margin_used_usd'),
               payload.get('equity_estimate_usd'),
               _normalize_json(payload),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def upsert_perp_position(db_path: Path | str, payload: dict) -> None:
   init_db(db_path)
   position_key = payload.get('position_key') or payload.get('positionKey') or payload.get('pubkey') or payload.get('id')
   if not position_key:
       raise ValueError('position_key is required')
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT INTO perp_positions(
               position_key, opened_ts, updated_ts, closed_ts, status, asset, side, collateral_token,
               entry_price_usd, mark_price_usd, liq_price_usd, size_usd, notional_usd, margin_used_usd, leverage,
               take_profit_price, stop_loss_price, unrealized_pnl_usd, realized_pnl_usd, fees_usd, funding_usd,
               strategy_tag, mode, decision_id, source, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(position_key) DO UPDATE SET
               opened_ts=excluded.opened_ts,
               updated_ts=excluded.updated_ts,
               closed_ts=excluded.closed_ts,
               status=excluded.status,
               asset=excluded.asset,
               side=excluded.side,
               collateral_token=excluded.collateral_token,
               entry_price_usd=excluded.entry_price_usd,
               mark_price_usd=excluded.mark_price_usd,
               liq_price_usd=excluded.liq_price_usd,
               size_usd=excluded.size_usd,
               notional_usd=excluded.notional_usd,
               margin_used_usd=excluded.margin_used_usd,
               leverage=excluded.leverage,
               take_profit_price=excluded.take_profit_price,
               stop_loss_price=excluded.stop_loss_price,
               unrealized_pnl_usd=excluded.unrealized_pnl_usd,
               realized_pnl_usd=excluded.realized_pnl_usd,
               fees_usd=excluded.fees_usd,
               funding_usd=excluded.funding_usd,
               strategy_tag=excluded.strategy_tag,
               mode=excluded.mode,
               decision_id=excluded.decision_id,
               source=excluded.source,
               raw_json=excluded.raw_json
           ''',
           (
               position_key,
               payload.get('opened_ts') or payload.get('openedTs') or payload.get('ts'),
               payload.get('updated_ts') or payload.get('updatedTs') or payload.get('ts'),
               payload.get('closed_ts') or payload.get('closedTs'),
               payload.get('status', 'open'),
               payload.get('asset'),
               payload.get('side'),
               payload.get('collateral_token') or payload.get('collateralToken'),
               payload.get('entry_price_usd', payload.get('entryPriceUsd')),
               payload.get('mark_price_usd', payload.get('markPriceUsd')),
               payload.get('liq_price_usd', payload.get('liqPriceUsd')),
               payload.get('size_usd', payload.get('sizeUsd')),
               payload.get('notional_usd', payload.get('notionalUsd')),
               payload.get('margin_used_usd', payload.get('marginUsedUsd')),
               payload.get('leverage'),
               payload.get('take_profit_price', payload.get('takeProfitPrice')),
               payload.get('stop_loss_price', payload.get('stopLossPrice')),
               payload.get('unrealized_pnl_usd', payload.get('unrealizedPnlUsd')),
               payload.get('realized_pnl_usd', payload.get('realizedPnlUsd')),
               payload.get('fees_usd', payload.get('feesUsd')),
               payload.get('funding_usd', payload.get('fundingUsd')),
               payload.get('strategy_tag'),
               payload.get('mode', 'paper'),
               payload.get('decision_id'),
               payload.get('source'),
               _normalize_json(payload),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def record_perp_order(db_path: Path | str, payload: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT OR REPLACE INTO perp_orders(
               ts, order_key, position_key, asset, side, order_type, status, size_usd, limit_price, trigger_price,
               slippage_bps, mode, strategy_tag, decision_id, reason, signature, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               payload.get('ts'),
               payload.get('order_key') or payload.get('orderKey') or payload.get('pubkey') or payload.get('id'),
               payload.get('position_key') or payload.get('positionKey'),
               payload.get('asset'),
               payload.get('side'),
               payload.get('order_type') or payload.get('orderType') or 'market',
               payload.get('status', 'observed'),
               payload.get('size_usd', payload.get('sizeUsd')),
               payload.get('limit_price', payload.get('limitPrice')),
               payload.get('trigger_price', payload.get('triggerPrice')),
               payload.get('slippage_bps', payload.get('slippageBps')),
               payload.get('mode', 'paper'),
               payload.get('strategy_tag'),
               payload.get('decision_id'),
               payload.get('reason'),
               payload.get('signature'),
               _normalize_json(payload),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def record_perp_fill(db_path: Path | str, payload: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       _ensure_perp_fill_identity_schema(conn)
       fill_key = _compute_perp_fill_key(payload)
       existing_row_id = _find_existing_perp_fill_row_id(conn, payload, fill_key)
       if existing_row_id is not None:
           conn.commit()
           return
       conn.execute(
           '''
           INSERT OR IGNORE INTO perp_fills(
               fill_key, ts, position_key, order_key, asset, side, action, price_usd, size_usd, fees_usd, funding_usd,
               realized_pnl_usd, mode, strategy_tag, decision_id, raw_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               fill_key,
               payload.get('ts'),
               payload.get('position_key') or payload.get('positionKey'),
               payload.get('order_key') or payload.get('orderKey'),
               payload.get('asset'),
               payload.get('side'),
               payload.get('action'),
               payload.get('price_usd', payload.get('priceUsd')),
               payload.get('size_usd', payload.get('sizeUsd')),
               payload.get('fees_usd', payload.get('feesUsd')),
               payload.get('funding_usd', payload.get('fundingUsd')),
               payload.get('realized_pnl_usd', payload.get('realizedPnlUsd')),
               payload.get('mode', 'paper'),
               payload.get('strategy_tag'),
               payload.get('decision_id'),
               _normalize_json(payload),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def record_risk_event(db_path: Path | str, payload: dict) -> None:
   init_db(db_path)
   conn = connect(db_path)
   try:
       conn.execute(
           '''
           INSERT INTO risk_events(ts, product_type, severity, event_type, scope, scope_key, message, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ''',
           (
               payload.get('ts'),
               payload.get('product_type', 'perps'),
               payload.get('severity', 'info'),
               payload.get('event_type'),
               payload.get('scope'),
               payload.get('scope_key'),
               payload.get('message'),
               _normalize_json(payload.get('metadata')),
           ),
       )
       conn.commit()
   finally:
       conn.close()


def get_trade_performance_split(db_path: Path | str, since_ts: str | None = None) -> Dict[str, dict]:
   init_db(db_path)
   recompute_trade_analytics(db_path)
   conn = connect(db_path)
   try:
       if since_ts is None:
           rows = conn.execute(
               '''
               SELECT COALESCE(mode, 'unknown') AS mode,
                      COUNT(*) AS trade_count,
                      ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                      SUM(CASE WHEN simulated = 1 THEN 1 ELSE 0 END) AS simulated_count,
                      ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
               FROM auto_trades
               WHERE COALESCE(product_type, 'spot') = 'spot'
               GROUP BY COALESCE(mode, 'unknown')
               '''
           ).fetchall()
       else:
           rows = conn.execute(
               '''
               SELECT COALESCE(mode, 'unknown') AS mode,
                      COUNT(*) AS trade_count,
                      ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                      SUM(CASE WHEN simulated = 1 THEN 1 ELSE 0 END) AS simulated_count,
                      ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
               FROM auto_trades
               WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
               GROUP BY COALESCE(mode, 'unknown')
               '''
           , (since_ts,)).fetchall()
       summary = {
           'paper': {'trade_count': 0, 'notional_usd': 0.0, 'simulated_count': 0, 'realized_pnl_usd': 0.0},
           'live': {'trade_count': 0, 'notional_usd': 0.0, 'simulated_count': 0, 'realized_pnl_usd': 0.0},
       }
       for row in rows:
           summary[row['mode']] = {
               'trade_count': int(row['trade_count']),
               'notional_usd': float(row['notional_usd'] or 0.0),
               'simulated_count': int(row['simulated_count'] or 0),
               'realized_pnl_usd': float(row['realized_pnl_usd'] or 0.0),
           }
       return summary
   finally:
       conn.close()


def get_strategy_risk_controls(db_path: Path | str, min_trades: int = 2, min_realized_pnl_usd: float = 0.0) -> dict:
   init_db(db_path)
   recompute_trade_analytics(db_path)
   conn = connect(db_path)
   try:
       rows = conn.execute(
           '''
           SELECT id, ts, symbol, side, mode, amount, size_usd, reason, strategy_tag,
                  out_amount, expected_out_amount, product_type
           FROM auto_trades
           WHERE COALESCE(product_type, 'spot') = 'spot'
           ORDER BY ts, id
           '''
       ).fetchall()
       _pnl_map, strategy_perf = compute_realized_pnl_rows(rows)
       breakdown = []
       paused = []
       for strategy_tag, perf in sorted(strategy_perf.items()):
           trade_count = int(perf['trade_count'])
           realized = round(float(perf['realized_pnl_usd']), 6)
           notional = round(float(perf['notional_usd']), 6)
           avg_pnl = realized / trade_count if trade_count else 0.0
           health_score = round((realized * 10.0) + (avg_pnl * 5.0), 6)
           item = {
               'strategy_tag': strategy_tag,
               'trade_count': trade_count,
               'notional_usd': notional,
               'realized_pnl_usd': realized,
               'avg_pnl_per_trade_usd': round(avg_pnl, 6),
               'health_score': health_score,
           }
           breakdown.append(item)
           if item['trade_count'] >= min_trades and item['realized_pnl_usd'] < min_realized_pnl_usd:
               paused.append(item)
       return {'breakdown': breakdown, 'paused_strategies': paused}
   finally:
       conn.close()


def get_open_positions(db_path: Path | str) -> list[dict]:
   init_db(db_path)
   recompute_trade_analytics(db_path)
   conn = connect(db_path)
   try:
       rows = conn.execute(
           '''
           SELECT mode, symbol, product_type, remaining_amount, cost_basis_usd, market_value_usd,
                  unrealized_pnl_usd, avg_cost_per_unit, last_price, opened_ts, updated_ts
           FROM open_positions
           WHERE COALESCE(product_type, 'spot') = 'spot'
           ORDER BY market_value_usd DESC, symbol
           '''
       ).fetchall()
       return [dict(r) for r in rows]
   finally:
       conn.close()


def get_accounting_audit(db_path: Path | str) -> dict:
   init_db(db_path)
   recompute_trade_analytics(db_path)
   return run_accounting_audit(db_path)


def get_perp_open_positions(db_path: Path | str) -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
       rows = conn.execute(
           '''
           SELECT position_key, opened_ts, updated_ts, closed_ts, status, asset, side, collateral_token,
                  entry_price_usd, mark_price_usd, liq_price_usd, size_usd, notional_usd, margin_used_usd, leverage,
                  take_profit_price, stop_loss_price, unrealized_pnl_usd, realized_pnl_usd, fees_usd, funding_usd,
                  strategy_tag, mode, decision_id, source
           FROM perp_positions
           WHERE status = 'open'
           ORDER BY ABS(COALESCE(notional_usd, size_usd, 0)) DESC, asset
           '''
       ).fetchall()
       return [dict(r) for r in rows]
   finally:
       conn.close()


def get_perp_summary(db_path: Path | str, since_ts: str | None = None) -> dict:
   init_db(db_path)
   conn = connect(db_path)
   try:
       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'
       market_rows = conn.execute(
           '''
           SELECT pms.*
           FROM perp_market_snapshots pms
           JOIN (
               SELECT asset, MAX(ts) AS max_ts
               FROM perp_market_snapshots
               GROUP BY asset
           ) latest ON latest.asset = pms.asset AND latest.max_ts = pms.ts
           ORDER BY asset
           '''
       ).fetchall()
       position_rows = conn.execute(
           '''
           SELECT asset, side, mode, leverage, size_usd, notional_usd, margin_used_usd, unrealized_pnl_usd,
                  realized_pnl_usd, liq_price_usd, mark_price_usd, position_key
           FROM perp_positions
           WHERE status = 'open'
           ORDER BY ABS(COALESCE(notional_usd, size_usd, 0)) DESC, asset
           '''
       ).fetchall()
       fill_rows = conn.execute(
           '''
           SELECT mode, COUNT(*) AS fill_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd,
                  ROUND(COALESCE(SUM(fees_usd), 0), 6) AS fees_usd,
                  ROUND(COALESCE(SUM(funding_usd), 0), 6) AS funding_usd
           FROM perp_fills
           WHERE ts >= ?
           GROUP BY mode
           ORDER BY mode
           ''',
           (since_ts,),
       ).fetchall()
       latest_account = conn.execute('SELECT * FROM perp_account_snapshots ORDER BY ts DESC LIMIT 1').fetchone()
       risk_rows = conn.execute(
           '''
           SELECT ts, product_type, severity, event_type, scope, scope_key, message
           FROM risk_events
           WHERE ts >= ?
           ORDER BY ts DESC
           LIMIT 20
           ''',
           (since_ts,),
       ).fetchall()
       open_notional = sum(float(r['notional_usd'] or r['size_usd'] or 0.0) for r in position_rows)
       unrealized = sum(float(r['unrealized_pnl_usd'] or 0.0) for r in position_rows)
       avg_leverage = round(sum(float(r['leverage'] or 0.0) for r in position_rows) / len(position_rows), 6) if position_rows else 0.0
       liquidation_buffers = []
       for r in position_rows:
           mark = _to_float(r['mark_price_usd'])
           liq = _to_float(r['liq_price_usd'])
           side = (r['side'] or '').lower()
           if mark and liq and mark != 0:
               if side in ('long', 'buy'):
                   liquidation_buffers.append(((mark - liq) / mark) * 100.0)
               elif side in ('short', 'sell'):
                   liquidation_buffers.append(((liq - mark) / mark) * 100.0)
       return {
           'markets': [dict(r) for r in market_rows],
           'open_positions': [dict(r) for r in position_rows],
           'trade_performance': [dict(r) for r in fill_rows],
           'risk_summary': {
               'open_position_count': len(position_rows),
               'open_notional_usd': round(open_notional, 6),
               'unrealized_pnl_usd': round(unrealized, 6),
               'avg_leverage': avg_leverage,
               'closest_liquidation_buffer_pct': round(min(liquidation_buffers), 6) if liquidation_buffers else None,
               'latest_account_snapshot': dict(latest_account) if latest_account else None,
               'recent_risk_events': [dict(r) for r in risk_rows],
           },
       }
   finally:
       conn.close()


def _lookup_future_candidate_price(conn: sqlite3.Connection, candidate: dict | sqlite3.Row, upper_ts: str):
   product_type = (candidate.get('product_type') if isinstance(candidate, dict) else candidate['product_type']) or 'spot'
   symbol = candidate.get('symbol') if isinstance(candidate, dict) else candidate['symbol']
   ts = candidate.get('ts') if isinstance(candidate, dict) else candidate['ts']
   if product_type == 'perps':
       future = conn.execute(
           '''
           SELECT price_usd AS price
           FROM perp_market_snapshots
           WHERE asset = ?
             AND ts > ?
             AND ts <= ?
           ORDER BY ts DESC
           LIMIT 1
           ''',
           (symbol, ts, upper_ts),
       ).fetchone()
   else:
       future = conn.execute(
           '''
           SELECT pp.price
           FROM price_points pp
           JOIN price_snapshots ps ON ps.id = pp.snapshot_id
           WHERE pp.symbol = ?
             AND ps.ts > ?
             AND ps.ts <= ?
           ORDER BY ps.ts DESC
           LIMIT 1
           ''',
           (symbol, ts, upper_ts),
       ).fetchone()
   if not future or future['price'] in (None, 0):
       return None
   return float(future['price'])


def _directional_forward_return_pct(side: str | None, entry_price: float, future_price: float) -> float:
   side = (side or 'buy').lower()
   if side == 'sell':
       return ((entry_price - future_price) / entry_price) * 100.0
   return ((future_price - entry_price) / entry_price) * 100.0


def get_recent_perp_market_history(db_path: Path | str, minutes: int = 180, assets: list[str] | None = None, limit_per_asset: int = 120) -> dict:
   init_db(db_path)
   conn = connect(db_path)
   try:
       since_ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat().replace('+00:00', 'Z')
       rows = conn.execute(
           '''
           SELECT ts, asset, price_usd, change_pct_24h, high_usd_24h, low_usd_24h, volume_usd_24h
           FROM perp_market_snapshots
           WHERE ts >= ?
           ORDER BY asset ASC, ts DESC
           ''',
           (since_ts,),
       ).fetchall()
       grouped = defaultdict(list)
       asset_filter = set(assets or [])
       for row in rows:
           asset = row['asset']
           if asset_filter and asset not in asset_filter:
               continue
           if len(grouped[asset]) >= limit_per_asset:
               continue
           grouped[asset].append(dict(row))
       return {
           'since_ts': since_ts,
           'minutes': minutes,
           'assets': {asset: list(reversed(items)) for asset, items in grouped.items()},
       }
   finally:
       conn.close()


def get_signal_candidate_outcomes(db_path: Path | str, since_ts: str | None = None, horizon_minutes: int = 60, product_type: str = 'spot') -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'

       candidates = conn.execute(
           '''
           SELECT ts, symbol, signal_type, status, side, price, COALESCE(product_type, 'spot') AS product_type
           FROM signal_candidates
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = ?
           ORDER BY ts
           ''',
           (since_ts, product_type),
       ).fetchall()

       by_group = defaultdict(lambda: {'count': 0, 'sum_forward_return_pct': 0.0, 'favorable_count': 0})
       for cand in candidates:
           try:
               cand_dt = datetime.fromisoformat(cand['ts'].replace('Z', '+00:00'))
           except Exception:
               continue
           upper_ts = (cand_dt + timedelta(minutes=horizon_minutes)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
           future_price = _lookup_future_candidate_price(conn, cand, upper_ts)
           if future_price is None or cand['price'] in (None, 0):
               continue
           forward_return = _directional_forward_return_pct(cand['side'], float(cand['price']), future_price)
           key = (cand['signal_type'], cand['status'])
           by_group[key]['count'] += 1
           by_group[key]['sum_forward_return_pct'] += forward_return
           if forward_return > 0:
               by_group[key]['favorable_count'] += 1

       rows = []
       for (signal_type, status), agg in sorted(by_group.items()):
           count = agg['count']
           rows.append({
               'signal_type': signal_type,
               'status': status,
               'product_type': product_type,
               'candidate_count': count,
               'avg_forward_return_pct': round(agg['sum_forward_return_pct'] / count, 6) if count else 0.0,
               'favorable_rate': round(agg['favorable_count'] / count, 6) if count else 0.0,
               'horizon_minutes': horizon_minutes,
           })
       return rows
   finally:
       conn.close()


def _candidate_forward_observations(conn: sqlite3.Connection, since_ts: str, horizon_minutes: int, product_type: str = 'spot') -> list[dict]:
   candidates = conn.execute(
       '''
       SELECT ts, signal_type, strategy_tag, symbol, side, status, price, metadata_json,
              COALESCE(product_type, 'spot') AS product_type, decision_id
       FROM signal_candidates
       WHERE ts >= ?
         AND COALESCE(product_type, 'spot') = ?
       ORDER BY ts
       ''',
       (since_ts, product_type),
   ).fetchall()
   obs = []
   for cand in candidates:
       try:
           cand_dt = datetime.fromisoformat(cand['ts'].replace('Z', '+00:00'))
       except Exception:
           continue
       upper_ts = (cand_dt + timedelta(minutes=horizon_minutes)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
       future_price = _lookup_future_candidate_price(conn, cand, upper_ts)
       if future_price is None or cand['price'] in (None, 0):
           continue
       metadata = _parse_json_object(cand['metadata_json'])
       forward_return = _directional_forward_return_pct(cand['side'], float(cand['price']), future_price)
       favorable = 1 if forward_return > 0 else 0
       obs.append({
           'signal_type': cand['signal_type'],
           'strategy_tag': cand['strategy_tag'],
           'symbol': cand['symbol'],
           'status': cand['status'],
           'product_type': product_type,
           'decision_id': cand['decision_id'],
           'forward_return_pct': forward_return,
           'favorable': favorable,
           'signal_tier': _to_float(metadata.get('tier')),
           'horizon_minutes': horizon_minutes,
           'ts': cand['ts'],
       })
   return obs


def _aggregate_candidate_observations(observations: list[dict], group_keys: list[str], min_sample_size: int = 5) -> list[dict]:
   grouped = defaultdict(lambda: {'count': 0, 'sum_forward_return_pct': 0.0, 'favorable_count': 0, 'sum_signal_tier': 0.0, 'signal_tier_count': 0})
   for obs in observations:
       key = tuple(obs[k] for k in group_keys)
       grouped[key]['count'] += 1
       grouped[key]['sum_forward_return_pct'] += obs['forward_return_pct']
       grouped[key]['favorable_count'] += obs['favorable']
       if obs.get('signal_tier') is not None:
           grouped[key]['sum_signal_tier'] += float(obs['signal_tier'])
           grouped[key]['signal_tier_count'] += 1
   rows = []
   for key, agg in sorted(grouped.items()):
       count = agg['count']
       sample_sufficient = count >= min_sample_size
       avg_signal_tier = (agg['sum_signal_tier'] / agg['signal_tier_count']) if agg['signal_tier_count'] else None
       item = {k: v for k, v in zip(group_keys, key)}
       item.update({
           'candidate_count': count,
           'avg_forward_return_pct': round(agg['sum_forward_return_pct'] / count, 6) if count else 0.0,
           'favorable_rate': round(agg['favorable_count'] / count, 6) if count else 0.0,
           'sample_sufficient': sample_sufficient,
           'min_sample_size': min_sample_size,
           'avg_signal_tier': round(avg_signal_tier, 6) if avg_signal_tier is not None else None,
       })
       rows.append(item)
   return rows


def _pressure_penalty(pressure: str | None) -> int:
   if pressure == 'aggressive':
       return 10
   if pressure == 'elevated':
       return 5
   return 0


def _tier_penalty(avg_signal_tier) -> int:
   tier = _to_float(avg_signal_tier)
   if tier is None or tier <= 1:
       return 0
   return int(round((tier - 1) * 5))


def _quality_execution_score(candidate_count: int, avg_forward_return_pct: float, favorable_rate: float, min_sample_size: int, avg_signal_tier=None, pressure: str | None = None) -> int:
   score = 50
   score += max(-20, min(20, round(avg_forward_return_pct * 25)))
   score += max(-20, min(20, round((favorable_rate - 0.5) * 100)))
   if candidate_count >= min_sample_size:
       score += min(10, max(0, candidate_count - min_sample_size))
   else:
       score -= min(15, (min_sample_size - candidate_count) * 3)
   score -= _tier_penalty(avg_signal_tier)
   score -= _pressure_penalty(pressure)
   return max(0, min(100, int(round(score))))


def _build_gate_decision(row: dict, scope: str, pressure: str | None = None) -> dict:
   execution_score = _quality_execution_score(
       row['candidate_count'],
       row['avg_forward_return_pct'],
       row['favorable_rate'],
       row['min_sample_size'],
       avg_signal_tier=row.get('avg_signal_tier'),
       pressure=pressure,
   )
   tier_penalty = _tier_penalty(row.get('avg_signal_tier'))
   name = row['strategy_tag'] if scope == 'strategy' else row['symbol']
   insufficient_label = 'sample' if scope == 'strategy' else 'symbol sample'
   if not row['sample_sufficient']:
       decision = 'insufficient_sample'
       decision_reason = f"Insufficient {insufficient_label} ({row['candidate_count']}/{row['min_sample_size']}) for promotion or demotion."
   elif row['avg_forward_return_pct'] >= PROMOTION_AVG_FORWARD_RETURN_PCT and row['favorable_rate'] >= PROMOTION_FAVORABLE_RATE and execution_score >= PROMOTION_EXECUTION_SCORE:
       decision = 'promote'
       decision_reason = f"{scope.title()} cleared promotion thresholds for avg forward return, favorable rate, and execution score."
   elif row['avg_forward_return_pct'] <= DEMOTION_AVG_FORWARD_RETURN_PCT or row['favorable_rate'] < DEMOTION_FAVORABLE_RATE or execution_score < DEMOTION_EXECUTION_SCORE:
       decision = 'demote'
       decision_reason = f"{scope.title()} breached demotion thresholds for candidate quality or execution score."
   else:
       decision = 'monitor'
       decision_reason = f"{scope.title()} has sufficient sample but remains monitor-only."
   return {
       'scope': scope,
       'name': name,
       'status': row['status'],
       'decision': decision,
       'decision_reason': decision_reason,
       'sample_sufficient': row['sample_sufficient'],
       'candidate_count': row['candidate_count'],
       'min_candidate_count': row['min_sample_size'],
       'avg_forward_return_pct': row['avg_forward_return_pct'],
       'avg_forward_return_threshold': PROMOTION_AVG_FORWARD_RETURN_PCT,
       'favorable_rate': row['favorable_rate'],
       'favorable_rate_threshold': PROMOTION_FAVORABLE_RATE,
       'execution_score': execution_score,
       'execution_score_threshold': PROMOTION_EXECUTION_SCORE,
       'demotion_execution_score_threshold': DEMOTION_EXECUTION_SCORE,
       'avg_signal_tier': row.get('avg_signal_tier'),
       'symbol_tier_penalty': tier_penalty,
       'pressure': pressure or 'neutral',
   }


def _deployment_mode_recommendation(accounting_status: str, promoted_strategies: list[dict], promoted_symbols: list[dict], demoted_strategies: list[dict], demoted_symbols: list[dict], insufficient_sample_strategies: list[dict]) -> str:
   if accounting_status != 'clean':
       return 'research_only'
   if promoted_strategies and promoted_symbols and not demoted_strategies and not demoted_symbols and not insufficient_sample_strategies:
       return 'tiny_live_allowlist_only'
   if promoted_strategies or promoted_symbols:
       return 'paper_ranked_only'
   return 'research_only'


def _build_pilot_candidate_context(
   *,
   product_type: str,
   strategy: str,
   symbol: str,
   reason: str,
   blockers: list[str] | None = None,
   horizon_minutes: int | None = None,
   metrics: dict | None = None,
   comparison_vs_no_trade: dict | None = None,
) -> dict:
   return {
       'product_type': product_type,
       'strategy': strategy,
       'symbol': symbol,
       'horizon_minutes': horizon_minutes,
       'reason': reason,
       'blockers': list(blockers or []),
       'metrics': metrics or {},
       'comparison_vs_no_trade': comparison_vs_no_trade,
   }


def _top_pilot_candidate_from_best_candidates(best_candidates: list[dict]) -> dict | None:
   actionable_candidates = [
       row for row in (best_candidates or [])
       if row.get('strategy') != 'perp_no_trade'
   ]
   if not actionable_candidates:
       return None
   return max(
       actionable_candidates,
       key=lambda row: (
           1 if row.get('product_type') == 'perps' else 0,
           float(row.get('avg_forward_return_pct') or 0.0),
           float(row.get('favorable_rate') or 0.0),
           float(row.get('execution_score') or 0.0),
           int(row.get('candidate_count') or 0),
       ),
   )


def _build_tiny_live_pilot_decision(
   accounting_status: str,
   deployment_mode_recommendation: str,
   promoted_strategies: list[dict],
   promoted_symbols: list[dict],
   perp_candidate_competition: list[dict],
   spot_candidate_pairs: list[dict] | None = None,
) -> dict:
   primary_horizon_minutes = 60
   min_candidate_count = max(8, GATING_MIN_SAMPLE_SIZE)
   min_perp_win_rate = 0.50
   min_perp_avg_edge_pct = 0.25
   min_perp_environment_edge_pct = 0.35
   min_spot_execution_score = 70
   basket_label = 'SOL/BTC/ETH basket'

   decision = {
       'approved': False,
       'mode': 'paper_only',
       'product_type': None,
       'strategy': None,
       'symbol': None,
       'reason': 'No strategy has cleared the safety and evidence bar for tiny live pilot yet.',
       'best_candidates': [],
       'actionable_candidate': None,
       'no_trade_benchmark': None,
       'recommended_candidate': None,
       'blocked_candidate': None,
       'evaluation_horizon_minutes': primary_horizon_minutes,
       'blockers': [],
       'approval_requirements': {
           'accounting_status': 'clean',
           'min_candidate_count': min_candidate_count,
           'perp_min_win_rate': min_perp_win_rate,
           'perp_min_avg_edge_pct': min_perp_avg_edge_pct,
           'perp_min_environment_edge_pct': min_perp_environment_edge_pct,
           'spot_min_execution_score': min_spot_execution_score,
       },
       'evidence': {
           'accounting_status': accounting_status,
           'deployment_mode_recommendation': deployment_mode_recommendation,
           'perp_available_horizons': sorted({int(row['horizon_minutes']) for row in perp_candidate_competition}),
       },
   }
   blockers = []
   if accounting_status != 'clean':
       blockers.append('accounting_not_clean')
       decision['blockers'] = blockers
       decision['blocked_candidate'] = _build_pilot_candidate_context(
           product_type='system',
           strategy='accounting_guardrail',
           symbol='all',
           reason='Accounting audit is not clean, so all tiny live pilot promotion remains disabled.',
           blockers=blockers,
           metrics={'accounting_status': accounting_status},
       )
       decision['reason'] = 'Accounting audit is not clean, so all tiny live pilot promotion remains disabled.'
       return decision

   best_candidates = []
   selected_spot_candidate = None
   spot_pair_candidates = spot_candidate_pairs or []
   if promoted_strategies and promoted_symbols and spot_pair_candidates:
       promoted_strategy_names = {row['name'] for row in promoted_strategies}
       promoted_symbol_names = {row['name'] for row in promoted_symbols}
       eligible_spot_pairs = [
           row for row in spot_pair_candidates
           if row.get('strategy') in promoted_strategy_names and row.get('symbol') in promoted_symbol_names
       ]
       if eligible_spot_pairs:
           selected_spot_candidate = max(
               eligible_spot_pairs,
               key=lambda row: (
                   float(row.get('execution_score') or 0.0),
                   float(row.get('avg_forward_return_pct') or 0.0),
                   float(row.get('favorable_rate') or 0.0),
                   int(row.get('candidate_count') or 0),
               ),
           )
           best_candidates.append({
               'product_type': 'spot',
               'strategy': selected_spot_candidate['strategy'],
               'symbol': selected_spot_candidate['symbol'],
               'execution_score': selected_spot_candidate['execution_score'],
               'avg_forward_return_pct': selected_spot_candidate['avg_forward_return_pct'],
               'favorable_rate': selected_spot_candidate['favorable_rate'],
               'candidate_count': selected_spot_candidate['candidate_count'],
               'decision': 'candidate',
               'reason': 'Spot strategy/symbol pair is promoted but still requires explicit tiny-live confirmation.' if deployment_mode_recommendation != 'tiny_live_allowlist_only' else 'Spot strategy/symbol pair cleared paper promotion gates.',
           })

   symbol_scoped_rows = [
       row for row in perp_candidate_competition
       if row['horizon_minutes'] == primary_horizon_minutes
       and row['signal_type'] in ('perp_short_continuation', 'perp_short_failed_bounce', 'perp_no_trade')
       and row.get('competition_scope') == 'symbol'
       and _normalize_symbol(row.get('symbol'))
   ]
   basket_rows_60 = [
       row for row in perp_candidate_competition
       if row['horizon_minutes'] == primary_horizon_minutes
       and row['signal_type'] in ('perp_short_continuation', 'perp_short_failed_bounce', 'perp_no_trade')
       and row.get('competition_scope') != 'symbol'
   ]
   short_perp_rows_60 = [row for row in symbol_scoped_rows if row['signal_type'] != 'perp_no_trade']
   short_perp_rows_60.sort(
       key=lambda row: (
           row['avg_edge_pct'],
           row['win_rate'],
           row['decision_count'],
           row.get('symbol') or '',
       ),
       reverse=True,
   )
   best_perp = short_perp_rows_60[0] if short_perp_rows_60 else None
   no_trade_row = None
   if best_perp is not None:
       no_trade_row = next(
           (
               row for row in symbol_scoped_rows
               if row['signal_type'] == 'perp_no_trade' and _normalize_symbol(row.get('symbol')) == _normalize_symbol(best_perp.get('symbol'))
           ),
           None,
       )
   decision['evidence']['perp_rows_at_primary_horizon'] = len(symbol_scoped_rows)
   decision['evidence']['perp_basket_rows_at_primary_horizon'] = len(basket_rows_60)
   decision['evidence']['perp_symbol_available_horizons'] = {
       symbol: sorted({int(row['horizon_minutes']) for row in perp_candidate_competition if row.get('competition_scope') == 'symbol' and _normalize_symbol(row.get('symbol')) == symbol})
       for symbol in sorted({_normalize_symbol(row.get('symbol')) for row in perp_candidate_competition if row.get('competition_scope') == 'symbol' and _normalize_symbol(row.get('symbol'))})
   }
   decision['evidence']['perp_no_trade_benchmark'] = dict(no_trade_row) if no_trade_row else None
   decision['evidence']['perp_basket_no_trade_benchmark'] = next((dict(row) for row in basket_rows_60 if row['signal_type'] == 'perp_no_trade'), None)
   decision['no_trade_benchmark'] = (
       _build_pilot_candidate_context(
           product_type='perps',
           strategy='perp_no_trade',
           symbol=_normalize_symbol(no_trade_row.get('symbol')) or basket_label,
           horizon_minutes=primary_horizon_minutes,
           reason=f"No-trade lane benchmark at {primary_horizon_minutes}m horizon for {_normalize_symbol(no_trade_row.get('symbol')) or basket_label}.",
           blockers=[],
           metrics={
               'candidate_count': no_trade_row.get('decision_count'),
               'execution_score': round(float(no_trade_row.get('win_rate') or 0.0) * 100, 2),
               'avg_forward_return_pct': no_trade_row.get('avg_edge_pct'),
               'favorable_rate': no_trade_row.get('win_rate'),
               'avg_best_short_edge_pct': no_trade_row.get('avg_best_short_edge_pct'),
           },
       )
       if no_trade_row is not None else None
   )

   if best_perp is not None:
       decision['evidence']['best_perp_candidate'] = dict(best_perp)
       best_candidates.append({
           'product_type': 'perps',
           'strategy': best_perp['signal_type'],
           'symbol': _normalize_symbol(best_perp.get('symbol')) or basket_label,
           'execution_score': round(best_perp['win_rate'] * 100, 2),
           'avg_forward_return_pct': best_perp['avg_edge_pct'],
           'favorable_rate': best_perp['win_rate'],
           'candidate_count': best_perp['decision_count'],
           'decision': 'candidate',
           'reason': f"Best short perp lane for {_normalize_symbol(best_perp.get('symbol')) or basket_label} at {primary_horizon_minutes}m horizon.",
           'horizon_minutes': primary_horizon_minutes,
           'avg_best_short_edge_pct': best_perp['avg_best_short_edge_pct'],
       })
       if no_trade_row is not None:
           best_candidates.append({
               'product_type': 'perps',
               'strategy': 'perp_no_trade',
               'symbol': _normalize_symbol(no_trade_row.get('symbol')) or basket_label,
               'execution_score': round(no_trade_row['win_rate'] * 100, 2),
               'avg_forward_return_pct': no_trade_row['avg_edge_pct'],
               'favorable_rate': no_trade_row['win_rate'],
               'candidate_count': no_trade_row['decision_count'],
               'decision': 'candidate',
               'reason': f"No-trade lane benchmark at {primary_horizon_minutes}m horizon for {_normalize_symbol(no_trade_row.get('symbol')) or basket_label}.",
               'horizon_minutes': primary_horizon_minutes,
               'avg_best_short_edge_pct': no_trade_row['avg_best_short_edge_pct'],
           })
       perp_blockers = []
       if not _normalize_symbol(best_perp.get('symbol')):
           perp_blockers.append('missing_perp_symbol_evidence')
       if best_perp['decision_count'] < min_candidate_count:
           perp_blockers.append('insufficient_perp_sample_size')
       if best_perp['win_rate'] < min_perp_win_rate:
           perp_blockers.append('perp_win_rate_below_threshold')
       if best_perp['avg_edge_pct'] < min_perp_avg_edge_pct:
           perp_blockers.append('perp_avg_edge_below_threshold')
       if best_perp['avg_best_short_edge_pct'] < min_perp_environment_edge_pct:
           perp_blockers.append('perp_environment_edge_too_small')
       if no_trade_row is None:
           perp_blockers.append('missing_perp_symbol_no_trade_benchmark')
       elif best_perp['win_rate'] <= no_trade_row['win_rate']:
           perp_blockers.append('perp_short_lane_not_beating_no_trade_win_rate')
       if not perp_blockers:
           approved_symbol = _normalize_symbol(best_perp.get('symbol')) or basket_label
           decision.update({
               'approved': True,
               'mode': 'tiny_live_pilot',
               'product_type': 'perps',
               'strategy': best_perp['signal_type'],
               'symbol': approved_symbol,
               'reason': f"Perps lane {best_perp['signal_type']} on {approved_symbol} has cleared the {primary_horizon_minutes}m tiny-live pilot gate with enough sample, win rate, and edge.",
               'blockers': [],
           })
       else:
           blockers.extend(perp_blockers)
   else:
       blockers.append('insufficient_60m_perp_samples')

   if not decision['approved'] and deployment_mode_recommendation == 'tiny_live_allowlist_only' and promoted_strategies and promoted_symbols:
       top_spot = next((row for row in best_candidates if row['product_type'] == 'spot'), None)
       spot_blockers = []
       if top_spot is None:
           spot_blockers.append('no_promoted_spot_pair')
       else:
           if top_spot['candidate_count'] < min_candidate_count:
               spot_blockers.append('insufficient_spot_sample_size')
           if top_spot['execution_score'] < min_spot_execution_score:
               spot_blockers.append('spot_execution_score_below_threshold')
       if not spot_blockers:
           decision.update({
               'approved': True,
               'mode': 'tiny_live_pilot',
               'product_type': 'spot',
               'strategy': top_spot['strategy'],
               'symbol': top_spot['symbol'],
               'reason': 'Spot promotion gates are clean and strong enough for a tiny live allowlist pilot.',
               'blockers': [],
           })
       elif not blockers:
           blockers.extend(spot_blockers)

   decision['best_candidates'] = best_candidates[:3]
   top_candidate = _top_pilot_candidate_from_best_candidates(decision['best_candidates'])
   if decision['approved']:
       no_trade_benchmark = decision['evidence'].get('perp_no_trade_benchmark') or {}
       best_perp = decision['evidence'].get('best_perp_candidate') or {}
       approved_candidate_row = best_perp if decision['product_type'] == 'perps' else (selected_spot_candidate or top_candidate or {})
       approved_metrics = {
           'candidate_count': approved_candidate_row.get('decision_count') if decision['product_type'] == 'perps' else approved_candidate_row.get('candidate_count'),
           'execution_score': round(float(approved_candidate_row.get('win_rate') or 0.0) * 100, 2) if decision['product_type'] == 'perps' else approved_candidate_row.get('execution_score'),
           'avg_forward_return_pct': approved_candidate_row.get('avg_edge_pct') if decision['product_type'] == 'perps' else approved_candidate_row.get('avg_forward_return_pct'),
           'favorable_rate': approved_candidate_row.get('win_rate') if decision['product_type'] == 'perps' else approved_candidate_row.get('favorable_rate'),
           'avg_best_short_edge_pct': approved_candidate_row.get('avg_best_short_edge_pct'),
       }
       decision['actionable_candidate'] = _build_pilot_candidate_context(
           product_type=decision['product_type'],
           strategy=decision['strategy'],
           symbol=decision['symbol'],
           horizon_minutes=primary_horizon_minutes if decision['product_type'] == 'perps' else None,
           reason=decision['reason'],
           blockers=[],
           metrics=approved_metrics,
           comparison_vs_no_trade=(
               {
                   'signal_type': 'perp_no_trade',
                   'win_rate_gap': round(float(best_perp.get('win_rate') or 0.0) - float(no_trade_benchmark.get('win_rate') or 0.0), 6),
                   'avg_edge_gap_pct': round(float(best_perp.get('avg_edge_pct') or 0.0) - float(no_trade_benchmark.get('avg_edge_pct') or 0.0), 6),
               }
               if decision['product_type'] == 'perps' and no_trade_benchmark else None
           ),
       )
       decision['recommended_candidate'] = dict(decision['actionable_candidate'])
   if not decision['approved']:
       decision['blockers'] = blockers
       if 'insufficient_60m_perp_samples' in blockers:
           decision['reason'] = 'Perp pilot remains paper-only because the primary 60m competition sample is still too small or unlabeled.'
       elif 'perp_short_lane_not_beating_no_trade_win_rate' in blockers:
           decision['reason'] = 'Perp pilot remains paper-only because short lanes are not outperforming the no-trade lane on win rate.'
       elif 'perp_environment_edge_too_small' in blockers:
           decision['reason'] = 'Perp pilot remains paper-only because the recent short-edge environment is too weak for promotion.'
       elif blockers:
           decision['reason'] = 'Tiny live pilot remains paper-only until the current evidence blockers are cleared.'
       if top_candidate:
           comparison_vs_no_trade = None
           if top_candidate.get('product_type') == 'perps':
               best_perp = decision['evidence'].get('best_perp_candidate') or {}
               no_trade_benchmark = decision['evidence'].get('perp_no_trade_benchmark') or {}
               if no_trade_benchmark:
                   comparison_vs_no_trade = {
                       'signal_type': 'perp_no_trade',
                       'win_rate': no_trade_benchmark.get('win_rate'),
                       'avg_edge_pct': no_trade_benchmark.get('avg_edge_pct'),
                       'decision_count': no_trade_benchmark.get('decision_count'),
                       'win_rate_gap': round(float(best_perp.get('win_rate') or 0.0) - float(no_trade_benchmark.get('win_rate') or 0.0), 6),
                       'avg_edge_gap_pct': round(float(best_perp.get('avg_edge_pct') or 0.0) - float(no_trade_benchmark.get('avg_edge_pct') or 0.0), 6),
                   }
           decision['blocked_candidate'] = _build_pilot_candidate_context(
               product_type=top_candidate['product_type'],
               strategy=top_candidate['strategy'],
               symbol=top_candidate['symbol'],
               horizon_minutes=top_candidate.get('horizon_minutes'),
               reason=decision['reason'],
               blockers=blockers,
               metrics={
                   'candidate_count': top_candidate.get('candidate_count'),
                   'execution_score': top_candidate.get('execution_score'),
                   'avg_forward_return_pct': top_candidate.get('avg_forward_return_pct'),
                   'favorable_rate': top_candidate.get('favorable_rate'),
                   'avg_best_short_edge_pct': top_candidate.get('avg_best_short_edge_pct'),
               },
               comparison_vs_no_trade=comparison_vs_no_trade,
           )
           decision['actionable_candidate'] = dict(decision['blocked_candidate'])
   return decision


def get_signal_candidate_outcomes_multi(db_path: Path | str, since_ts: str | None = None, horizons: list[int] | None = None, product_type: str = 'spot') -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'
       if horizons is None:
           horizons = [10, 30, 60, 240]
       rows = []
       for horizon in horizons:
           observations = _candidate_forward_observations(conn, since_ts, horizon, product_type=product_type)
           agg_rows = _aggregate_candidate_observations(observations, ['signal_type', 'status'])
           for row in agg_rows:
               row['horizon_minutes'] = horizon
               row['product_type'] = product_type
           rows.extend(agg_rows)
       return rows
   finally:
       conn.close()


def get_signal_candidate_comparison(db_path: Path | str, since_ts: str | None = None, product_type: str = 'spot') -> dict:
   init_db(db_path)
   conn = connect(db_path)
   try:
       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'

       status_rows = conn.execute(
           '''
           SELECT signal_type, status, COUNT(*) AS candidate_count
           FROM signal_candidates
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = ?
           GROUP BY signal_type, status
           ORDER BY signal_type, status
           ''',
           (since_ts, product_type),
       ).fetchall()
       by_signal = defaultdict(lambda: {'signal_type': None, 'candidate_count': 0, 'executed_count': 0, 'skipped_count': 0, 'product_type': product_type})
       for row in status_rows:
           item = by_signal[row['signal_type']]
           item['signal_type'] = row['signal_type']
           item['candidate_count'] += int(row['candidate_count'])
           if row['status'] == 'executed':
               item['executed_count'] += int(row['candidate_count'])
           if row['status'] == 'skipped':
               item['skipped_count'] += int(row['candidate_count'])

       conversion = []
       for _, item in sorted(by_signal.items()):
           total = item['candidate_count']
           conversion.append({
               **item,
               'conversion_rate': round(item['executed_count'] / total, 6) if total else 0.0,
               'skip_rate': round(item['skipped_count'] / total, 6) if total else 0.0,
           })

       return {
           'status_comparison': conversion,
           'conversion': conversion,
       }
   finally:
       conn.close()


def _candidate_notional_from_metadata(metadata_json: str | None) -> float:
   metadata = _parse_json_object(metadata_json)
   return float(_to_float(metadata.get('planned_size_usd')) or 0.0)


def _get_symbol_candidate_volume_summary(conn: sqlite3.Connection, since_ts: str, recent_minutes: int = 180, product_type: str = 'spot') -> dict:
   product_type = product_type or 'spot'
   anchor_row = conn.execute(
       "SELECT MAX(ts) AS max_ts FROM signal_candidates WHERE ts >= ? AND COALESCE(product_type, 'spot') = ?",
       (since_ts, product_type),
   ).fetchone()
   anchor_ts = anchor_row['max_ts'] if anchor_row and anchor_row['max_ts'] else datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
   try:
       anchor_dt = datetime.fromisoformat(anchor_ts.replace('Z', '+00:00'))
   except Exception:
       anchor_dt = datetime.now(timezone.utc)
       anchor_ts = anchor_dt.isoformat().replace('+00:00', 'Z')
   recent_since_ts = (anchor_dt - timedelta(minutes=recent_minutes)).isoformat().replace('+00:00', 'Z')
   rows = conn.execute(
       '''
       SELECT symbol, side, status, metadata_json
       FROM signal_candidates
       WHERE ts >= ?
         AND COALESCE(product_type, 'spot') = ?
       ORDER BY ts
       ''',
       (since_ts, product_type),
   ).fetchall()
   recent_rows = conn.execute(
       '''
       SELECT symbol, side, status
       FROM signal_candidates
       WHERE ts >= ? AND ts <= ?
         AND COALESCE(product_type, 'spot') = ?
       ORDER BY ts
       ''',
       (recent_since_ts, anchor_ts, product_type),
   ).fetchall()

   by_symbol = defaultdict(lambda: {'candidate_count': 0, 'executed_count': 0, 'skipped_count': 0, 'candidate_notional_usd': 0.0, 'executed_notional_usd': 0.0})
   by_symbol_side = defaultdict(lambda: {'candidate_count': 0, 'executed_count': 0, 'skipped_count': 0, 'candidate_notional_usd': 0.0, 'executed_notional_usd': 0.0})
   recent_symbol_side = defaultdict(int)

   for row in rows:
       symbol = row['symbol']
       side = row['side'] or 'unknown'
       status = row['status']
       planned_size_usd = _candidate_notional_from_metadata(row['metadata_json'])
       by_symbol[symbol]['candidate_count'] += 1
       by_symbol_side[(symbol, side)]['candidate_count'] += 1
       by_symbol[symbol]['candidate_notional_usd'] += planned_size_usd
       by_symbol_side[(symbol, side)]['candidate_notional_usd'] += planned_size_usd
       if status == 'executed':
           by_symbol[symbol]['executed_count'] += 1
           by_symbol_side[(symbol, side)]['executed_count'] += 1
           by_symbol[symbol]['executed_notional_usd'] += planned_size_usd
           by_symbol_side[(symbol, side)]['executed_notional_usd'] += planned_size_usd
       elif status == 'skipped':
           by_symbol[symbol]['skipped_count'] += 1
           by_symbol_side[(symbol, side)]['skipped_count'] += 1

   for row in recent_rows:
       recent_symbol_side[(row['symbol'], row['side'] or 'unknown')] += 1

   symbol_rows = []
   for symbol, agg in sorted(by_symbol.items()):
       candidate_count = agg['candidate_count']
       executed_count = agg['executed_count']
       symbol_rows.append({
           'symbol': symbol,
           'product_type': product_type,
           'candidate_count': candidate_count,
           'executed_count': executed_count,
           'skipped_count': agg['skipped_count'],
           'candidate_notional_usd': round(agg['candidate_notional_usd'], 6),
           'executed_notional_usd': round(agg['executed_notional_usd'], 6),
           'execution_rate': round(executed_count / candidate_count, 6) if candidate_count else 0.0,
       })

   symbol_side_rows = []
   for (symbol, side), agg in sorted(by_symbol_side.items()):
       candidate_count = agg['candidate_count']
       executed_count = agg['executed_count']
       symbol_side_rows.append({
           'symbol': symbol,
           'side': side,
           'product_type': product_type,
           'candidate_count': candidate_count,
           'executed_count': executed_count,
           'skipped_count': agg['skipped_count'],
           'candidate_notional_usd': round(agg['candidate_notional_usd'], 6),
           'executed_notional_usd': round(agg['executed_notional_usd'], 6),
           'execution_rate': round(executed_count / candidate_count, 6) if candidate_count else 0.0,
           'recent_candidate_count': recent_symbol_side[(symbol, side)],
           'lookback_minutes': recent_minutes,
       })

   return {
       'since_ts': since_ts,
       'lookback_minutes': recent_minutes,
       'product_type': product_type,
       'by_symbol': symbol_rows,
       'by_symbol_side': symbol_side_rows,
   }


def get_strategy_execution_policy(db_path: Path | str, min_trades: int = 2, min_realized_pnl_usd: float = 0.0) -> dict:
   report = get_daily_analytics(db_path, '1970-01-01T00:00:00.000Z')
   controls = get_strategy_risk_controls(db_path, min_trades=min_trades, min_realized_pnl_usd=min_realized_pnl_usd)
   return {
       'paused_strategies': controls['paused_strategies'],
       'promotion_rules': report['promotion_rules'],
       'symbol_promotion_rules': report['symbol_promotion_rules'],
       'promoted_strategies': report['promoted_strategies'],
       'demoted_strategies': report['demoted_strategies'],
       'promoted_symbols': report['promoted_symbols'],
       'demoted_symbols': report['demoted_symbols'],
       'insufficient_sample_strategies': report['insufficient_sample_strategies'],
       'strategy_health': report['strategy_health'],
       'execution_scores': report['execution_scores'],
       'candidate_volume_by_symbol': report['candidate_volume_by_symbol'],
       'candidate_volume_by_symbol_side': report['candidate_volume_by_symbol_side'],
       'deployment_mode_recommendation': report['deployment_mode_recommendation'],
       'tiny_live_pilot_decision': report['tiny_live_pilot_decision'],
   }


def get_trusted_trade_summary(db_path: Path | str, since_ts: str | None = None) -> dict:
   init_db(db_path)
   conn = connect(db_path)
   try:
       params = []
       where = ["COALESCE(product_type, 'spot') = 'spot'"]
       if since_ts:
           where.append('ts >= ?')
           params.append(since_ts)
       rows = conn.execute(
           f'''
           SELECT id, ts, symbol, side, mode, amount, size_usd, out_amount, expected_out_amount,
                  realized_pnl_usd, cost_basis_usd, strategy_tag
           FROM auto_trades
           WHERE {' AND '.join(where)}
           ORDER BY ts, id
           ''',
           params,
       ).fetchall()
       trusted_rows = []
       untrusted_rows = []
       for row in rows:
           assessment = assess_trade_trust(row)
           row_dict = dict(row)
           row_dict['trusted'] = assessment['trusted']
           row_dict['trust_reason'] = assessment['reason']
           if 'ratio' in assessment:
               row_dict['trust_ratio'] = round(float(assessment['ratio']), 6)
           if assessment['trusted']:
               trusted_rows.append(row_dict)
           else:
               untrusted_rows.append(row_dict)
       by_mode = {}
       for mode in sorted({row['mode'] or 'unknown' for row in trusted_rows} | {row['mode'] or 'unknown' for row in untrusted_rows}):
           trusted_mode_rows = [row for row in trusted_rows if (row['mode'] or 'unknown') == mode]
           untrusted_mode_rows = [row for row in untrusted_rows if (row['mode'] or 'unknown') == mode]
           by_mode[mode] = {
               'trusted_trade_count': len(trusted_mode_rows),
               'trusted_notional_usd': round(sum(float(row['size_usd'] or 0.0) for row in trusted_mode_rows), 6),
               'trusted_realized_pnl_usd': round(sum(float(row['realized_pnl_usd'] or 0.0) for row in trusted_mode_rows), 6),
               'untrusted_trade_count': len(untrusted_mode_rows),
               'untrusted_notional_usd': round(sum(float(row['size_usd'] or 0.0) for row in untrusted_mode_rows), 6),
           }
       return {
           'trusted_trade_count': len(trusted_rows),
           'untrusted_trade_count': len(untrusted_rows),
           'trusted_notional_usd': round(sum(float(row['size_usd'] or 0.0) for row in trusted_rows), 6),
           'trusted_realized_pnl_usd': round(sum(float(row['realized_pnl_usd'] or 0.0) for row in trusted_rows), 6),
           'untrusted_rows': untrusted_rows[:10],
           'by_mode': by_mode,
       }
   finally:
       conn.close()


def get_candidate_score_summary(db_path: Path | str, since_ts: str | None = None, product_type: str = 'spot') -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
       params = [product_type]
       where = ["score IS NOT NULL", "COALESCE(product_type, 'spot') = ?"]
       if since_ts:
           where.append('ts >= ?')
           params.append(since_ts)
       rows = conn.execute(
           f'''
           SELECT symbol, status,
                  COUNT(*) AS candidate_count,
                  AVG(score) AS avg_score,
                  MIN(score) AS min_score,
                  MAX(score) AS max_score
           FROM signal_candidates
           WHERE {' AND '.join(where)}
           GROUP BY symbol, status
           ORDER BY avg_score DESC, candidate_count DESC, symbol ASC
           ''',
           params,
       ).fetchall()
       return [dict(row) | {'product_type': product_type} for row in rows]
   finally:
       conn.close()


def get_recent_scored_candidates(db_path: Path | str, since_ts: str | None = None, limit: int = 10, product_type: str = 'spot') -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
       params = [product_type]
       where = ["score IS NOT NULL", "COALESCE(product_type, 'spot') = ?"]
       if since_ts:
           where.append('ts >= ?')
           params.append(since_ts)
       params.append(limit)
       rows = conn.execute(
           f'''
           SELECT ts, symbol, signal_type, status, score, regime_tag, metadata_json
           FROM signal_candidates
           WHERE {' AND '.join(where)}
           ORDER BY ts DESC
           LIMIT ?
           ''',
           params,
       ).fetchall()
       normalized_rows = []
       for row in rows:
           row_dict = dict(row)
           metadata_json = row_dict.pop('metadata_json', None)
           normalized_rows.append(
               row_dict
               | {
                   'threshold_distance': _to_float(_json_extract_path(metadata_json, 'score_components', 'threshold_distance')),
                   'liquidity_component_monitor': _to_float(_json_extract_path(metadata_json, 'score_components', 'liquidity')),
                   'liquidity_component_execution': _to_float(_json_extract_path(metadata_json, 'score_components', 'liquidity_quality')),
                   'quote_price_impact': _to_float(_json_extract_path(metadata_json, 'score_components', 'quote_price_impact')),
                   'symbol_expectancy': _to_float(_json_extract_path(metadata_json, 'score_components', 'symbol_expectancy')),
                   'strategy_expectancy': _to_float(_json_extract_path(metadata_json, 'score_components', 'strategy_expectancy')),
                   'whale_pressure': _to_float(_json_extract_path(metadata_json, 'score_components', 'whale_pressure')),
                   'regime_component': _to_float(_json_extract_path(metadata_json, 'score_components', 'regime')),
                   'saturation': _to_float(_json_extract_path(metadata_json, 'score_components', 'saturation')),
               }
           )
       return normalized_rows
   finally:
       conn.close()


def get_candidate_regime_summary(db_path: Path | str, since_ts: str | None = None, product_type: str = 'spot') -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   try:
      params = [product_type]
      where = ["COALESCE(regime_tag, '') <> ''", "COALESCE(product_type, 'spot') = ?"]
      if since_ts:
          where.append('ts >= ?')
          params.append(since_ts)
      rows = conn.execute(
          f'''
          SELECT regime_tag, status,
                 COUNT(*) AS candidate_count,
                 AVG(score) AS avg_score
          FROM signal_candidates
          WHERE {' AND '.join(where)}
          GROUP BY regime_tag, status
          ORDER BY candidate_count DESC, avg_score DESC
          ''',
          params,
      ).fetchall()
      return [dict(row) | {'product_type': product_type} for row in rows]
   finally:
      conn.close()



SPOT_LIVE_DAILY_LOSS_KILL_USD = 2.0
SPOT_PAPER_DAILY_LOSS_WARN_USD = 3.0
PERPS_DAILY_LOSS_KILL_USD = 1.5
MAX_CONSECUTIVE_LOSSES_BEFORE_KILL = 3

PRODUCTION_STRATEGY_RULES = {
   'mean_reversion_near_buy': {
       'product_type': 'spot',
       'family': 'pullback_continuation',
       'entry_rule': 'Hard-buy only when price is at/below static buy threshold, liquidity is healthy, quote impact is below max, and regime is not trend_down/panic_selloff.',
       'exit_rule': 'Exit on take-profit threshold, paper stop-loss, paper time-stop, or risk-off de-risking.',
   },
   'paper_near_buy_probe': {
       'product_type': 'spot',
       'family': 'breakout_retest',
       'entry_rule': 'Probe only when price is within the configured near-buy band above the hard threshold, symbol/tier allows probes, and ranked-paper gates remain open.',
       'exit_rule': 'Reduce on partial-profit, exit on stop-loss/time-stop, or upgrade to full hard-buy only on renewed confirmation.',
   },
   'perp_short_continuation': {
       'product_type': 'perps',
       'family': 'bearish_perps_continuation',
       'entry_rule': 'Short only when continuation wins against failed-bounce and no-trade with fresh signal age and policy approval.',
       'exit_rule': 'Exit on stop, invalidation, target, max hold, stale market guard, or executor risk block.',
   },
   'perp_short_failed_bounce': {
       'product_type': 'perps',
       'family': 'bearish_perps_failed_bounce',
       'entry_rule': 'Short only when failed-bounce outranks continuation/no-trade with enough edge and policy approval.',
       'exit_rule': 'Exit on stop, invalidation, target, max hold, stale market guard, or executor risk block.',
   },
   'perp_no_trade': {
       'product_type': 'perps',
       'family': 'veto',
       'entry_rule': 'Stay flat when no-trade lane beats or blocks actionable short lanes.',
       'exit_rule': 'N/A',
   },
}

PRODUCTION_SIZING_FRAMEWORK = {
   'spot': {
       'base_risk_pct_of_portfolio': 0.02,
       'max_symbol_risk_cap_usd': 5.0,
       'min_live_order_usd': 5.0,
   },
   'perps': {
       'paper_notional_usd': 3.0,
       'max_paper_notional_usd': 5.0,
       'max_open_positions': 1,
       'daily_loss_cap_usd': PERPS_DAILY_LOSS_KILL_USD,
   },
}

PRODUCTION_KILL_SWITCH_RULES = {
   'spot_live_daily_loss_kill_usd': SPOT_LIVE_DAILY_LOSS_KILL_USD,
   'spot_paper_daily_loss_warn_usd': SPOT_PAPER_DAILY_LOSS_WARN_USD,
   'perps_daily_loss_kill_usd': PERPS_DAILY_LOSS_KILL_USD,
   'max_consecutive_losses': MAX_CONSECUTIVE_LOSSES_BEFORE_KILL,
}


def _utc_day_start_iso() -> str:
   now = datetime.now(timezone.utc)
   return datetime(now.year, now.month, now.day, tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')


def _ensure_production_columns(conn: sqlite3.Connection) -> None:
   _ensure_column(conn, 'auto_trades', 'trade_key', 'trade_key TEXT')
   _ensure_column(conn, 'auto_trades', 'entry_signal_type', 'entry_signal_type TEXT')
   _ensure_column(conn, 'auto_trades', 'entry_regime_tag', 'entry_regime_tag TEXT')
   _ensure_column(conn, 'auto_trades', 'entry_strategy_tag', 'entry_strategy_tag TEXT')
   _ensure_column(conn, 'auto_trades', 'exit_reason', 'exit_reason TEXT')
   _ensure_column(conn, 'auto_trades', 'validation_mode', 'validation_mode TEXT')
   _ensure_column(conn, 'auto_trades', 'approval_status', 'approval_status TEXT')


def get_fill_quality_summary(db_path: Path | str, since_ts: str | None = None) -> dict:
   init_db(db_path)
   since_ts = since_ts or _utc_day_start_iso()
   conn = connect(db_path)
   try:
       spot = [dict(r) for r in conn.execute(
           '''
           SELECT COALESCE(mode, 'unknown') AS mode,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(AVG(slippage_bps), 0), 6) AS avg_slippage_bps,
                  ROUND(COALESCE(MAX(slippage_bps), 0), 6) AS max_slippage_bps,
                  ROUND(COALESCE(AVG(quote_price_impact), 0), 6) AS avg_quote_impact_pct
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY COALESCE(mode, 'unknown')
           ''',
           (since_ts,),
       ).fetchall()]
       perps = []
       for row in conn.execute(
           '''
           SELECT COALESCE(o.mode, 'unknown') AS mode,
                  COUNT(*) AS order_count,
                  ROUND(COALESCE(AVG(o.slippage_bps), 0), 6) AS avg_order_slippage_bps,
                  ROUND(COALESCE(MAX(o.slippage_bps), 0), 6) AS max_order_slippage_bps,
                  ROUND(COALESCE(SUM(f.fees_usd), 0), 6) AS fees_usd,
                  ROUND(COALESCE(SUM(f.size_usd), 0), 6) AS filled_notional_usd
           FROM perp_orders o
           LEFT JOIN perp_fills f ON f.order_key = o.order_key
           WHERE o.ts >= ?
           GROUP BY COALESCE(o.mode, 'unknown')
           ''',
           (since_ts,),
       ).fetchall():
           item = dict(row)
           notional = float(item.get('filled_notional_usd') or 0.0)
           fees = float(item.get('fees_usd') or 0.0)
           item['avg_fee_bps'] = round((fees / notional) * 10000, 6) if notional > 0 else 0.0
           perps.append(item)
       return {'since_ts': since_ts, 'spot': spot, 'perps': perps}
   finally:
       conn.close()


def get_trade_attribution_summary(db_path: Path | str, since_ts: str | None = None) -> dict:
   init_db(db_path)
   since_ts = since_ts or _utc_day_start_iso()
   conn = connect(db_path)
   try:
       rows = conn.execute(
           '''
           SELECT COALESCE(product_type, 'spot') AS product_type,
                  COALESCE(strategy_family, 'unassigned') AS strategy_family,
                  COALESCE(entry_strategy_tag, strategy_tag, 'unknown') AS setup,
                  COALESCE(entry_signal_type, 'unknown') AS entry_signal_type,
                  COALESCE(entry_regime_tag, 'unknown') AS entry_regime_tag,
                  COALESCE(exit_reason, CASE WHEN side = 'sell' THEN reason ELSE 'open_or_entry' END, 'open_or_entry') AS exit_reason,
                  COALESCE(validation_mode, mode, 'unknown') AS validation_mode,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
           FROM auto_trades
           WHERE ts >= ?
           GROUP BY 1,2,3,4,5,6,7
           ORDER BY realized_pnl_usd DESC, trade_count DESC
           ''',
           (since_ts,),
       ).fetchall()
       return {'since_ts': since_ts, 'rows': [dict(r) for r in rows]}
   finally:
       conn.close()


def get_validation_status(db_path: Path | str, since_ts: str | None = None) -> dict:
   since_ts = since_ts or _utc_day_start_iso()
   trusted = get_trusted_trade_summary(db_path, since_ts)
   audit = get_accounting_audit(db_path)
   conn = connect(db_path)
   try:
       rows = [dict(r) for r in conn.execute(
           '''
           SELECT COALESCE(validation_mode, 'unclassified') AS validation_mode,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
           FROM auto_trades
           WHERE ts >= ?
           GROUP BY COALESCE(validation_mode, 'unclassified')
           ORDER BY trade_count DESC
           ''',
           (since_ts,),
       ).fetchall()]
       approval_rows = [dict(r) for r in conn.execute(
           '''
           SELECT COALESCE(approval_status, 'unclassified') AS approval_status,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
           FROM auto_trades
           WHERE ts >= ?
           GROUP BY COALESCE(approval_status, 'unclassified')
           ORDER BY trade_count DESC
           ''',
           (since_ts,),
       ).fetchall()]
   finally:
       conn.close()
   blockers = []
   if audit.get('status') != 'clean':
       blockers.append('accounting_not_clean')
   if trusted.get('trusted_trade_count', 0) < 4:
       blockers.append('insufficient_trusted_trade_samples')
   shadow_count = sum(int(r['trade_count']) for r in rows if r['validation_mode'] == 'shadow')
   paper_count = sum(int(r['trade_count']) for r in rows if r['validation_mode'] in {'paper', 'shadow'})
   return {
       'since_ts': since_ts,
       'paper_validation_status': 'validated' if not blockers and paper_count >= 4 else ('blocked' if blockers else 'in_progress'),
       'shadow_validation_status': 'validated' if not blockers and shadow_count >= 2 else ('blocked' if blockers else 'in_progress'),
       'blockers': blockers,
       'trusted_trade_count': trusted.get('trusted_trade_count', 0),
       'untrusted_trade_count': trusted.get('untrusted_trade_count', 0),
       'accounting_status': audit.get('status'),
       'validation_modes': rows,
       'approval_statuses': approval_rows,
   }


def get_risk_guardrail_status(db_path: Path | str, since_ts: str | None = None) -> dict:
   since_ts = since_ts or _utc_day_start_iso()
   conn = connect(db_path)
   try:
       spot_live = conn.execute("SELECT ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized FROM auto_trades WHERE ts >= ? AND mode = 'live' AND COALESCE(product_type, 'spot') = 'spot'", (since_ts,)).fetchone()
       spot_paper = conn.execute("SELECT ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized FROM auto_trades WHERE ts >= ? AND mode = 'paper' AND COALESCE(product_type, 'spot') = 'spot'", (since_ts,)).fetchone()
       perps_paper = conn.execute("SELECT ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized FROM perp_fills WHERE ts >= ? AND mode = 'paper'", (since_ts,)).fetchone()
       recent = conn.execute("SELECT realized_pnl_usd FROM auto_trades WHERE COALESCE(realized_pnl_usd, 0) != 0 ORDER BY ts DESC, id DESC LIMIT 10").fetchall()
       recent_critical = [dict(r) for r in conn.execute("SELECT ts, product_type, event_type, severity, message FROM risk_events WHERE ts >= ? AND severity IN ('critical', 'error') ORDER BY ts DESC LIMIT 10", (since_ts,)).fetchall()]
       spot_journal_events = [dict(r) for r in conn.execute(
           '''
           SELECT ts, event_type, severity, message, metadata_json
           FROM system_events
           WHERE ts >= ?
             AND COALESCE(source, '') = 'auto-trade.mjs'
             AND event_type IN ('spot_trade_submit_timeout_ambiguous', 'spot_trade_journal_stale_submitted', 'spot_trade_journal_reconciled', 'spot_live_approval_requested', 'spot_trade_external_partial_fill_detected')
           ORDER BY ts DESC, id DESC
           LIMIT 20
           ''',
           (since_ts,),
       ).fetchall()]
   finally:
       conn.close()
   streak = 0
   for row in recent:
       pnl = float(row['realized_pnl_usd'] or 0.0)
       if pnl < 0:
           streak += 1
       elif pnl > 0:
           break
   blockers = []
   kill = False
   if float((spot_live['realized'] if spot_live else 0.0) or 0.0) <= -SPOT_LIVE_DAILY_LOSS_KILL_USD:
       blockers.append('spot_live_daily_loss_limit')
       kill = True
   if float((perps_paper['realized'] if perps_paper else 0.0) or 0.0) <= -PERPS_DAILY_LOSS_KILL_USD:
       blockers.append('perps_daily_loss_limit')
   if streak >= MAX_CONSECUTIVE_LOSSES_BEFORE_KILL:
       blockers.append('consecutive_losses_limit')
       kill = True
   if recent_critical:
       blockers.append('critical_risk_events_present')
   spot_journal_summary = {
       'ambiguous_submit_count': sum(1 for row in spot_journal_events if row['event_type'] == 'spot_trade_submit_timeout_ambiguous'),
       'stale_submitted_count': sum(1 for row in spot_journal_events if row['event_type'] == 'spot_trade_journal_stale_submitted'),
       'approval_request_count': sum(1 for row in spot_journal_events if row['event_type'] == 'spot_live_approval_requested'),
       'reconciliation_count': sum(1 for row in spot_journal_events if row['event_type'] == 'spot_trade_journal_reconciled'),
       'partial_fill_count': sum(1 for row in spot_journal_events if row['event_type'] == 'spot_trade_external_partial_fill_detected'),
       'recent_events': [
           {
               'ts': row['ts'],
               'event_type': row['event_type'],
               'severity': row['severity'],
               'message': row['message'],
               'metadata': _parse_json_object(row['metadata_json']),
           }
           for row in spot_journal_events[:8]
       ],
   }
   if spot_journal_summary['ambiguous_submit_count']:
       blockers.append('spot_ambiguous_submit_present')
   if spot_journal_summary['stale_submitted_count']:
       blockers.append('spot_stale_submitted_present')
   if spot_journal_summary['partial_fill_count']:
       blockers.append('spot_partial_fill_present')
   return {
       'since_ts': since_ts,
       'spot_live_realized_pnl_usd': float((spot_live['realized'] if spot_live else 0.0) or 0.0),
       'spot_paper_realized_pnl_usd': float((spot_paper['realized'] if spot_paper else 0.0) or 0.0),
       'perps_paper_realized_pnl_usd': float((perps_paper['realized'] if perps_paper else 0.0) or 0.0),
       'consecutive_losses': streak,
       'kill_switch_recommended': kill,
       'blockers': blockers,
       'limits': dict(PRODUCTION_KILL_SWITCH_RULES),
       'recent_critical_risk_events': recent_critical,
       'spot_journal_summary': spot_journal_summary,
   }



def _resolve_perp_no_trade_threshold(rows: list[sqlite3.Row | dict]) -> float:
   thresholds = []
   for row in rows:
       metadata = _parse_json_object(row['metadata_json'] if isinstance(row, sqlite3.Row) else row.get('metadata_json'))
       candidates = [
           _json_extract_path(metadata, 'no_trade_min_short_edge_pct'),
           _json_extract_path(metadata, 'thresholds', 'no_trade_min_short_edge_pct'),
           _json_extract_path(metadata, 'competition', 'no_trade_min_short_edge_pct'),
       ]
       for value in candidates:
           threshold = _to_float(value)
           if threshold is not None:
               thresholds.append(threshold)
   return max(thresholds) if thresholds else 0.0


def get_perp_candidate_competition(db_path: Path | str, since_ts: str | None = None, horizons: list[int] | None = None) -> list[dict]:
   init_db(db_path)
   conn = connect(db_path)
   basket_label = 'SOL/BTC/ETH basket'
   try:
       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'
       if horizons is None:
           horizons = [10, 30, 60, 240]
       rows = conn.execute(
           '''
           SELECT ts, source, symbol, signal_type, side, price, status, decision_id, metadata_json,
                  COALESCE(product_type, 'spot') AS product_type
           FROM signal_candidates
           WHERE ts >= ?
             AND COALESCE(product_type, 'spot') = 'perps'
             AND decision_id IS NOT NULL
             AND signal_type IN ('perp_short_continuation', 'perp_short_failed_bounce', 'perp_no_trade')
           ORDER BY ts, decision_id, signal_type
           ''',
           (since_ts,),
       ).fetchall()
       grouped = defaultdict(list)
       for row in rows:
           grouped[row['decision_id']].append(row)
       agg = defaultdict(lambda: {'decision_count': 0, 'win_count': 0, 'sum_edge_pct': 0.0, 'sum_best_short_edge_pct': 0.0})
       all_lanes = ['perp_short_continuation', 'perp_short_failed_bounce', 'perp_no_trade']
       for decision_rows in grouped.values():
           canonical_rows = _canonicalize_perp_decision_rows(decision_rows)
           if not canonical_rows:
               continue
           representative = canonical_rows[0]
           symbol = _normalize_symbol(representative['symbol']) or basket_label
           try:
               decision_dt = datetime.fromisoformat(representative['ts'].replace('Z', '+00:00'))
           except Exception:
               continue
           representative_price = _to_float(representative['price'])
           if representative_price in (None, 0):
               continue
           lane_map = {row['signal_type']: row for row in canonical_rows}
           threshold = _resolve_perp_no_trade_threshold(decision_rows)
           for horizon in horizons:
               upper_ts = (decision_dt + timedelta(minutes=horizon)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
               future_price = _lookup_future_candidate_price(conn, representative, upper_ts)
               if future_price is None:
                   continue
               lane_edges = {}
               for lane in all_lanes:
                   row = lane_map.get(lane)
                   lane_price = representative_price if row is None else _to_float(row['price'])
                   if lane_price in (None, 0):
                       lane_edges[lane] = 0.0
                       continue
                   lane_edges[lane] = _directional_forward_return_pct('sell', lane_price, future_price)
               best_short_lane = max(
                   ('perp_short_continuation', 'perp_short_failed_bounce'),
                   key=lambda lane: lane_edges.get(lane, 0.0),
               )
               best_short_edge = lane_edges.get(best_short_lane, 0.0)
               winner = 'perp_no_trade' if best_short_edge < threshold else best_short_lane
               for competition_scope, competition_symbol in (('basket', basket_label), ('symbol', symbol)):
                   for lane in all_lanes:
                       key = (competition_scope, competition_symbol, lane, horizon)
                       agg[key]['decision_count'] += 1
                       agg[key]['sum_edge_pct'] += lane_edges.get(lane, 0.0)
                       agg[key]['sum_best_short_edge_pct'] += best_short_edge
                       if lane == winner:
                           agg[key]['win_count'] += 1
       results = []
       for (competition_scope, symbol, lane, horizon), item in sorted(agg.items()):
           decision_count = item['decision_count']
           results.append({
               'competition_scope': competition_scope,
               'symbol': symbol,
               'signal_type': lane,
               'horizon_minutes': horizon,
               'decision_count': decision_count,
               'win_count': item['win_count'],
               'win_rate': round(item['win_count'] / decision_count, 6) if decision_count else 0.0,
               'avg_edge_pct': round(item['sum_edge_pct'] / decision_count, 6) if decision_count else 0.0,
               'avg_best_short_edge_pct': round(item['sum_best_short_edge_pct'] / decision_count, 6) if decision_count else 0.0,
           })
       return results
   finally:
       conn.close()


def _normalize_executor_cycle_reason(result: dict | None) -> str:
   if not isinstance(result, dict):
       return 'unknown'
   action = str(result.get('action') or '').strip()
   reason = str(result.get('reason') or '').strip()
   if action == 'skip' and reason:
       return reason
   if action == 'no_trade':
       return reason or 'no_trade'
   if action:
       return action
   if reason:
       return reason
   return 'unknown'


SOURCE_PRIORITY_PERP_LANES = {
   'perps-monitor.mjs': 0,
   'perps-auto-trade.mjs': 2,
}

STATUS_PRIORITY_PERP_LANES = {
   'candidate': 0,
   'skipped': 1,
   'executed': 2,
}


def _perp_lane_sort_key(row: sqlite3.Row | dict) -> tuple:
   if isinstance(row, sqlite3.Row):
       keys = set(row.keys())
       source = row['source'] if 'source' in keys else None
       status = row['status'] if 'status' in keys else None
       ts = row['ts'] if 'ts' in keys else None
       score_value = row['score'] if 'score' in keys else None
   else:
       source = row.get('source')
       status = row.get('status')
       ts = row.get('ts')
       score_value = row.get('score')
   score = _to_float(score_value)
   return (
       SOURCE_PRIORITY_PERP_LANES.get(source, 1),
       STATUS_PRIORITY_PERP_LANES.get(status, 3),
       ts or '',
       -(score if score is not None else -1e9),
   )


def _canonicalize_perp_decision_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
   lane_map: dict[str, sqlite3.Row] = {}
   for row in rows:
       signal_type = row['signal_type']
       existing = lane_map.get(signal_type)
       if existing is None or _perp_lane_sort_key(row) < _perp_lane_sort_key(existing):
           lane_map[signal_type] = row
   return sorted(lane_map.values(), key=_perp_lane_sort_key)



def _summarize_perp_executor_cycles(cycle_rows: list[sqlite3.Row], limit: int | None = 5) -> tuple[list[dict], dict, dict]:
   recent_cycles = []
   entry_reason_counts: Counter[str] = Counter()
   close_reason_counts: Counter[str] = Counter()
   for row in cycle_rows:
       metadata = _parse_json(row['metadata_json'])
       summary = metadata.get('summary') if isinstance(metadata, dict) else {}
       if not isinstance(summary, dict):
           summary = {}
       candidate_choice = summary.get('candidate_choice') if isinstance(summary.get('candidate_choice'), dict) else {}
       pilot_policy = _normalize_perp_pilot_policy(summary.get('pilot_policy'))
       entry_result = summary.get('entry_result') if isinstance(summary.get('entry_result'), dict) else {}
       live_adapter_request = entry_result.get('live_adapter_request') if isinstance(entry_result.get('live_adapter_request'), dict) else {}
       live_submit_attempt = entry_result.get('live_submit_attempt') if isinstance(entry_result.get('live_submit_attempt'), dict) else {}
       live_adapter_capabilities = entry_result.get('live_adapter_capabilities') if isinstance(entry_result.get('live_adapter_capabilities'), dict) else {}
       position_result = summary.get('position_result') if isinstance(summary.get('position_result'), dict) else {}
       position_results = position_result.get('results') if isinstance(position_result.get('results'), list) else []
       close_reasons = []
       closed_count = 0
       for item in position_results:
           if not isinstance(item, dict):
               continue
           if str(item.get('action') or '').strip() != 'closed':
               continue
           closed_count += 1
           close_reason = str(item.get('reason') or '').strip() or 'closed'
           close_reasons.append(close_reason)
           close_reason_counts[close_reason] += 1
       entry_outcome = _normalize_executor_cycle_reason(entry_result)
       entry_reason_counts[entry_outcome] += 1
       recent_cycles.append({
           'event_ts': row['ts'],
           'summary_ts': summary.get('ts') or row['ts'],
           'severity': row['severity'],
           'message': row['message'],
           'requested_mode': summary.get('requested_mode'),
           'active_mode': summary.get('active_mode'),
           'open_positions_before': summary.get('open_positions_before'),
           'daily_realized_pnl_usd': _to_float(summary.get('daily_realized_pnl_usd')),
           'daily_trade_notional_usd': _to_float(summary.get('daily_trade_notional_usd')),
           'candidate_choice': {
               'type': candidate_choice.get('type'),
               'decision_id': candidate_choice.get('decision_id'),
               'symbol': candidate_choice.get('symbol'),
               'market': candidate_choice.get('market'),
               'selected_signal_type': candidate_choice.get('selected_signal_type'),
               'selected_score': _to_float(candidate_choice.get('selected_score')),
               'selected_reason': candidate_choice.get('selected_reason'),
               'best_short_signal_type': candidate_choice.get('best_short_signal_type'),
               'best_short_score': _to_float(candidate_choice.get('best_short_score')),
               'no_trade_signal_type': candidate_choice.get('no_trade_signal_type'),
               'no_trade_score': _to_float(candidate_choice.get('no_trade_score')),
               'score_gap_vs_no_trade': _to_float(candidate_choice.get('score_gap_vs_no_trade')),
               'blocked_trade': bool(candidate_choice.get('blocked_trade')),
               'block_reason': candidate_choice.get('block_reason'),
               'market_age_minutes': _to_float(candidate_choice.get('market_age_minutes')),
               'invalidation_price': _to_float(candidate_choice.get('invalidation_price')),
               'stop_loss_price': _to_float(candidate_choice.get('stop_loss_price')),
               'take_profit_price': _to_float(candidate_choice.get('take_profit_price')),
               'planned_size_usd': _to_float(candidate_choice.get('planned_size_usd')),
               'paper_notional_usd': _to_float(candidate_choice.get('paper_notional_usd')),
               'candidate_strategy': candidate_choice.get('candidate_strategy'),
           } if candidate_choice else {},
           'pilot_policy': pilot_policy,
           'entry_action': entry_result.get('action'),
           'entry_outcome': entry_outcome,
           'entry_reason': entry_result.get('reason'),
           'entry_symbol': entry_result.get('symbol'),
           'entry_signal_type': entry_result.get('signal_type'),
           'entry_decision_id': entry_result.get('decision_id'),
           'entry_score': _to_float(entry_result.get('score')),
           'live_adapter_request': live_adapter_request,
           'live_submit_attempt': live_submit_attempt,
           'live_adapter_capabilities': live_adapter_capabilities,
           'position_action': position_result.get('action'),
           'closed_count': closed_count if closed_count else int(position_result.get('closed_count') or 0),
           'close_reasons': close_reasons,
       })
   if limit is not None:
       recent_cycles = recent_cycles[:limit]
   return recent_cycles, dict(entry_reason_counts.most_common()), dict(close_reason_counts.most_common())



def _normalize_perp_pilot_policy(pilot_policy: object) -> dict:
   if not isinstance(pilot_policy, dict):
       return {}

   tiny_live_pilot_decision = pilot_policy.get('tiny_live_pilot_decision')
   if not isinstance(tiny_live_pilot_decision, dict):
       tiny_live_pilot_decision = {}

   blockers = tiny_live_pilot_decision.get('blockers')
   if not isinstance(blockers, list):
       blockers = []

   compact_decision = {
       'approved': bool(tiny_live_pilot_decision.get('approved')),
       'mode': tiny_live_pilot_decision.get('mode'),
       'product_type': tiny_live_pilot_decision.get('product_type'),
       'strategy': tiny_live_pilot_decision.get('strategy'),
       'symbol': tiny_live_pilot_decision.get('symbol'),
       'reason': tiny_live_pilot_decision.get('reason'),
       'blockers': [str(item) for item in blockers if item not in (None, '')],
   } if tiny_live_pilot_decision else {}

   return {
       'requested_mode': pilot_policy.get('requested_mode'),
       'env_live_allowed': bool(pilot_policy.get('env_live_allowed')),
       'env_live_enabled': bool(pilot_policy.get('env_live_enabled')),
       'active_mode': pilot_policy.get('active_mode'),
       'strategy_family': pilot_policy.get('strategy_family'),
       'candidate_strategy': pilot_policy.get('candidate_strategy'),
       'candidate_symbol': pilot_policy.get('candidate_symbol'),
       'live_eligible_for_executor': bool(pilot_policy.get('live_eligible_for_executor')),
       'policy_status': pilot_policy.get('policy_status'),
       'denial_reason': pilot_policy.get('denial_reason'),
       'tiny_live_pilot_decision': compact_decision,
   }



def _score_gap_vs_no_trade(best_short_lane: dict | None, no_trade_lane: dict | None) -> float | None:
   if not best_short_lane or not no_trade_lane:
       return None
   best_score = _to_float(best_short_lane.get('score'))
   no_trade_score = _to_float(no_trade_lane.get('score'))
   if best_score is None or no_trade_score is None:
       return None
   return round(best_score - no_trade_score, 6)



def _parse_iso_utc(ts: str | None) -> datetime | None:
   if not ts:
       return None
   try:
       return datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(timezone.utc)
   except Exception:
       return None



def _age_minutes_from_anchor(ts: str | None, anchor_dt: datetime) -> float | None:
   event_dt = _parse_iso_utc(ts)
   if event_dt is None:
       return None
   return round((anchor_dt - event_dt).total_seconds() / 60.0, 3)



def _extract_perp_operator_plan_fields(best_short_lane: dict | None, candidate_choice: dict | None = None) -> dict:
   lane_metadata = (best_short_lane or {}).get('metadata')
   if not isinstance(lane_metadata, dict):
       lane_metadata = {}
   candidate_choice = candidate_choice or {}
   risk_plan = lane_metadata.get('risk_plan') if isinstance(lane_metadata.get('risk_plan'), dict) else {}

   def _first_value(*values):
       for value in values:
           if value is None or value == '':
               continue
           return value
       return None

   return {
       'invalidation_price': _to_float(_first_value(
           lane_metadata.get('invalidation_price'),
           risk_plan.get('invalidation_price'),
           candidate_choice.get('invalidation_price'),
       )),
       'stop_loss_price': _to_float(_first_value(
           lane_metadata.get('stop_loss_price'),
           risk_plan.get('stop_loss_price'),
           candidate_choice.get('stop_loss_price'),
       )),
       'take_profit_price': _to_float(_first_value(
           lane_metadata.get('take_profit_price'),
           risk_plan.get('take_profit_price'),
           candidate_choice.get('take_profit_price'),
       )),
       'planned_size_usd': _to_float(_first_value(
           lane_metadata.get('planned_size_usd'),
           candidate_choice.get('planned_size_usd'),
           candidate_choice.get('paper_notional_usd'),
           lane_metadata.get('paper_notional_usd'),
       )),
       'paper_notional_usd': _to_float(_first_value(
           candidate_choice.get('paper_notional_usd'),
           lane_metadata.get('paper_notional_usd'),
           candidate_choice.get('planned_size_usd'),
           lane_metadata.get('planned_size_usd'),
       )),
       'market_age_minutes': _to_float(_first_value(
           lane_metadata.get('market_age_minutes'),
           candidate_choice.get('market_age_minutes'),
       )),
   }



def _infer_perp_executor_block_reason(matched_cycle: dict, candidate_choice: dict) -> str | None:
   explicit_block_reason = candidate_choice.get('block_reason')
   if explicit_block_reason:
       return str(explicit_block_reason)
   if candidate_choice.get('blocked_trade'):
       return 'blocked_trade'

   entry_action = str(matched_cycle.get('entry_action') or '').strip()
   entry_outcome = str(matched_cycle.get('entry_outcome') or '').strip()
   selected_signal_type = str(candidate_choice.get('selected_signal_type') or '').strip()
   if entry_action == 'skip' and selected_signal_type != 'perp_no_trade' and entry_outcome not in {'', 'unknown', 'no_trade'}:
       return entry_outcome
   return None



def _build_perp_executor_decision_summary(decisions: list[dict], recent_cycles: list[dict], anchor_dt: datetime) -> dict:
   cycle_by_decision_id = {}
   for cycle in recent_cycles:
       candidate_choice = cycle.get('candidate_choice') or {}
       decision_id = candidate_choice.get('decision_id') or cycle.get('entry_decision_id')
       if decision_id and decision_id not in cycle_by_decision_id:
           cycle_by_decision_id[decision_id] = cycle

   def _is_stale_blocker(blockers: list[str]) -> bool:
       return any(blocker in {'stale_signal', 'market_stale'} for blocker in blockers)

   def _decision_priority(item: dict) -> tuple[int, float, float, str]:
       blockers = item.get('blockers') or []
       if item['operator_status'] == 'trade_ready':
           status_rank = 3
       elif item['operator_status'] == 'blocked' and not _is_stale_blocker(blockers):
           status_rank = 2
       elif item['operator_status'] == 'no_trade_selected':
           status_rank = 1
       else:
           status_rank = 0
       return (
           status_rank,
           float(item.get('score_gap_vs_no_trade') or -1e9),
           float(item.get('best_short_score') or -1e9),
           item.get('ts') or '',
       )

   blocker_counts: Counter[str] = Counter()
   summarized_decisions = []
   top_opportunities = []
   trade_ready_count = 0
   blocked_count = 0
   no_trade_selected_count = 0

   for decision in decisions:
       lanes = decision.get('lanes') or []
       best_short_lane = decision.get('best_short_lane') or {}
       no_trade_lane = next((lane for lane in lanes if lane.get('signal_type') == 'perp_no_trade'), None)
       score_gap = _score_gap_vs_no_trade(best_short_lane, no_trade_lane)
       matched_cycle = cycle_by_decision_id.get(decision.get('decision_id')) or {}
       candidate_choice = matched_cycle.get('candidate_choice') or {}
       block_reason = _infer_perp_executor_block_reason(matched_cycle, candidate_choice)
       age_minutes = _age_minutes_from_anchor(decision.get('ts'), anchor_dt)
       blockers = []
       operator_status = 'trade_ready'
       selected_signal_type = candidate_choice.get('selected_signal_type')

       if not best_short_lane:
           operator_status = 'no_trade_selected'
           blockers.append('no_short_lane_available')
       elif block_reason:
           operator_status = 'blocked'
           blockers.append(block_reason)
       elif selected_signal_type == 'perp_no_trade':
           operator_status = 'no_trade_selected'
           blockers.append('no_trade_lane_selected')
       elif score_gap is not None and score_gap <= 0:
           operator_status = 'no_trade_selected'
           blockers.append('no_trade_outscored_best_short')
       elif age_minutes is not None and age_minutes > PERP_EXECUTOR_MAX_SIGNAL_AGE_MINUTES:
           operator_status = 'blocked'
           blockers.append('stale_signal')

       plan_fields = _extract_perp_operator_plan_fields(best_short_lane, candidate_choice)

       for blocker in blockers:
           blocker_counts[blocker] += 1
       if operator_status == 'trade_ready':
           trade_ready_count += 1
       elif operator_status == 'blocked':
           blocked_count += 1
       else:
           no_trade_selected_count += 1

       summary_entry = {
           'decision_id': decision.get('decision_id'),
           'ts': decision.get('ts'),
           'age_minutes': age_minutes,
           'symbol': decision.get('symbol'),
           'selected_signal_type': selected_signal_type or (best_short_lane.get('signal_type') if operator_status != 'no_trade_selected' else 'perp_no_trade'),
           'best_short_signal_type': best_short_lane.get('signal_type'),
           'best_short_score': _to_float(best_short_lane.get('score')),
           'no_trade_score': _to_float((no_trade_lane or {}).get('score')),
           'score_gap_vs_no_trade': score_gap,
           'operator_status': operator_status,
           'blockers': blockers,
       } | plan_fields
       summarized_decisions.append(summary_entry)
       if best_short_lane and (
           operator_status == 'trade_ready'
           or (operator_status == 'blocked' and not _is_stale_blocker(blockers))
       ):
           top_opportunities.append(summary_entry)

   summarized_decisions.sort(key=_decision_priority, reverse=True)
   top_opportunities.sort(key=_decision_priority, reverse=True)
   return {
       'pending_decision_count': len(decisions),
       'trade_ready_count': trade_ready_count,
       'blocked_count': blocked_count,
       'no_trade_selected_count': no_trade_selected_count,
       'signal_age_limit_minutes': PERP_EXECUTOR_MAX_SIGNAL_AGE_MINUTES,
       'top_blockers': [
           {'blocker': blocker, 'count': count}
           for blocker, count in blocker_counts.most_common(5)
       ],
       'best_current_opportunity': summarized_decisions[0] if summarized_decisions else {},
       'top_opportunities': top_opportunities[:5],
       'decisions': summarized_decisions,
   }



def get_perp_executor_state(
   db_path: Path | str,
   lookback_minutes: int = 240,
   analytics_lookback_hours: int = 24,
   recent_fill_limit: int = 25,
   recent_order_limit: int = 25,
   anchor_ts: str | None = None,
) -> dict:
   init_db(db_path)
   conn = connect(db_path)
   try:
       now = datetime.now(timezone.utc)
       anchor_dt = now
       if anchor_ts:
           try:
               anchor_dt = datetime.fromisoformat(anchor_ts.replace('Z', '+00:00')).astimezone(timezone.utc)
           except Exception:
               anchor_dt = now
               anchor_ts = None
       since_ts = (anchor_dt - timedelta(minutes=lookback_minutes)).isoformat().replace('+00:00', 'Z')
       analytics_since_ts = (anchor_dt - timedelta(hours=analytics_lookback_hours)).isoformat().replace('+00:00', 'Z')
       day_start = anchor_dt.replace(hour=0, minute=0, second=0, microsecond=0).isoformat().replace('+00:00', 'Z')

       candidate_rows = conn.execute(
           '''
           SELECT ts, source, symbol, market, signal_type, strategy_tag, side, price, reference_level, distance_pct,
                  liquidity, quote_price_impact, score, regime_tag, decision_id, status, reason, metadata_json
           FROM signal_candidates
           WHERE ts >= ?
             AND COALESCE(product_type, 'spot') = 'perps'
             AND decision_id IS NOT NULL
             AND signal_type IN ('perp_short_continuation', 'perp_short_failed_bounce', 'perp_no_trade')
           ORDER BY ts DESC, decision_id, score DESC
           ''',
           (since_ts,),
       ).fetchall()
       grouped_candidates = defaultdict(list)
       for row in candidate_rows:
           grouped_candidates[row['decision_id']].append(row)
       decisions = []
       for decision_id, rows in grouped_candidates.items():
           canonical_rows = _canonicalize_perp_decision_rows(rows)
           if not canonical_rows:
               continue
           representative = canonical_rows[0]
           lanes = []
           best_short_lane = None
           for row in sorted(canonical_rows, key=lambda item: float(item['score'] or 0), reverse=True):
               lane = dict(row)
               lane['metadata'] = _parse_json(row['metadata_json'])
               lanes.append(lane)
               if lane['signal_type'] in ('perp_short_continuation', 'perp_short_failed_bounce'):
                   if best_short_lane is None or float(lane.get('score') or 0) > float(best_short_lane.get('score') or 0):
                       best_short_lane = lane
           decisions.append({
               'decision_id': decision_id,
               'ts': representative['ts'],
               'symbol': representative['symbol'],
               'market': representative['market'],
               'regime_tag': representative['regime_tag'],
               'price': representative['price'],
               'lanes': lanes,
               'best_short_lane': best_short_lane,
           })
       decisions.sort(key=lambda item: item['ts'], reverse=True)

       latest_market_rows = conn.execute(
           '''
           SELECT pms.asset, pms.ts, pms.price_usd, pms.change_pct_24h, pms.high_usd_24h, pms.low_usd_24h, pms.volume_usd_24h
           FROM perp_market_snapshots pms
           JOIN (
               SELECT asset, MAX(ts) AS max_ts
               FROM perp_market_snapshots
               GROUP BY asset
           ) latest ON latest.asset = pms.asset AND latest.max_ts = pms.ts
           ORDER BY pms.asset
           '''
       ).fetchall()

       open_positions = [
           dict(row) | {'raw': _parse_json(row['raw_json'])}
           for row in conn.execute(
               '''
               SELECT position_key, opened_ts, updated_ts, closed_ts, status, asset, side, collateral_token,
                      entry_price_usd, mark_price_usd, liq_price_usd, size_usd, notional_usd, margin_used_usd, leverage,
                      take_profit_price, stop_loss_price, unrealized_pnl_usd, realized_pnl_usd, fees_usd, funding_usd,
                      strategy_tag, mode, decision_id, source, raw_json
               FROM perp_positions
               WHERE status = 'open' AND mode = 'paper'
               ORDER BY ABS(COALESCE(notional_usd, size_usd, 0)) DESC, asset
               '''
           ).fetchall()
       ]

       recent_orders = [
           dict(row) | {'raw': _parse_json(row['raw_json'])}
           for row in conn.execute(
               '''
               SELECT ts, order_key, position_key, asset, side, order_type, status, size_usd, limit_price, trigger_price,
                      slippage_bps, mode, strategy_tag, decision_id, reason, signature, raw_json
               FROM perp_orders
               WHERE mode = 'paper' AND COALESCE(strategy_tag, '') LIKE 'tiny_live_pilot%'
               ORDER BY ts DESC, id DESC
               LIMIT ?
               ''',
               (recent_order_limit,),
           ).fetchall()
       ]

       recent_fills = [
           dict(row) | {'raw': _parse_json(row['raw_json'])}
           for row in conn.execute(
               '''
               SELECT ts, position_key, order_key, asset, side, action, price_usd, size_usd, fees_usd, funding_usd,
                      realized_pnl_usd, mode, strategy_tag, decision_id, raw_json
               FROM perp_fills
               WHERE mode = 'paper' AND COALESCE(strategy_tag, '') LIKE 'tiny_live_pilot%'
               ORDER BY ts DESC, id DESC
               LIMIT ?
               ''',
               (recent_fill_limit,),
           ).fetchall()
       ]

       recent_risk_events = [
           dict(row) | {'metadata': _parse_json(row['metadata_json'])}
           for row in conn.execute(
               '''
               SELECT ts, product_type, severity, event_type, scope, scope_key, message, metadata_json
               FROM risk_events
               WHERE ts >= ? AND COALESCE(product_type, 'perps') = 'perps'
               ORDER BY ts DESC, id DESC
               LIMIT 25
               ''',
               (day_start,),
           ).fetchall()
       ]

       cycle_rows = conn.execute(
           '''
           SELECT ts, severity, message, metadata_json
           FROM system_events
           WHERE ts >= ?
             AND event_type = 'perp_executor_cycle'
           ORDER BY ts DESC, id DESC
           ''',
           (since_ts,),
       ).fetchall()
       all_recent_cycles, recent_entry_reason_counts, recent_close_reason_counts = _summarize_perp_executor_cycles(cycle_rows, limit=None)
       decision_readiness = _build_perp_executor_decision_summary(decisions, all_recent_cycles, anchor_dt)
       recent_cycles = all_recent_cycles[:5]
       summary_by_decision_id = {row['decision_id']: row for row in decision_readiness['decisions']}
       for decision in decisions:
           decision_summary = summary_by_decision_id.get(decision['decision_id']) or {}
           decision['operator_status'] = decision_summary.get('operator_status', 'trade_ready')
           decision['operator_blockers'] = decision_summary.get('blockers', [])
           decision['score_gap_vs_no_trade'] = decision_summary.get('score_gap_vs_no_trade')
           decision['selected_signal_type'] = decision_summary.get('selected_signal_type')

       daily_fill_metrics = conn.execute(
           '''
           SELECT COUNT(*) AS fill_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd,
                  ROUND(COALESCE(SUM(fees_usd), 0), 6) AS fees_usd,
                  ROUND(COALESCE(SUM(funding_usd), 0), 6) AS funding_usd
           FROM perp_fills
           WHERE ts >= ?
             AND mode = 'paper'
             AND COALESCE(strategy_tag, '') LIKE 'tiny_live_pilot%'
           ''',
           (day_start,),
       ).fetchone()

       daily_trade_metrics = conn.execute(
           '''
           SELECT COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd
           FROM auto_trades
           WHERE ts >= ?
             AND mode = 'paper'
             AND COALESCE(product_type, 'spot') = 'perps'
             AND COALESCE(strategy_family, '') = 'tiny_live_pilot'
           ''',
           (day_start,),
       ).fetchone()

       competition = get_perp_candidate_competition(db_path, since_ts=analytics_since_ts)
       latest_candidate_choice = next((cycle.get('candidate_choice') for cycle in recent_cycles if cycle.get('candidate_choice')), {})
       latest_pilot_policy = next((cycle.get('pilot_policy') for cycle in recent_cycles if cycle.get('pilot_policy')), {})
       return {
           'generated_ts': now.isoformat().replace('+00:00', 'Z'),
           'anchor_ts': anchor_dt.isoformat().replace('+00:00', 'Z'),
           'lookback_minutes': lookback_minutes,
           'analytics_lookback_hours': analytics_lookback_hours,
           'day_start_ts': day_start,
           'decisions': decisions,
           'latest_markets': [dict(row) for row in latest_market_rows],
           'open_positions': open_positions,
           'recent_orders': recent_orders,
           'recent_fills': recent_fills,
           'recent_risk_events': recent_risk_events,
           'recent_cycles': recent_cycles,
           'latest_candidate_choice': latest_candidate_choice,
           'latest_pilot_policy': latest_pilot_policy,
           'best_current_opportunity': decision_readiness['best_current_opportunity'],
           'decision_readiness': {
               'pending_decision_count': decision_readiness['pending_decision_count'],
               'trade_ready_count': decision_readiness['trade_ready_count'],
               'blocked_count': decision_readiness['blocked_count'],
               'no_trade_selected_count': decision_readiness['no_trade_selected_count'],
               'signal_age_limit_minutes': decision_readiness['signal_age_limit_minutes'],
               'top_blockers': decision_readiness['top_blockers'],
               'best_current_opportunity': decision_readiness['best_current_opportunity'],
               'top_opportunities': decision_readiness['top_opportunities'],
               'decisions': decision_readiness['decisions'],
           },
           'recent_entry_reason_counts': recent_entry_reason_counts,
           'recent_close_reason_counts': recent_close_reason_counts,
           'daily_paper_metrics': {
               'trade_count': int(daily_trade_metrics['trade_count'] or 0),
               'trade_notional_usd': float(daily_trade_metrics['notional_usd'] or 0.0),
               'fill_count': int(daily_fill_metrics['fill_count'] or 0),
               'fill_notional_usd': float(daily_fill_metrics['notional_usd'] or 0.0),
               'realized_pnl_usd': float(daily_fill_metrics['realized_pnl_usd'] or 0.0),
               'fees_usd': float(daily_fill_metrics['fees_usd'] or 0.0),
               'funding_usd': float(daily_fill_metrics['funding_usd'] or 0.0),
           },
           'competition': competition,
       }
   finally:
       conn.close()


def get_daily_analytics(db_path: Path | str, since_ts: str | None = None) -> dict:
   init_db(db_path)
   recompute_trade_analytics(db_path)
   conn = connect(db_path)
   try:
       latest_snapshot = conn.execute(
           'SELECT ts, wallet_sol, wallet_usdc, wallet_total_usd FROM price_snapshots ORDER BY ts DESC LIMIT 1'
       ).fetchone()

       if since_ts is None:
           since_row = conn.execute("SELECT datetime('now', '-1 day')").fetchone()
           since_ts = since_row[0].replace(' ', 'T') + 'Z'

       trade_split = get_trade_performance_split(db_path, since_ts)
       trusted_trade_summary = get_trusted_trade_summary(db_path, since_ts)
       accounting_audit = get_accounting_audit(db_path)
       perp_summary = get_perp_summary(db_path, since_ts)
       alert_rows = conn.execute(
           'SELECT message, COUNT(*) AS cnt FROM alerts WHERE ts >= ? GROUP BY message ORDER BY cnt DESC, message LIMIT 10',
           (since_ts,),
       ).fetchall()
       whale_rows = conn.execute(
           '''
           SELECT w.wallet_label, w.wallet_address, w.focus_symbol, w.recent_tx_count, w.self_fee_payer_count, w.summary
           FROM whale_observations w
           JOIN (
               SELECT wallet_address, MAX(ts) AS max_ts
               FROM whale_observations
               WHERE ts >= ?
               GROUP BY wallet_address
           ) latest
             ON latest.wallet_address = w.wallet_address AND latest.max_ts = w.ts
           ORDER BY COALESCE(w.recent_tx_count, 0) DESC, COALESCE(w.self_fee_payer_count, 0) DESC
           LIMIT 10
           ''',
           (since_ts,),
       ).fetchall()
       event_rows = conn.execute(
           '''
           SELECT ts, event_type, severity, message
           FROM system_events
           WHERE ts >= ?
           ORDER BY ts DESC
           LIMIT 20
           ''',
           (since_ts,),
       ).fetchall()
       strategy_rows = conn.execute(
           '''
           SELECT COALESCE(strategy_tag, 'unclassified') AS strategy_tag,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY COALESCE(strategy_tag, 'unclassified')
           ORDER BY notional_usd DESC, strategy_tag
           ''',
           (since_ts,),
       ).fetchall()
       strategy_family_rows = conn.execute(
           '''
           SELECT COALESCE(strategy_family, 'unclassified') AS strategy_family,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY COALESCE(strategy_family, 'unclassified')
           ORDER BY notional_usd DESC, strategy_family
           ''',
           (since_ts,),
       ).fetchall()
       open_positions = conn.execute(
           '''
           SELECT mode, symbol, product_type, remaining_amount, cost_basis_usd, market_value_usd,
                  unrealized_pnl_usd, avg_cost_per_unit, last_price, opened_ts, updated_ts
           FROM open_positions
           WHERE COALESCE(product_type, 'spot') = 'spot'
           ORDER BY market_value_usd DESC, symbol
           '''
       ).fetchall()
       symbol_rows = conn.execute(
           '''
           SELECT symbol,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(SUM(size_usd), 0), 6) AS notional_usd,
                  ROUND(COALESCE(SUM(realized_pnl_usd), 0), 6) AS realized_pnl_usd,
                  SUM(CASE WHEN realized_pnl_usd > 0 THEN 1 ELSE 0 END) AS win_count,
                  SUM(CASE WHEN realized_pnl_usd < 0 THEN 1 ELSE 0 END) AS loss_count
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY symbol
           ORDER BY notional_usd DESC, symbol
           ''',
           (since_ts,),
       ).fetchall()
       strategy_controls = get_strategy_risk_controls(db_path)
       whale_signal_rows = conn.execute(
           '''
           SELECT focus_symbol,
                  COUNT(*) AS wallet_count,
                  SUM(COALESCE(recent_tx_count, 0)) AS tx_activity,
                  SUM(COALESCE(self_fee_payer_count, 0)) AS self_fee_activity
           FROM (
               SELECT w.*
               FROM whale_observations w
               JOIN (
                   SELECT wallet_address, MAX(ts) AS max_ts
                   FROM whale_observations
                   WHERE ts >= ?
                   GROUP BY wallet_address
               ) latest
                 ON latest.wallet_address = w.wallet_address AND latest.max_ts = w.ts
           ) latest_obs
           GROUP BY focus_symbol
           ORDER BY tx_activity DESC, self_fee_activity DESC
           ''',
           (since_ts,),
       ).fetchall()
       execution_rows = conn.execute(
           '''
           SELECT mode,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(AVG(slippage_bps), 0), 6) AS avg_slippage_bps,
                  ROUND(COALESCE(MAX(slippage_bps), 0), 6) AS max_slippage_bps,
                  SUM(CASE WHEN slippage_bps > 0 THEN 1 ELSE 0 END) AS slipped_count
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY mode
           ''',
           (since_ts,),
       ).fetchall()

       latest_price_map = {row['symbol']: row for row in open_positions}
       order_flow_signals = []
       for row in whale_signal_rows:
           focus_symbol = row['focus_symbol']
           if not focus_symbol:
               continue
           tx_activity = int(row['tx_activity'] or 0)
           self_fee = int(row['self_fee_activity'] or 0)
           wallet_count = int(row['wallet_count'] or 0)
           pressure = 'neutral'
           if tx_activity + self_fee * 2 >= 150:
               pressure = 'aggressive'
           elif tx_activity + self_fee * 2 >= 60:
               pressure = 'elevated'
           order_flow_signals.append({
               'symbol': focus_symbol,
               'wallet_count': wallet_count,
               'tx_activity': tx_activity,
               'self_fee_activity': self_fee,
               'pressure': pressure,
               'proxy': 'whale_flow_and_transfer_activity',
           })

       execution_quality = [dict(r) for r in execution_rows]
       for item in execution_quality:
           adverse = 0
           considered = 0
           for row in conn.execute(
               "SELECT symbol, side, price FROM auto_trades WHERE mode = ? AND ts >= ? AND COALESCE(product_type, 'spot') = 'spot'",
               (item['mode'], since_ts),
           ).fetchall():
               latest_open = latest_price_map.get(row['symbol'])
               if not latest_open or row['price'] is None or latest_open['last_price'] is None:
                   continue
               considered += 1
               if row['side'] == 'buy' and float(latest_open['last_price']) < float(row['price']):
                   adverse += 1
               if row['side'] == 'sell' and float(latest_open['last_price']) > float(row['price']):
                   adverse += 1
           item['run_over_rate'] = round((adverse / considered), 6) if considered else 0.0
           item['post_trade_drift_observations'] = considered

       unrealized = sum(float(row['unrealized_pnl_usd'] or 0.0) for row in open_positions)
       recommendations = []
       total_value = float(latest_snapshot['wallet_total_usd']) if latest_snapshot else 0.0
       perp_open_positions = perp_summary['open_positions']
       if strategy_controls['paused_strategies']:
           recommendations.append('Review paused strategies before enabling more live automation.')
       if accounting_audit['status'] != 'clean':
           recommendations.append(
               f"Accounting audit is {accounting_audit['status']}; inspect flagged trades before trusting spot PnL or position sizing."
           )
       if open_positions:
           recommendations.append('Review open positions for stop/exit hygiene and unrealized PnL drift.')
       if perp_open_positions:
           recommendations.append('Review open perp positions for leverage, liquidation buffer, and stop discipline.')
       high_concentration = [r for r in open_positions if total_value > 0 and float(r['market_value_usd'] or 0.0) / total_value > 0.25]
       for row in high_concentration:
           recommendations.append(f"Reduce concentration in {row['symbol']} ({row['mode']}) — position exceeds 25% of portfolio value.")
       whale_signal_rows = conn.execute(
           '''
           SELECT focus_symbol,
COUNT(*) AS wallet_count,
                  SUM(COALESCE(recent_tx_count, 0)) AS tx_activity,
                  SUM(COALESCE(self_fee_payer_count, 0)) AS self_fee_activity
           FROM (
               SELECT w.*
               FROM whale_observations w
               JOIN (
                   SELECT wallet_address, MAX(ts) AS max_ts
                   FROM whale_observations
                   WHERE ts >= ?
                   GROUP BY wallet_address
               ) latest
                 ON latest.wallet_address = w.wallet_address AND latest.max_ts = w.ts
           ) latest_obs
           GROUP BY focus_symbol
           ORDER BY tx_activity DESC, self_fee_activity DESC
           ''',
           (since_ts,),
       ).fetchall()
       execution_rows = conn.execute(
           '''
           SELECT mode,
                  COUNT(*) AS trade_count,
                  ROUND(COALESCE(AVG(slippage_bps), 0), 6) AS avg_slippage_bps,
                  ROUND(COALESCE(MAX(slippage_bps), 0), 6) AS max_slippage_bps,
                  SUM(CASE WHEN slippage_bps > 0 THEN 1 ELSE 0 END) AS slipped_count
           FROM auto_trades
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY mode
           ''',
           (since_ts,),
       ).fetchall()

       latest_price_map = {row['symbol']: row for row in open_positions}
       order_flow_signals = []
       for row in whale_signal_rows:
           focus_symbol = row['focus_symbol']
           if not focus_symbol:
               continue
           tx_activity = int(row['tx_activity'] or 0)
           self_fee = int(row['self_fee_activity'] or 0)
           wallet_count = int(row['wallet_count'] or 0)
           pressure = 'neutral'
           if tx_activity + self_fee * 2 >= 150:
               pressure = 'aggressive'
           elif tx_activity + self_fee * 2 >= 60:
               pressure = 'elevated'
           order_flow_signals.append({
               'symbol': focus_symbol,
               'wallet_count': wallet_count,
               'tx_activity': tx_activity,
               'self_fee_activity': self_fee,
               'pressure': pressure,
               'proxy': 'whale_flow_and_transfer_activity',
           })

       execution_quality = [dict(r) for r in execution_rows]
       for item in execution_quality:
           adverse = 0
           considered = 0
           for row in conn.execute(
               "SELECT symbol, side, price FROM auto_trades WHERE mode = ? AND ts >= ? AND COALESCE(product_type, 'spot') = 'spot'",
               (item['mode'], since_ts),
           ).fetchall():
               latest_open = latest_price_map.get(row['symbol'])
               if not latest_open or row['price'] is None or latest_open['last_price'] is None:
                   continue
               considered += 1
               if row['side'] == 'buy' and float(latest_open['last_price']) < float(row['price']):
                   adverse += 1
               if row['side'] == 'sell' and float(latest_open['last_price']) > float(row['price']):
                   adverse += 1
           item['run_over_rate'] = round((adverse / considered), 6) if considered else 0.0
           item['post_trade_drift_observations'] = considered

       unrealized = sum(float(row['unrealized_pnl_usd'] or 0.0) for row in open_positions)
       recommendations = []
       total_value = float(latest_snapshot['wallet_total_usd']) if latest_snapshot else 0.0
       perp_open_positions = perp_summary['open_positions']
       if strategy_controls['paused_strategies']:
           recommendations.append('Review paused strategies before enabling more live automation.')
       if accounting_audit['status'] != 'clean':
           recommendations.append(
               f"Accounting audit is {accounting_audit['status']}; inspect flagged trades before trusting spot PnL or position sizing."
           )
       if open_positions:
           recommendations.append('Review open positions for stop/exit hygiene and unrealized PnL drift.')
       if perp_open_positions:
           recommendations.append('Review open perp positions for leverage, liquidation buffer, and stop discipline.')
       high_concentration = [r for r in open_positions if total_value > 0 and float(r['market_value_usd'] or 0.0) / total_value > 0.25]
       for row in high_concentration:
           recommendations.append(f"Reduce concentration in {row['symbol']} ({row['mode']}) — position exceeds 25% of portfolio value.")
       losers = [r for r in open_positions if float(r['unrealized_pnl_usd'] or 0.0) < -1.0]
       for row in losers:
           recommendations.append(f"Reduce risk in {row['symbol']} ({row['mode']}) — unrealized PnL is ${float(row['unrealized_pnl_usd']):.2f}.")
       if perp_summary['risk_summary']['closest_liquidation_buffer_pct'] is not None and perp_summary['risk_summary']['closest_liquidation_buffer_pct'] < 20:
           recommendations.append('A perp position has a tight liquidation buffer; reduce leverage or close exposure.')
       if any(sig['pressure'] == 'aggressive' for sig in order_flow_signals):
           recommendations.append('Whale-flow pressure is elevated in at least one symbol; avoid blindly fading informed flow.')
       if alert_rows:
           recommendations.append('Check whether repeated near-buy alerts are translating into profitable entries.')
       if event_rows:
           recommendations.append('Inspect recent system events / kill switches before increasing automation.')

       spot_candidate_rows = conn.execute(
           '''
           SELECT signal_type, status, symbol, COUNT(*) AS candidate_count
           FROM signal_candidates
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'spot'
           GROUP BY signal_type, status, symbol
           ORDER BY candidate_count DESC, symbol
           LIMIT 20
           ''',
           (since_ts,),
       ).fetchall()
       perp_candidate_rows = conn.execute(
           '''
           SELECT signal_type, status, symbol, COUNT(*) AS candidate_count
           FROM signal_candidates
           WHERE ts >= ? AND COALESCE(product_type, 'spot') = 'perps'
           GROUP BY signal_type, status, symbol
           ORDER BY candidate_count DESC, symbol
           LIMIT 20
           ''',
           (since_ts,),
       ).fetchall()

       candidate_volume_summary = _get_symbol_candidate_volume_summary(conn, since_ts, recent_minutes=180, product_type='spot')
       perp_candidate_volume_summary = _get_symbol_candidate_volume_summary(conn, since_ts, recent_minutes=180, product_type='perps')
       min_sample_size = GATING_MIN_SAMPLE_SIZE

       candidate_outcomes = get_signal_candidate_outcomes(db_path, since_ts, 60, product_type='spot')
       candidate_outcomes_multi = get_signal_candidate_outcomes_multi(db_path, since_ts, [10, 30, 60, 240], product_type='spot')
       candidate_compare = get_signal_candidate_comparison(db_path, since_ts, product_type='spot')
       candidate_score_summary = get_candidate_score_summary(db_path, since_ts, product_type='spot')
       candidate_regime_summary = get_candidate_regime_summary(db_path, since_ts, product_type='spot')
       recent_scored_candidates = get_recent_scored_candidates(db_path, since_ts, limit=10, product_type='spot')
       observations_60 = _candidate_forward_observations(conn, since_ts, 60, product_type='spot')
       candidate_outcomes_by_strategy = _aggregate_candidate_observations(observations_60, ['strategy_tag', 'status'], min_sample_size=min_sample_size)
       candidate_outcomes_by_symbol = _aggregate_candidate_observations(observations_60, ['symbol', 'status'], min_sample_size=min_sample_size)
       candidate_outcomes_by_pair = _aggregate_candidate_observations(observations_60, ['strategy_tag', 'symbol', 'status'], min_sample_size=min_sample_size)

       perp_candidate_outcomes = get_signal_candidate_outcomes(db_path, since_ts, 60, product_type='perps')
       perp_candidate_outcomes_multi = get_signal_candidate_outcomes_multi(db_path, since_ts, [10, 30, 60, 240], product_type='perps')
       perp_candidate_compare = get_signal_candidate_comparison(db_path, since_ts, product_type='perps')
       perp_candidate_score_summary = get_candidate_score_summary(db_path, since_ts, product_type='perps')
       perp_candidate_regime_summary = get_candidate_regime_summary(db_path, since_ts, product_type='perps')
       perp_recent_scored_candidates = get_recent_scored_candidates(db_path, since_ts, limit=10, product_type='perps')
       perp_observations_60 = _candidate_forward_observations(conn, since_ts, 60, product_type='perps')
       perp_candidate_outcomes_by_strategy = _aggregate_candidate_observations(perp_observations_60, ['strategy_tag', 'status'], min_sample_size=min_sample_size)
       perp_candidate_outcomes_by_symbol = _aggregate_candidate_observations(perp_observations_60, ['symbol', 'status'], min_sample_size=min_sample_size)
       perp_candidate_competition = get_perp_candidate_competition(db_path, since_ts, [10, 30, 60, 240])
       perp_anchor_row = conn.execute(
           '''
           SELECT MAX(ts) AS max_ts
           FROM (
               SELECT MAX(ts) AS ts FROM signal_candidates WHERE COALESCE(product_type, 'spot') = 'perps'
               UNION ALL
               SELECT MAX(ts) AS ts FROM perp_market_snapshots
               UNION ALL
               SELECT MAX(ts) AS ts FROM perp_orders WHERE mode = 'paper'
               UNION ALL
               SELECT MAX(ts) AS ts FROM perp_fills WHERE mode = 'paper'
               UNION ALL
               SELECT MAX(COALESCE(updated_ts, opened_ts, closed_ts)) AS ts FROM perp_positions WHERE mode = 'paper'
               UNION ALL
               SELECT MAX(ts) AS ts FROM risk_events WHERE COALESCE(product_type, 'perps') = 'perps'
           ) anchors
           '''
       ).fetchone()
       perp_executor_state = get_perp_executor_state(
           db_path,
           lookback_minutes=240,
           analytics_lookback_hours=24,
           anchor_ts=perp_anchor_row['max_ts'] if perp_anchor_row and perp_anchor_row['max_ts'] else None,
       )

       symbol_pressure_map = {sig['symbol']: sig['pressure'] for sig in order_flow_signals}
       promotion_rules = [_build_gate_decision(row, 'strategy') for row in candidate_outcomes_by_strategy]
       symbol_promotion_rules = [
           _build_gate_decision(row, 'symbol', pressure=symbol_pressure_map.get(row['symbol'], 'neutral'))
           for row in candidate_outcomes_by_symbol
       ]
       symbol_rule_map = {(r['name'], r['status']): r for r in symbol_promotion_rules}
       execution_scores = []
       for row in candidate_outcomes_by_symbol:
           symbol = row['symbol']
           status = row['status']
           symbol_rule = symbol_rule_map.get((symbol, status), {})
           pressure = symbol_rule.get('pressure', symbol_pressure_map.get(symbol, 'neutral'))
           score = int(symbol_rule.get('execution_score') or _quality_execution_score(
               row['candidate_count'],
               row['avg_forward_return_pct'],
               row['favorable_rate'],
               row['min_sample_size'],
               avg_signal_tier=row.get('avg_signal_tier'),
               pressure=pressure,
           ))
           tier = 1 if score < 20 else 2 if score < 40 else 3 if score < 60 else 4 if score < 80 else 5
           execution_scores.append({
               'symbol': symbol,
               'status': status,
               'score': score,
               'execution_score': score,
               'tier': tier,
               'pressure': pressure,
               'decision': symbol_rule.get('decision', 'monitor'),
               'decision_reason': symbol_rule.get('decision_reason'),
               'avg_forward_return_pct': row['avg_forward_return_pct'],
               'favorable_rate': row['favorable_rate'],
               'candidate_count': row['candidate_count'],
           })
       promoted_strategies = [row for row in promotion_rules if row['decision'] == 'promote']
       demoted_strategies = [row for row in promotion_rules if row['decision'] == 'demote']
       insufficient_sample_strategies = [row for row in promotion_rules if row['decision'] == 'insufficient_sample']
       promoted_symbols = [row for row in symbol_promotion_rules if row['decision'] == 'promote']
       demoted_symbols = [row for row in symbol_promotion_rules if row['decision'] == 'demote']
       spot_candidate_pairs = []
       for row in candidate_outcomes_by_pair:
           pressure = symbol_pressure_map.get(row['symbol'], 'neutral')
           execution_score = _quality_execution_score(
               row['candidate_count'],
               row['avg_forward_return_pct'],
               row['favorable_rate'],
               row['min_sample_size'],
               avg_signal_tier=row.get('avg_signal_tier'),
               pressure=pressure,
           )
           spot_candidate_pairs.append({
               'strategy': row['strategy_tag'],
               'symbol': row['symbol'],
               'status': row['status'],
               'candidate_count': row['candidate_count'],
               'avg_forward_return_pct': row['avg_forward_return_pct'],
               'favorable_rate': row['favorable_rate'],
               'avg_signal_tier': row.get('avg_signal_tier'),
               'execution_score': execution_score,
               'pressure': pressure,
               'sample_sufficient': row['sample_sufficient'],
           })
       deployment_mode_recommendation = _deployment_mode_recommendation(
           accounting_audit['status'],
           promoted_strategies,
           promoted_symbols,
           demoted_strategies,
           demoted_symbols,
           insufficient_sample_strategies,
       )
       tiny_live_pilot_decision = _build_tiny_live_pilot_decision(
           accounting_audit['status'],
           deployment_mode_recommendation,
           promoted_strategies,
           promoted_symbols,
           perp_candidate_competition,
           spot_candidate_pairs=spot_candidate_pairs,
       )
       missed_opportunities = sorted(
           [
               {
                   'signal_type': obs['signal_type'],
                   'strategy_tag': obs['strategy_tag'],
                   'symbol': obs['symbol'],
                   'status': obs['status'],
                   'forward_return_pct': round(obs['forward_return_pct'], 6),
                   'horizon_minutes': obs['horizon_minutes'],
                   'ts': obs['ts'],
               }
               for obs in observations_60 if obs['status'] != 'executed'
           ],
           key=lambda x: x['forward_return_pct'],
           reverse=True,
       )[:10]

       if perp_candidate_competition:
           recommendations.append('Compare perps short-continuation vs failed-bounce vs no-trade before enabling a tiny live pilot.')
       if tiny_live_pilot_decision['approved']:
           recommendations.append(
               f"Tiny live pilot approved for {tiny_live_pilot_decision['product_type']} {tiny_live_pilot_decision['strategy']} / {tiny_live_pilot_decision['symbol']} — keep it tiny and allowlist-only."
           )
       else:
           recommendations.append(f"Tiny live pilot remains paper-only: {tiny_live_pilot_decision['reason']}")
       if not recommendations:
           recommendations.append('No urgent action items. Keep collecting paper-trading data.')
       recommendations.append(f"Deployment mode recommendation: {deployment_mode_recommendation}.")

       validation_status = get_validation_status(db_path, since_ts)
       risk_guardrails = get_risk_guardrail_status(db_path, since_ts)
       post_trade_attribution = get_trade_attribution_summary(db_path, since_ts)
       fill_quality_summary = get_fill_quality_summary(db_path, since_ts)

       return {
           'since_ts': since_ts,
           'latest_snapshot': dict(latest_snapshot) if latest_snapshot else None,
           'trade_performance': trade_split,
           'trusted_trade_summary': trusted_trade_summary,
           'accounting_audit': accounting_audit,
           'strategy_rules': PRODUCTION_STRATEGY_RULES,
           'position_sizing_framework': PRODUCTION_SIZING_FRAMEWORK,
           'kill_switch_rules': PRODUCTION_KILL_SWITCH_RULES,
           'validation_status': validation_status,
           'risk_guardrails': risk_guardrails,
           'post_trade_attribution': post_trade_attribution,
           'fill_quality_summary': fill_quality_summary,
           'perp_market_overview': perp_summary['markets'],
           'perp_position_summary': perp_summary['open_positions'],
           'perp_trade_performance': perp_summary['trade_performance'],
           'perp_risk_summary': perp_summary['risk_summary'],
           'combined_exposure_summary': {
               'spot_open_market_value_usd': round(sum(float(row['market_value_usd'] or 0.0) for row in open_positions), 6),
               'perp_open_notional_usd': perp_summary['risk_summary']['open_notional_usd'],
               'spot_unrealized_pnl_usd': round(unrealized, 6),
               'perp_unrealized_pnl_usd': perp_summary['risk_summary']['unrealized_pnl_usd'],
           },
           'top_alerts': [dict(r) for r in alert_rows],
           'top_whales': [dict(r) for r in whale_rows],
           'system_events': [dict(r) for r in event_rows],
           'strategy_breakdown': [dict(r) for r in strategy_rows],
           'strategy_family_breakdown': [dict(r) for r in strategy_family_rows],
           'symbol_breakdown': [dict(r) for r in symbol_rows],
           'open_positions': [dict(r) for r in open_positions],
           'portfolio_pnl': {'unrealized_pnl_usd': round(unrealized, 6)},
           'strategy_controls': strategy_controls,
           'strategy_health': strategy_controls['breakdown'],
           'order_flow_signals': order_flow_signals,
           'execution_quality': execution_quality,
           'signal_candidates': [dict(r) for r in spot_candidate_rows],
           'candidate_outcomes': candidate_outcomes,
           'candidate_outcomes_multi': candidate_outcomes_multi,
           'candidate_status_comparison': candidate_compare['status_comparison'],
           'candidate_conversion': candidate_compare['conversion'],
           'candidate_score_summary': candidate_score_summary,
           'candidate_regime_summary': candidate_regime_summary,
           'recent_scored_candidates': recent_scored_candidates,
           'candidate_outcomes_by_strategy': candidate_outcomes_by_strategy,
           'candidate_outcomes_by_symbol': candidate_outcomes_by_symbol,
           'candidate_volume_by_symbol': candidate_volume_summary['by_symbol'],
           'candidate_volume_by_symbol_side': candidate_volume_summary['by_symbol_side'],
           'missed_opportunities': missed_opportunities,
           'promotion_rules': promotion_rules,
           'symbol_promotion_rules': symbol_promotion_rules,
           'promoted_strategies': promoted_strategies,
           'demoted_strategies': demoted_strategies,
           'promoted_symbols': promoted_symbols,
           'demoted_symbols': demoted_symbols,
           'insufficient_sample_strategies': insufficient_sample_strategies,
           'execution_scores': execution_scores,
           'recommendations': recommendations,
           'deployment_mode_recommendation': deployment_mode_recommendation,
           'tiny_live_pilot_decision': tiny_live_pilot_decision,
           'perp_signal_candidates': [dict(r) for r in perp_candidate_rows],
           'perp_candidate_outcomes': perp_candidate_outcomes,
           'perp_candidate_outcomes_multi': perp_candidate_outcomes_multi,
           'perp_candidate_status_comparison': perp_candidate_compare['status_comparison'],
           'perp_candidate_conversion': perp_candidate_compare['conversion'],
           'perp_candidate_score_summary': perp_candidate_score_summary,
           'perp_candidate_regime_summary': perp_candidate_regime_summary,
           'perp_recent_scored_candidates': perp_recent_scored_candidates,
           'perp_candidate_outcomes_by_strategy': perp_candidate_outcomes_by_strategy,
           'perp_candidate_outcomes_by_symbol': perp_candidate_outcomes_by_symbol,
           'perp_candidate_volume_by_symbol': perp_candidate_volume_summary['by_symbol'],
           'perp_candidate_volume_by_symbol_side': perp_candidate_volume_summary['by_symbol_side'],
           'perp_candidate_competition': perp_candidate_competition,
           'perp_executor_state': perp_executor_state,
       }
   finally:
       conn.close()

