# Live Spot Bot Upgrade Roadmap

> For Hermes: execute this roadmap in small, validated slices. Prioritize operator visibility and symbol-specific tuning before broader strategy expansion.

Goal: improve the actual supervised-live spot bot by increasing decision quality, visibility into blockers, and speed of iteration without loosening global safety controls.

Architecture: keep the current paper-first / supervised-live architecture, but upgrade it in layers. First improve operator visibility and config/tuning ergonomics, then improve regime/scoring logic, then add replay/testing infrastructure, and only then expand live symbol promotion logic.

Tech stack: Node.js monitor/executor (`monitor.mjs`, `auto-trade.mjs`), Python SQLite analytics/reporting (`trading_system/*.py`), Telegram operator workflows, GitHub-hosted portable repo.

---

## Phase 1 — Operator visibility and tuning ergonomics

Objective: make it obvious why the bot is or is not trading, and make narrow symbol changes safe.

Files:
- Modify: `trading_system/spot_live_candidate_summary.py`
- Modify: `trading_system/daily_analytics_report.py`
- Modify: `monitor.mjs`
- Modify: `auto-trade.mjs`
- Optional docs: `docs/operations.md`, `docs/setup.md`

Tasks:
1. Add richer live candidate fields
   - `next_trigger_price`
   - `distance_to_buy_pct`
   - `next_price_move_pct`
   - `top_blocker`
   - `secondary_blocker` when available
   - `live_eligible`
   - `paper_eligible`

2. Add clearer blocker categories
   - `price_not_at_threshold`
   - `score_gate`
   - `demotion`
   - `approval_pending`
   - `choppy_or_context`
   - `liquidity`
   - `policy_gate`
   - `strategy_paused`
   - `symbol_live_disabled`

3. Update daily operator output
   - show top live candidate with exact trigger distance
   - show “closest unblocked candidate” separately from “top blocked candidate”

4. Add symbol-specific config file examples for common tuning knobs
   - threshold changes
   - liquidity overrides
   - live score overrides

Success criteria:
- operator can see exactly what would need to change for a symbol to become approval-eligible
- no more ambiguous “all quiet” without context

---

## Phase 2 — Regime classifier hardening

Objective: stop relying on coarse/static regime signals.

Files:
- Modify: `monitor.mjs`
- Modify: `auto-trade.mjs`
- Modify: `trading_system/trading_db.py`
- Modify: `trading_system/daily_analytics_report.py`

Tasks:
1. Introduce explicit regime states
   - `stable_bullish`
   - `stable_bearish`
   - `choppy`
   - `expansion`
   - `risk_off`

2. Persist regime evidence and explanation
   - market 24h move
   - local symbol change
   - recent threshold clustering / volatility proxy

3. Route strategy families by regime
   - mean reversion only in supportive/stable contexts
   - no-trade by default in degraded contexts

Success criteria:
- regime appears in operator output with an explanation
- live candidate decisions show regime suitability clearly

---

## Phase 3 — Score model expansion

Objective: make candidate scoring more explainable and more predictive.

Files:
- Modify: `monitor.mjs`
- Modify: `auto-trade.mjs`
- Modify: `trading_system/trading_db.py`
- Modify: `trading_system/spot_live_candidate_summary.py`

Tasks:
1. Expand score components
   - threshold distance
   - liquidity quality
   - market context
   - symbol trend context
   - regime suitability
   - execution quality proxy

2. Store full score breakdown per candidate
3. Show the breakdown in operator summary output

Success criteria:
- each blocked or approved candidate shows why its score was high/low
- promotion/demotion tuning becomes evidence-driven

---

## Phase 4 — Pair-level promotion / demotion

Objective: stop over-penalizing symbols because of broad strategy-level averages.

Files:
- Modify: `trading_system/trading_db.py`
- Modify: `trading_system/trading_db_cli.py`
- Modify: `auto-trade.mjs`

Tasks:
1. compute promotion/demotion by `symbol + strategy_tag`
2. preserve global strategy and symbol rollups only as secondary context
3. use pair-level evidence first in supervised-live policy

Success criteria:
- PYTH can remain viable even if broad mean-reversion remains mediocre
- operator can inspect pair-level evidence directly

---

## Phase 5 — Replay / backtest harness

Objective: iterate faster than live waiting.

Files:
- Add: `trading_system/spot_replay_runner.py` or equivalent
- Add: fixture-driven tests for replay
- Add: docs for replay mode

Tasks:
1. feed historical snapshots through monitor/scoring logic
2. record which candidates would have triggered
3. compare approvals/trades vs realized forward returns

Success criteria:
- threshold and score changes can be tested offline before touching live supervised mode

---

## Immediate next implementation slice

Start with Phase 1, Task 1 + Task 2:
- richer blocker reporting
- exact next-trigger visibility

Why:
- lowest risk
- immediate operator value
- improves every later tuning decision
