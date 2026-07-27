# Performance Issues — Main Strategy Execution

> Instrumented with per-tick timing in `engine.py`. Use `analysis/perf_analysis.py` to analyze.
> 计时打点已加入 `engine.py` 的 tick loop，用 `analysis/perf_analysis.py` 分析。

---

## P0 — Fix Immediately

### 1. Deadlock: imbalance + rebalance + min_gap

**Location**: `strategy.py` — `atomic_pre_check`, `place_order` flow; `engine.py` — `rebalance` cap logic.

**Symptoms**: Window stuck — no orders placed on either side for the entire window.

**Cause**: Three guards chain-block:
1. `imbalance` guard blocks the heavy side (filled+pending exceeds `POLY_MAX_IMBALANCE`)
2. `rebalance` cap presses the light side price to a fixed level
3. `min_gap` (`POLY_MIN_PRICE_GAP`) blocks the light side because new price is too close to an existing pending order

Result: heavy side can't place (imbalance), light side can't place (min_gap). Both sides stuck until window ends.

**Example**: Window 00:30-00:45, U=4 D=14, `heav_avg=0.4857`. Up price repeatedly pressed to 0.4843 by rebalance cap, pending Up also at 0.48, min_gap blocks.

**Potential fix**: When min_gap blocks but the pending order price has drifted from the market, cancel the stale order and re-place at the new price.

---

### 2. SQLite per-event commit (Disk I/O spikes)

**Location**: `engine.py` — `SqliteRecorder._on_placed` (L254), `_on_filled` (L273),
`_on_cancelled` (L280), `_on_failed` (L289).

**Cause**: Each event handler calls `self._conn.commit()` individually. Every commit
triggers an fsync to flush the WAL to disk. In a 15-min window with 1s ticks, up to
3600+ fsyncs per window.

**Fix**: Batch commits — accumulate writes and commit every 50 events or every 5s.

```python
def _maybe_commit(self):
    self._pending += 1
    if self._pending >= 50 or time.time() - self._last_commit > 5:
        self._conn.commit()
        self._pending = 0
        self._last_commit = time.time()
```

---

### 3. Blocking `urllib.request.urlopen` in async event loop

**Location**: `client.py:262` — `get_tick_size()` fallback path.

```python
with urllib.request.urlopen(req, timeout=5) as resp:  # BLOCKS event loop
```

**Cause**: `urllib.request.urlopen` is synchronous. When called from an asyncio
coroutine, it blocks all other coroutines (WS messages, fill tracking, other windows)
for up to 5 seconds. Triggered when SDK's cached `get_tick_size()` throws.

**Fix**: Replace with `httpx.AsyncClient` (already imported elsewhere):

```python
async with httpx.AsyncClient() as client:
    resp = await client.get(url, timeout=5)
    data = resp.json()
```

---

## P1 — High Priority

### 3. Excessive INFO logging on the hot path

**Locations**:

| File | Lines | Content | Frequency |
|------|-------|---------|-----------|
| `engine.py` | 733-738 | Idle tick state | Every 30s |
| `engine.py` | 748-755 | Pending order state dump | **Every tick** with pending |
| `engine.py` | 832-861 | Price validation failures | Every invalid tick |
| `engine.py` | 889-940 | Coupled pricing branch logs | Every valid tick |
| `strategy.py` | 76-79 | Atomic skip reason | Every skip |
| `strategy.py` | 90-91 | Per-side at limit | Every skip |
| `client.py` | 387-390 | SDK placement detail | Every order |
| `client.py` | 522-525 | SDK signed order detail | Every order |

**Cause**: (a) Python f-string formatting evaluates eagerly even if log level filters
the message. (b) Each `logger.info()` acquires a lock and performs a system call.

**Fix**:
- Price validation / skip logs → `DEBUG` level
- Pending order state dump → throttle to every 10s
- SDK detail logs → `DEBUG` level
- For unavoidable INFO logs, use `%s` format (lazy evaluation)

---

### 4. Repeated tick size lookups every tick

**Location**: `engine.py:654-655`.

```python
up_tick = await self.sdk.get_tick_size(market.up_token_id)    # every tick
down_tick = await self.sdk.get_tick_size(market.down_token_id) # every tick
```

**Cause**: Tick size is a static property per token_id. The value never changes
during a window. Yet it's re-fetched (with `await` overhead) on every tick.

**Fix**: Cache at window start:
```python
# After market discovery:
up_tick = await self.sdk.get_tick_size(market.up_token_id)
down_tick = await self.sdk.get_tick_size(market.down_token_id)
# Use local variables in the tick loop.
```

---

### 5. `guaranteed_pnl` O(n) scan per access

**Location**: `models.py:214`.

```python
@property
def guaranteed_pnl(self) -> float:
    paired_cost = sum(l.price * l.paired_qty for l in self.lots)  # O(n)
    return self.guaranteed_pairs - paired_cost
```

**Accessed by**: kill-switch check (every tick), `WindowState.report()`, logging.

**Cause**: `lots` grows linearly with fills. 900 ticks × N fills per tick = large
list. `guaranteed_pnl` is called at least once per tick, each time scanning all lots.

**Fix**: Maintain a running `_paired_cost` accumulator in WindowState, increment on
each `_create_lot_and_pair()` call. Makes the property O(1).

---

### 6. Repeated pending_orders iteration per tick

**Location**: `strategy.py:50-57`, `69-73`, `131-135`. Plus `engine.py:748-755`.

**Cause**: `pending_up`/`pending_down` sums, atomic pre-check proximity, per-side
proximity, and state dump log each do a full `ws.pending_orders.values()` scan.

**Fix**: Maintain `_pending_up_count` / `_pending_down_count` accumulators in
WindowState, updated on order placed / filled / cancelled events.

---

### 7. Sequential order placement delay (REST API serialization)

**Location**: `executors.py` — `place_order` calls for Up and Down within the same tick.

**Cause**: Within a single tick, Up and Down orders are placed sequentially via
REST API calls. Each call takes ~0.5s, so the second order's price may be stale
by the time it reaches the exchange.

**Example**: In fast-moving markets, the 0.5s gap between Up and Down placement
means the second side's limit price no longer reflects current best-bid, leading
to worse fill probability or worse pair economics.

**Potential fix**: Use `asyncio.gather()` to place Up and Down concurrently within
the same tick. Or use batch order API if available.

---

## P2 — Medium Priority

### 8. Settlement blocks for up to 30 seconds

**Location**: `engine.py:1014-1019`.

```python
for attempt in range(10):
    winner = await self.sdk.get_resolved_winner(market.slug)
    if winner is not None:
        break
    await asyncio.sleep(3)  # 10 × 3s = 30s worst case
```

**Cause**: Flat 3s × 10 retries. Polymarket oracle typically resolves in 5-10s.
Exponential backoff would save ~15s in the common case.

**Fix**: `delays = [1, 2, 3, 5, 8]` (19s total, stops earlier for fast resolution).

---

### 9. `_resolve_pair_prices` is 147 lines

**Location**: `engine.py:810-956`.

**Cause**: Monolithic method mixes validation, settled detection, two coupled-pricing
branches, independent pricing, and logging. Hard to profile and optimize.

**Fix**: Extract `_resolve_coupled_prices()` and `_resolve_independent_prices()`.

---

## P3 — Low Priority

### 10. `_emit` runs handlers sequentially

**Location**: `engine.py:370-376`.

**Cause**: `for handler in handlers: await handler(data)` — slow handler blocks all
subsequent handlers.

**Fix**: `asyncio.gather()` with `return_exceptions=True` for independent handlers.

---

### 11. `_orphan_fills` prune on every unmatched fill

**Location**: `executors.py:439-442`.

**Cause**: Orphan list full scan on every unmatched fill (rare path, but still).

**Fix**: Prune on 60s timer or size threshold, not per-event.

---

### 12. `cancel_all` 3s hard wait

**Location**: `executors.py:338`.

**Cause**: `await asyncio.sleep(3)` for in-flight fills. Conservative.

**Fix**: Reduce to 1.5s.

---

### 13. `_active_token_ids` creates new set on every WS event

**Location**: `engine.py:439, 469-475`.

**Cause**: `set()` construction inside the WS event loop for every price event.

**Fix**: Cache the set, invalidate on market changes.

---

## Summary Table

| # | Issue | Impact | File:Lines |
|---|-------|--------|------------|
| 1 | Imbalance+rebalance+min_gap deadlock | Window stuck (zero orders) | strategy.py, engine.py |
| 2 | SQLite per-event commits | Disk I/O (fsync) per event | engine.py:254,273,280,289 |
| 3 | Blocking urllib in async | Event loop freeze ≤5s | client.py:262 |
| 4 | Hot-path INFO logging | CPU + I/O per tick | engine.py, strategy.py |
| 5 | Repeated tick size fetch | await overhead per tick | engine.py:654-655 |
| 6 | guaranteed_pnl O(n) scan | CPU grows with fills | models.py:214 |
| 7 | Pending order O(n) scans | CPU per tick | strategy.py |
| 8 | Sequential order placement | 0.5s stale price risk | executors.py |
| 9 | 30s settlement block | Window gap latency | engine.py:1014-1019 |
| 10 | 147-line pricing method | Maintainability | engine.py:810-956 |
| 11 | Sequential event handlers | Handler coupling | engine.py:370-376 |
| 12 | Orphan prune on hot path | Minor overhead | executors.py:439 |
| 13 | 3s cancel_all wait | Settlement delay | executors.py:338 |
| 14 | Set creation per WS event | Minor overhead | engine.py:469 |

---

## Log Validation (nohup.out — 2026-07-25~27, 14,212 lines, 7 engine sessions)

### Aggregate Metrics

| Metric | Count |
|--------|-------|
| Engine restarts | 7 |
| Unique windows traded | 10 |
| Orders submitted (ACCEPTED) | 176 |
| Fills tracked (✓ Fill) | 182 |
| UNMATCHED fills | 524 |
| Order rejections (post-only crossing) | 4 |
| Total `[place]` events | 193 |

### Tick Timing (last session, 53 ticks, 1 window)

| Metric | Value |
|--------|-------|
| Total ticks | 53 |
| Active ticks (decisions > 0) | 6 (11%) |
| Idle ticks (all skip) | 47 (89%) |
| Place phase P95 | 634ms |
| Compute phases P95 (price+decide+emit) | 0.6ms |
| Place / total wall-clock | 84% |
| Slowest tick | tick#1: 1,207ms (price=617ms + place=590ms) |

### Bottleneck Breakdown

1. **Place phase dominates** — The 6 active ticks average 563ms each, all spent in `submit_orders` POST API calls. Compute is essentially zero cost (0.6ms P95).

2. **tick#1 price phase = 617ms** — Confirms PERFORMANCE.md #5: first tick fetches `tick-size` ×2 + `neg-risk` ×2 (4 sequential HTTP calls) before pricing decisions can start. Subsequent ticks show 0.0ms price phase (cached).

3. **89% idle ticks** — Almost 9 out of 10 ticks produce zero decisions. Primary causes:
   - **min_gap atomic skip** (~551 occurrences): "price too close to pending" blocks both sides
   - **per-side at limit** (~2,400+ occurrences): "Up 20/20 at limit → skip" (max exposure reached early, then idle for minutes)
   - Combined: once `max_per_side=20` is reached (or a pending order sits at a close price), the engine has nothing to do for extended periods.

4. **524 UNMATCHED fills vs 182 tracked fills** — The engine receives fill events for orders from *previous* sessions (stale SDK state). Each engine restart re-subscribes to the user WS channel and receives all fills from the current window, even those not from this engine instance. Mostly noise, but wastes processing.

5. **Serial vs batch placement** — Earlier sessions placed Up/Down sequentially (~500ms gap between `[place] Up` and `[place] Down`). Newer sessions use `place_pair`/`submit_orders` which batch-submits both orders in a single POST, eliminating the gap.
