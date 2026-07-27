"""
Message-driven trading engine — multi-market concurrent edition.

Runs one independent trading loop per MarketSpec (e.g. BTC-5m, BTC-15m,
SOL-15m, ETH-15m), each with its own window lifecycle and state, sharing
the WebSocket connection and price cache.

All lifecycle events (order placed/filled, tick, window start/end) are
emitted as typed dataclass events.  Subscribe with ``engine.on()``:

    async def on_fill(ev: OrderFilled):
        print(ev)

    engine.on("order_filled", on_fill)

Built-in subscribers:
  - LogSubscriber  → formatted logging (always active)
  - SqliteRecorder → SQLite trade log (live mode)
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from .config import Config
from polymarket.models.clob.market_events import (
    MarketBestBidAskEvent,
    MarketLastTradePriceEvent,
    MarketPriceChangeEvent,
)

from .client import SdkClient
from .executors import LiveExecutor, OrderExecutor
from .models import (
    Decision,
    MarketInfo,
    MarketSpec,
    OrderBookSnapshot,
    OrderCancelled,
    OrderFailed,
    OrderFilled,
    OrderPlaced,
    PendingOrder,
    PriceLevel,
    TickEvent,
    WindowEnd,
    WindowStart,
    WindowState,
)
from .strategy import MakerStrategy

logger = logging.getLogger(__name__)


# ============================================================
# Price Cache (async-safe, event-driven)
# ============================================================


class PriceCache:
    """Holds latest L2 orderbook snapshot + last trade price per token.

    Updated by WebSocket price events.  The tick loop awaits
    ``wait_update()`` to be notified of new prices.
    """

    def __init__(self):
        self._data: dict[str, OrderBookSnapshot] = {}
        self._last_trade: dict[str, float] = {}  # token_id → last trade price
        self._event = asyncio.Event()

    def update(self, token_id: str, snap: OrderBookSnapshot):
        self._data[token_id] = snap
        self._event.set()

    def update_from_ws(self, token_id: str, best_bid: float, best_ask: float,
                       bid_size: float = 0.0, ask_size: float = 0.0):
        self.update(
            token_id,
            OrderBookSnapshot(
                token_id=token_id,
                bids=[PriceLevel(price=best_bid, size=bid_size)],
                asks=[PriceLevel(price=best_ask, size=ask_size)],
                updated_at=time.time(),
            ),
        )

    def update_trade(self, token_id: str, price: float):
        self._last_trade[token_id] = price

    def get(self, token_id: str) -> Optional[OrderBookSnapshot]:
        return self._data.get(token_id)

    def get_last_trade(self, token_id: str) -> Optional[float]:
        return self._last_trade.get(token_id)

    def wake(self):
        """Wake up any task blocked in ``wait_update()`` immediately."""
        self._event.set()

    async def wait_update(self, timeout: float = 5.0):
        """Wait for next price update or *timeout* seconds, whichever comes first.

        The caller should re-read prices from ``get()`` after this returns.
        """
        self._event.clear()
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except asyncio.TimeoutError:
            pass


# ============================================================
# Event Subscribers
# ============================================================


class LogSubscriber:
    """Default subscriber — prints structured events to the log."""

    def __init__(self, engine: TradingEngine):
        engine.on("order_placed", self._on_placed)
        engine.on("order_filled", self._on_filled)
        engine.on("order_cancelled", self._on_cancelled)
        engine.on("order_failed", self._on_failed)
        engine.on("tick", self._on_tick)
        engine.on("window_start", self._on_win_start)
        engine.on("window_end", self._on_win_end)

    async def _on_placed(self, ev: OrderPlaced):
        if ev.order_id:
            logger.info("    [LIVE] Order %s %d @ %.4f  ID=%s…", ev.outcome, ev.amount, ev.price, ev.order_id[:16])
        else:
            logger.info("    [LIVE] Buy %s %d @ %.4f  — not filled", ev.outcome, ev.amount, ev.price)

    async def _on_filled(self, ev: OrderFilled):
        logger.info("      ✓ Fill %s %d @ %.4f  (Up=%d Down=%d)",
                    ev.outcome, ev.amount, ev.price,
                    ev.total_inv_up, ev.total_inv_down)

    async def _on_cancelled(self, ev: OrderCancelled):
        logger.info("      ✗ Cancel %s %d @ %.4f", ev.outcome, ev.amount, ev.price)

    async def _on_failed(self, ev: OrderFailed):
        logger.warning("    [LIVE]  Order %s %d @ %.4f  FAILED  (%s)",
                       ev.outcome, ev.amount, ev.price, ev.reason)

    async def _on_tick(self, ev: TickEvent):
        logger.info("  [%s][%4ds] Up=%.4f  Down=%.4f  sum=%.4f  → Up=%d Down=%d [%s]",
                    ev.slug, ev.elapsed, ev.up_price, ev.down_price, ev.price_sum,
                    ev.up_buy, ev.down_buy, ev.roles)

    async def _on_win_start(self, ev: WindowStart):
        logger.info("")
        logger.info("═" * 58)
        logger.info("Window #%d: %s", ev.window_num, ev.slug)
        logger.info("  Up token:   %s…", ev.up_token_id[:20])
        logger.info("  Down token: %s…", ev.down_token_id[:20])
        logger.info("═" * 58)

    async def _on_win_end(self, ev: WindowEnd):
        r = ev.report
        logger.info("  ─" * 18)
        logger.info("  Window #%d  %s", ev.window_num, ev.slug)
        logger.info("    Up:     %d @ %.4f", r["inv_up"], r["avg_up"])
        logger.info("    Down:   %d @ %.4f", r["inv_down"], r["avg_down"])
        logger.info("    Pairs:  %d   Imbalance: %d   Guaranteed PnL: $%+.2f",
                    r["pairs"], r["imbalance"], r["guaranteed_pnl"])
        logger.info("    Spent:  $%.2f", r["total_spent"])
        if r["winner"]:
            logger.info("    Winner: %s → payout $%d",
                        r["winner"], r["inv_up" if r["winner"] == "Up" else "inv_down"])
            logger.info("    P&L:    $%+.2f", r["pnl"])
        logger.info("    Cum:    $%+.2f", ev.cum_pnl)
        logger.info("  ─" * 18)


class SqliteRecorder:
    """Persists orders and fills to SQLite.

    Schema::

        orders  — id, window_num, outcome, side, price, amount, status, created_at
        fills   — id, window_num, order_id, outcome, price, amount, filled_at

    Subscribes to: order_placed, order_filled, order_cancelled, order_failed.
    """

    def __init__(self, engine: TradingEngine, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()
        engine.on("order_placed", self._on_placed)
        engine.on("order_filled", self._on_filled)
        engine.on("order_cancelled", self._on_cancelled)
        engine.on("order_failed", self._on_failed)

    def close(self):
        self._conn.commit()
        self._conn.close()

    # ── schema ──

    def _init_schema(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                window_num  INTEGER NOT NULL,
                outcome     TEXT    NOT NULL,
                side        TEXT    NOT NULL,
                price       REAL    NOT NULL,
                amount      INTEGER NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'placed',
                created_at  TEXT    NOT NULL,
                order_id    TEXT    NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS fills (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                window_num  INTEGER NOT NULL,
                order_id    TEXT    NOT NULL DEFAULT '',
                outcome     TEXT    NOT NULL,
                price       REAL    NOT NULL,
                amount      INTEGER NOT NULL,
                filled_at   TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_orders_window  ON orders(window_num);
            CREATE INDEX IF NOT EXISTS idx_fills_window   ON fills(window_num);
        """)
        self._conn.commit()
        # Migrate older DBs that lack order_id column
        try:
            self._conn.execute("ALTER TABLE orders ADD COLUMN order_id TEXT NOT NULL DEFAULT ''")
            self._conn.commit()
        except Exception:
            pass

    # ── handlers ──

    async def _on_placed(self, ev: OrderPlaced):
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO orders (window_num, outcome, side, price, amount, status, created_at, order_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ev.window_num, ev.outcome, ev.side, ev.price, ev.amount, "placed", now, ev.order_id),
        )
        self._conn.commit()

    async def _on_filled(self, ev: OrderFilled):
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO fills (window_num, order_id, outcome, price, amount, filled_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (ev.window_num, ev.order_id, ev.outcome, ev.price, ev.amount, now),
        )
        # Precise update: only mark the specific filled order
        row = self._conn.execute(
            "SELECT amount FROM orders WHERE order_id=?", (ev.order_id,)
        ).fetchone()
        if row:
            new_status = 'filled' if ev.total_filled >= row[0] else 'partial'
            self._conn.execute(
                "UPDATE orders SET status=? WHERE order_id=?",
                (new_status, ev.order_id),
            )
        self._conn.commit()

    async def _on_cancelled(self, ev: OrderCancelled):
        self._conn.execute(
            "UPDATE orders SET status='cancelled' WHERE order_id=? AND status IN ('placed','partial')",
            (ev.order_id,),
        )
        self._conn.commit()

    async def _on_failed(self, ev: OrderFailed):
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO orders (window_num, outcome, side, price, amount, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ev.window_num, ev.outcome, "BUY", ev.price, ev.amount, "failed", now),
        )
        self._conn.commit()


# ============================================================
# Trading Engine
# ============================================================


class TradingEngine:
    """Orchestrates WS connection and concurrent market window loops.

    Each MarketSpec gets its own async task that independently:
      1. Aligns to its window schedule (5m, 15m, etc.)
      2. Discovers the current market via Gamma API
      3. Subscribes to WS price data for its tokens
      4. Runs a tick loop: wait for price → strategy.decide() → place orders
      5. Settles at window end

    Events (subscribe via ``.on()``):
        order_placed    → OrderPlaced
        order_filled    → OrderFilled
        order_cancelled → OrderCancelled
        order_failed    → OrderFailed
        tick            → TickEvent
        window_start    → WindowStart
        window_end      → WindowEnd

    Usage::

        engine = TradingEngine(cfg, LiveExecutor())
        engine.on("order_filled", my_handler)
        await engine.run()
    """

    def __init__(self, cfg: Config, executor: OrderExecutor | None = None):
        self.cfg = cfg
        self.executor = executor or LiveExecutor()
        self.executor.engine = self
        self.sdk = SdkClient(cfg)
        self.prices = PriceCache()
        self.strategy = MakerStrategy(cfg)

        # Multi-market state: keyed by slug_pattern (e.g. "btc-updown-5m")
        self._windows: dict[str, WindowState] = {}   # slug → current window state
        self._markets: dict[str, MarketInfo] = {}     # slug → current market info
        self._window_counts: dict[str, int] = {}  # slug_pattern → window counter

        for spec in cfg.market_specs:
            self._window_counts[spec.slug_pattern] = 0

        # Global accumulators
        self._running = False
        self._cum_pnl = 0.0
        self._last_window_loss = False

        # WS subscription state
        self._sub_handle = None
        self._sub_token_ids: set[str] = set()

        # Tick throttling
        self._last_tick_time: float = 0.0

        # Event system
        self._event_handlers: dict[str, list[Callable]] = {}

        # Attach default subscribers
        self._log = LogSubscriber(self)
        self._sqlite: Optional[SqliteRecorder] = None

    def on(self, event_type: str, handler: Callable):
        """Subscribe to an engine event.

        ``handler`` is an async callable taking a single dataclass argument.
        """
        self._event_handlers.setdefault(event_type, []).append(handler)

    def off(self, event_type: str, handler: Callable):
        self._event_handlers[event_type] = [
            h for h in self._event_handlers.get(event_type, []) if h is not handler
        ]

    async def _emit(self, event_type: str, data: Any):
        """Fire an event to all registered subscribers."""
        for handler in self._event_handlers.get(event_type, []):
            try:
                await handler(data)
            except Exception as e:
                logger.exception("Event handler %s(%s): %s", event_type, type(data).__name__, e)

    # ════════════════════════════════════════════════════════════
    # Public API
    # ════════════════════════════════════════════════════════════

    async def run(self):
        """Start the engine — runs WS listener + concurrent market loops."""
        self._running = True
        if self.cfg.market_specs:
            spec = self.cfg.market_specs[0]
            logger.info("Engine started (market=%s)", spec.slug_pattern)
        else:
            logger.info("Engine started (no markets)")

        # Create secure client for live trading
        await self.sdk.create_secure(self.cfg)

        # Cancel any lingering orders from a previous instance
        if self.sdk.is_secure:
            await self.sdk.cancel_all_open_orders()

        db_path = f"poly_trader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        self._sqlite = SqliteRecorder(self, db_path)
        logger.info("SQLite log: %s", db_path)

        if not self.cfg.market_specs:
            logger.error("No market specs configured — nothing to trade")
            return

        spec = self.cfg.market_specs[0]
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._run_ws(), name="ws")
            tg.create_task(
                self._run_market_loop(spec),
                name=f"market-{spec.slug_pattern}",
            )
            tg.create_task(self._run_user_ws(), name="user-ws")

    def stop(self):
        self._running = False
        self.prices.wake()  # unblock tick loop immediately

    # ════════════════════════════════════════════════════════════
    # WebSocket
    # ════════════════════════════════════════════════════════════

    async def _run_ws(self):
        """Run SDK-based WS subscription — auto-restarts when tokens change.

        Collects all active market token IDs, subscribes once, and
        dispatches typed SDK events to the PriceCache.
        """
        while self._running:
            tokens = self._active_token_ids()
            if tokens == self._sub_token_ids and self._sub_handle:
                # Subscription is current — wait for events
                handle = self._sub_handle
                try:
                    async for event in handle:
                        if not self._running:
                            return
                        # Re-check tokens on each event
                        if self._active_token_ids() != self._sub_token_ids:
                            break
                        await self._dispatch_sdk_event(event)
                except Exception as e:
                    logger.warning("WS subscription error: %s", e)
                    # Clear broken handle so reconnect path is taken
                    self._sub_handle = None
                    self._sub_token_ids = set()
                finally:
                    continue

            # Tokens changed or no subscription — (re)subscribe
            if self._sub_handle:
                await self._sub_handle.close()
                self._sub_handle = None
            self._sub_token_ids = set()

            if not tokens:
                await asyncio.sleep(1)
                continue

            try:
                handle = await self.sdk.subscribe(list(tokens))
                self._sub_handle = handle
                self._sub_token_ids = tokens
                logger.info("WS subscribed to %d tokens", len(tokens))
            except Exception as e:
                logger.warning("WS subscription failed: %s, retrying…", e)
                await asyncio.sleep(3)

    def _active_token_ids(self) -> set[str]:
        """Collect all token IDs from currently active markets."""
        ids: set[str] = set()
        for market in self._markets.values():
            ids.add(market.up_token_id)
            ids.add(market.down_token_id)
        return ids

    async def _dispatch_sdk_event(self, event):
        """Dispatch a typed SDK event to the PriceCache."""
        if isinstance(event, MarketBestBidAskEvent):
            p = event.payload
            if p.best_bid is not None and p.best_ask is not None:
                self.prices.update_from_ws(
                    p.token_id,
                    float(p.best_bid),
                    float(p.best_ask),
                )
        elif isinstance(event, MarketPriceChangeEvent):
            # PriceChange has side-level prices with best_bid/best_ask
            for pc in event.payload.price_changes:
                if pc.best_bid is not None and pc.best_ask is not None:
                    self.prices.update_from_ws(
                        pc.token_id,
                        float(pc.best_bid),
                        float(pc.best_ask),
                    )
        elif isinstance(event, MarketLastTradePriceEvent):
            p = event.payload
            price_f = float(p.price)
            self.prices.update_trade(p.token_id, price_f)

    async def _run_user_ws(self):
        """Subscribe to authenticated user events for fill tracking (live only).

        Runs in parallel with the public market WS. Reconnects on error.
        """
        if not self.sdk.is_secure:
            logger.warning("Secure client not available — user WS disabled")
            return
        logger.info("Starting user WS (fill tracking)")
        while self._running:
            try:
                handle = await self.sdk.subscribe_user()
                async for event in handle:
                    if not self._running:
                        return
                    await self.executor.handle_user_event(event)
            except Exception as e:
                if self._running:
                    logger.warning("User WS error: %s, reconnecting…", e)
                    await asyncio.sleep(self.cfg.ws_reconnect_delay)

    # ════════════════════════════════════════════════════════════
    # Per-market window lifecycle
    # ════════════════════════════════════════════════════════════

    async def _run_market_loop(self, spec: MarketSpec):
        """Run continuous trading for one market spec (e.g. BTC-5m).

        Aligns to the spec's window schedule, discovers the market,
        executes one window at a time, then loops to the next.
        """
        duration = spec.duration_min * 60
        logger.info("[%s] Starting market loop (duration=%ds)", spec.slug_pattern, duration)

        while self._running:
            try:
                await self._run_single_window(spec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("[%s] Window error: %s", spec.slug_pattern, e)
                await asyncio.sleep(5)

            # Wait for the next window in this spec's schedule
            await self._wait_for_next_window(spec)

    async def _wait_for_next_window(self, spec: MarketSpec):
        """Sleep until the next window start for this spec.

        Uses a minimum-remaining check to avoid joining windows
        that are too far progressed.
        """
        duration = spec.duration_min * 60
        while self._running:
            now = time.time()
            ws_ts = (int(now) // duration) * duration
            elapsed = now - ws_ts
            remaining = duration - elapsed

            # Minimum remaining: at least min_remaining_time + 60s so the
            # cheap-seeker has enough runway to accumulate before the cut-off.
            min_remain = max(90, self.cfg.min_remaining_time + 60)

            if remaining >= min_remain:
                # Window has enough time left — join it
                if elapsed < 1.0:
                    await asyncio.sleep(ws_ts + 1.0 - now)
                return

            # Too little time — skip to next window boundary
            next_start = ws_ts + duration + 1.0
            wait = next_start - now
            if wait > 0:
                await asyncio.sleep(wait)

    async def _run_single_window(self, spec: MarketSpec):
        """Execute one full window for a spec: discover → trade → settle."""
        duration = spec.duration_min * 60

        # ── risk checks before entering window ──
        if self.cfg.stop_on_window_loss and self._last_window_loss:
            logger.info("[%s] Skipping window due to previous loss", spec.slug_pattern)
            self._last_window_loss = False
            return

        if self._cum_pnl <= self.cfg.max_drawdown:
            logger.warning("[%s] Cum PnL %.2f <= %.2f → stopping",
                           spec.slug_pattern, self._cum_pnl, self.cfg.max_drawdown)
            self._running = False
            return

        # ── market discovery ──
        market = await self.sdk.find_market_for_spec(spec)
        if not market:
            logger.error("[%s] No market found, retrying in 10s…", spec.slug_pattern)
            await asyncio.sleep(10)
            return

        # ── init window state ──
        self._window_counts[spec.slug_pattern] += 1
        win_num = self._window_counts[spec.slug_pattern]
        ws_state = WindowState(
            slug=market.slug,
            start_time=time.time(),
            window_num=win_num,
        )
        self._windows[market.slug] = ws_state
        self._markets[market.slug] = market
        window_end = market.window_end

        await self._emit("window_start", WindowStart(
            window_num=win_num,
            slug=market.slug,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
        ))

        # ── Restore current positions (engine restart mid-window) ──
        state = await self.sdk.load_current_state(market)
        if state:
            ws_state.inventory = state["inventory"]
            ws_state.cost["Up"] = state["avg_cost"]["Up"] * state["inventory"]["Up"]
            ws_state.cost["Down"] = state["avg_cost"]["Down"] * state["inventory"]["Down"]
            ws_state.total_spent = ws_state.cost["Up"] + ws_state.cost["Down"]
            ws_state.pending_orders = state["pending"]
            ws_state.trades = sum(state["inventory"].values())
            logger.info(
                "  [restore] Loaded positions: Up=%d@%.4f Down=%d@%.4f "
                "pending=%d balance=%d",
                state["inventory"]["Up"], state["avg_cost"]["Up"],
                state["inventory"]["Down"], state["avg_cost"]["Down"],
                len(state["pending"]), state["balance"],
            )

        # ── tick loop ──
        consecutive_failures = 0
        idle_log_ts = 0.0
        no_price_log_ts = 0.0
        tick_count = 0
        while self._running:
            now = time.time()
            remaining = window_end - now
            if remaining <= self.cfg.settle_buffer:
                break

            await self.prices.wait_update(timeout=5.0)

            # Throttle: minimum interval between ticks (default 1s)
            now = time.time()
            if now - self._last_tick_time < self.cfg.min_tick_interval:
                continue

            t_wake = now
            tick_count += 1

            # Round to tick size (before price validation)
            up_tick = await self.sdk.get_tick_size(market.up_token_id)
            down_tick = await self.sdk.get_tick_size(market.down_token_id)

            # ── Normal pricing ──
            up_price, down_price = self._resolve_pair_prices(
                market.up_token_id, market.down_token_id, market.slug,
                up_tick, down_tick,
                ws_state=ws_state,
            )
            t_priced = time.time()

            if not up_price and not down_price:
                if time.time() - no_price_log_ts > 30:
                    up_snap = self.prices.get(market.up_token_id)
                    down_snap = self.prices.get(market.down_token_id)
                    logger.info(
                        "  [%s] No valid prices — up=%s down=%s  up_age=%ds down_age=%ds",
                        market.slug,
                        f"{up_snap.best_bid:.4f}/{up_snap.best_ask:.4f}" if up_snap else "N/A",
                        f"{down_snap.best_bid:.4f}/{down_snap.best_ask:.4f}" if down_snap else "N/A",
                        time.time() - (up_snap.updated_at if up_snap else 0),
                        time.time() - (down_snap.updated_at if down_snap else 0),
                    )
                    no_price_log_ts = time.time()
                continue

            # Cancel-replace: reprice stale pending orders
            await self.executor.flush_cancelled()
            await self._cancel_stale_pending(ws_state, up_price, down_price)

            decisions = self.strategy.decide(
                ws_state, up_price, down_price,
                remaining_time=remaining,
            )
            t_decided = time.time()

            # Summarise decisions for tick event
            up_buy = sum(d.amount for d in decisions if d.side == "Up")
            down_buy = sum(d.amount for d in decisions if d.side == "Down")
            sides = "+".join(d.side for d in decisions) if decisions else "idle"

            # Only emit tick events for active ticks (reduce idle log spam)
            if decisions:
                await self._emit("tick", TickEvent(
                    window_num=win_num,
                    slug=market.slug,
                    elapsed=int(now - ws_state.start_time),
                    up_price=up_price,
                    down_price=down_price,
                    price_sum=round(up_price + down_price, 4),
                    up_buy=up_buy,
                    down_buy=down_buy,
                    roles=sides,
                ))
                idle_log_ts = now  # reset idle timer on activity

            # Idle log — throttled to every 30s
            if not decisions and now - idle_log_ts > 30:
                logger.info(
                    "  [%s] Idle — Up=%.4f Down=%.4f sum=%.4f  inv=(U=%d/D=%d) pending=%d  remaining=%ds",
                    market.slug, up_price, down_price, up_price + down_price,
                    ws_state.inventory["Up"], ws_state.inventory["Down"],
                    len(ws_state.pending_orders), int(remaining),
                )
                idle_log_ts = now

            t_emitted = time.time()

            # Mark tick completion time for throttle (idle path — no orders placed)
            if not decisions:
                self._last_tick_time = time.time()

            # ── Place orders — batch pair when both sides ──
            if decisions:
                # Log pending order state for anomaly detection
                for side in ("Up", "Down"):
                    side_pending = [po for po in ws_state.pending_orders.values()
                                    if po.side == side and po.cancelled_at == 0]
                    if side_pending:
                        prices = {po.order_id[:8]: f"{po.price:.4f}" for po in side_pending}
                        logger.info("  [%s] Pending %s: %d orders  prices=%s  inv=%d",
                                    market.slug, side, len(side_pending), prices,
                                    ws_state.inventory[side])

            # ── Place orders, track success/failure ──
            tick_any_ok = False
            if len(decisions) == 2:
                # Both sides → sign together, submit together
                up_d = next(d for d in decisions if d.side == "Up")
                down_d = next(d for d in decisions if d.side == "Down")
                up_ok, down_ok = await self.executor.place_pair(
                    market.slug,
                    market.up_token_id, market.down_token_id,
                    up_d.price, up_d.amount,
                    down_d.price, down_d.amount,
                )
                tick_any_ok = up_ok or down_ok
            else:
                # Single side → use existing individual placement
                for d in decisions:
                    token_id = market.up_token_id if d.side == "Up" else market.down_token_id
                    ok = await self._place_order(market.slug, token_id, d.side, d.price, d.amount, pair_id=d.pair_id)
                    if ok:
                        tick_any_ok = True

            t_placed = time.time()

            # Mark tick completion for throttle (busy path — orders placed)
            if decisions:
                self._last_tick_time = time.time()

            # ── Tick timing summary (every tick, compact single-line) ──
            dt_price  = (t_priced   - t_wake)    * 1000  # pricing phase (tick_size + resolve)
            dt_decide = (t_decided  - t_priced)   * 1000  # cancel-replace + strategy
            dt_emit   = (t_emitted  - t_decided)  * 1000  # event emit / idle log
            dt_place  = (t_placed   - t_emitted)  * 1000  # order placement
            dt_total  = (t_placed   - t_wake)     * 1000  # total tick wall-clock
            logger.info(
                "  ⏱ tick#%d total=%.1fms (price=%.1fms decide=%.1fms emit=%.1fms place=%.1fms) "
                "decisions=%d pending=%d inv=U%d/D%d",
                tick_count, dt_total, dt_price, dt_decide, dt_emit, dt_place,
                len(decisions), len(ws_state.pending_orders),
                ws_state.inventory["Up"], ws_state.inventory["Down"],
            )

            # ── Consecutive failure → idle (likely out of balance) ──
            if decisions:
                if tick_any_ok:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= self.cfg.max_consecutive_failures:
                        logger.warning(
                            "[%s] %d consecutive ticks ALL orders rejected "
                            "(likely out of balance) → idle until settlement",
                            market.slug, consecutive_failures,
                        )
                        await asyncio.sleep(max(0.0, window_end - time.time() - 1))
                        break

        # ── settlement ──
        pnl = await self._settle_window(win_num, ws_state, market)
        self._last_window_loss = pnl is not None and pnl < 0

        # Cleanup this window's state
        self._windows.pop(market.slug, None)
        self._markets.pop(market.slug, None)

    def _resolve_pair_prices(
        self, up_token_id: str, down_token_id: str, slug: str,
        up_tick: float = 0.01, down_tick: float = 0.01,
        ws_state: Optional[WindowState] = None,
    ) -> tuple[Optional[float], Optional[float]]:
        """Cross-validate Up/Down prices using WS orderbook data only.

        Rounds to tick size before validation, so returned prices are
        ready to use for order placement.

        Skips the tick if:
          - No fresh price data (WS data > 30s old)
          - Up + Down deviates from 1.0 beyond max_price_dev

        Returns (up_price, down_price) or (None, None) to skip.
        """
        now = time.time()
        up_snap = self.prices.get(up_token_id)
        down_snap = self.prices.get(down_token_id)

        # Must have fresh data for both sides
        if not up_snap or not down_snap:
            logger.info("[%s] OrderBook snap missing → skip  (up=%s down=%s)",
                        slug, bool(up_snap), bool(down_snap))
            return None, None

        up_age = now - up_snap.updated_at
        down_age = now - down_snap.updated_at
        if up_age > 30 or down_age > 30:
            logger.info("[%s] Price data stale → skip  (up_age=%.1fs down_age=%.1fs)",
                        slug, up_age, down_age)
            return None, None

        # Must have both bid and ask
        if not up_snap.best_bid or not up_snap.best_ask:
            logger.info("[%s] Up no bid/ask (%.4f/%.4f) → skip",
                        slug, up_snap.best_bid or 0, up_snap.best_ask or 0)
            return None, None
        if not down_snap.best_bid or not down_snap.best_ask:
            logger.info("[%s] Down no bid/ask (%.4f/%.4f) → skip",
                        slug, down_snap.best_bid or 0, down_snap.best_ask or 0)
            return None, None

        # Skip if either side is already settled (best_bid > threshold)
        if up_snap.best_bid > self.cfg.max_extreme_price:
            logger.info("[%s] Up best_bid %.4f > %.2f → market settled, skip",
                        slug, up_snap.best_bid, self.cfg.max_extreme_price)
            return None, None
        if down_snap.best_bid > self.cfg.max_extreme_price:
            logger.info("[%s] Down best_bid %.4f > %.2f → market settled, skip",
                        slug, down_snap.best_bid, self.cfg.max_extreme_price)
            return None, None

            # ── Standard independent pricing ──
            up_price = self._maker_price(up_snap)
            down_price = self._maker_price(down_snap)
            up_price = self.sdk.round_to_tick(up_price, up_tick)
            down_price = self.sdk.round_to_tick(down_price, down_tick)

            # Cross-validate: Up + Down ≤ 1.0 — skip if sum exceeds 1.0 (guaranteed loss)
            total = up_price + down_price
            if total > 1.0:
                logger.info("[%s] Price sum %.4f > 1.0 → skip (independent pricing)",
                            slug, total)
                return None, None

        return up_price, down_price

    def _maker_price(self, snap: OrderBookSnapshot) -> float:
        """Derive a maker limit price from an orderbook snapshot."""
        if not snap.best_bid or not snap.best_ask:
            return snap.best_bid or snap.best_ask or 0.0
        return round(snap.best_bid + snap.spread * self.cfg.aggressiveness, 4)

    async def _cancel_stale_pending(self, ws: WindowState, up_price: float,
                                     down_price: float):
        """Cancel pending orders whose price has moved beyond the threshold.

        Compares each pending order's price to the current maker reference
        price computed by _maker_price.  Skips orders younger than cancel_min_age.
        """
        threshold = self.cfg.cancel_replace_threshold
        min_age = self.cfg.cancel_min_age
        now = time.time()
        for oid in list(ws.pending_orders):
            po = ws.pending_orders[oid]
            if po.cancelled_at > 0:
                continue
            if now - po.placed_at < min_age:
                continue
            current_price = up_price if po.side == "Up" else down_price
            deviation = abs(po.price - current_price) / max(po.price, 0.001)
            if deviation > threshold:
                logger.info(
                    "  [cancel-replace] %s order=%s… old=%.4f new=%.4f "
                    "dev=%.1f%% thresh=%.0f%% age=%.0fs",
                    po.side, oid[:12], po.price, current_price,
                    deviation * 100, threshold * 100,
                    now - po.placed_at,
                )
                await self.executor.cancel(oid)

    # ── Order execution ──

    async def _place_order(self, slug: str, token_id: str, outcome: str,
                           price: float, amount: int,
                           pair_id: str = "") -> bool:
        oid = await self.executor.place(
            slug, token_id, outcome, price, amount,
            pair_id=pair_id,
        )
        return oid is not None

    # ── Settlement ──

    async def _settle_window(self, win_num: int, ws: WindowState, market: MarketInfo) -> Optional[float]:
        """Settle and return PnL (None if winner unknown)."""
        logger.info("  [%s] Window ended → settling…", market.slug)

        # Cancel remaining pending orders
        await self.executor.cancel_all(ws.pending_orders)
        ws.pending_orders.clear()

        winner = None

        # Tier 1: Gamma API — retry longer for oracle to resolve
        for attempt in range(10):
            winner = await self.sdk.get_resolved_winner(market.slug)
            if winner is not None:
                logger.info("  [%s] API resolved winner: %s (attempt %d)", market.slug, winner, attempt + 1)
                break
            await asyncio.sleep(3)

        # Tier 2: Last trade price from Gamma Data API
        if winner is None:
            logger.warning("  [%s] API not resolved, querying last trade prices…", market.slug)
            try:
                import httpx
                url = "https://data-api.polymarket.com/trades"
                params = {"market": market.condition_id, "limit": 100}
                async with httpx.AsyncClient() as client:
                    resp = await client.get(url, params=params, timeout=10)
                    if resp.status_code == 200:
                        trades = resp.json()
                        if trades:
                            # Use last trade of each outcome
                            last_up = 0.0
                            last_down = 0.0
                            for t in trades:
                                ts = int(t.get("timestamp", 0))
                                price = float(t.get("price", 0))
                                if t.get("asset", "") == market.up_token_id or t.get("outcome") == "Up":
                                    last_up = price
                                elif t.get("asset", "") == market.down_token_id or t.get("outcome") == "Down":
                                    last_down = price
                            if last_up >= 0.99 and last_down <= 0.01:
                                winner = "Up"
                                logger.info("  [%s] Trades resolved: Up=%.4f Down=%.4f → %s",
                                            market.slug, last_up, last_down, winner)
                            elif last_down >= 0.99 and last_up <= 0.01:
                                winner = "Down"
                                logger.info("  [%s] Trades resolved: Up=%.4f Down=%.4f → %s",
                                            market.slug, last_up, last_down, winner)
                            else:
                                logger.info("  [%s] Trades ambiguous: Up=%.4f Down=%.4f → fallback",
                                            market.slug, last_up, last_down)
            except Exception as e:
                logger.warning("  [%s] Trades API error: %s", market.slug, e)

        # Tier 3: WS midpoint as last resort
        if winner is None:
            logger.warning("  [%s] Using WS midpoint as last resort", market.slug)
            up_snap = self.prices.get(market.up_token_id)
            down_snap = self.prices.get(market.down_token_id)
            up_mid = up_snap.mid_price if up_snap else None
            down_mid = down_snap.mid_price if down_snap else None
            if up_mid and down_mid:
                if up_mid > 0.75:
                    winner = "Up"
                elif up_mid < 0.25:
                    winner = "Down"
                elif up_mid > 0.65 and down_mid < 0.35:
                    winner = "Up"
                elif down_mid > 0.65 and up_mid < 0.35:
                    winner = "Down"

        rpt = ws.report(winner=winner)
        pnl = rpt.get("pnl")  # None if no winner
        self._cum_pnl += pnl or 0.0

        await self._emit("window_end", WindowEnd(
            window_num=win_num,
            slug=market.slug,
            report=rpt,
            cum_pnl=round(self._cum_pnl, 2),
        ))

        return pnl

    # ════════════════════════════════════════════════════════════
    # Info
    # ════════════════════════════════════════════════════════════

    async def show_info(self):
        """Print current market info for all configured specs."""
        for spec in self.cfg.market_specs:
            market = await self.sdk.find_market_for_spec(spec)
            if not market:
                logger.info("%s: not found", spec)
                continue
            logger.info("Market: %s", market.slug)
            for outcome, info in market.tokens.items():
                mid = await self.sdk.get_midpoint(info["token_id"])
                logger.info("  %s: token=%s…  midpoint=%s",
                            outcome, info["token_id"][:20], mid)
