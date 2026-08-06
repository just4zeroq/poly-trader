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
    MarketPriceChangeEvent,
)

from .poly_client import SdkClient
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
    PriceLevel,
    TickEvent,
    WindowEnd,
    WindowStart,
    WindowState,
)
from .strategy import V4Strategy
from .predictor import BinanceFeed, Predictor

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
        self._event = asyncio.Event()
        # Update generation + last-consumed marker.  Bumped on every
        # update/wake so wait_update() can tell whether an update landed
        # while it was clearing/waiting and react immediately instead of
        # sleeping the full timeout (the lost-wakeup bug).
        self._generation = 0
        self._consumed = 0

    def update(self, token_id: str, snap: OrderBookSnapshot):
        self._data[token_id] = snap
        self._bump()

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

    def get(self, token_id: str) -> Optional[OrderBookSnapshot]:
        return self._data.get(token_id)

    def wake(self):
        """Wake up any task blocked in ``wait_update()`` immediately."""
        self._bump()

    def _bump(self):
        self._generation += 1
        self._event.set()

    async def wait_update(self, timeout: float = 5.0):
        """Wait for the next price update or *timeout* seconds, whichever first.

        The caller re-reads prices from ``get()`` after this returns.  The
        generation checks close the lost-wakeup race: an update that lands
        while the previous tick was still processing, or in the gap between
        ``clear()`` and the event ``wait()``, moves the generation — and this
        returns immediately instead of sleeping the full timeout.
        """
        gen = self._generation
        if gen != self._consumed:
            self._consumed = gen
            return  # update arrived while the previous tick was running
        self._event.clear()
        if self._generation != gen:
            self._consumed = self._generation
            return  # update landed in the clear→wait gap — don't sleep
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        self._consumed = self._generation


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
        self.feed = BinanceFeed()
        self.predictor = Predictor(cfg)
        self.strategy = V4Strategy(cfg, self.predictor)

        # Multi-market state: keyed by slug_pattern (e.g. "btc-updown-5m")
        self._windows: dict[str, WindowState] = {}   # slug → current window state
        self._markets: dict[str, MarketInfo] = {}     # slug → current market info
        self._window_counts: dict[str, int] = {}  # slug_pattern → window counter

        for spec in cfg.market_specs:
            self._window_counts[spec.slug_pattern] = 0

        # Global accumulators
        self._running = False

        # WS subscription state
        self._sub_handle = None
        self._sub_token_ids: set[str] = set()

        # Tick throttling
        self._last_tick_time: float = 0.0

        # In-flight order reconciliation (every positions_interval).
        # oid → consecutive polls the order was missing from the CLOB open
        # book.  A drop can be a real fill whose positions snapshot hasn't
        # caught up yet, so a ghost is only closed after it has been absent
        # for this many consecutive polls.  See _reconcile_orders.
        self._ghost_polls: dict[str, int] = {}
        self._ghost_polls_threshold = 2

        # Window-scoped feature-loader task.  A bare asyncio.create_task holds
        # only a weak ref in the loop; without this strong reference the task
        # can be garbage-collected mid-await ("Task was destroyed but it is
        # pending"), silently disabling favorites for that window.
        self._window_task: Optional[asyncio.Task] = None

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
            tg.create_task(self._position_loop(), name="position")
            tg.create_task(self._run_btc_price_cache(), name="btc-price")
        if self.feed is not None:
            await self.feed.close()

    def stop(self):
        self._running = False
        self.prices.wake()  # unblock tick loop immediately

    # ════════════════════════════════════════════════════════════
    # WebSocket
    # ════════════════════════════════════════════════════════════

    async def _run_ws(self):
        """Run SDK-based WS subscription — auto-restarts on token change or error."""
        while self._running:
            tokens = self._active_token_ids()
            if tokens and tokens == self._sub_token_ids and self._sub_handle:
                await self._consume_ws_events()
                continue  # event stream ended → re-check tokens at loop top

            # (Re)subscribe: close old handle, create new one
            await self._close_ws_handle()
            if not tokens:
                await asyncio.sleep(1)
                continue
            await self._subscribe_ws(tokens)

    async def _consume_ws_events(self):
        """Consume events from current subscription until tokens change or error."""
        try:
            async for event in self._sub_handle:
                if not self._running:
                    return
                if self._active_token_ids() != self._sub_token_ids:
                    break
                await self._dispatch_sdk_event(event)
        except Exception as e:
            logger.warning("WS subscription error: %s", e)
            self._sub_handle = None
            self._sub_token_ids = set()

    async def _close_ws_handle(self):
        """Close current WS subscription handle if any."""
        if self._sub_handle:
            await self._sub_handle.close()
            self._sub_handle = None
        self._sub_token_ids = set()

    async def _subscribe_ws(self, tokens: set[str]):
        """Subscribe to a new set of token IDs."""
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

    async def _run_btc_price_cache(self):
        """Background: keep the cached Binance BTC price fresh via WebSocket push.

        HTTP seed once, then stream over WS; on disconnect do one HTTP fallback
        and reconnect.  Never blocks the tick loop.
        """
        if self.feed is None or self.predictor is None:
            return
        # Initial HTTP seed
        try:
            self.predictor.set_btc(await self.feed.ticker_price())
        except Exception as e:
            logger.warning("[predict] initial BTC fetch failed: %s", e)
        while self._running:
            try:
                async for price in self.feed.stream_btc_price():
                    self.predictor.set_btc(price)
            except asyncio.CancelledError:
                raise  # shutdown — propagate cancellation, don't reconnect
            except Exception as e:
                logger.warning("[predict] BTC WS disconnected: %s", e)
                try:
                    self.predictor.set_btc(await self.feed.ticker_price())
                except Exception as e2:
                    logger.warning("[predict] BTC HTTP fallback failed: %s", e2)
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

            # Skip straight to next window — settlement is fast now
            continue

    async def _run_single_window(self, spec: MarketSpec):
        """Execute one full window for a spec: discover → trade → settle."""

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

        self._window_task = asyncio.create_task(self._load_window_features(market))

        # ── Restore current positions (engine restart mid-window) ──
        state = await self.sdk.load_current_state(market)
        if state:
            ws_state.inventory = state["inventory"]
            ws_state.auth_inv = dict(state["inventory"])
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
            if remaining <= 0:
                break
            # PriceCache 内部有个 asyncio.Event，WS 线程收到新 bid/ask 时 event.set() 唤醒它。没有新价格就等 timeout（5s）自动继续。
            #效果：tick 循环不会空转，有新价格立刻处理，没价格 5s 一轮保活。
            await self.prices.wait_update(timeout=5.0)

            # Throttle: minimum interval between ticks (default 1s)
            now = time.time()
            # 防止处理过快
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

            # ── Place orders ──
            tick_any_ok = False
            if decisions:
                self._log_pending_state(market.slug, ws_state)
                tick_any_ok = await self._place_decisions(market, ws_state, decisions)

            t_placed = time.time()
            self._last_tick_time = t_placed

            self._log_tick_timing(tick_count, t_wake, t_priced, t_decided,
                                  t_emitted, t_placed, decisions, ws_state)

            # ── Consecutive failure → idle (likely out of balance) ──
            # Only actual placement attempts move the counter.  A tick whose
            # decisions are pure cancel directives (re-pricing a stale
            # favorite) neither resets nor increments it: a failed cancel is
            # not a placement rejection, and a successful cancel is not
            # evidence of balance — counting either would skew the shutdown
            # decision toward a spurious idle.
            placed = [d for d in decisions if not d.cancel_prior]
            if placed:
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
        await self._settle_window(win_num, ws_state, market)

        # Cleanup this window's state
        self._windows.pop(market.slug, None)
        self._markets.pop(market.slug, None)

    async def _load_window_features(self, market: MarketInfo):
        """Fetch and cache window-start features (5m klines) for the predictor.

        Retries a few times: Binance may not have formed the window-start
        candle if we discovered the window immediately at open.
        """
        for attempt in range(5):
            try:
                feat = await self.feed.window_features(market.window_start)
                if feat is not None:
                    open_, prior15, prior1h, sigma5 = feat
                    self.predictor.set_window(
                        market.window_start, open_, prior15, prior1h, sigma5)
                    logger.info(
                        "[predict] window %d open=%.2f prior15=%+.4f%% "
                        "prior1h=%+.4f%% sigma5=%.3f%%",
                        market.window_start, open_, prior15, prior1h, sigma5)
                    return
            except Exception as e:
                logger.warning("[predict] feature fetch attempt %d failed: %s",
                               attempt + 1, e)
            await asyncio.sleep(5)
        logger.warning(
            "[predict] window %d features unavailable — favorite orders disabled this window",
            market.window_start)

    def _resolve_pair_prices(
        self, up_token_id: str, down_token_id: str, slug: str,
        up_tick: float = 0.01, down_tick: float = 0.01,
    ) -> tuple[Optional[float], Optional[float]]:
        """Resolve maker limit prices for both sides (independent, per-side).

        Each side uses its own book's maker price (best_bid + spread ×
        aggressiveness), rounded to tick.  No anchor/target derivation — the
        v4 strategy gates on the model's favorite and the observed fills, so
        each leg is priced to rest in its own book.
        """
        up_snap = self.prices.get(up_token_id)
        down_snap = self.prices.get(down_token_id)

        if not up_snap or not down_snap:
            logger.info("[%s] OrderBook snap missing → skip", slug)
            return None, None

        if not (self._snapshot_ok(up_snap, slug, "Up")
                and self._snapshot_ok(down_snap, slug, "Down")):
            return None, None

        up_price = self.sdk.round_to_tick(self._maker_price(up_snap), up_tick)
        down_price = self.sdk.round_to_tick(self._maker_price(down_snap), down_tick)

        # Safety: invalid prices → skip
        if up_price <= 0 or down_price <= 0:
            logger.info("[%s] Invalid prices Up=%.4f Down=%.4f → skip", slug,
                        up_price, down_price)
            return None, None

        logger.debug("[%s] Prices up=%.4f down=%.4f sum=%.4f",
                     slug, up_price, down_price, up_price + down_price)
        return up_price, down_price

    def _snapshot_ok(self, snap: OrderBookSnapshot, slug: str, side: str) -> bool:
        """Return True if the snapshot is fresh, has bid/ask, and isn't settled."""
        age = time.time() - snap.updated_at
        if age > 30:
            logger.info("[%s] %s data stale (%.1fs) → skip", slug, side, age)
            return False
        if not snap.best_bid or not snap.best_ask:
            logger.info("[%s] %s no bid/ask → skip", slug, side)
            return False
        if snap.best_bid > self.cfg.max_extreme_price:
            logger.info("[%s] %s best_bid %.4f > %.2f → settled, skip",
                        slug, side, snap.best_bid, self.cfg.max_extreme_price)
            return False
        return True

    def _maker_price(self, snap: OrderBookSnapshot) -> float:
        """Derive a maker limit price from an orderbook snapshot."""
        if not snap.best_bid or not snap.best_ask:
            return snap.best_bid or snap.best_ask or 0.0
        return round(snap.best_bid + snap.spread * self.cfg.aggressiveness, 4)

    async def _position_loop(self):
        """Background: poll CLOB positions every positions_interval and update auth_inv."""
        interval = self.cfg.positions_interval
        if interval <= 0:
            return
        while self._running:
            await asyncio.sleep(interval)
            if not self._windows:
                continue
            market = None
            ws = None
            for slug, _ws in self._windows.items():
                ws = _ws
                market = self._markets.get(slug)
                break
            if ws is None or market is None:
                continue
            try:
                await self._poll_positions(ws, market)
                await self._reconcile_orders(ws, market)
            except Exception as e:
                # This task runs as a TaskGroup child — an uncaught exception
                # would cancel every engine task.  A malformed SDK field or a
                # transient API shape change must not kill the bot.
                logger.warning("  [reconcile] position/order poll error: %s", e)

    async def _poll_positions(self, ws: WindowState, market: MarketInfo) -> bool:
        """One CLOB position poll; reconcile auth_inv + inventory upward.

        Polymarket's data-api is eventually consistent — a poll can return a
        stale (lower/empty) snapshot that predates fills already written into
        auth_inv.  auth_inv and inventory therefore only ever move UP toward
        the CLOB numbers; a lagging poll can add knowledge, never erase it.
        (Within a window the strategy only buys, so the true position is
        monotonic non-decreasing and this is exact.)

        When CLOB shows more than the WS recorded (a dropped fill event), the
        delta is absorbed into the side's active pending order at its limit
        price so inventory / pending / auth_inv converge — this clears the
        phantom-pending anti-stack block and makes the log match the page.
        """
        seq_before = self.executor.fill_seq
        try:
            positions = await self.sdk.get_positions(
                market.condition_id, market.up_token_id, market.down_token_id)
        except Exception as e:
            logger.warning("  [position] poll failed: %s", e)
            return False
        # A fill written through while this snapshot was in flight predates
        # it — skip the pending-order attribution (which would double-count)
        # but still let auth_inv/inventory move up toward CLOB truth.
        fill_landed = self.executor.fill_seq != seq_before

        changed = False
        for side in ("Up", "Down"):
            known = ws.inventory[side]
            if positions[side] <= known:
                continue
            # A fill the WS never reported: CLOB caught up beyond our record.
            missing = positions[side] - known
            if not fill_landed:
                # Attribute it to the side's active pending order(s).
                for po in list(ws.pending_orders.values()):
                    if po.side != side or po.cancelled_at != 0 or po.remaining <= 0:
                        continue
                    take = min(po.remaining, missing)
                    if take <= 0:
                        continue
                    ws.inventory[side] += take
                    ws.cost[side] += take * po.price
                    ws.total_spent += take * po.price
                    ws.trades += 1
                    po.filled += take
                    self.executor._create_lot(ws, po, take, po.price)
                    missing -= take
                    changed = True
                    # Reconcile-attributed fills must reach the bound hedge
                    # plan too, or a dropped WS fill stalls the hedge forever.
                    plan = ws.hedge_plan
                    if plan is not None and plan.order_id == po.order_id:
                        plan.filled = po.filled
                    if missing <= 0:
                        break
            if missing > 0:
                ws.inventory[side] += missing
                changed = True
            if positions[side] > ws.auth_inv[side]:
                ws.auth_inv[side] = positions[side]

        if changed:
            logger.info("  [position] pos=%s ws=%s", positions, ws.inventory)
        return changed

    async def _reconcile_orders(self, ws: WindowState, market: MarketInfo) -> bool:
        """Reconcile in-flight orders (pending_orders) against the CLOB open book.

        The CLOB is the authority on which orders are actually live.  Scope is
        the current window only — late fills / orders from other windows are
        left alone (cross-window is handled elsewhere).

        PURGE direction — orders we track that CLOB no longer lists:
          * A filled phantom (remaining <= 0) is just cosmetically stale;
            drop it from the dict immediately.
          * An unfilled order absent from CLOB is a ghost — cancelled out from
            under us (lost cancel response, manual cancel elsewhere).  Left in
            pending it blocks ``_has_active_pending`` → anti-stack → favorites
            for the entire window.  It is closed only after it has been
            missing for ``_ghost_polls_threshold`` consecutive polls: a real
            fill leaves the open book an instant before its position lands in
            the (eventually-consistent) positions API, and that window lets
            ``_poll_positions`` absorb the fill into the order instead.

        Returns True if pending_orders changed.  A query failure (None) is a
        no-op — never treat an API error as "no orders".
        """
        orders = await self.sdk.get_open_orders_for_market(market.condition_id)
        if orders is None:
            return False

        up_tid = market.up_token_id
        down_tid = market.down_token_id
        live: set[str] = set()
        for order in orders:
            tid = str(order.token_id)
            if tid not in (up_tid, down_tid):
                continue  # not our tokens
            # A fully-filled order can no longer move the position (a
            # 5-contract maker fill reports size_matched=4.992 → rounds to 5).
            # Same skip as load_current_state — no order archaeology.
            if int(round(order.size_matched)) >= int(order.original_size):
                continue
            live.add(order.id)

        changed = False
        for oid, po in list(ws.pending_orders.items()):
            if po.cancelled_at != 0 or oid in live:
                self._ghost_polls.pop(oid, None)  # closed or still live — not a ghost
                continue
            if po.remaining <= 0:
                del ws.pending_orders[oid]  # filled phantom — cosmetic cleanup
                self._ghost_polls.pop(oid, None)
                logger.info(
                    "  [reconcile] %s (filled %d/%d) gone from CLOB → purged",
                    oid[:12], po.filled, po.amount)
                changed = True
                continue
            # Missing from CLOB but not filled on our books.  First observation
            # may be a fill whose position hasn't landed yet — hold for
            # consecutive polls before declaring it a ghost.
            n = self._ghost_polls.get(oid, 0) + 1
            if n < self._ghost_polls_threshold:
                self._ghost_polls[oid] = n
                continue
            self._ghost_polls.pop(oid, None)
            po.cancelled_at = time.time()
            logger.warning(
                "  [reconcile] ghost %s (%s %d @ %.4f) absent from CLOB %d polls → closed",
                oid[:12], po.side, po.remaining, po.price, n)
            changed = True

        # Drop counters for orders that left pending by another path (a fill
        # that purged the entry, or settlement clearing the window).
        for oid in list(self._ghost_polls):
            if oid not in ws.pending_orders:
                del self._ghost_polls[oid]

        return changed


    # ── Order execution ──

    async def _place_order(self, slug: str, token_id: str, outcome: str,
                           price: float, amount: int,
                           is_favorite: bool = False) -> bool:
        oid = await self.executor.place(
            slug, token_id, outcome, price, amount, is_favorite=is_favorite,
        )
        return oid is not None

    async def _place_decisions(self, market: MarketInfo, ws: WindowState,
                               decisions: list[Decision]) -> bool:
        """Place a batch of single-leg orders (one per side).

        Returns True if any ORDER was placed.  Cancel directives (re-pricing
        a stale favorite) are handled here but never count toward the return:
        the caller uses it to reset/increment the consecutive-placement-
        failure counter, and a cancel — success or failure — is not evidence
        about the account balance.  The caller additionally ignores that
        counter on cancel-only ticks.
        """
        any_ok = False
        for d in decisions:
            if d.cancel_prior:
                # Re-price directive: cancel a stale pending favorite; the
                # next tick places fresh at the updated price.
                await self.executor.cancel(d.cancel_prior)
                continue
            token_id = market.up_token_id if d.side == "Up" else market.down_token_id
            ok = await self._place_order(
                market.slug, token_id, d.side, d.price, d.amount,
                is_favorite=d.creates_hedge_plan,
            )
            if ok:
                any_ok = True
        return any_ok


    @staticmethod
    def _log_tick_timing(tick_count: int, t_wake: float, t_priced: float,
                         t_decided: float, t_emitted: float, t_placed: float,
                         decisions: list[Decision], ws: WindowState):
        """Log compact single-line tick timing summary."""
        dt_price  = (t_priced   - t_wake)    * 1000
        dt_decide = (t_decided  - t_priced)   * 1000
        dt_emit   = (t_emitted  - t_decided)  * 1000
        dt_place  = (t_placed   - t_emitted)  * 1000
        dt_total  = (t_placed   - t_wake)     * 1000
        logger.info(
            "  ⏱ tick#%d total=%.1fms (price=%.1fms decide=%.1fms emit=%.1fms place=%.1fms) "
            "decisions=%d pending=%d inv=U%d/D%d",
            tick_count, dt_total, dt_price, dt_decide, dt_emit, dt_place,
            len(decisions), len(ws.pending_orders),
            ws.inventory["Up"], ws.inventory["Down"],
        )

    @staticmethod
    def _log_pending_state(slug: str, ws: WindowState):
        """Log active pending orders grouped by side."""
        for side in ("Up", "Down"):
            active = [po for po in ws.pending_orders.values()
                      if po.side == side and po.cancelled_at == 0]
            if active:
                prices = {po.order_id[:8]: f"{po.price:.4f}" for po in active}
                logger.info("  [%s] Pending %s: %d orders  prices=%s  inv=%d",
                            slug, side, len(active), prices, ws.inventory[side])

    # ── Settlement ──

    async def _settle_window(self, win_num: int, ws: WindowState,
                             market: MarketInfo):
        """Cancel remaining orders and emit window end (no winner resolution).

        Settlement of the completed window is done by the separate
        tools/onchain/settle_window.py script at the next window start.
        """
        logger.info("  [%s] Window ended → settling…", market.slug)

        await self.executor.cancel_all(ws.pending_orders)
        ws.pending_orders.clear()
        ws.hedge_plan = None  # the window's bound hedge dies with the window

        rpt = ws.report(winner=None)
        await self._emit("window_end", WindowEnd(
            window_num=win_num, slug=market.slug,
            report=rpt, cum_pnl=0.0,
        ))

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
