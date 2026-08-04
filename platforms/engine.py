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
    Pair,
    PendingOrder,
    PriceLevel,
    TickEvent,
    WindowEnd,
    WindowStart,
    WindowState,
)
from .strategy import MakerStrategy
from .predictive import PredictiveMakerStrategy
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
        self.feed: Optional[BinanceFeed] = None
        self.predictor: Optional[Predictor] = None
        if cfg.predictive_enabled:
            self.feed = BinanceFeed()
            self.predictor = Predictor(cfg)
            self.strategy = PredictiveMakerStrategy(cfg, self.predictor)
        else:
            self.strategy = MakerStrategy(cfg)

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
        # Trade history API rate-limit: at most once per 60s
        self._last_trade_history_check: float = 0.0

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
            tg.create_task(self._reconcile_loop(), name="reconcile")
            if self.cfg.predictive_enabled and self.feed is not None:
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

            if remaining >= min_remain and elapsed < 60:
                # Window has enough time left and we're near the start — join it
                if elapsed < 1.0:
                    await asyncio.sleep(ws_ts + 1.0 - now)
                return

            # Too little time — skip to next window boundary
            next_start = ws_ts + duration + 1.0
            wait = next_start - now
            if wait > 0:
                await asyncio.sleep(wait)
            else:
                await asyncio.sleep(0.1)  # overslept past window boundary

    async def _run_single_window(self, spec: MarketSpec):
        """Execute one full window for a spec: discover → trade → settle."""
        # duration = spec.duration_min * 60

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

        if self.cfg.predictive_enabled and self.feed is not None:
            asyncio.create_task(self._load_window_features(market))

        # Cancel lingering orders from a previous instance (scoped to this market)
        # if self.sdk.is_secure and market.condition_id:
        #     await self.sdk.cancel_all_open_orders(market=market.condition_id)

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
            # Reconcile gate — blocks only when reconciliation is actively
            # applying corrections (rare).  Otherwise passes through instantly.
            await ws_state.reconcile_gate.wait()

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
            # flush_cancelled() is disabled — reconciliation handles cleanup
            # instead of blind time-based deletion, so cancelled orders stay
            # visible for CLOB verification before removal.
            # await self.executor.flush_cancelled()
            # 取消过期订单
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

            # ── Place orders ──
            if decisions:
                self._log_pending_state(market.slug, ws_state)

            step3_new = [d for d in decisions if not d.pair_id]
            step2_repair = [d for d in decisions if d.pair_id]
            tick_any_ok = False

            if step3_new:
                tick_any_ok = await self._place_step3_pair(
                    market, ws_state, step3_new,
                )

            if step2_repair:
                any_ok = await self._place_step2_repair(
                    market, ws_state, step2_repair,
                )
                tick_any_ok = tick_any_ok or any_ok

            t_placed = time.time()
            self._last_tick_time = t_placed

            self._log_tick_timing(tick_count, t_wake, t_priced, t_decided,
                                  t_emitted, t_placed, decisions, ws_state)

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
        ws_state: Optional[WindowState] = None,
    ) -> tuple[Optional[float], Optional[float]]:
        """Resolve pair prices with anchor+target logic.

        On each tick:
          1. Compute raw maker prices for both sides
          2. Check price range: any side at extreme → use extreme target,
             else use normal target
          3. Choose anchor side: normal range → anchor higher price (aggressive
             derived side); extreme → anchor lower price (safer)
          4. Anchor side uses maker price, derive the other as target - anchor
          5. Round both to tick size

        This guarantees total cost ≈ target, and the derived side naturally
        lands near the opposite book's ask (aggressive, high fill probability).
        """
        up_snap = self.prices.get(up_token_id)
        down_snap = self.prices.get(down_token_id)

        if not up_snap or not down_snap:
            logger.info("[%s] OrderBook snap missing → skip", slug)
            return None, None

        if not (self._snapshot_ok(up_snap, slug, "Up")
                and self._snapshot_ok(down_snap, slug, "Down")):
            return None, None

        cfg = self.cfg
        up_maker = self._maker_price(up_snap)
        down_maker = self._maker_price(down_snap)

        # Determine price range mode
        tl, th = cfg.pair_cost_threshold_low, cfg.pair_cost_threshold_high
        up_extreme = up_maker <= tl or up_maker >= th
        down_extreme = down_maker <= tl or down_maker >= th

        if up_extreme or down_extreme:
            target = cfg.pair_cost_target_extreme
            # Extreme → anchor lower price (more conservative)
            anchor_up = up_maker <= down_maker
        else:
            target = cfg.pair_cost_target
            # Normal → anchor higher price (derived side is aggressive)
            anchor_up = up_maker >= down_maker

        # Anchor side uses maker price, derive the other as target - anchor
        if anchor_up:
            up_price = self.sdk.round_to_tick(up_maker, up_tick)
            down_raw = target - up_price
            down_price = self.sdk.round_to_tick(down_raw, down_tick)
        else:
            down_price = self.sdk.round_to_tick(down_maker, down_tick)
            up_raw = target - down_price
            up_price = self.sdk.round_to_tick(up_raw, up_tick)

        # Safety: invalid prices → skip
        if up_price <= 0 or down_price <= 0:
            logger.info("[%s] Invalid prices Up=%.4f Down=%.4f → skip", slug,
                        up_price, down_price)
            return None, None

        logger.debug(
            "[%s] Prices up=%.4f down=%.4f sum=%.4f target=%.2f anchor=%s",
            slug, up_price, down_price, up_price + down_price, target,
            "Up" if anchor_up else "Down",
        )
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

    async def _cancel_stale_pending(self, ws: WindowState, up_price: float,
                                     down_price: float):
        """Cancel stale 0-fill pending orders from fresh Pairs.

        Processed by Pair, not by order:
          1. Single-leg: cancel the pending order if age >= cancel_min_age.
          2. Two-leg (both pending): cancel BOTH orders if
             age >= cancel_min_age AND price_diff >= cancel_replace_threshold.

        Removes the Pair from ws.pairs and frees accumulate on cancel.
        """
        min_age = self.cfg.cancel_min_age
        price_dev = self.cfg.cancel_replace_threshold
        now = time.time()

        for pair in list(ws.pairs):
            # Determine active pending orders on this pair
            up_po = ws.pending_orders.get(pair.up_order_id) if pair.up_order_id else None
            down_po = ws.pending_orders.get(pair.down_order_id) if pair.down_order_id else None

            has_up = up_po is not None and up_po.cancelled_at == 0
            has_down = down_po is not None and down_po.cancelled_at == 0

            if not has_up and not has_down:
                continue  # dead pair — no active orders

            # Only fresh pairs (no fills)
            if pair.up_filled != 0 or pair.down_filled != 0:
                continue
            if (has_up and up_po.filled > 0) or (has_down and down_po.filled > 0):
                continue

            is_two_leg = has_up and has_down

            if is_two_leg:
                # Two-leg: require BOTH age >= min_age AND price moved significantly
                max_age = max(now - up_po.placed_at, now - down_po.placed_at)
                max_diff = max(
                    abs(up_po.price - up_price),
                    abs(down_po.price - down_price),
                )
                if not (max_age >= min_age and max_diff >= price_dev):
                    continue

                logger.info(
                    "  [cancel-2leg] Up=%s… Down=%s… age=%.0fs diff=%.4f  "
                    "pair=%s accumulate=%d",
                    pair.up_order_id[:12], pair.down_order_id[:12],
                    max_age, max_diff, pair.pair_id, ws.accumulate,
                )
                batch_cancelled = await self.executor.cancel_batch(
                    [pair.up_order_id, pair.down_order_id],
                )
                if batch_cancelled == 0:
                    logger.warning(
                        "  [cancel-2leg] batch cancel failed entirely  pair=%s",
                        pair.pair_id,
                    )
                    continue
                if batch_cancelled < 2:
                    logger.warning(
                        "  [cancel-2leg] batch cancel: %d/2 succeeded  pair=%s",
                        batch_cancelled, pair.pair_id,
                    )
                self._clear_pair_order(pair)  # both sides
                ws.pairs.remove(pair)
                ws.accumulate = max(0, ws.accumulate - pair.qty)
            else:
                # Single-leg: cancel on age
                side, po = ("Up", up_po) if has_up else ("Down", down_po)
                age = now - po.placed_at
                if age < min_age:
                    continue

                logger.info(
                    "  [cancel-1leg] %s order=%s… age=%.0fs  "
                    "pair=%s accumulate=%d",
                    side, po.order_id[:12], age, pair.pair_id, ws.accumulate,
                )
                ok, _ = await self.executor.cancel(po.order_id)
                if not ok:
                    logger.warning(
                        "  [cancel-1leg] cancel failed  order=%s…  pair=%s",
                        po.order_id[:12], pair.pair_id,
                    )
                    continue
                self._clear_pair_order(pair, side)
                ws.pairs.remove(pair)
                ws.accumulate = max(0, ws.accumulate - pair.qty)

    # ── State reconciliation ──

    async def _reconcile_loop(self):
        """Periodic background task: compare local state with CLOB REST API.

        Runs every 30 s independently of the tick loop.  Only pauses the
        tick loop (via ``reconcile_gate``) when it finds a real discrepancy
        that needs a correction — and even then only for the brief window
        while the correction is applied.
        """
        interval = self.cfg.reconcile_interval
        if interval <= 0:
            return

        while self._running:
            await asyncio.sleep(interval)

            # ── Find active window ──
            if not self._windows:
                continue
            # Take the first (and typically only) active window
            market = None
            ws = None
            for slug, _ws in self._windows.items():
                ws = _ws
                market = self._markets.get(slug)
                break
            if ws is None or market is None:
                continue

            cid = market.condition_id
            up_tid = market.up_token_id
            down_tid = market.down_token_id
            now = time.time()

            # 1. Quiet-period check: skip if there was recent activity
            if now - ws.last_activity < 5.0:
                continue

            logger.debug(
                "  [reconcile] Fetching CLOB state  pending=%d inv=(U=%d/D=%d)",
                len(ws.pending_orders), ws.inventory["Up"], ws.inventory["Down"],
            )

            # 2. Fetch CLOB ground truth (async — tick loop keeps running)
            fetch_start = time.time()
            try:
                clob_open_ids = await self.sdk.get_open_order_ids(cid)
                clob_positions = await self.sdk.get_positions(cid, up_tid, down_tid)
            except Exception as e:
                logger.warning("  [reconcile] Fetch failed: %s", e)
                continue

            # 3. Snapshot local state (read-only)
            local_snapshot: dict[str, tuple] = {}  # oid → (filled, cancelled_at, side, price, amount, placed_at)
            for oid, po in ws.pending_orders.items():
                local_snapshot[oid] = (po.filled, po.cancelled_at, po.side, po.price, po.amount, po.placed_at)

            # 4. Diff: find orders that need investigation
            #    (in local but not in CLOB open orders, and not placed during fetch)
            investigate: list[str] = []
            for oid, (filled, cancelled_at, side, price, amount, placed_at) in local_snapshot.items():
                if placed_at > fetch_start:
                    continue  # was placed during fetch → not in clob_open_ids is expected
                if cancelled_at > 0 and now - cancelled_at < 3.0:
                    continue  # recently cancelled, give CLOB time
                if oid not in clob_open_ids:
                    investigate.append(oid)

            # 5. Early exit: no suspect orders and no inventory drift
            inventory_drift = {}
            for side, tid in [("Up", up_tid), ("Down", down_tid)]:
                clob_inv = clob_positions.get(side, 0)
                if clob_inv > ws.inventory[side]:
                    inventory_drift[side] = (clob_inv, tid)

            # 5b. Trade history cross-reference (at most once per 60s):
            #     catches fills that both User WS *and* CLOB positions API miss.
            if not investigate and not inventory_drift:
                now_ts = time.time()
                if now_ts - self._last_trade_history_check > 60.0:
                    self._last_trade_history_check = now_ts
                    trade_history = await self.sdk.get_trade_history(
                        cid, self.cfg.wallet_address, ws.start_time,
                    )
                    if trade_history:
                        trade_qty = {"Up": 0, "Down": 0}
                        for t in trade_history:
                            asset = t.get("asset", "")
                            try:
                                sz = int(t.get("size", 0))
                            except (ValueError, TypeError):
                                sz = 0
                            if asset == up_tid:
                                trade_qty["Up"] += sz
                            elif asset == down_tid:
                                trade_qty["Down"] += sz

                        for side, tid in [("Up", up_tid), ("Down", down_tid)]:
                            if trade_qty[side] > ws.inventory[side]:
                                inventory_drift[side] = (trade_qty[side], tid)
                                logger.warning(
                                    "  [reconcile] Trade history %s: local=%d trades=%d",
                                    side, ws.inventory[side], trade_qty[side],
                                )

            if not investigate and not inventory_drift:
                logger.debug(
                    "  [reconcile] All clear  pending=%d inv=(U=%d/D=%d) → no diffs",
                    len(ws.pending_orders), ws.inventory["Up"], ws.inventory["Down"],
                )
                continue

            logger.info(
                "  [reconcile] Diffs found  suspect_orders=%d drift=%s  → investigating",
                len(investigate), list(inventory_drift.keys()) or "none",
            )

            # 6. Query individual order status for suspect orders
            filled_on_clob: dict[str, int | None] = {}
            for oid in investigate:
                try:
                    filled_on_clob[oid] = await self.sdk.get_order_filled(oid)
                except Exception:
                    filled_on_clob[oid] = None

            # 7. Second confirmation: any activity during the fetch?
            if ws.last_activity > fetch_start:
                logger.info(
                    "  [reconcile] Activity during fetch → discarding  "
                    "(last_activity %.1fs after fetch_start)",
                    ws.last_activity - fetch_start,
                )
                continue

            # 8. Build correction list
            corrections: list[dict] = []
            for oid in investigate:
                if oid not in local_snapshot:
                    continue
                local_filled, cancelled_at, side, price, amount, placed_at = local_snapshot[oid]
                clob_filled = filled_on_clob.get(oid)

                if cancelled_at > 0:
                    if oid in clob_open_ids:
                        corrections.append({"action": "revive", "oid": oid, "side": side})
                    elif clob_filled is not None and clob_filled > local_filled:
                        corrections.append({"action": "post_cancel_fill", "oid": oid,
                                            "side": side, "missing": clob_filled - local_filled,
                                            "price": price})
                    else:
                        corrections.append({"action": "remove", "oid": oid})
                else:
                    if clob_filled is None:
                        continue
                    if clob_filled > local_filled:
                        corrections.append({"action": "missing_fill", "oid": oid,
                                            "side": side, "missing": clob_filled - local_filled,
                                            "price": price, "clob_filled": clob_filled})
                    elif clob_filled == local_filled:
                        corrections.append({"action": "ghost", "oid": oid,
                                            "side": side, "remaining": amount - local_filled,
                                            "price": price})

            if not corrections and not inventory_drift:
                logger.debug(
                    "  [reconcile] Investigated %d orders → no real diffs",
                    len(investigate),
                )
                continue

            logger.info(
                "  [reconcile] Applying corrections  actions=%d drift=%s  → pausing tick",
                len(corrections), list(inventory_drift.keys()) or "none",
            )

            # 9. Pause tick loop, apply corrections
            ws.reconcile_gate.clear()
            await asyncio.sleep(0)  # yield so tick loop can hit the gate

            self.executor.start_reconcile()
            try:
                applied = 0
                now = time.time()

                for c in corrections:
                    oid = c["oid"]
                    po = ws.pending_orders.get(oid)
                    if po is None:
                        continue

                    action = c["action"]

                    if action == "revive":
                        if po.cancelled_at > 0:
                            logger.warning(
                                "  [reconcile] CANCEL FAILED  order=%s… → reviving",
                                oid[:12],
                            )
                            po.cancelled_at = 0
                            applied += 1

                    elif action == "post_cancel_fill":
                        if po.cancelled_at > 0:
                            missing = min(c["missing"], po.remaining)
                            if missing > 0:
                                logger.warning(
                                    "  [reconcile] POST-CANCEL FILL  order=%s…  "
                                    "side=%s missing=%d",
                                    oid[:12], c["side"], missing,
                                )
                                self.executor._process_fill(ws, po, missing, c["price"])
                                await self._emit("order_filled", OrderFilled(
                                    window_num=ws.window_num,
                                    outcome=c["side"], price=c["price"], amount=missing,
                                    order_id=oid,
                                    total_filled=po.filled,
                                    total_inv_up=ws.inventory["Up"],
                                    total_inv_down=ws.inventory["Down"],
                                ))
                                applied += 1
                        if oid in ws.pending_orders:
                            del ws.pending_orders[oid]
                            applied += 1

                    elif action == "remove":
                        if po.cancelled_at > 0:
                            del ws.pending_orders[oid]
                            applied += 1

                    elif action == "missing_fill":
                        if po.cancelled_at == 0:
                            actual_missing = min(c["missing"], po.remaining)
                            if actual_missing > 0:
                                logger.warning(
                                    "  [reconcile] MISSING FILL  order=%s…  "
                                    "side=%s local=%d clob=%d missing=%d",
                                    oid[:12], c["side"], po.filled,
                                    c["clob_filled"], actual_missing,
                                )
                                self.executor._process_fill(ws, po, actual_missing, c["price"])
                                await self._emit("order_filled", OrderFilled(
                                    window_num=ws.window_num,
                                    outcome=c["side"], price=c["price"],
                                    amount=actual_missing, order_id=oid,
                                    total_filled=po.filled,
                                    total_inv_up=ws.inventory["Up"],
                                    total_inv_down=ws.inventory["Down"],
                                ))
                                applied += 1
                            # Fully filled → remove
                            if po.remaining <= 0 and oid in ws.pending_orders:
                                del ws.pending_orders[oid]
                                applied += 1
                            elif po.cancelled_at == 0:
                                # Still not resolved → ghost
                                logger.warning(
                                    "  [reconcile] GHOST ORDER  order=%s… → cancel",
                                    oid[:12],
                                )
                                po.cancelled_at = now
                                await self._emit("order_cancelled", OrderCancelled(
                                    window_num=ws.window_num,
                                    outcome=c["side"], amount=po.remaining,
                                    price=c["price"], order_id=oid,
                                ))
                                applied += 1

                    elif action == "ghost":
                        if po.cancelled_at == 0:
                            logger.warning(
                                "  [reconcile] GHOST ORDER  order=%s… → cancel",
                                oid[:12],
                            )
                            po.cancelled_at = now
                            await self._emit("order_cancelled", OrderCancelled(
                                window_num=ws.window_num,
                                outcome=c["side"], amount=po.remaining,
                                price=c["price"], order_id=oid,
                            ))
                            applied += 1

                # Inventory drift
                for side, tid in [("Up", up_tid), ("Down", down_tid)]:
                    if side not in inventory_drift:
                        continue
                    clob_inv, _ = inventory_drift[side]
                    local_inv = ws.inventory[side]
                    if clob_inv > local_inv:
                        delta = clob_inv - local_inv
                        logger.warning(
                            "  [reconcile] Inventory drift %s: local=%d clob=%d → +%d",
                            side, local_inv, clob_inv, delta,
                        )
                        snap = self.prices.get(tid)
                        est_price = snap.mid_price if snap and snap.mid_price else 0.5
                        ws.inventory[side] = clob_inv
                        ws.cost[side] += delta * est_price
                        ws.total_spent += delta * est_price
                        ws.trades += delta
                        applied += 1

                if applied:
                    logger.info(
                        "  [reconcile] %d corrections applied  "
                        "pending=%d inv=(U=%d/D=%d)",
                        applied, len(ws.pending_orders),
                        ws.inventory["Up"], ws.inventory["Down"],
                    )

            finally:
                await self.executor.finish_reconcile()
                ws.reconcile_gate.set()

    @staticmethod
    def _clear_pair_order(pair: Pair, side: str = "") -> None:
        """Clear order_id(s) on *pair*.  Clears both sides if *side* is empty."""
        if side:
            if side == "Up":
                pair.up_order_id = ""
            else:
                pair.down_order_id = ""
        else:
            pair.up_order_id = ""
            pair.down_order_id = ""

    # ── Order execution ──

    async def _place_order(self, slug: str, token_id: str, outcome: str,
                           price: float, amount: int,
                           pair_id: str = "") -> bool:
        oid = await self.executor.place(
            slug, token_id, outcome, price, amount,
            pair_id=pair_id,
        )
        return oid is not None

    # ── Tick helpers ──

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

    async def _place_step3_pair(self, market: MarketInfo, ws: WindowState,
                                 step3: list[Decision]) -> bool:
        """Place a new Up+Down pair — sign both, submit together. Returns True if any side OK."""
        up_d = next((d for d in step3 if d.side == "Up"), None)
        down_d = next((d for d in step3 if d.side == "Down"), None)

        # Predictive single-leg favorite: only one side present
        if up_d is None or down_d is None:
            return await self._place_step3_single(market, ws, up_d or down_d)

        pair = Pair(
            pair_id=f"pair_{ws.window_num}_{len(ws.pairs)}",
            up_price=up_d.price, down_price=down_d.price, qty=up_d.amount,
        )
        ws.pairs.append(pair)
        up_d.pair_id = pair.pair_id
        down_d.pair_id = pair.pair_id

        up_ok, down_ok = await self.executor.place_pair(
            market.slug, market.up_token_id, market.down_token_id,
            up_d.price, up_d.amount, down_d.price, down_d.amount,
            pair_id=up_d.pair_id,
        )

        if up_ok or down_ok:
            ws.accumulate += up_d.amount
        logger.info("  [engine] step3 accumulate=%d  (%s/%s)  pair=%s",
                    ws.accumulate,
                    "Up" if up_ok else "-", "Down" if down_ok else "-",
                    pair.pair_id)
        return up_ok or down_ok

    async def _place_step3_single(self, market: MarketInfo, ws: WindowState,
                                  d: Decision) -> bool:
        """Place a single-leg order (predictive favorite) — creates a single-leg Pair.

        The missing leg's price is left 0.0; the strategy pairs it later via
        step-2 repair once the favorite fills.
        """
        pair = Pair(
            pair_id=f"pair_{ws.window_num}_{len(ws.pairs)}",
            up_price=d.price if d.side == "Up" else 0.0,
            down_price=d.price if d.side == "Down" else 0.0,
            qty=d.amount,
        )
        ws.pairs.append(pair)
        d.pair_id = pair.pair_id

        token_id = market.up_token_id if d.side == "Up" else market.down_token_id
        ok = await self._place_order(
            market.slug, token_id, d.side, d.price, d.amount,
            pair_id=pair.pair_id,
        )
        if ok:
            ws.accumulate += d.amount
        logger.info("  [engine] step3 single %s=%d@%.4f %s pair=%s",
                    d.side, d.amount, d.price,
                    "OK" if ok else "FAIL", pair.pair_id)
        return ok

    async def _handle_cancel_replace(self, d: Decision, ws: WindowState):
        """Cancel the old blocking order and clear pair state (no dissolve)."""
        old_po = ws.pending_orders.get(d.cancel_order_id)
        if not old_po or old_po.cancelled_at > 0:
            return
        await self.sdk.cancel_order(d.cancel_order_id)
        ws.last_activity = time.time()
        old_po.cancelled_at = time.time()
        # Clear pair's order_id so new order can link
        for p in ws.pairs:
            if p.pair_id == d.pair_id:
                self._clear_pair_order(p, d.side)
                break
        del ws.pending_orders[d.cancel_order_id]
        logger.info("  [engine] cancel-replace %s… → new %s order",
                    d.cancel_order_id[:12], d.side)

    async def _place_step2_repair(self, market: MarketInfo, ws: WindowState,
                                   repairs: list[Decision]) -> bool:
        """Place individual repair orders. Returns True if any placed successfully."""
        any_ok = False
        for d in repairs:
            if d.cancel_order_id:
                await self._handle_cancel_replace(d, ws)
            token_id = market.up_token_id if d.side == "Up" else market.down_token_id
            ok = await self._place_order(
                market.slug, token_id, d.side, d.price, d.amount,
                pair_id=d.pair_id,
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

    # ── Settlement ──

    async def _settle_window(self, win_num: int, ws: WindowState,
                             market: MarketInfo):
        """Cancel remaining orders and emit window end (no winner resolution)."""
        logger.info("  [%s] Window ended → settling…", market.slug)

        await self.executor.cancel_all(ws.pending_orders)
        ws.pending_orders.clear()

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
