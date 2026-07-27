"""
Tail-end sweep strategy: in the last 3 minutes, if one side's best_bid
is between 0.85 and 0.95, buy the winner first, wait for it to fill,
then buy the loser. Repeats in rounds until the window ends.

Profit is dynamic:
  - winner best_bid 0.85-0.90 → 4¢ per pair
  - winner best_bid 0.90-0.95 → 2¢ per pair

Usage::

    # As library
    sweeper = TailSweepStrategy(cfg)
    up_p, down_p, decisions = sweeper.decide(
        ws_state, up_snap, down_snap,
        remaining_time=120, up_tick=0.01, down_tick=0.01,
    )

    # Standalone
    python -m poly_trader.tail_sweep --market btc-updown-15m
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from typing import Optional

from polymarket.models.clob.market_events import MarketBestBidAskEvent
from polymarket.models.clob.user_events import UserTradeEvent

from .config import Config
from .models import Decision, OrderBookSnapshot, PendingOrder, WindowState

logger = logging.getLogger(__name__)


class TailSweepStrategy:
    """Sweep the tail: buy the winner and pair the loser with locked profit.

    Activates only when:
      - remaining_time <= 180 s (last 3 minutes of the window)
      - one side's best_bid is between 0.85 and 0.95

    Profit per pair:
      - 0.85 ≤ best_bid < 0.90 → 4¢
      - 0.90 ≤ best_bid < 0.95 → 2¢
    """

    def __init__(self, cfg: Config, max_per_side: int = 10):
        self.cfg = cfg
        self.max_per_side = max_per_side

    # ── helpers ──

    MIN_BID = 0.85
    MID_BID = 0.90
    MAX_BID = 0.95
    MAX_ACTIVATE = 180

    @staticmethod
    def _profit_for(bid: float) -> Optional[float]:
        """Return profit target for a given winner best_bid, or None if out of range."""
        if TailSweepStrategy.MIN_BID <= bid < TailSweepStrategy.MID_BID:
            return 0.04
        if TailSweepStrategy.MID_BID <= bid < TailSweepStrategy.MAX_BID:
            return 0.02
        return None

    def decide(
        self,
        ws: WindowState,
        up_snap: Optional[OrderBookSnapshot],
        down_snap: Optional[OrderBookSnapshot],
        remaining_time: float = 999,
        up_tick: float = 0.01,
        down_tick: float = 0.01,
    ) -> tuple[Optional[float], Optional[float], list[Decision]]:
        """Evaluate tail sweep conditions.

        Returns (up_price, down_price, decisions).
        Returns (None, None, []) if conditions are not met.
        """
        if remaining_time > self.MAX_ACTIVATE:
            return None, None, []

        if not up_snap or not down_snap:
            return None, None, []
        if not up_snap.best_bid or not down_snap.best_bid:
            return None, None, []

        up_bid = up_snap.best_bid
        down_bid = down_snap.best_bid

        # Determine which side is the "winner"
        up_in_range = self.MIN_BID <= up_bid < self.MAX_BID
        down_in_range = self.MIN_BID <= down_bid < self.MAX_BID
        if up_in_range and down_in_range:
            # Both in range — pick higher bid (stronger winner signal)
            winner_side = "Up" if up_bid >= down_bid else "Down"
            winner_bid = up_bid if winner_side == "Up" else down_bid
        elif up_in_range:
            winner_side = "Up"
            winner_bid = up_bid
        elif down_in_range:
            winner_side = "Down"
            winner_bid = down_bid
        else:
            winner_side = None
            winner_bid = None

        if winner_side is None or winner_bid is None:
            return None, None, []

        profit = self._profit_for(winner_bid)
        if profit is None:
            return None, None, []

        # ── Price the winner ──
        if winner_side == "Up":
            win_snap = up_snap
            win_tick = up_tick
            lose_tick = down_tick
        else:
            win_snap = down_snap
            win_tick = down_tick
            lose_tick = up_tick

        ask = win_snap.best_ask if win_snap.best_ask else 1.0
        max_win = 1.0 - profit - lose_tick
        winner_price = min(ask, max_win)
        winner_price = round(winner_price / win_tick) * win_tick

        raw_loser = 1.0 - winner_price - profit
        loser_price = round(raw_loser / lose_tick) * lose_tick
        if loser_price < lose_tick:
            loser_price = lose_tick
            winner_price = 1.0 - loser_price - profit
            winner_price = round(winner_price / win_tick) * win_tick

        if winner_price <= 0 or loser_price <= 0:
            logger.info("[tail] Invalid prices: winner=%.4f loser=%.4f → skip",
                        winner_price, loser_price)
            return None, None, []

        total = winner_price + loser_price
        if total >= 1.0:
            logger.info("[tail] Price sum %.4f >= 1.0 → skip", total)
            return None, None, []

        up_price = winner_price if winner_side == "Up" else loser_price
        down_price = winner_price if winner_side == "Down" else loser_price

        logger.info(
            "[tail] %s winning (bid=%.4f ask=%.4f profit=%.2f) "
            "→ Up=%.4f Down=%.4f sum=%.4f",
            winner_side, win_snap.best_bid, ask, profit,
            up_price, down_price, total,
        )

        # ── Build decisions ──
        decisions: list[Decision] = []
        cfg = self.cfg

        for side, price in [("Up", up_price), ("Down", down_price)]:
            pending = sum(
                po.remaining for po in ws.pending_orders.values()
                if po.side == side and po.cancelled_at == 0
            )
            exposure = ws.inventory[side] + pending
            room = self.max_per_side - exposure
            if room <= 0:
                logger.info("  [tail] %s %d/%d at limit → skip",
                            side, exposure, self.max_per_side)
                continue
            qty = min(cfg.per_tick, room)
            if qty >= cfg.min_order_size:
                decisions.append(Decision(side=side, amount=qty, price=price))

        return up_price, down_price, decisions


# ════════════════════════════════════════════════════════════
# Standalone runner (WS-based, two-phase per round)
# ════════════════════════════════════════════════════════════

_local_logger = logging.getLogger("poly_trader.tail_sweep")


class _RoundState:
    """Tracks the current round's phase and prices."""

    IDLE = "idle"
    WINNER_PENDING = "winner_pending"
    LOSER_PENDING = "loser_pending"

    def __init__(self):
        self.phase = self.IDLE
        self.round = 0
        self.winner_side: Optional[str] = None
        self.winner_price: Optional[float] = None
        self.loser_price: Optional[float] = None
        self.winner_oid: Optional[str] = None
        self.loser_oid: Optional[str] = None
        self.phase_start: float = 0.0
        self.profit_target: float = 0.0
        self.lose_tick: float = 0.01

    @property
    def is_active(self) -> bool:
        return self.phase != self.IDLE

    def start_winner(self, side: str, price: float) -> None:
        self.round += 1
        self.phase = self.WINNER_PENDING
        self.winner_side = side
        self.winner_price = price
        self.loser_price = None
        self.winner_oid = None
        self.loser_oid = None
        self.phase_start = time.time()

    def start_loser(self, price: float, loser_oid: str) -> None:
        self.phase = self.LOSER_PENDING
        self.loser_price = price
        self.loser_oid = loser_oid
        self.phase_start = time.time()

    def reset(self) -> None:
        self.phase = self.IDLE
        self.winner_side = None
        self.winner_price = None
        self.loser_price = None
        self.winner_oid = None
        self.loser_oid = None


async def async_main(args: argparse.Namespace):
    """Standalone tail-sweep with two-phase execution.

    Each round: place winner → wait for fill → place loser → next round.
    Price + order book from WS. Fill tracking from user WS.
    """
    cfg = Config()
    if args.per_tick is not None:
        cfg.per_tick = args.per_tick
    if args.max_side is not None:
        cfg.max_per_side = args.max_side

    from .main import parse_market_spec
    spec = parse_market_spec(args.market)
    cfg.market_specs = [spec]

    from ..tools.polymarket.client import SdkClient
    sdk = SdkClient(cfg)
    await sdk.create_secure(cfg)
    if not sdk.is_secure:
        _local_logger.error("Failed to create secure client — check credentials")
        return

    market = await sdk.find_market_for_spec(spec)
    if not market:
        _local_logger.error("No active market found for %s", spec.slug_pattern)
        await sdk.close()
        return

    # Cancel only this market's lingering orders (safe for multi-bot setups)
    await sdk.cancel_all_open_orders(market=market.condition_id)

    _local_logger.info("Market found: %s  (window end: %d)", market.slug, market.window_end)
    up_token = market.up_token_id
    down_token = market.down_token_id

    # ── Load current positions ──
    state = await sdk.load_current_state(market)
    ws = WindowState(slug=market.slug, start_time=time.time())
    if state:
        ws.inventory = state["inventory"]
        ws.cost["Up"] = state["avg_cost"]["Up"] * state["inventory"]["Up"]
        ws.cost["Down"] = state["avg_cost"]["Down"] * state["inventory"]["Down"]
        ws.total_spent = ws.cost["Up"] + ws.cost["Down"]
        ws.pending_orders = state["pending"]
        ws.trades = sum(state["inventory"].values())
        _local_logger.info(
            "  [restore] Up=%d@%.4f Down=%d@%.4f pending=%d balance=%d",
            state["inventory"]["Up"], state["avg_cost"]["Up"],
            state["inventory"]["Down"], state["avg_cost"]["Down"],
            len(state["pending"]), state["balance"],
        )

    sweeper = TailSweepStrategy(cfg, max_per_side=cfg.max_per_side)
    up_tick = await sdk.get_tick_size(up_token)
    down_tick = await sdk.get_tick_size(down_token)
    token_map = {"Up": up_token, "Down": down_token}

    # ── Price cache (market data WS) ──
    from .engine import PriceCache
    prices = PriceCache()
    market_handle = await sdk.subscribe([up_token, down_token])

    # ── User event channel (fill tracking) ──
    user_handle = await sdk.subscribe_user()

    _local_logger.info(
        "Tail sweep started — %s  (0.85 ≤ bid < 0.95, last 3 min, multi-round)",
        market.slug,
    )

    # ═══════════════════════════════════════════════
    # WS listeners
    # ═══════════════════════════════════════════════

    async def _market_listen():
        try:
            async for event in market_handle:
                if isinstance(event, MarketBestBidAskEvent):
                    p = event.payload
                    tid = p.token_id
                    if tid == up_token or tid == down_token:
                        prices.update_from_ws(
                            tid, float(p.best_bid), float(p.best_ask),
                        )
        except asyncio.CancelledError:
            pass

    # Shared fill event queue between user listener and main loop
    fill_queue: asyncio.Queue[tuple[str, str, int, float]] = asyncio.Queue()
    """(order_id, side, fill_size, fill_price)"""

    async def _user_listen():
        try:
            async for event in user_handle:
                if isinstance(event, UserTradeEvent):
                    payload = event.payload
                    if not payload.maker_orders:
                        continue
                    for mo in payload.maker_orders:
                        oid = mo.order_id
                        fill_size = int(float(mo.matched_amount))
                        fill_price = float(mo.price)
                        if fill_size <= 0:
                            continue
                        # Find side from pending_orders
                        po = ws.pending_orders.get(oid)
                        if po is None:
                            continue
                        await fill_queue.put((oid, po.side, fill_size, fill_price))
        except asyncio.CancelledError:
            pass

    # ═══════════════════════════════════════════════
    # Tick loop
    # ═══════════════════════════════════════════════

    rs = _RoundState()

    async def _process_fill(oid: str, side: str, fill_size: int, fill_price: float):
        """Process a fill event: update inventory, cost, pending."""
        po = ws.pending_orders.get(oid)
        if po is None:
            return
        filled = min(fill_size, po.remaining)
        if filled <= 0:
            return
        po.filled += filled
        ws.inventory[side] += filled
        ws.cost[side] += filled * fill_price
        ws.total_spent += filled * fill_price
        ws.trades += 1

        _local_logger.info(
            "  [tail] ✓ Fill %s %d @ %.4f  (inv: Up=%d Down=%d) remaining=%d",
            side, filled, fill_price,
            ws.inventory["Up"], ws.inventory["Down"], po.remaining,
        )

        if po.remaining <= 0:
            del ws.pending_orders[oid]

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_market_listen(), name="ws-market")
            tg.create_task(_user_listen(), name="ws-user")

            idle_log_ts = 0.0   # throttle idle/decide logs
            loser_log_ts = 0.0  # throttle loser-waiting logs

            while True:
                now = time.time()
                remaining = market.window_end - now

                if remaining <= 0:
                    _local_logger.info(
                        "[tail] Window ended — final: Up=%d Down=%d rounds=%d/%d",
                        ws.inventory["Up"], ws.inventory["Down"],
                        rs.round, sum(1 for _ in [1]),  # completed rounds tracked in rs.round
                    )
                    break

                # Process any queued fills
                while not fill_queue.empty():
                    oid, side, fsize, fprice = fill_queue.get_nowait()
                    await _process_fill(oid, side, fsize, fprice)

                # ── State machine ──
                if rs.phase == _RoundState.WINNER_PENDING:
                    # Check if winner filled
                    if rs.winner_oid and rs.winner_oid not in ws.pending_orders:
                        _local_logger.info(
                            "  [tail] Round %d: winner %s filled → placing loser",
                            rs.round, rs.winner_side,
                        )

                        # ── Re-price loser against current order book ──
                        loser_side = "Down" if rs.winner_side == "Up" else "Up"
                        loser_token = token_map[loser_side]
                        loser_snap = prices.get(loser_token)
                        max_loser = 1.0 - rs.winner_price - rs.profit_target
                        if loser_snap and loser_snap.best_ask:
                            loser_price = min(loser_snap.best_ask, max_loser)
                        else:
                            loser_price = max_loser
                        loser_price = round(loser_price / rs.lose_tick) * rs.lose_tick
                        if loser_price < rs.lose_tick:
                            loser_price = rs.lose_tick

                        _local_logger.info(
                            "  [tail] Round %d: loser price %.4f (max_loser=%.4f, profit=%.1f¢)",
                            rs.round, loser_price, max_loser, rs.profit_target * 100,
                        )

                        loser_oid = await sdk.place_limit_order(
                            token_id=token_map["Up" if rs.winner_side == "Down" else "Down"],
                            side="BUY",
                            price=loser_price,
                            size=cfg.per_tick,
                        )
                        if loser_oid:
                            rs.start_loser(loser_price, loser_oid)
                            loser_side = "Up" if rs.winner_side == "Down" else "Down"
                            ws.pending_orders[loser_oid] = PendingOrder(
                                order_id=loser_oid,
                                token_id=token_map[loser_side],
                                side=loser_side,
                                buy_sell="BUY",
                                price=loser_price,
                                amount=cfg.per_tick,
                            )
                            _local_logger.info(
                                "  [tail] Round %d: placed loser %s %d @ %.4f",
                                rs.round, "Down" if rs.winner_side == "Up" else "Up",
                                cfg.per_tick, loser_price,
                            )
                        else:
                            _local_logger.warning(
                                "  [tail] Round %d: loser FAILED → next round",
                                rs.round,
                            )
                            rs.reset()
                    # Check timeout (30s)
                    elif now - rs.phase_start > 30:
                        _local_logger.warning(
                            "  [tail] Round %d: winner %s @ %.4f timed out (30s) → cancel",
                            rs.round, rs.winner_side, rs.winner_price,
                        )
                        if rs.winner_oid:
                            await sdk.cancel_order(rs.winner_oid)
                            ws.pending_orders.pop(rs.winner_oid, None)
                        rs.reset()
                    else:
                        await prices.wait_update(timeout=0.5)

                elif rs.phase == _RoundState.LOSER_PENDING:
                    # Wait for loser to fill (no timeout — stay until done)
                    if rs.loser_oid and rs.loser_oid not in ws.pending_orders:
                        _local_logger.info(
                            "  [tail] ★ Round %d complete! (winner=%s %.4f loser=%.4f)",
                            rs.round, rs.winner_side, rs.winner_price, rs.loser_price,
                        )
                        rs.reset()  # ready for next round
                    else:
                        if now - loser_log_ts > 30:
                            _local_logger.info(
                                "  [tail] Round %d: waiting for loser @ %.4f (%ds)",
                                rs.round, rs.loser_price, now - rs.phase_start,
                            )
                            loser_log_ts = now
                        await prices.wait_update(timeout=0.5)

                else:  # IDLE
                    # Log periodically while waiting for tail window
                    if remaining > sweeper.MAX_ACTIVATE:
                        if now - idle_log_ts > 30:
                            _local_logger.info(
                                "  [tail] Waiting for tail window — %ds until activation",
                                remaining - sweeper.MAX_ACTIVATE,
                            )
                            idle_log_ts = now
                        await asyncio.sleep(1)
                        continue

                    await prices.wait_update(timeout=1.0)

                    up_snap = prices.get(up_token)
                    down_snap = prices.get(down_token)

                    _up, _down, decisions = sweeper.decide(
                        ws, up_snap, down_snap,
                        remaining_time=remaining,
                        up_tick=up_tick, down_tick=down_tick,
                    )

                    if not decisions:
                        if now - idle_log_ts > 10:
                            _local_logger.info(
                                "  [tail] No opportunity — Up bid=%s Down bid=%s",
                                f"{up_snap.best_bid:.4f}" if up_snap and up_snap.best_bid else "N/A",
                                f"{down_snap.best_bid:.4f}" if down_snap and down_snap.best_bid else "N/A",
                            )
                            idle_log_ts = now
                        await asyncio.sleep(0.5)
                        continue

                    # Don't start new rounds in the last 60s
                    if remaining <= 60:
                        if now - idle_log_ts > 10:
                            _local_logger.info(
                                "  [tail] < 60s remaining \u2014 no new orders"
                            )
                            idle_log_ts = now
                        await asyncio.sleep(0.5)
                        continue

                    # Place winner order (higher price side = winner)
                    if len(decisions) > 1 and _up is not None and _down is not None:
                        d = decisions[0] if _up > _down else decisions[1]
                    else:
                        d = decisions[0]  # only one side has room
                    oid = await sdk.place_limit_order(
                        token_id=token_map[d.side],
                        side="BUY",
                        price=d.price,
                        size=d.amount,
                    )
                    if oid:
                        rs.start_winner(d.side, d.price)
                        # Store pricing params for loser re-pricing later
                        rs.profit_target = sweeper._profit_for(
                            up_snap.best_bid if d.side == "Up" else down_snap.best_bid
                        ) or 0.02
                        rs.lose_tick = down_tick if d.side == "Up" else up_tick
                        rs.winner_oid = oid
                        ws.pending_orders[oid] = PendingOrder(
                            order_id=oid,
                            token_id=token_map[d.side],
                            side=d.side,
                            buy_sell="BUY",
                            price=d.price,
                            amount=d.amount,
                        )
                        # Store loser price for later (second decision)
                        rs.loser_price = decisions[1].price if len(decisions) > 1 else None
                        _local_logger.info(
                            "  [tail] Round %d: placed winner %s %d @ %.4f  (loser=%.4f)",
                            rs.round, d.side, d.amount, d.price, rs.loser_price,
                        )
                    else:
                        _local_logger.warning(
                            "  [tail] Winner order FAILED: %s %d @ %.4f",
                            d.side, d.amount, d.price,
                        )

    except asyncio.CancelledError:
        pass
    finally:
        await sdk.close()


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Tail-Sweep Strategy (WS, two-phase, multi-round)",
    )
    parser.add_argument(
        "--market", type=str, default="btc-updown-15m",
        help='Market slug pattern, e.g. "btc-updown-15m"',
    )
    parser.add_argument("--per-tick", type=int, default=None,
                        help="max contracts per tick per side")
    parser.add_argument("--max-side", type=int, default=None,
                        help="max position per side")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="debug-level logging")
    args = parser.parse_args()

    from .main import setup_logging
    setup_logging(args.verbose)

    print(f"{'═' * 50}")
    print("  Tail-Sweep Strategy (WS · Two-Phase · Multi-Round)")
    print(f"{'═' * 50}")
    print(f"  Market: {args.market}")
    print()

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        _local_logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
