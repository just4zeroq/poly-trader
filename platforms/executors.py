"""
Order execution — live trading via Polymarket CLOB.

LiveExecutor — REST API + WS user channel for fill tracking.

Multi-market support: place() takes a slug parameter to identify which
window the order belongs to. cancel() searches across all active windows.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from polymarket.models.clob.user_events import UserTradeEvent

logger = logging.getLogger(__name__)

from .models import (
    HedgePlan,
    Lot,
    OrderCancelled,
    OrderFilled,
    OrderFailed,
    OrderPlaced,
    PendingOrder,
    WindowState,
)

if TYPE_CHECKING:
    from .engine import TradingEngine


class OrderExecutor:
    """Base executor — defines the interface for the engine."""

    def __init__(self):
        self.engine: TradingEngine | None = None

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int) -> Optional[str]:
        raise NotImplementedError

    async def cancel(self, order_id: str) -> bool:
        raise NotImplementedError

    async def handle_user_event(self, event):
        """Handle authenticated user WS event — no-op in base."""
        pass

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        """Cancel all orders in the given pending dict."""
        for oid in list(pending_orders):
            await self.cancel(oid)

    async def flush_cancelled(self, grace: float = 5.0):
        """Remove soft-deleted orders past grace period. No-op in base."""
        pass

    def _create_lot(self, ws: WindowState, po: PendingOrder,
                      fill_size: int, fill_price: float):
        """Create a pure cost-record Lot for this fill."""
        lot_id = f"lot_{ws.window_num}_{po.side}_{len(ws.lots)}"
        lot = Lot(
            lot_id=lot_id, side=po.side,
            amount=fill_size, price=fill_price,
            created_at=time.time(),
        )
        ws.lots.append(lot)
        logger.info(
            "  [fill] %s lot=%s (%d @ %.4f)  inv=(U=%d/D=%d)",
            po.side, lot_id, fill_size, fill_price,
            ws.inventory["Up"], ws.inventory["Down"],
        )

class LiveExecutor(OrderExecutor):
    """Live trading — SDK place_limit_order + cancel_order."""

    def __init__(self):
        super().__init__()
        # Orphan fill buffer: fills that arrived before pending_orders entry
        self._orphan_fills: dict[str, tuple] = {}
        # Dedup dict: order_id → cumulative_matched, skip historical fill replay.
        # Uses cumulative matched (not timestamp) so partial fills (e.g. 4 then 1)
        # aren't incorrectly deduped as "already seen".  Only skip when the incoming
        # matched_amount ≤ previously recorded cumulative amount.
        self._fill_seen: dict[str, int] = {}
        # Throttle for CLOB order-status queries used to resolve ambiguous
        # WS fill events (matched_amount ≤ seen cumulative).  At most one
        # get_order_filled query per order per throttle window.
        self._fill_query_throttle_s: float = 10.0
        self._fill_query_ts: dict[str, float] = {}
        # Monotonic counter of fills applied via _process_fill.  Lets the
        # engine's position poll detect a fill that landed while its CLOB
        # snapshot was in flight, so the poll skips overwriting (which would
        # clobber that optimistic write-through).  See engine._poll_positions.
        self.fill_seq: int = 0

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int,
                    is_favorite: bool = False) -> Optional[str]:
        ws = self.engine._windows.get(slug)
        if ws is None:
            return None

        # Count current pending for this side (anomaly detection)
        side_pending = sum(1 for po in ws.pending_orders.values()
                          if po.side == outcome and po.cancelled_at == 0)
        logger.info(
            "  [place] %s %d @ %.4f  side_pending=%d  inv=(U=%d/D=%d)",
            outcome, amount, price,
            side_pending,
            ws.inventory["Up"], ws.inventory["Down"],
        )

        oid = await self.engine.sdk.place_limit_order(
            token_id=token_id, side="BUY",
            price=price, size=amount,
        )
        if oid:
            ws.last_activity = time.time()
            ws.pending_orders[oid] = PendingOrder(
                order_id=oid, token_id=token_id,
                side=outcome, buy_sell="BUY",
                price=price, amount=amount,
                placed_at=time.time(),
            )
            # Record the bound hedge plan ONLY now that the favorite order
            # exists (never at decision time): the plan always corresponds to
            # a real live order.  Side/amount/max_price derive from the order
            # itself, so nothing needs to be carried across from decide().
            if is_favorite:
                ws.hedge_plan = HedgePlan(
                    order_id=oid,
                    side="Down" if outcome == "Up" else "Up",
                    amount=amount,
                    fav_price=price,
                    max_price=round(self.engine.cfg.hedge_price_bound - price, 4),
                )
            await self.engine._emit("order_placed", OrderPlaced(
                window_num=ws.window_num,
                outcome=outcome, side="BUY",
                price=price, amount=amount,
                order_id=oid,
            ))

            # Check if a fill for this order arrived before we registered it
            orphan = self._orphan_fills.pop(oid, None)
            if orphan:
                logger.info("  [place] Processing orphan fill for %s…", oid[:12])
                ws2 = self.engine._windows.get(slug)
                if ws2:
                    po2 = ws2.pending_orders.get(oid)
                    if po2:
                        fill_size, fill_price, _ = orphan
                        self._process_fill(ws2, po2, fill_size, fill_price)
                        await self.engine._emit("order_filled", OrderFilled(
                            window_num=ws2.window_num,
                            outcome=outcome, price=fill_price, amount=fill_size,
                            order_id=oid,
                            total_filled=po2.filled,
                            total_inv_up=ws2.inventory["Up"],
                            total_inv_down=ws2.inventory["Down"],
                        ))
                        if po2.remaining <= 0:
                            del ws2.pending_orders[oid]

            return oid

        await self.engine._emit("order_failed", OrderFailed(
            window_num=ws.window_num,
            outcome=outcome, price=price, amount=amount,
            reason="SDK place failed",
        ))
        return None

    async def cancel(self, order_id: str) -> tuple[bool, str | None]:
        """Soft-delete: mark cancelled_at, emit event.

        Returns (ok, side) — side is "Up"/"Down" for the caller's logging.
        """
        ok = await self.engine.sdk.cancel_order(order_id)
        if ok:
            for ws in list(self.engine._windows.values()):
                po = ws.pending_orders.get(order_id)
                if po is None:
                    continue
                ws.last_activity = time.time()
                po.cancelled_at = time.time()
                await self.engine._emit("order_cancelled", OrderCancelled(
                    window_num=ws.window_num,
                    outcome=po.side, amount=po.remaining,
                    price=po.price, order_id=order_id,
                ))
                return True, po.side
        logger.warning("  [cancel] SDK cancel_order(%s…) FAILED", order_id[:12])
        return ok, None

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        """Soft-cancel all pending orders, wait for in-flight fills, then flush."""
        if not pending_orders:
            return
        logger.info("  [cancel_all] Cancelling %d orders…", len(pending_orders))
        for oid in list(pending_orders):
            await self.cancel(oid)
        # Give in-flight fills time to arrive via UserTradeEvent
        await asyncio.sleep(0.3)
        # Remove soft-deleted orders (grace=0 because we already waited above)
        await self.flush_cancelled(grace=0.0)
        # Keep any orders where cancel failed — they may still be live on CLOB
        failed = [po for po in pending_orders.values() if po.cancelled_at == 0]
        if failed:
            logger.warning(
                "  [cancel_all] %d orders could not be cancelled (still live on CLOB)",
                len(failed),
            )

    async def flush_cancelled(self, grace: float = 5.0):
        """Remove soft-deleted orders whose grace period has elapsed."""
        now = time.time()
        for ws in list(self.engine._windows.values()):
            for oid in list(ws.pending_orders):
                po = ws.pending_orders[oid]
                if po.cancelled_at > 0 and now - po.cancelled_at > grace:
                    del ws.pending_orders[oid]

    def _process_fill(self, ws: WindowState, po: PendingOrder,
                      fill_size: int, fill_price: float):
        """Apply a fill to a pending order and update inventory + cost.

        ``ws.auth_inv`` is written through optimistically (the strategy's
        fast path); the engine's 2s position poll later overwrites it with
        CLOB truth, so a dropped WS fill self-corrects within one poll.
        """
        actual = min(fill_size, po.remaining)
        if actual <= 0:
            return
        if actual < fill_size:
            logger.warning(
                "  [fill] Overfill capped: fill=%d > remaining=%d for %s…",
                fill_size, po.remaining, po.order_id[:12],
            )

        self.fill_seq += 1
        po.filled += actual
        ws.inventory[po.side] += actual
        ws.auth_inv[po.side] += actual
        ws.cost[po.side] += actual * fill_price
        ws.total_spent += actual * fill_price
        ws.trades += 1

        # Keep the bound hedge plan's running fill in lockstep so the hedge
        # fires the moment its favorite order crosses the fill threshold.
        plan = ws.hedge_plan
        if plan is not None and plan.order_id == po.order_id:
            plan.filled = po.filled

        self._create_lot(ws, po, actual, fill_price)

        remaining = po.amount - po.filled
        logger.info(
            "  [fill] %s %d @ %.4f  order=%s…  "
            "inv=(U=%d/D=%d) remaining=%d",
            po.side, actual, fill_price, po.order_id[:12],
            ws.inventory["Up"], ws.inventory["Down"],
            remaining,
        )

    # ── _fill_seen time-based pruning ──

    def _prune_fill_seen(self, max_count: int = 10000):
        """Remove oldest entries when dict exceeds *max_count*.

        Uses insertion order (Python 3.7+ dict guarantee) as a proxy for age.
        No longer tracks timestamps — _fill_seen stores cumulative matched instead.
        """
        if len(self._fill_seen) <= max_count:
            return
        # Drop oldest half (entries at front of ordered dict)
        excess = len(self._fill_seen) - max_count + (max_count // 4)
        for oid in list(self._fill_seen)[:excess]:
            del self._fill_seen[oid]
            self._fill_query_ts.pop(oid, None)
        logger.info("  [fill] Pruned %d oldest entries from _fill_seen  remaining=%d",
                    excess, len(self._fill_seen))

    async def _resolve_ambiguous_fill(self, oid: str,
                                      prev_cum: int) -> Optional[tuple[int, int]]:
        """Decide whether a ≤prev_cum WS event is a true replay.

        Queries CLOB (throttled per-order) for the authoritative cumulative
        fill.  Returns (clob_cum, incremental) when CLOB shows more fill than
        we recorded — the ambiguous event masked real progress.  Returns None
        when CLOB agrees with *prev_cum* (genuine replay), the query fails,
        or the per-order throttle hasn't elapsed.
        """
        now = time.time()
        if now - self._fill_query_ts.get(oid, 0.0) < self._fill_query_throttle_s:
            return None
        self._fill_query_ts[oid] = now
        sdk = getattr(self.engine, "sdk", None)
        try:
            clob_cum = await sdk.get_order_filled(oid) if sdk else None
        except Exception:
            clob_cum = None
        if clob_cum is None or clob_cum <= prev_cum:
            return None
        return clob_cum, clob_cum - prev_cum

    async def handle_user_event(self, event):
        """Process authenticated user channel events for fill tracking."""
        if isinstance(event, UserTradeEvent):
            await self._handle_trade(event.payload)

    async def _handle_trade(self, payload):
        """Match a UserTradeEvent against our pending orders.

        Handles three edge cases:
          1. Historical fill replay on reconnect → dedup via _fill_seen
          2. Fill arrives before pending_orders entry → orphan buffer
          3. Unknown order (previous instance) → warning
        """
        if not payload.maker_orders:
            return
        for mo in payload.maker_orders:
            oid = mo.order_id
            # Round, not truncate: a 4.9992 maker fill must count as 5, not 4,
            # or the bound hedge and auth_inv both under-count the favorite.
            fill_size = int(round(float(mo.matched_amount)))
            fill_price = float(mo.price)
            if fill_size <= 0:
                continue

            # Dedup: compute incremental fill vs previous cumulative.
            # Polymarket WS may replay historical fills on reconnect, and
            # matched_amount can be either incremental or cumulative per event.
            prev_cum = self._fill_seen.get(oid, 0)
            if fill_size <= prev_cum:
                # Ambiguous: a true replay (cumulative semantics) or an
                # incremental-style event Polymarket sent without a bigger
                # cumulative.  Resolve against CLOB ground truth instead of
                # silently dropping — the old behavior lost real fills.
                resolved = await self._resolve_ambiguous_fill(oid, prev_cum)
                if resolved is None:
                    continue  # CLOB agrees with prev_cum (or query failed) → replay
                clob_cum, incremental = resolved
                self._fill_seen[oid] = clob_cum
                logger.warning(
                    "  [fill] Ambiguous event order=%s… size=%d ≤ seen=%d → "
                    "CLOB resolved cum=%d (+%d)",
                    oid[:12], fill_size, prev_cum, clob_cum, incremental,
                )
            else:
                incremental = fill_size - prev_cum
                self._fill_seen[oid] = fill_size
            if len(self._fill_seen) > 10000:
                self._prune_fill_seen()

            # Locate the pending order across all active windows
            found = False
            for ws in self.engine._windows.values():
                po = ws.pending_orders.get(oid)
                if po is None:
                    continue
                found = True

                ws.last_activity = time.time()
                self._process_fill(ws, po, incremental, fill_price)

                await self.engine._emit("order_filled", OrderFilled(
                    window_num=ws.window_num,
                    outcome=po.side, price=fill_price, amount=incremental,
                    order_id=oid,
                    total_filled=po.filled,
                    total_inv_up=ws.inventory["Up"],
                    total_inv_down=ws.inventory["Down"],
                ))
                if po.remaining <= 0:
                    del ws.pending_orders[oid]
                break  # found the order — stop scanning windows, next maker_order

            if not found:
                # Could be a race: fill arrived before place() registered the order.
                # Store in orphan buffer; place() will check after registering.
                self._orphan_fills[oid] = (fill_size, fill_price, time.time())
                # Prune orphan buffer (stale entries > 60s)
                now = time.time()
                stale = [k for k, v in self._orphan_fills.items() if now - v[2] > 60]
                for k in stale:
                    del self._orphan_fills[k]
                # Only warn if not in orphan buffer already (first time)
                if len(self._orphan_fills) <= 50:
                    logger.warning(
                        "  [fill] ORPHAN order=%s… size=%d price=%.4f  "
                        "(may be post-cancel_all late fill, buffered for analysis)",
                        oid[:12], fill_size, fill_price,
                    )
