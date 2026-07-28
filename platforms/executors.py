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
    Lot,
    OrderCancelled,
    OrderFilled,
    OrderFailed,
    OrderPlaced,
    Pair,
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

    async def place_pair(
        self, slug: str,
        up_token_id: str, down_token_id: str,
        up_price: float, up_amount: int,
        down_price: float, down_amount: int,
        pair_id: str = "",
    ) -> tuple[bool, bool]:
        """Place paired Up+Down orders — sign both, submit together.

        Returns (up_ok, down_ok).  Base class raises NotImplementedError.
        """
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

    def _find_pair_for_order(self, ws: WindowState, po: PendingOrder) -> Optional[Pair]:
        """Find the Pair by PendingOrder.pair_id, if any."""
        if not po.pair_id:
            return None
        for pair in ws.pairs:
            if pair.pair_id == po.pair_id:
                return pair
        return None

class LiveExecutor(OrderExecutor):
    """Live trading — SDK place_limit_order + cancel_order."""

    def __init__(self):
        super().__init__()
        # Orphan fill buffer: fills that arrived before pending_orders entry
        self._orphan_fills: dict[str, tuple] = {}
        # Dedup dict: order_id → timestamp, skip historical fill replay on User WS reconnect
        # Time-based pruning avoids catastrophic state loss from set.clear()
        self._fill_seen: dict[str, float] = {}

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int,
                    pair_id: str = "") -> Optional[str]:
        ws = self.engine._windows.get(slug)
        if ws is None:
            return None

        # Count current pending for this side (anomaly detection)
        side_pending = sum(1 for po in ws.pending_orders.values()
                          if po.side == outcome and po.cancelled_at == 0)
        logger.info(
            "  [place] %s %d @ %.4f  side_pending=%d  inv=(U=%d/D=%d)  pair=%s",
            outcome, amount, price,
            side_pending,
            ws.inventory["Up"], ws.inventory["Down"],
            pair_id or "-",
        )

        oid = await self.engine.sdk.place_limit_order(
            token_id=token_id, side="BUY",
            price=price, size=amount,
        )
        if oid:
            ws.pending_orders[oid] = PendingOrder(
                order_id=oid, token_id=token_id,
                side=outcome, buy_sell="BUY",
                price=price, amount=amount,
                placed_at=time.time(),
                pair_id=pair_id,
            )
            await self.engine._emit("order_placed", OrderPlaced(
                window_num=ws.window_num,
                outcome=outcome, side="BUY",
                price=price, amount=amount,
                order_id=oid,
            ))

            # Link to pair if this is a pair order
            if pair_id:
                for pair in ws.pairs:
                    if pair.pair_id == pair_id:
                        if outcome == "Up":
                            pair.up_order_id = oid
                        else:
                            pair.down_order_id = oid
                        break

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

    async def place_pair(
        self, slug: str,
        up_token_id: str, down_token_id: str,
        up_price: float, up_amount: int,
        down_price: float, down_amount: int,
        pair_id: str = "",
    ) -> tuple[bool, bool]:
        """Place paired Up+Down orders — sign both, submit together.

        1. Sign both orders in parallel (EIP-712)
        2. Submit both via post_orders (single HTTP request)
        3. Process each result: register pending orders, emit events
        4. If pair_id given, link order IDs to the Pair object
        """
        ws = self.engine._windows.get(slug)
        if ws is None:
            return False, False

        # 1. Sign both orders in parallel
        up_signed, down_signed = await asyncio.gather(
            self.engine.sdk.create_signed_limit_order(up_token_id, "BUY", up_price, up_amount),
            self.engine.sdk.create_signed_limit_order(down_token_id, "BUY", down_price, down_amount),
        )

        # Map signed orders to their sides
        to_submit = []
        side_map: list[str] = []
        if up_signed:
            to_submit.append(up_signed)
            side_map.append("Up")
        if down_signed:
            to_submit.append(down_signed)
            side_map.append("Down")

        if not to_submit:
            logger.warning("  [place_pair] Both signed order creation FAILED")
            await self.engine._emit("order_failed", OrderFailed(
                window_num=ws.window_num,
                outcome="Up", price=up_price, amount=up_amount,
                reason="Sign failed",
            ))
            await self.engine._emit("order_failed", OrderFailed(
                window_num=ws.window_num,
                outcome="Down", price=down_price, amount=down_amount,
                reason="Sign failed",
            ))
            return False, False

        # Logging
        logger.info(
            "  [place_pair] Submitting %d/%d orders (post_only)  "
            "Up=%d@%.4f Down=%d@%.4f",
            len(to_submit), 2,
            up_amount, up_price, down_amount, down_price,
        )

        # 2. Submit together
        order_ids = await self.engine.sdk.submit_orders(to_submit)

        # 3. Process results
        up_ok = False
        down_ok = False

        for side, oid in zip(side_map, order_ids):
            price = up_price if side == "Up" else down_price
            amount = up_amount if side == "Up" else down_amount
            token_id = up_token_id if side == "Up" else down_token_id

            if oid is None:
                logger.warning("  [place_pair] %s REJECTED", side)
                await self.engine._emit("order_failed", OrderFailed(
                    window_num=ws.window_num,
                    outcome=side, price=price, amount=amount,
                    reason="Pair submission rejected",
                ))
                continue

            # Accepted — register pending order
            ws.pending_orders[oid] = PendingOrder(
                order_id=oid, token_id=token_id,
                side=side, buy_sell="BUY",
                price=price, amount=amount,
                placed_at=time.time(),
                pair_id=pair_id,
            )
            await self.engine._emit("order_placed", OrderPlaced(
                window_num=ws.window_num,
                outcome=side, side="BUY",
                price=price, amount=amount,
                order_id=oid,
            ))

            # Link order_id to Pair if this is a pair order
            if pair_id:
                for pair in ws.pairs:
                    if pair.pair_id == pair_id:
                        if side == "Up":
                            pair.up_order_id = oid
                        else:
                            pair.down_order_id = oid
                        break

            # Check orphan fills (fill arrived before we registered the order)
            orphan = self._orphan_fills.pop(oid, None)
            if orphan:
                logger.info("  [place_pair] Processing orphan fill for %s…", oid[:12])
                po = ws.pending_orders.get(oid)
                if po:
                    fill_size, fill_price, _ = orphan
                    self._process_fill(ws, po, fill_size, fill_price)
                    await self.engine._emit("order_filled", OrderFilled(
                        window_num=ws.window_num,
                        outcome=side, price=fill_price, amount=fill_size,
                        order_id=oid,
                        total_filled=po.filled,
                        total_inv_up=ws.inventory["Up"],
                        total_inv_down=ws.inventory["Down"],
                    ))
                    if po.remaining <= 0:
                        del ws.pending_orders[oid]

            if side == "Up":
                up_ok = True
            else:
                down_ok = True

        return up_ok, down_ok

    async def cancel(self, order_id: str) -> bool:
        """Soft-delete: mark cancelled_at instead of deleting immediately.

        SDK cancel_order returning True means the CLOB accepted the cancel
        request.  The fill may still be in-flight — UserTradeEvent could
        arrive after this returns.  By keeping the order in pending_orders
        with cancelled_at set, _handle_trade can still match it and correctly
        update inventory/cost.  flush_cancelled() removes them after the
        grace period.

        Also cleans up the Pair object: clears the cancelled order_id.
        Dissolves the Pair if the cancelled order never filled and the Pair
        only has pre-filled inventory (no real fills from the other side).
        """
        ok = await self.engine.sdk.cancel_order(order_id)
        if ok:
            for ws in list(self.engine._windows.values()):
                po = ws.pending_orders.get(order_id)
                if po is None:
                    continue
                po.cancelled_at = time.time()

                # Clear the cancelled order from its Pair
                pair = self._find_pair_for_order(ws, po)
                if pair:
                    if pair.up_order_id == order_id:
                        pair.up_order_id = ""
                    elif pair.down_order_id == order_id:
                        pair.down_order_id = ""

                    # Dissolve if the cancelled order never filled and the
                    # Pair only has pre-fill on one side (step2 prefill pattern).
                    prefill_only = (
                        (pair.up_filled == pair.qty and pair.down_filled == 0)
                        or (pair.down_filled == pair.qty and pair.up_filled == 0)
                    )
                    if po.filled == 0 and prefill_only:
                        # Prefilled inventory reverts to unpaired — no lot surgery needed
                        pair.up_filled = 0
                        pair.down_filled = 0
                        ws.pairs.remove(pair)
                        logger.info(
                            "  [cancel] Pair %s dissolved (order %s had no fills)",
                            pair.pair_id, order_id[:12],
                        )
                    elif pair.up_filled == 0 and pair.down_filled == 0:
                        ws.pairs.remove(pair)
                        logger.info(
                            "  [cancel] Pair %s dissolved (no fills on either side)",
                            pair.pair_id,
                        )

                await self.engine._emit("order_cancelled", OrderCancelled(
                    window_num=ws.window_num,
                    outcome=po.side, amount=po.remaining,
                    price=po.price, order_id=order_id,
                ))
                return True
        logger.warning("  [cancel] SDK cancel_order(%s…) FAILED", order_id[:12])
        return ok

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        """Soft-cancel all pending orders, wait for in-flight fills, then flush."""
        if pending_orders:
            logger.info("  [cancel_all] Cancelling %d orders…", len(pending_orders))
        for oid in list(pending_orders):
            await self.cancel(oid)
        # Give in-flight fills time to arrive via UserTradeEvent
        await asyncio.sleep(3)
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
        """Apply a fill to a pending order and update Pair tracking."""
        actual = min(fill_size, po.remaining)
        if actual <= 0:
            return
        if actual < fill_size:
            logger.warning(
                "  [fill] Overfill capped: fill=%d > remaining=%d for %s…",
                fill_size, po.remaining, po.order_id[:12],
            )

        po.filled += actual
        ws.inventory[po.side] += actual
        ws.cost[po.side] += actual * fill_price
        ws.total_spent += actual * fill_price
        ws.trades += 1

        # Find which pair this order belongs to, if any, and update its fills
        pair = self._find_pair_for_order(ws, po)
        if pair:
            if po.order_id == pair.up_order_id:
                pair.up_filled += actual
            else:
                pair.down_filled += actual

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

    def _prune_fill_seen(self, max_count: int = 10000, max_age: float = 960):
        """Remove oldest entries when dict exceeds *max_count* or entries exceed *max_age*.

        Keeps dedup coverage for the CLOB replay window (typically ≤5 min after
        reconnect) while bounding memory.  Called on each new fill when the dict
        grows past *max_count*.
        """
        now = time.time()
        stale = [oid for oid, ts in self._fill_seen.items() if now - ts > max_age]
        for oid in stale:
            del self._fill_seen[oid]
        if len(self._fill_seen) > max_count:
            sorted_by_age = sorted(self._fill_seen.items(), key=lambda kv: kv[1])
            for oid, _ in sorted_by_age[:len(self._fill_seen) - max_count]:
                del self._fill_seen[oid]
        if stale:
            logger.info("  [fill] Pruned %d stale entries from _fill_seen  remaining=%d",
                        len(stale), len(self._fill_seen))

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
            fill_size = int(float(mo.matched_amount))
            fill_price = float(mo.price)
            if fill_size <= 0:
                continue

            # Dedup: skip historical replay from User WS reconnect
            if oid in self._fill_seen:
                continue
            self._fill_seen[oid] = time.time()
            if len(self._fill_seen) > 10000:
                self._prune_fill_seen()

            # Locate the pending order across all active windows
            found = False
            for ws in self.engine._windows.values():
                po = ws.pending_orders.get(oid)
                if po is None:
                    continue
                found = True

                self._process_fill(ws, po, fill_size, fill_price)

                await self.engine._emit("order_filled", OrderFilled(
                    window_num=ws.window_num,
                    outcome=po.side, price=fill_price, amount=fill_size,
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
                        "  [fill] UNMATCHED fill for unknown order=%s… "
                        "size=%d price=%.4f  (buffered as orphan)",
                        oid[:12], fill_size, fill_price,
                    )
