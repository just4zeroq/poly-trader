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

    def _create_lot_and_pair(self, ws: WindowState, po: PendingOrder,
                              fill_size: int, fill_price: float):
        """Create a Lot for this fill and pair with any unpaired opposite-side lot."""
        lot_id = f"lot_{ws.window_num}_{po.side}_{len(ws.lots)}"
        lot = Lot(
            lot_id=lot_id, side=po.side,
            amount=fill_size, price=fill_price,
            paired_qty=0, created_at=time.time(),
        )
        ws.lots.append(lot)

        # Generic pairing: find any unpaired opposite-side lot
        opp_side = "Down" if po.side == "Up" else "Up"
        paired_with = None
        for existing in ws.lots:
            if (existing is not lot
                    and existing.side == opp_side
                    and existing.unpaired_qty > 0):
                capped = min(fill_size, existing.unpaired_qty)
                existing.paired_qty += capped
                lot.paired_qty += capped
                paired_with = existing.lot_id
                break

        if paired_with:
            logger.info(
                "  [pair] %s lot=%s paired %d/%d with %s  "
                "unpaired remaining: Up=%d Down=%d",
                po.side, lot_id, lot.paired_qty, lot.amount,
                paired_with,
                sum(l.unpaired_qty for l in ws.lots if l.side == "Up"),
                sum(l.unpaired_qty for l in ws.lots if l.side == "Down"),
            )
        else:
            logger.info(
                "  [pair] %s lot=%s (%d @ %.4f) UNPAIRED — no opposite-side open lot  "
                "unpaired: Up=%d Down=%d",
                po.side, lot_id, fill_size, fill_price,
                sum(l.unpaired_qty for l in ws.lots if l.side == "Up"),
                sum(l.unpaired_qty for l in ws.lots if l.side == "Down"),
            )


class LiveExecutor(OrderExecutor):
    """Live trading — SDK place_limit_order + cancel_order."""

    def __init__(self):
        super().__init__()
        # Orphan fill buffer: fills that arrived before pending_orders entry
        self._orphan_fills: dict[str, tuple] = {}
        # Dedup set: skip historical fill replay on User WS reconnect
        self._fill_seen: set[str] = set()

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int) -> Optional[str]:
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
            ws.pending_orders[oid] = PendingOrder(
                order_id=oid, token_id=token_id,
                side=outcome, buy_sell="BUY",
                price=price, amount=amount,
                placed_at=time.time(),
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

    async def cancel(self, order_id: str) -> bool:
        """Soft-delete: mark cancelled_at instead of deleting immediately.

        SDK cancel_order returning True means the CLOB accepted the cancel
        request.  The fill may still be in-flight — UserTradeEvent could
        arrive after this returns.  By keeping the order in pending_orders
        with cancelled_at set, _handle_trade can still match it and correctly
        update inventory/cost.  flush_cancelled() removes them after the
        grace period.
        """
        ok = await self.engine.sdk.cancel_order(order_id)
        if ok:
            for ws in list(self.engine._windows.values()):
                po = ws.pending_orders.get(order_id)
                if po is None:
                    continue
                po.cancelled_at = time.time()
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
        # Force-clear any stragglers (failed cancels that couldn't be soft-deleted)
        for oid in list(pending_orders):
            pending_orders.pop(oid, None)

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
        """Apply a fill to a pending order (shared by _handle_trade and orphan replay)."""
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

        self._create_lot_and_pair(ws, po, actual, fill_price)

        remaining = po.amount - po.filled
        logger.info(
            "  [fill] %s %d @ %.4f  order=%s…  "
            "inv=(U=%d/D=%d) remaining=%d",
            po.side, actual, fill_price, po.order_id[:12],
            ws.inventory["Up"], ws.inventory["Down"],
            remaining,
        )

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
            self._fill_seen.add(oid)
            if len(self._fill_seen) > 2000:
                self._fill_seen.clear()

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
