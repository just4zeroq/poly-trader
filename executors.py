"""
Order execution — live trading via Polymarket CLOB.

LiveExecutor — REST API + WS user channel for fill tracking.

Multi-market support: place() takes a slug parameter to identify which
window the order belongs to. cancel() searches across all active windows.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional

from polymarket.models.clob.user_events import UserTradeEvent

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
                    price: float, amount: int,
                    pairing_lot_id: str | None = None) -> Optional[str]:
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
        """Create a Lot for this fill and update paired lot if pairing."""
        lot_id = f"lot_{ws.window_num}_{po.side}_{len(ws.lots)}"
        lot = Lot(
            lot_id=lot_id, side=po.side,
            amount=fill_size, price=fill_price,
            paired_qty=0, created_at=time.time(),
        )
        ws.lots.append(lot)

        # If this was a pairing order, mark the paired lot AND mirror the
        # paired_qty on the fill-side lot so both sides reflect the truth.
        # Cap at lot.amount to guard against race: a cancelled pairing order's
        # in-flight fill and a replacement pairing order both incrementing the
        # same lot's paired_qty (see review bug #1).
        if po.pairing_lot_id:
            for existing in ws.lots:
                if existing.lot_id == po.pairing_lot_id:
                    capped = min(fill_size, existing.amount - existing.paired_qty)
                    existing.paired_qty += capped
                    lot.paired_qty += capped  # mirror on the fill lot
                    break


class LiveExecutor(OrderExecutor):
    """Live trading — SDK place_limit_order + cancel_order."""

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int,
                    pairing_lot_id: str | None = None) -> Optional[str]:
        ws = self.engine._windows.get(slug)
        if ws is None:
            return None
        # Enforce one pending order per side per *role*.
        # Pairing orders (pairing_lot_id is not None) have priority: they can
        # coexist alongside a cheap order on the same side.  Cheap orders are
        # blocked by any existing order (cheap or pairing) on the same side.
        if pairing_lot_id is not None:
            # Pairing — only block if another pairing order exists on this side
            if any(po.side == outcome and po.cancelled_at == 0
                   and po.pairing_lot_id is not None
                   for po in ws.pending_orders.values()):
                return None
        else:
            # Cheap — blocked by any pending order on this side
            if any(po.side == outcome and po.cancelled_at == 0
                   for po in ws.pending_orders.values()):
                return None
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
                pairing_lot_id=pairing_lot_id,
            )
            await self.engine._emit("order_placed", OrderPlaced(
                window_num=ws.window_num,
                outcome=outcome, side="BUY",
                price=price, amount=amount,
                order_id=oid,
            ))
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
        return ok

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        """Soft-cancel all pending orders, wait for in-flight fills, then flush."""
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

    async def handle_user_event(self, event):
        """Process authenticated user channel events for fill tracking."""
        if isinstance(event, UserTradeEvent):
            await self._handle_trade(event.payload)

    async def _handle_trade(self, payload):
        """Match a UserTradeEvent against our pending orders."""
        if not payload.maker_orders:
            return
        for mo in payload.maker_orders:
            oid = mo.order_id
            fill_size = int(float(mo.matched_amount))
            fill_price = float(mo.price)
            if fill_size <= 0:
                continue
            # Locate the pending order across all active windows
            for ws in self.engine._windows.values():
                po = ws.pending_orders.get(oid)
                if po is None:
                    continue
                po.filled += fill_size
                ws.inventory[po.side] += fill_size
                ws.cost[po.side] += fill_size * fill_price
                ws.total_spent += fill_size * fill_price
                ws.trades += 1

                # Create lot and handle pairing
                self._create_lot_and_pair(ws, po, fill_size, fill_price)

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
