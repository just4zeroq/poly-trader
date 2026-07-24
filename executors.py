"""
Pluggable order execution for paper or live trading.

PaperExecutor  — trades matched against real WS trade events
LiveExecutor   — REST API + WS user channel

Multi-market support: place() takes a slug parameter to identify which
window the order belongs to. cancel() searches across all active windows.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Optional

from polymarket.models.clob.user_events import UserTradeEvent

from .models import (
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
    """Base executor. Subclasses implement place/cancel/on_trade."""

    def __init__(self):
        self.engine: TradingEngine | None = None

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int) -> Optional[str]:
        raise NotImplementedError

    async def cancel(self, order_id: str) -> bool:
        raise NotImplementedError

    async def on_trade(self, asset_id: str, price: float):
        """Optional callback for trade events."""
        pass

    async def try_fill_pending(self, ws: WindowState):
        """Periodic fill check — no-op in base."""
        pass

    async def handle_user_event(self, event):
        """Handle authenticated user WS event — no-op in base."""
        pass

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        """Cancel all orders in the given pending dict."""
        for oid in list(pending_orders):
            await self.cancel(oid)


class PaperExecutor(OrderExecutor):
    """Paper trading — orders sit pending until matched by real WS trades."""

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int) -> Optional[str]:
        ws = self.engine._windows.get(slug)
        if ws is None:
            return None
        # One pending order per side at a time
        if any(po.side == outcome for po in ws.pending_orders.values()):
            return None
        oid = (
            f"paper_{ws.window_num}"
            f"_{outcome}_{int(time.time() * 1000_000) % 100000}"
        )
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
            order_id=oid, is_paper=True, is_filled=False,
        ))
        return oid

    async def cancel(self, order_id: str) -> bool:
        """Find order across all active windows and cancel it."""
        for ws in list(self.engine._windows.values()):
            po = ws.pending_orders.get(order_id)
            if po is None:
                continue
            del ws.pending_orders[order_id]
            await self.engine._emit("order_cancelled", OrderCancelled(
                window_num=ws.window_num,
                outcome=po.side, amount=po.remaining,
                price=po.price, order_id=order_id,
            ))
            return True
        return False

    async def on_trade(self, asset_id: str, price: float):
        """Match pending orders when a real WS trade arrives.

        Both sides are equally competitive (both priced at 30% into
        their respective spreads), so fill probability is symmetric —
        NOT based on comparing against the trade price (which creates
        false asymmetry in near-50/50 markets).
        """
        for ws in list(self.engine._windows.values()):
            for oid in list(ws.pending_orders):
                po = ws.pending_orders[oid]
                if po.token_id != asset_id:
                    continue

                # Symmetric: both sides equally competitive (30% into spread)
                age = time.time() - po.placed_at
                fill_prob = min(0.60, 0.25 + age * 0.005)

                if random.random() > fill_prob:
                    continue

                ws.inventory[po.side] += po.amount
                ws.cost[po.side] += po.amount * po.price
                ws.total_spent += po.amount * po.price
                ws.trades += 1
                del ws.pending_orders[oid]
                await self.engine._emit("order_filled", OrderFilled(
                    window_num=ws.window_num,
                    outcome=po.side, price=po.price, amount=po.amount,
                    order_id=oid,
                    total_inv_up=ws.inventory["Up"],
                    total_inv_down=ws.inventory["Down"],
                ))

    async def try_fill_pending(self, ws: WindowState):
        """Gentle fallback filler for stale orders.

        Primary fills come from ``on_trade`` (real WS trade events).
        This catches orders that slipped through — time-dependent fill
        probability so stale orders eventually clear:

          fresh  → 5%/tick   (on_trade is the primary path)
          30s    → ~14%/tick
          60s+   → 30%/tick  (cap)
        """
        for oid in list(ws.pending_orders):
            po = ws.pending_orders[oid]
            age = time.time() - po.placed_at

            # Low base rate — on_trade handles the normal case
            fill_prob = min(0.30, 0.05 + age * 0.003)

            if random.random() > fill_prob:
                continue

            ws.inventory[po.side] += po.amount
            ws.cost[po.side] += po.amount * po.price
            ws.total_spent += po.amount * po.price
            ws.trades += 1
            del ws.pending_orders[oid]
            await self.engine._emit("order_filled", OrderFilled(
                window_num=ws.window_num,
                outcome=po.side, price=po.price, amount=po.amount,
                order_id=oid,
                total_inv_up=ws.inventory["Up"],
                total_inv_down=ws.inventory["Down"],
            ))


class LiveExecutor(OrderExecutor):
    """Live trading — SDK place_limit_order + cancel_order."""

    async def place(self, slug: str, token_id: str, outcome: str,
                    price: float, amount: int) -> Optional[str]:
        ws = self.engine._windows.get(slug)
        if ws is None:
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
            )
            await self.engine._emit("order_placed", OrderPlaced(
                window_num=ws.window_num,
                outcome=outcome, side="BUY",
                price=price, amount=amount,
                order_id=oid, is_paper=False,
            ))
            return oid

        await self.engine._emit("order_failed", OrderFailed(
            window_num=ws.window_num,
            outcome=outcome, price=price, amount=amount,
            reason="SDK place failed",
        ))
        return None

    async def cancel(self, order_id: str) -> bool:
        ok = await self.engine.sdk.cancel_order(order_id)
        if ok:
            for ws in list(self.engine._windows.values()):
                po = ws.pending_orders.get(order_id)
                if po is None:
                    continue
                del ws.pending_orders[order_id]
                await self.engine._emit("order_cancelled", OrderCancelled(
                    window_num=ws.window_num,
                    outcome=po.side, amount=po.remaining,
                    price=po.price, order_id=order_id,
                ))
                return True
        return ok

    async def cancel_all(self, pending_orders: dict[str, PendingOrder]):
        await super().cancel_all(pending_orders)
        await asyncio.sleep(1)

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
                await self.engine._emit("order_filled", OrderFilled(
                    window_num=ws.window_num,
                    outcome=po.side, price=fill_price, amount=fill_size,
                    order_id=oid,
                    total_inv_up=ws.inventory["Up"],
                    total_inv_down=ws.inventory["Down"],
                ))
                if po.remaining <= 0:
                    del ws.pending_orders[oid]
                return  # order ID is unique — stop scanning
