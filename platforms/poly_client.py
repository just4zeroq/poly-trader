"""
SDK-based client for Polymarket — wraps AsyncPublicClient + AsyncSecureClient.

Market discovery:   ``get_market_by_slug()``, ``find_market_for_spec()``
Streaming:          ``subscribe(token_ids)`` → ``SubscriptionHandle``
Order execution:    ``place_limit_order()``, ``cancel_order()`` (via SDK's EIP-712 signing)
"""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from typing import Optional

from polymarket.clients.async_public import AsyncPublicClient
from polymarket.clients.async_secure import AsyncSecureClient
from polymarket.models.clob.market_events import (
    MarketBestBidAskEvent,
    MarketBestBidAskPayload,
    MarketLastTradePriceEvent,
    MarketLastTradePricePayload,
)
from polymarket.models.clob.order_book import OrderBook
from polymarket.models.clob.order_response import AcceptedOrder, OrderResponse, RejectedOrder
from polymarket.models.clob.orders import SignedOrder
from polymarket.streams._specs import MarketSpec as SdkMarketSpec

from polymarket.streams._specs import UserSpec

from .config import Config
from .models import MarketInfo, MarketSpec, PendingOrder

logger = logging.getLogger(__name__)


class SdkClient:
    """Wraps AsyncPublicClient (data) + AsyncSecureClient (trading).

    Usage::

        sdk = SdkClient(cfg)
        await sdk.create_secure(cfg)       # only if credentials available
        market = await sdk.get_market_by_slug("btc-updown-15m-12345")
        handle = await sdk.subscribe([token_id])
        async for event in handle:
            ...
        await sdk.close()
    """

    def __init__(self, cfg: Config):
        self.public = AsyncPublicClient()
        self._secure: Optional[AsyncSecureClient] = None
        self._tick_sizes: dict[str, float] = {}  # token_id → tick size

    # ── Auth ──

    async def create_secure(self, cfg: Config):
        """Create the authenticated client (for live trading).

        Note: do NOT pass wallet=cfg.wallet_address — the SDK derives
        the deposit wallet automatically (required by Polymarket CLOB).
        """
        if not cfg.has_credentials:
            return
        self._secure = await AsyncSecureClient.create(
            private_key=cfg.private_key,
        )
        logger.info("Secure client created (wallet=%s)", self._secure._ctx.wallet)

    @property
    def is_secure(self) -> bool:
        return self._secure is not None

    # ── Market discovery ──

    async def get_market_by_slug(self, slug: str) -> Optional[MarketInfo]:
        """Query SDK for a market by slug.

        Returns a MarketInfo with token IDs for Up/Down outcomes.
        """
        try:
            market = await self.public.get_market(slug=slug)
        except Exception as e:
            logger.debug("get_market_by_slug(%s) not found: %s", slug, e)
            return None

        if not market or not market.outcomes:
            return None

        tokens: dict[str, dict] = {}
        if market.outcomes.yes and market.outcomes.yes.token_id:
            tokens["Up"] = {
                "token_id": str(market.outcomes.yes.token_id),
                "outcome": "Up",
            }
        if market.outcomes.no and market.outcomes.no.token_id:
            tokens["Down"] = {
                "token_id": str(market.outcomes.no.token_id),
                "outcome": "Down",
            }
        if not tokens or "Up" not in tokens or "Down" not in tokens:
            return None

        return MarketInfo(
            slug=slug,
            condition_id=str(market.condition_id) if market.condition_id else "",
            tokens=tokens,
            open_time=str(market.state.start_date) if market.state and market.state.start_date else None,
            close_time=str(market.state.end_date) if market.state and market.state.end_date else None,
        )

    async def get_resolved_winner(self, slug: str) -> Optional[str]:
        """Query Gamma API for a market's resolved outcome.

        Returns "Up", "Down", or None if not yet resolved.
        Used at settlement time to get the actual winner instead of guessing from WS midpoints.
        """
        try:
            market = await self.public.get_market(slug=slug)
        except Exception:
            logger.debug("get_resolved_winner(%s): API query failed", slug)
            return None

        if not market or not market.outcomes:
            return None

        yes_outcome = market.outcomes.yes
        no_outcome = market.outcomes.no
        if not yes_outcome or not no_outcome:
            return None

        yes_price = yes_outcome.price
        no_price = no_outcome.price

        # Resolved: winning side at $1, loser at $0
        if yes_price is not None and float(yes_price) >= 0.99:
            return "Up"
        if no_price is not None and float(no_price) >= 0.99:
            return "Down"

        # Market closed but prices not yet at extremes — use which is higher
        if market.state and market.state.closed:
            if yes_price is not None and no_price is not None:
                yp = float(yes_price)
                np = float(no_price)
                if yp > np:
                    return "Up"
                elif np > yp:
                    return "Down"

        return None

    async def find_market_for_spec(self, spec: MarketSpec) -> Optional[MarketInfo]:
        """Find the current market window for a given spec.

        Tries the aligned slug first, then searches back up to 3 windows.
        Only returns windows that are still active (not yet ended).
        """
        now = int(time.time())
        duration = spec.duration_min * 60
        ws_ts = (now // duration) * duration

        for offset in range(4):  # current window + 3 back
            ts = ws_ts - (offset * duration)
            slug = f"{spec.slug_pattern}-{ts}"
            market = await self.get_market_by_slug(slug)
            if not market:
                continue
            if market.window_end > now:
                if offset > 0:
                    logger.info("Found %s at offset %d: %s",
                                spec.slug_pattern, offset, slug)
                return market
            logger.info("Skipping expired window: %s (ended at %d)",
                        slug, market.window_end)
        return None

    async def discover_markets(self, specs: list[MarketSpec]) -> dict[str, MarketInfo]:
        """Discover markets for all specs at once.

        Returns dict mapping slug_pattern → MarketInfo for found markets.
        """
        results: dict[str, MarketInfo] = {}
        for spec in specs:
            market = await self.find_market_for_spec(spec)
            if market:
                results[spec.slug_pattern] = market
            else:
                logger.warning("No market found for %s", spec)
        return results

    # ── WebSocket subscription ──

    async def subscribe(self, token_ids: list[str]):
        """Subscribe to real-time market data for the given token IDs.

        Returns a SubscriptionHandle.  Usage::

            handle = await sdk.subscribe([token_id_1, token_id_2])
            async for event in handle:
                if isinstance(event, MarketBestBidAskEvent):
                    ...
        """
        spec = SdkMarketSpec(
            token_ids=token_ids,
            custom_feature_enabled=True,
        )
        return await self.public.subscribe(spec)

    # ── User channel (fill tracking) ──

    async def subscribe_user(self):
        """Subscribe to authenticated user order/trade events.

        Returns a SubscriptionHandle[UserEvent] for tracking fills.
        Requires a secure client (created via create_secure).
        """
        if not self._secure:
            raise RuntimeError("No secure client — cannot subscribe to user events")
        return await self._secure.subscribe(UserSpec())

    # ── Order book (REST fallback) ──

    async def get_order_book(self, token_id: str) -> Optional[OrderBook]:
        """Fetch L2 order book for a token via REST."""
        try:
            return await self.public.get_order_book(token_id=token_id)
        except Exception as e:
            logger.warning("get_order_book(%s…) failed: %s", token_id[:12], e)
            return None

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """Fetch midpoint price for a token via REST."""
        try:
            val = await self.public.get_midpoint(token_id=token_id)
            return float(val) if val is not None else None
        except Exception as e:
            logger.warning("get_midpoint(%s…) failed: %s", token_id[:12], e)
            return None

    # ── Tick size ──

    async def get_tick_size(self, token_id: str) -> float:
        """Get tick size for a token (cached)."""
        cached = self._tick_sizes.get(token_id)
        if cached is not None:
            return cached
        try:
            ts = await self.public.get_tick_size(token_id=token_id)
            if ts is not None:
                self._tick_sizes[token_id] = float(ts)
                return float(ts)
        except Exception:
            pass
        # Fallback: direct HTTP call to CLOB API
        try:
            import json, urllib.request
            url = f"https://clob.polymarket.com/tick-size?token_id={token_id}"
            req = urllib.request.Request(url, headers={"User-Agent": "python"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                ts = float(data["minimum_tick_size"])
                self._tick_sizes[token_id] = ts
                return ts
        except Exception:
            pass
        return 0.01  # safe default for most Polymarket binary markets

    @staticmethod
    def round_to_tick(price: float, tick_size: float) -> float:
        """Round a price to the nearest valid tick."""
        if tick_size <= 0:
            return round(price, 4)
        price = round(price / tick_size) * tick_size
        # Determine decimal places from tick_size
        tick_str = f"{tick_size:.10f}".rstrip("0")
        decimals = len(tick_str.split(".")[1]) if "." in tick_str else 0
        return round(price, decimals)

    # ── Order execution ──

    async def load_current_state(self, market: MarketInfo) -> Optional[dict]:
        """Load current positions + open orders for a market from the CLOB.

        Called at engine startup to restore WindowState mid-window so the
        strategy doesn't re-buy what's already held.

        Returns dict with::

            inventory:  {"Up": int, "Down": int}
            avg_cost:   {"Up": float, "Down": float}
            pending:    dict[order_id, PendingOrder]
            balance:    int  (raw USDC.e units, 1e6 = $1)

        Returns None if secure client is unavailable or any API call fails.
        """
        if not self._secure:
            return None

        up_tid = market.up_token_id
        down_tid = market.down_token_id
        cid = market.condition_id

        inventory: dict[str, int] = {"Up": 0, "Down": 0}
        avg_cost: dict[str, float] = {"Up": 0.0, "Down": 0.0}
        pending: dict[str, PendingOrder] = {}
        balance = 0

        try:
            # ── 1. Open orders → pending_orders ──
            paginator = self._secure.list_open_orders(market=cid)
            async for page in paginator:
                for order in page.items:
                    oid = order.id
                    tid = str(order.token_id)
                    remaining = int(order.original_size - order.size_matched)
                    if remaining <= 0:
                        continue
                    side = "Up" if tid == up_tid else ("Down" if tid == down_tid else None)
                    if side is None:
                        continue
                    pending[oid] = PendingOrder(
                        order_id=oid,
                        token_id=tid,
                        side=side,
                        buy_sell="BUY",
                        price=float(order.price),
                        amount=int(order.original_size),
                        filled=int(order.size_matched),
                        placed_at=order.created_at.timestamp() if order.created_at else 0,
                    )

            # ── 2. Positions → inventory + avg_cost ──
            pos_paginator = self._secure.list_positions(market=[cid])
            async for page in pos_paginator:
                for pos in page.items:
                    if not pos.token_id:
                        continue
                    tid = str(pos.token_id)
                    if tid not in (up_tid, down_tid):
                        continue
                    sz = int(pos.size) if pos.size else 0
                    if sz <= 0:
                        continue
                    if tid == up_tid:
                        inventory["Up"] = sz
                        avg_cost["Up"] = float(pos.avg_price) if pos.avg_price else 0.0
                    elif tid == down_tid:
                        inventory["Down"] = sz
                        avg_cost["Down"] = float(pos.avg_price) if pos.avg_price else 0.0

            # ── 3. Wallet balance (raw USDC.e units, 1e6 = $1) ──
            bal = await self._secure.get_balance_allowance(asset_type="COLLATERAL")
            balance = bal.balance

        except Exception as e:
            logger.warning("load_current_state failed: %s", e)
            return None

        return {
            "inventory": inventory,
            "avg_cost": avg_cost,
            "pending": pending,
            "balance": balance,
        }

    async def place_limit_order(
        self, token_id: str, side: str, price: float, size: int,
    ) -> Optional[str]:
        """Place a maker limit order via SDK.

        Returns order_id on success, None on failure.
        """
        if not self._secure:
            logger.error("Cannot place order: no secure client")
            return None
        tick_size = await self.get_tick_size(token_id)
        price_raw = price
        price = self.round_to_tick(price, tick_size)
        if price != price_raw:
            logger.info(
                "  [sdk] Price rounded from %.6f to %.6f (tick=%.4f) token=%s",
                price_raw, price, tick_size, token_id,
            )
        logger.info(
            "  [sdk] place_limit_order  token=%s side=%s price=%.6f size=%d "
            "post_only=True tick=%.4f",
            token_id, side, price, size, tick_size,
        )
        try:
            result = await self._secure.place_limit_order(
                token_id=token_id,
                price=str(price),
                size=str(size),
                side=side,
                post_only=True,
            )
            if isinstance(result, AcceptedOrder):
                logger.info(
                    "  [sdk] Order ACCEPTED  id=%s  token=%s  price=%.6f size=%d",
                    result.order_id, token_id, price, size,
                )
                return result.order_id
            if isinstance(result, RejectedOrder):
                logger.warning(
                    "  [sdk] Order REJECTED: code=%s msg=%s "
                    "token=%s side=%s price=%.6f size=%d post_only=True",
                    result.code, result.message,
                    token_id, side, price, size,
                )
                return None
            # Fallback for unexpected response types
            logger.warning("  [sdk] Unexpected order response: %r", result)
            return None
        except Exception as e:
            logger.warning(
                "  [sdk] place_limit_order EXCEPTION: %s\n"
                "    token=%s side=%s price=%.6f size=%d post_only=True\n%s",
                e, token_id, side, price, size, traceback.format_exc(),
            )
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order via SDK.

        Returns True if the cancel request was accepted by the CLOB (HTTP 2xx).
        """
        if not self._secure:
            logger.warning("  [sdk] cancel_order(%s…) FAILED: no secure client", order_id[:12])
            return False
        logger.info("  [sdk] cancel_order(%s…)", order_id[:12])
        try:
            await self._secure.cancel_order(order_id=order_id)
            logger.info("  [sdk] cancel_order(%s…) OK", order_id[:12])
            return True
        except Exception as e:
            logger.warning("  [sdk] cancel_order(%s…) failed: %s", order_id[:12], e)
            return False

    async def cancel_orders_batch(self, order_ids: list[str]) -> int:
        """Cancel multiple orders in one API call.

        Returns the number of orders successfully cancelled.
        """
        if not self._secure:
            logger.warning("  [sdk] cancel_orders_batch FAILED: no secure client")
            return 0
        if not order_ids:
            return 0
        logger.info("  [sdk] cancel_orders_batch(%d orders)", len(order_ids))
        try:
            resp = await self._secure.cancel_orders(order_ids=order_ids)
            cancelled = len(resp.canceled) if hasattr(resp, 'canceled') else 0
            if cancelled > 0:
                logger.info("  [sdk] cancel_orders_batch: %d/%d cancelled",
                            cancelled, len(order_ids))
            return int(cancelled)
        except Exception as e:
            logger.warning("  [sdk] cancel_orders_batch failed: %s", e)
            return 0

    async def cancel_all_open_orders(self, market: Optional[str] = None) -> int:
        """Cancel open orders, optionally scoped to a market (condition ID).

        When market is given, lists open orders for that market and cancels
        each individually (safer for multi-bot setups).  Otherwise cancels
        ALL orders on the CLOB (legacy behavior).

        Returns the number of orders cancelled.
        """
        if not self._secure:
            logger.warning("  [sdk] cancel_all_open_orders FAILED: no secure client")
            return 0

        if market:
            return await self._cancel_by_market(market)

        logger.info("  [sdk] cancel_all_open_orders…")
        try:
            resp = await self._secure.cancel_all()
            cancelled = len(resp.canceled) if hasattr(resp, 'canceled') else 0
            if cancelled > 0:
                logger.warning("  [sdk] cancel_all: %d lingering orders cancelled", cancelled)
            else:
                logger.info("  [sdk] cancel_all: no lingering orders")
            return int(cancelled)
        except Exception as e:
            logger.warning("  [sdk] cancel_all failed: %s", e)
            return 0

    async def _cancel_by_market(self, cid: str) -> int:
        """Cancel only orders belonging to a specific market (condition ID)."""
        cancelled = 0
        try:
            paginator = self._secure.list_open_orders(market=cid)
            async for page in paginator:
                for order in page.items:
                    ok = await self.cancel_order(order.id)
                    if ok:
                        cancelled += 1
            if cancelled > 0:
                logger.info("  [sdk] cancelled %d orders for market %s", cancelled, cid[:12])
            return cancelled
        except Exception as e:
            logger.warning("  [sdk] _cancel_by_market failed: %s", e)
            return cancelled

    async def get_open_orders(self) -> list:
        """Fetch all open orders (uses the underlying public client's method)."""
        # SDK's get_open_orders requires the secure client
        if not self._secure:
            return []
        try:
            paginator = self._secure.list_open_orders()
            return list(paginator)
        except Exception as e:
            logger.warning("get_open_orders failed: %s", e)
            return []

    # ── Paired order submission ──

    async def create_signed_limit_order(
        self, token_id: str, side: str, price: float, size: int,
        post_only: bool = True,
    ) -> Optional[SignedOrder]:
        """Create and sign a limit order without submitting.

        Returns SignedOrder for batch submission via submit_orders().
        Use when you want to submit multiple orders together (e.g. Up+Down pair).
        """
        if not self._secure:
            logger.error("Cannot create signed order: no secure client")
            return None
        tick_size = await self.get_tick_size(token_id)
        price_raw = price
        price = self.round_to_tick(price, tick_size)
        if price != price_raw:
            logger.info(
                "  [sdk] Signed order price rounded from %.6f to %.6f (tick=%.4f) token=%s",
                price_raw, price, tick_size, token_id,
            )
        logger.info(
            "  [sdk] create_signed_limit_order  token=%s side=%s price=%.6f size=%d "
            "post_only=%s tick=%.4f",
            token_id, side, price, size, post_only, tick_size,
        )
        try:
            signed = await self._secure.create_limit_order(
                token_id=token_id,
                price=str(price),
                size=str(size),
                side=side,
                post_only=post_only,
            )
            logger.info(
                "  [sdk] Signed order created  token=%s side=%s price=%.6f size=%d",
                token_id, side, price, size,
            )
            return signed
        except Exception as e:
            logger.warning(
                "  [sdk] create_signed_limit_order EXCEPTION: %s\n"
                "    token=%s side=%s price=%.6f size=%d post_only=%s",
                e, token_id, side, price, size, post_only,
            )
            return None

    async def submit_orders(self, signed_orders: list[SignedOrder]) -> list[Optional[str]]:
        """Submit multiple signed orders together via post_orders.

        Orders arrive at the CLOB in the same HTTP request, minimizing the
        race window between them.  Returns a list of order_ids in the same
        order as the input (None for rejected/failed orders).
        """
        if not self._secure:
            logger.error("Cannot submit orders: no secure client")
            return [None] * len(signed_orders)
        logger.info("  [sdk] submit_orders: %d orders", len(signed_orders))
        try:
            responses: tuple[OrderResponse, ...] = await self._secure.post_orders(signed_orders)
            results = []
            for i, resp in enumerate(responses):
                if isinstance(resp, AcceptedOrder):
                    logger.info(
                        "  [sdk] Order %d ACCEPTED  id=%s", i, resp.order_id,
                    )
                    results.append(resp.order_id)
                else:
                    code = getattr(resp, 'code', 'unknown')
                    msg = getattr(resp, 'message', '')
                    logger.warning(
                        "  [sdk] Order %d REJECTED: code=%s msg=%s", i, code, msg,
                    )
                    results.append(None)
            return results
        except Exception as e:
            logger.warning("  [sdk] submit_orders EXCEPTION: %s", e)
            return [None] * len(signed_orders)

    # ── Reconciliation ──

    async def get_open_order_ids(self, condition_id: str) -> set[str]:
        """Return the set of open order IDs for a market (reconciliation helper).

        Only returns orders with remaining > 0.
        """
        if not self._secure:
            return set()
        ids: set[str] = set()
        try:
            paginator = self._secure.list_open_orders(market=condition_id)
            async for page in paginator:
                for order in page.items:
                    remaining = int(order.original_size) - int(order.size_matched)
                    if remaining > 0:
                        ids.add(order.id)
        except Exception as e:
            logger.warning("get_open_order_ids failed: %s", e)
        return ids

    async def get_order_filled(self, order_id: str) -> Optional[int]:
        """Query a single order's filled amount via REST.

        Returns cumulative filled size, or None if the order can't be found.
        """
        if not self._secure:
            return None
        try:
            order = await self._secure.get_order(order_id=order_id)
            return int(order.size_matched) if order else None
        except Exception as e:
            logger.debug("get_order(%s…) failed: %s", order_id[:12], e)
            return None

    async def get_positions(self, condition_id: str,
                            up_tid: str, down_tid: str) -> dict[str, int]:
        """Return current positions for a market: {"Up": int, "Down": int}."""
        if not self._secure:
            return {"Up": 0, "Down": 0}
        result = {"Up": 0, "Down": 0}
        try:
            paginator = self._secure.list_positions(market=[condition_id])
            async for page in paginator:
                for pos in page.items:
                    if not pos.token_id:
                        continue
                    tid = str(pos.token_id)
                    sz = int(pos.size) if pos.size else 0
                    if tid == up_tid:
                        result["Up"] = sz
                    elif tid == down_tid:
                        result["Down"] = sz
        except Exception as e:
            logger.warning("get_positions failed: %s", e)
        return result

    # ── Cleanup ──

    async def close(self):
        if self._secure:
            await self._secure.close()
        await self.public.close()
