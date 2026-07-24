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
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder
from polymarket.streams._specs import MarketSpec as SdkMarketSpec

from polymarket.streams._specs import UserSpec

from .config import Config
from .models import MarketInfo, MarketSpec

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
        price = self.round_to_tick(price, tick_size)
        try:
            result = await self._secure.place_limit_order(
                token_id=token_id,
                price=str(price),
                size=str(size),
                side=side,
                post_only=True,
            )
            if isinstance(result, AcceptedOrder):
                return result.order_id
            if isinstance(result, RejectedOrder):
                logger.warning("Order REJECTED: code=%s msg=%s token=%s… side=%s price=%s size=%s",
                               result.code, result.message, token_id[:12], side, price, size)
                return None
            # Fallback for unexpected response types
            logger.warning("Unexpected order response: %r", result)
            return None
        except Exception as e:
            logger.warning("place_limit_order exception: %s", e)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order via SDK.

        Returns True if the cancel request was accepted by the CLOB (HTTP 2xx).
        """
        if not self._secure:
            return False
        try:
            await self._secure.cancel_order(order_id=order_id)
            return True
        except Exception as e:
            logger.warning("cancel_order(%s…) failed: %s", order_id[:12], e)
            return False

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

    # ── Cleanup ──

    async def close(self):
        if self._secure:
            await self._secure.close()
        await self.public.close()
