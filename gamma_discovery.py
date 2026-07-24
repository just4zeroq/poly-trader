"""
Gamma API market discovery for Polymarket updown markets.

Queries ``gamma-api.polymarket.com/events`` by slug to find the currently
active market windows.

Usage::
    from poly_trader.gamma_discovery import discover_markets

    results = await discover_markets([MarketSpec("BTC", 5, "btc-updown-5m")])
    # → [{"condition_id": ..., "slug": ..., "tokens": [...], ...}]
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from .models import MarketSpec

logger = logging.getLogger("poly_trader.gamma")

# Gamma API base URL
GAMMA_API = "https://gamma-api.polymarket.com"

# How many seconds to try backwards when slug doesn't match
MAX_RETRY_WINDOWS = 3


def compute_window_timestamp(duration_min: int) -> int:
    """Compute the current aligned window start timestamp."""
    now = int(time.time())
    window_sec = duration_min * 60
    return (now // window_sec) * window_sec


async def fetch_event_by_slug(slug: str) -> Optional[dict]:
    """Query Gamma API for a single event by slug."""
    url = f"{GAMMA_API}/events?slug={slug}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning("Gamma API %s returned %d", slug, resp.status)
                    return None
                data = await resp.json()
                return data[0] if data else None
    except Exception as e:
        logger.warning("Gamma API query %s failed: %s", slug, e)
        return None


async def discover_markets(
    specs: list[MarketSpec],
) -> list[dict]:
    """
    Find current market info for each MarketSpec via Gamma API.

    Returns a list of dicts::
        {
            "spec": MarketSpec,
            "condition_id": str,
            "slug": str,
            "tokens": [{"token_id": str, "outcome": str}, ...],
            "end_date_iso": str | None,
        }
    """
    results: list[dict] = []

    for spec in specs:
        slug_base = spec.slug_pattern
        duration = spec.duration_min

        # Try current window, then go back up to MAX_RETRY_WINDOWS windows
        found = None
        for offset in range(MAX_RETRY_WINDOWS + 1):
            ts = compute_window_timestamp(duration) - (offset * duration * 60)
            slug = f"{slug_base}-{ts}"
            ev = await fetch_event_by_slug(slug)
            if ev and ev.get("markets"):
                logger.info("Found market: %s (slug=%s, offset=%d)", spec, slug, offset)
                found = ev
                break
            logger.debug("No market for slug=%s", slug)

        if not found:
            logger.warning("No current market found for %s", spec)
            continue

        market = found["markets"][0]
        condition_id = market.get("conditionId")
        if not condition_id:
            logger.warning("Market for %s has no conditionId", spec)
            continue

        # Parse tokens from Gamma format
        import json as _json
        raw_ids = market.get("clobTokenIds", "[]")
        raw_outcomes = market.get("outcomes", '["Up","Down"]')
        token_ids = _json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        outcomes = _json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

        tokens = [
            {"token_id": token_ids[i], "outcome": outcomes[i]}
            for i in range(min(len(token_ids), len(outcomes)))
        ]

        results.append({
            "spec": spec,
            "condition_id": condition_id,
            "slug": found.get("slug", slug),
            "tokens": tokens,
            "end_date_iso": found.get("end_date_iso") or market.get("endDateIso"),
            "question": market.get("question", ""),
        })

    return results
