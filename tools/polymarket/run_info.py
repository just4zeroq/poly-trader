#!/usr/bin/env python3
"""
Discover Polymarket updown markets via Gamma API.

Usage:
  python -m poly_trader.run_info
  python -m poly_trader.run_info --markets btc-updown-15m sol-updown-15m
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import aiohttp

from ...platforms.models import MarketSpec

DEFAULT_MARKET_SPECS = [
    MarketSpec(symbol="BTC", duration_min=15, slug_pattern="btc-updown-15m"),
]

GAMMA_API = "https://gamma-api.polymarket.com"


async def fetch_event(session: aiohttp.ClientSession, slug: str) -> dict | None:
    try:
        async with session.get(f"{GAMMA_API}/events?slug={slug}", timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data[0] if data else None
    except Exception:
        return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Discover Polymarket updown markets")
    parser.add_argument(
        "--markets", nargs="+", default=None,
        help='Slug patterns, e.g. "btc-updown-15m" (default: all specs)',
    )
    parser.add_argument(
        "--lookback", type=int, default=3,
        help="Number of windows to look back (default: 3)",
    )
    args = parser.parse_args()

    if args.markets:
        specs = []
        for pattern in args.markets:
            parts = pattern.split("-")
            symbol = parts[0].upper() if parts else "?"
            dur = int(parts[2].rstrip("m")) if len(parts) > 2 else 15
            specs.append(MarketSpec(symbol=symbol, duration_min=dur, slug_pattern=pattern))
    else:
        specs = DEFAULT_MARKET_SPECS

    print(f"{'═' * 50}")
    print("  Market Discovery — Gamma API")
    print(f"{'═' * 50}")
    print()

    async with aiohttp.ClientSession() as session:
        for spec in specs:
            print(f"  ── {spec} [{spec.slug_pattern}] ──")
            base = spec.slug_pattern
            dur = spec.duration_min
            found = False

            for offset in range(args.lookback + 1):
                ts = (int(time.time()) // (dur * 60)) * (dur * 60) - (offset * dur * 60)
                slug = f"{base}-{ts}"
                ev = await fetch_event(session, slug)
                if ev:
                    found = True
                    m = ev["markets"][0] if ev.get("markets") else {}
                    raw_ids = m.get("clobTokenIds", "[]")
                    raw_outcomes = m.get("outcomes", '["Up","Down"]')
                    token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
                    outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

                    print(f"     ✅ offset={offset} slug={slug}")
                    print(f"        condition_id: {m.get('conditionId', '?')}")
                    print(f"        question: {m.get('question', '?')}")
                    for i in range(min(len(token_ids), len(outcomes))):
                        print(f"        {outcomes[i]}: token_id={token_ids[i]}")
                    print(f"        active: {ev.get('active', '?')}")
                    print(f"        end_date: {ev.get('end_date_iso', '?')}")
                    print()
                    break

            if not found:
                print(f"     ❌ No market found in last {args.lookback} windows")
                print()


if __name__ == "__main__":
    asyncio.run(main())
