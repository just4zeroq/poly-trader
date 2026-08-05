#!/usr/bin/env python3
"""Settle the previous trading window — run at a new window start.

Determines the window that just ended from the market schedule (or takes
``--slug``), then polls every ``--interval`` seconds until the market
resolves.  Once resolved, redeems the winning positions (merging
complementary pairs).  Does NOT compute PnL — pure settlement, decoupled
from the main trading flow.

Usage:
    python3 poly_trader/tools/onchain/settle_window.py
    python3 poly_trader/tools/onchain/settle_window.py --slug btc-updown-15m-1700000000
    python3 poly_trader/tools/onchain/settle_window.py --interval 5 --max-attempts 60
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

from poly_trader.platforms.config import Config
from poly_trader.platforms.poly_client import SdkClient

logger = logging.getLogger("settle_window")


def previous_window_slug(market_slug: str) -> str:
    """Slug of the window that just ended, e.g. 'btc-updown-15m-{prev_ts}'."""
    duration_min = 5
    for part in market_slug.split("-"):
        if part.endswith("m") and part[:-1].isdigit():
            duration_min = int(part[:-1])
            break
    duration = duration_min * 60
    now = int(time.time())
    cur_ts = (now // duration) * duration
    return f"{market_slug}-{cur_ts - duration}"


async def settle_previous_window(sdk: SdkClient, slug: str,
                                 interval: float, max_attempts: int) -> int:
    market = await sdk.get_market_by_slug(slug)
    if not market:
        print(f"✗ 找不到上一窗口 {slug}（可能已过期或不存在）")
        return 1

    print("═" * 56)
    print(f"  上一窗口结算 — {slug}")
    print(f"  condition_id: {market.condition_id[:40]}…")
    print("═" * 56)

    attempts = 0
    while True:
        attempts += 1
        winner = await sdk.get_resolved_winner(slug)
        if winner is None:
            if max_attempts and attempts >= max_attempts:
                print(f"  达到最大尝试次数 {max_attempts}，仍未 resolved — 退出")
                return 1
            print(f"  [{attempts}] 尚未 resolved，{interval:.0f}s 后重试…")
            await asyncio.sleep(interval)
            continue

        print(f"  ✓ 赢家: {winner}")
        if not sdk.is_secure:
            print("  ⚠ 无 secure client，只完成解析，跳过赎回")
            return 0

        try:
            result = await sdk._secure.redeem_positions(condition_id=market.condition_id)
            if hasattr(result, "wait"):
                await result.wait()
            print("  ✓ 结算完成（已赎回赢方持仓）")
        except Exception as e:
            msg = str(e)[:120]
            if "No market found" in msg:
                print(f"  ⚠ 市场已过期，需链上手动赎回: {msg}")
            else:
                print(f"  ✗ 赎回失败: {msg}")
            print("    可稍后用 tools/onchain/settle.py --do-it 统一处理")
        return 0


async def main() -> int:
    parser = argparse.ArgumentParser(description="Settle the previous trading window")
    parser.add_argument("--slug", default=None,
                        help="override the previous window slug (auto-derived by default)")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="resolution poll interval in seconds (default 5)")
    parser.add_argument("--max-attempts", type=int, default=0,
                        help="max resolution polls before giving up (0 = keep polling)")
    args = parser.parse_args()

    cfg = Config()
    slug = args.slug or previous_window_slug(cfg.market_slug)
    print(f"目标窗口: {slug}")

    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        return await settle_previous_window(sdk, slug, args.interval, args.max_attempts)
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    finally:
        await sdk.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
