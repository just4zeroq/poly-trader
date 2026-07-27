#!/usr/bin/env python3
"""Polymarket position settlement — redeem resolved positions, merge complementary pairs.

Usage:
    python3 poly_trader/settle.py          # dry-run: show what can be settled
    python3 poly_trader/settle.py --do-it  # actually execute redemptions
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("poly_trader.platforms.poly_client").setLevel(logging.ERROR)

from poly_trader.platforms.poly_client import SdkClient
from poly_trader.platforms.config import Config


async def main(do_it: bool = False):
    cfg = Config()
    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("ERROR: 私钥无效或未设置")
            return

        paginator = sdk._secure.list_positions()
        positions: list[dict] = []
        async for page in paginator:
            for pos in page.items:
                if not pos.token_id or not pos.size or int(pos.size) <= 0:
                    continue
                positions.append({
                    "condition_id": str(pos.condition_id) if pos.condition_id else "",
                    "token_id": str(pos.token_id),
                    "size": int(pos.size),
                    "avg_price": float(pos.avg_price) if pos.avg_price else 0.0,
                })

        if not positions:
            print("没有持仓需要结算")
            return

        print(f"{'═' * 56}")
        mode = "执行结算" if do_it else "预览 (dry-run)"
        print(f"  Polymarket 仓位结算 — {mode}")
        print(f"{'═' * 56}")
        print()

        settled_value = 0.0
        stuck: list[dict] = []
        settled: list[dict] = []

        # Group by condition_id to detect complementary pairs
        by_cid: dict[str, list[dict]] = {}
        for p in positions:
            by_cid.setdefault(p["condition_id"], []).append(p)

        for cid, cid_positions in by_cid.items():
            total_size = sum(p["size"] for p in cid_positions)
            total_cost = sum(p["size"] * p["avg_price"] for p in cid_positions)
            tokens = len(cid_positions)

            sides = [p["token_id"][:20] for p in cid_positions]
            print(f"  market: {cid[:40]}…")
            print(f"    tokens: {tokens}  total_size={total_size}  cost=${total_cost:.2f}")
            print(f"    sides:  {sides}")

            try:
                result = await sdk._secure.redeem_positions(condition_id=cid)
                if do_it:
                    handle_type = type(result).__name__
                    if hasattr(result, "wait"):
                        await result.wait()
                    outcome_text = "merged" if "merge" in str(type(result)).lower() else "redeemed"
                    print(f"    ✓ {outcome_text} ({handle_type})")
                else:
                    print(f"    → 可结算 (would execute via SDK)")
                settled.append({"cid": cid, "positions": cid_positions})
                # Assume winners are worth $1 each
                settled_value += total_size
            except Exception as e:
                msg = str(e)[:120]
                if "No market found" in msg:
                    print(f"    ⚠ 市场已过期，无法通过 CLOB 结算 (stale)")
                    print(f"       需要在链上手动赎回")
                else:
                    print(f"    ✗ 结算失败: {msg}")
                stuck.append({"cid": cid, "positions": cid_positions})
            print()

        # ── Summary ──
        print(f"  {'═' * 56}")
        if settled:
            print(f"  可结算:  {len(settled)} markets → ${settled_value:.2f}")
        if stuck:
            stuck_cost = sum(
                p["size"] * p["avg_price"]
                for m in stuck
                for p in m["positions"]
            )
            stuck_size = sum(
                p["size"] for m in stuck for p in m["positions"]
            )
            print(f"  滞留:    {len(stuck)} markets  (成本 ${stuck_cost:.2f}, {stuck_size} 张)")
            print(f"           历史已结算市场残留仓位")
            print(f"           loser token = 价值 $0, winner token = $1/张")
            print(f"           需 Builder API Key 或链上手动赎回")
        if not settled and not stuck:
            print(f"  无仓位需要结算")
        print(f"  {'═' * 56}")

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await sdk.close()


if __name__ == "__main__":
    do_it = "--do-it" in sys.argv
    asyncio.run(main(do_it=do_it))
