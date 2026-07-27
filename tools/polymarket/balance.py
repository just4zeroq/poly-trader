#!/usr/bin/env python3
"""Polymarket account balance & portfolio snapshot — standalone script."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("poly_trader.platforms.poly_client").setLevel(logging.ERROR)

# ── ensure poly_trader is importable ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

from poly_trader.platforms.poly_client import SdkClient
from poly_trader.platforms.config import Config


def _fmt_usd(raw: int) -> str:
    return f"${raw / 1_000_000:,.2f}"


async def main():
    cfg = Config()
    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("ERROR: 私钥无效或未设置")
            return

        wallet = sdk._secure._ctx.wallet
        print(f"{'═' * 52}")
        print(f"  Polymarket 账户余额")
        print(f"{'═' * 52}")
        print(f"  Deposit wallet:  {wallet}")

        # ── CLOB collateral balance + allowance ──
        bal = await sdk._secure.get_balance_allowance(asset_type="COLLATERAL")
        print(f"  CLOB 余额:        {_fmt_usd(bal.balance)} USDC.e")
        print()

        # ── Open positions ──
        print(f"  {'─' * 48}")
        print(f"  持仓")
        print(f"  {'─' * 48}")
        total_pos_value = 0.0
        pos_count = 0
        try:
            paginator = sdk._secure.list_positions()
            async for page in paginator:
                for pos in page.items:
                    if not pos.token_id or not pos.size or int(pos.size) <= 0:
                        continue
                    size = int(pos.size)
                    avg_px = float(pos.avg_price) if pos.avg_price else 0.0
                    try:
                        cur_px = await sdk.get_midpoint(str(pos.token_id))
                    except Exception:
                        cur_px = None
                    cur_str = f"  cur=${cur_px:.4f}" if cur_px else ""
                    mkt_val = size * (cur_px or avg_px)
                    total_pos_value += mkt_val
                    pos_count += 1
                    print(f"  {pos.condition_id[:20]}…  "
                          f"size={size}  avg=${avg_px:.4f}  {cur_str}  "
                          f"val=${mkt_val:.2f}")
            if pos_count == 0:
                print("  (无持仓)")
        except Exception as e:
            print(f"  (查询失败: {e})")
        print()

        # ── Open orders ──
        print(f"  {'─' * 48}")
        print(f"  挂单")
        print(f"  {'─' * 48}")
        order_count = 0
        order_value = 0.0
        try:
            paginator = sdk._secure.list_open_orders()
            async for page in paginator:
                for order in page.items:
                    remaining = int(order.original_size) - int(order.size_matched)
                    if remaining <= 0:
                        continue
                    total = remaining * float(order.price)
                    order_value += total
                    order_count += 1
                    print(f"  {order.id[:16]}…  "
                          f"{'BUY' if order.side.upper() == 'BUY' else 'SELL'}  "
                          f"{remaining}/{order.original_size} @ ${float(order.price):.4f}  "
                          f"=${total:.2f}")
            if order_count == 0:
                print("  (无挂单)")
        except Exception as e:
            print(f"  (查询失败: {e})")
        print()

        # ── Summary ──
        print(f"  {'═' * 52}")
        print(f"  可用余额:     {_fmt_usd(bal.balance)}")
        print(f"  挂单占用:     ${order_value:,.2f}")
        print(f"  持仓市值:     ${total_pos_value:,.2f}")
        print(f"  总资产:       ${bal.balance / 1_000_000 + total_pos_value:,.2f}")
        print(f"  {'═' * 52}")

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await sdk.close()


if __name__ == "__main__":
    asyncio.run(main())
