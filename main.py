"""
Polymarket WebSocket Temporal Arbitrage Bot — CLI entry point.

Usage:
  python -m poly_trader info                  # query current markets
  python -m poly_trader paper                 # paper trade (simulated fills)
  python -m poly_trader run                   # live trade (real orders)

  python -m poly_trader paper --per-tick 10 --max-side 500
  python -m poly_trader paper --market btc-updown-15m
  python -m poly_trader run --market btc-updown-5m
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .config import Config
from .engine import TradingEngine
from .executors import LiveExecutor, PaperExecutor
from .models import MarketSpec

logger = logging.getLogger("poly_trader")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def parse_market_spec(pattern: str) -> MarketSpec:
    """Convert a single CLI slug pattern (e.g. 'btc-updown-15m') to MarketSpec."""
    parts = pattern.split("-")
    symbol = parts[0].upper() if parts else "BTC"
    dur = 5
    for part in parts:
        if part.endswith("m") and part[:-1].isdigit():
            dur = int(part[:-1])
            break
    return MarketSpec(symbol=symbol, duration_min=dur, slug_pattern=pattern)


async def async_main(args: argparse.Namespace):
    cfg = Config()

    # Live mode: conservative defaults + credential check
    if args.mode == "run":
        if not cfg.private_key:
            print("ERROR: Live mode requires POLY_PRIVATE_KEY environment variable")
            print("See config.py for all credential options")
            sys.exit(1)
        cfg.per_tick = args.per_tick
        cfg.max_per_side = args.max_side
    else:
        cfg.per_tick = args.per_tick
        cfg.max_per_side = args.max_side

    if args.market:
        cfg.market_specs = [parse_market_spec(args.market)]

    if args.mode == "info":
        engine = TradingEngine(cfg)
        await engine.show_info()
        return

    if args.mode == "check":
        await check_credentials(cfg)
        return

    if not cfg.market_specs:
        print("ERROR: --market is required in paper/run mode")
        print("  e.g. python -m poly_trader paper --market btc-updown-15m")
        sys.exit(1)

    engine = TradingEngine(cfg, executor=PaperExecutor() if args.mode == "paper" else LiveExecutor())
    try:
        await engine.run()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass


async def check_credentials(cfg: Config):
    """Verify Polymarket credentials work."""
    from .client import SdkClient

    print(f"{'═' * 50}")
    print("  Credential Check")
    print(f"{'═' * 50}")
    print()

    if not cfg.private_key:
        print("❌  POLY_PRIVATE_KEY / POLYMARKET_PK 未设置")
        return

    print(f"  私钥:  {cfg.private_key[:20]}...")
    print(f"  钱包:  {cfg.wallet_address or '(自动派生)'}")
    print()

    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("❌  SecureClient 创建失败 — 私钥或钱包地址无效")
            return
        print("✅  私钥有效")
        print(f"✅  钱包: {cfg.wallet_address}")
        print()
        print("  凭证就绪，可以启动实盘。")
    except Exception as e:
        print(f"❌  凭证无效: {e}")
    finally:
        await sdk.close()


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Temporal Arbitrage Bot (WebSocket)",
    )
    parser.add_argument(
        "mode", nargs="?", default="info",
        choices=["info", "paper", "run", "check"],
        help="info=query market, paper=simulate, run=live trading, check=verify credentials",
    )
    parser.add_argument("--per-tick", type=int, default=5,
                        help="max contracts per tick per side (default: 5)")
    parser.add_argument("--max-side", type=int, default=500,
                        help="max position per side (default: 500)")
    parser.add_argument(
        "--market", type=str, default=None,
        help='Market slug pattern, e.g. "btc-updown-15m" (required for paper/run)',
    )
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="debug-level logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    # Print banner
    mode = "Paper" if args.mode == "paper" else ("Live" if args.mode == "run" else ("Check" if args.mode == "check" else "Info"))
    spec = parse_market_spec(args.market) if args.market else None
    print(f"{'═' * 50}")
    print(f"  Polymarket Temporal Arbitrage — {mode} Mode")
    print(f"{'═' * 50}")
    if spec:
        print(f"  {spec} [{spec.slug_pattern}]")
    if args.mode != "check":
        print(f"  per_tick={args.per_tick}, max_per_side={args.max_side}")
    print()

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
