#!/usr/bin/env python3
"""Auto-settle resolved btc-updown-15m markets from the last 40 minutes.

Polls every 3 seconds:
  1. Computes window-aligned timestamps for the last 40 min
  2. Queries Gamma API for each slug to check if resolved
  3. If resolved and we hold positions → auto redeem

Usage:
    python3 tools/auto_settle.py          # dry-run
    python3 tools/auto_settle.py --do-it  # execute settlements
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import httpx

# Ensure poly_trader is importable
_script_dir = str(Path(__file__).resolve().parent)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
# The script's own dir (tools/) shadows the site-packages `polymarket` SDK
# with our local `tools/polymarket` tooling package (empty __init__), which
# breaks `from polymarket.models.clob.market_events import …` in engine.py.
# Drop it so the real SDK resolves.
sys.path = [p for p in sys.path if p != _script_dir]

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("poly_trader").setLevel(logging.INFO)

from poly_trader.platforms.poly_client import SdkClient
from poly_trader.platforms.config import Config

GAMMA_API = "https://gamma-api.polymarket.com"
DURATION_SEC = 15 * 60  # 900s
LOOKBACK_SEC = 40 * 60  # 2400s
SLUG_BASE = "btc-updown-15m"


def window_timestamps() -> list[int]:
    """Return window-aligned timestamps for the last LOOKBACK_SEC seconds."""
    now = int(time.time())
    current_ts = (now // DURATION_SEC) * DURATION_SEC
    tss = []
    # Walk back from previous window (current might not be settled yet)
    for offset in range(1, (LOOKBACK_SEC // DURATION_SEC) + 2):
        ts = current_ts - offset * DURATION_SEC
        if ts > 0:
            tss.append(ts)
    return tss


async def find_resolved_markets(
    client: httpx.AsyncClient, slugs: list[str],
) -> dict[str, str]:
    """Check which slugs are resolved. Returns {condition_id: winner}.

    A market is resolved when outcomePrices show >= 0.99 for one side.
    """
    resolved: dict[str, str] = {}
    for slug in slugs:
        url = f"{GAMMA_API}/events?slug={slug}"
        try:
            resp = await client.get(url, timeout=httpx.Timeout(10))
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue

        events = data if isinstance(data, list) else [data]
        for evt in events:
            for m in evt.get("markets", []):
                cid = m.get("conditionId", "")
                outcomes = m.get("outcomes")
                outcome_prices = m.get("outcomePrices")
                if not cid or not outcomes:
                    continue

                # Parse JSON string fields from Gamma API
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except (json.JSONDecodeError, TypeError):
                        pass
                if isinstance(outcome_prices, str):
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except (json.JSONDecodeError, TypeError):
                        pass

                if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                    prices = [float(p) for p in outcome_prices[:2]]
                    if prices[0] >= 0.99:
                        resolved[cid] = str(outcomes[0])
                    elif prices[1] >= 0.99:
                        resolved[cid] = str(outcomes[1])

    return resolved


async def run(do_it: bool = False):
    cfg = Config()
    sdk = SdkClient(cfg)
    settled_set: set[str] = set()

    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("ERROR: 私钥无效或未设置")
            return

        async with httpx.AsyncClient() as http:
            logging.info("Auto-settle started — scanning every 3s")
            while True:
                # 1. Find resolved markets in the last 40 min
                tss = window_timestamps()
                slugs = [f"{SLUG_BASE}-{t}" for t in tss]
                resolved = await find_resolved_markets(http, slugs)

                if not resolved:
                    await asyncio.sleep(3)
                    continue

                logging.info(
                    "Scan: %d resolved market(s) in last 40min",
                    len(resolved),
                )

                # 2. List positions and match against resolved markets
                matched = 0
                try:
                    paginator = sdk._secure.list_positions()
                    async for page in paginator:
                        for pos in page.items:
                            if not pos.token_id or not pos.size:
                                continue
                            if int(pos.size) <= 0:
                                continue
                            cid = str(pos.condition_id) if pos.condition_id else ""
                            if not cid or cid in settled_set:
                                continue
                            winner = resolved.get(cid)
                            if winner is None:
                                continue

                            matched += 1
                            size = int(pos.size)
                            logging.info(
                                "Market resolved winner=%s  cid=%s…  size=%d",
                                winner, cid[:40], size,
                            )

                            if do_it:
                                try:
                                    result = await sdk._secure.redeem_positions(
                                        condition_id=cid,
                                    )
                                    if hasattr(result, "wait"):
                                        await result.wait()
                                    settled_set.add(cid)
                                    logging.info(
                                        "  ✓ settled  size=%d", size,
                                    )
                                except Exception as e:
                                    msg = str(e)[:100]
                                    if "No market found" in msg:
                                        logging.info(
                                            "  ⚠ stale (on-chain only)"
                                        )
                                        settled_set.add(cid)
                                    else:
                                        logging.warning("  ✗ %s", msg)
                            else:
                                logging.info("  → would settle  size=%d", size)
                                settled_set.add(cid)

                    if matched == 0:
                        logging.info("  No unmatched positions to settle")
                except Exception as e:
                    logging.warning("list_positions error: %s", e)

                await asyncio.sleep(3)

    except KeyboardInterrupt:
        logging.info("Stopped")
    finally:
        await sdk.close()


if __name__ == "__main__":
    do_it = "--do-it" in sys.argv
    asyncio.run(run(do_it=do_it))
