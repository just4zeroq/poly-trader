"""Simulate tail-end strategies across historical BTC-updown-15m windows — v2.

Key fix: loser leg is a LIMIT order at calculated price, so it can fill LATER
in the tail. Track whether loser ever reaches target price after winner entry.
"""
import asyncio, time
from collections import defaultdict
from datetime import datetime, timezone

import httpx


async def fetch_trades(market_id: str, start_ts: int, end_ts: int, limit: int = 2000):
    url = "https://data-api.polymarket.com/trades"
    params = {"market": market_id, "start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        return []


def simulate_sweep(win_trades, lose_trades, tail_start, ws_end, threshold=0.85, cutoff=60):
    """Simulate sweep strategy on historical trade data.

    Phase 1: winner price >= threshold during tail → buy winner at that price
    Phase 2: place limit order for loser at max_loser_price
    Phase 3: limit fills if loser ever trades at or below max_loser_price

    Returns dict with outcome details.
    """
    min_remaining = cutoff

    result = {"entered": False, "winner_price": None,
              "entry_time": None, "remaining": 0, "profit": None,
              "loser_limit_filled": False, "loser_limit_price": None,
              "dipped_after_entry": False, "winner_lost": False}

    for ts, p in win_trades:
        if ts < tail_start:
            continue
        remaining = ws_end - ts
        if p >= threshold and remaining > min_remaining:
            result["entered"] = True
            result["winner_price"] = p
            result["entry_time"] = ts
            result["remaining"] = remaining

            # Target loser price to lock 5¢ profit
            # winner_price + loser_price <= 1.0 - 0.05
            max_loser_price = round(1.0 - p - 0.05, 4)
            result["loser_limit_price"] = max_loser_price

            if max_loser_price > 0:
                # Check if loser ever trades at or below this price AFTER entry
                future_loser = [(lt, lp) for lt, lp in lose_trades
                                if lt > ts and lt <= ws_end]
                min_future_loser = min((lp for _, lp in future_loser), default=None)
                if min_future_loser is not None and min_future_loser <= max_loser_price:
                    result["loser_limit_filled"] = True
                    result["profit"] = 0.05
                else:
                    # Check if loser is already cheap enough at entry time
                    loser_at_entry = [lp for lt, lp in lose_trades if abs(lt - ts) < 10]
                    if loser_at_entry and min(loser_at_entry) <= max_loser_price:
                        result["loser_limit_filled"] = True
                        result["profit"] = 0.05
                    else:
                        result["profit"] = 0.0

            # Track what happens after entry
            post_win = [(t, p_) for t, p_ in win_trades if t > ts and t <= ws_end]
            if post_win:
                result["dipped_after_entry"] = any(p_ < threshold for _, p_ in post_win)
                final_win = post_win[-1][1]
                result["winner_lost"] = final_win < 0.5
            break

    return result


def simulate_maker_only(up_trades, down_trades, tail_start, ws_end):
    """Simulate continuing maker: assume we get filled on limit orders at bid price.

    Rough approx: use avg trade price as proxy for bid.
    """
    tail_up = [(ts, p) for ts, p in up_trades if ts >= tail_start]
    tail_down = [(ts, p) for ts, p in down_trades if ts >= tail_start]
    tail_len = max(len(tail_up), len(tail_down))

    if tail_len < 2:
        return {"avg_up": 0, "avg_down": 0, "pair_cost": 0, "pairs": 0, "pnl": 0}

    avg_up = sum(p for _, p in tail_up) / len(tail_up) if tail_up else 0
    avg_down = sum(p for _, p in tail_down) / len(tail_down) if tail_down else 0

    # Realistic: maker fills maybe 1 pair per 30s, so ~6 pairs in 3min
    pairs = min(tail_len // 2, 6)
    pair_cost = avg_up + avg_down
    pnl = (1.0 - pair_cost) * pairs if pair_cost < 1.0 else 0

    return {
        "avg_up": round(avg_up, 4),
        "avg_down": round(avg_down, 4),
        "pair_cost": round(pair_cost, 4),
        "pairs": pairs,
        "pnl": round(pnl, 2),
    }


async def main():
    import sys
    sys.path.insert(0, "/home/ubuntu/code")
    from poly_trader.platforms.poly_client import SdkClient
    from poly_trader.platforms.config import Config
    from poly_trader.platforms.main import parse_market_spec

    cfg = Config()
    spec = parse_market_spec("btc-updown-15m")
    sdk = SdkClient(cfg)

    duration = spec.duration_min * 60
    now = int(time.time())
    current_ws = (now // duration) * duration

    results = []

    for offset in range(24):
        ws_start = current_ws - (offset * duration)
        ws_end = ws_start + duration
        slug = f"{spec.slug_pattern}-{ws_start}"

        market = await sdk.get_market_by_slug(slug)
        if not market:
            continue

        trades = await fetch_trades(market.condition_id, ws_start, ws_end)
        await asyncio.sleep(0.15)

        if not trades:
            continue

        up_series, down_series = [], []
        for t in trades:
            ts = int(t.get("timestamp", 0))
            price = float(t.get("price", 0))
            if t.get("asset", "") == market.up_token_id or t.get("outcome") == "Up":
                up_series.append((ts, price))
            elif t.get("asset", "") == market.down_token_id or t.get("outcome") == "Down":
                down_series.append((ts, price))
        up_series.sort(key=lambda x: x[0])
        down_series.sort(key=lambda x: x[0])

        if not up_series or not down_series:
            continue

        up_end = up_series[-1][1]
        down_end = down_series[-1][1]
        winner = "Up" if up_end > down_end else "Down"
        win_series = up_series if winner == "Up" else down_series
        lose_series = down_series if winner == "Up" else up_series

        tail_start = ws_end - 180

        # Sweep simulations
        sweeps = {}
        for label, thresh, cutoff in [
            ("s85_c60", 0.85, 60),
            ("s85_c30", 0.85, 30),
            ("s85_c0",  0.85, 0),
            ("s90_c60", 0.90, 60),
            ("s90_c0",  0.90, 0),
        ]:
            s = simulate_sweep(win_series, lose_series, tail_start, ws_end, thresh, cutoff)
            sweeps[label] = s

        maker = simulate_maker_only(up_series, down_series, tail_start, ws_end)

        start_str = datetime.fromtimestamp(ws_start, tz=timezone.utc).strftime('%H:%M')
        results.append({
            "time": start_str,
            "winner": winner,
            "sweeps": sweeps,
            "maker": maker,
        })

        # One-line
        def sw(key):
            s = sweeps[key]
            if not s["entered"]:
                return "—"
            return f"Y(p={s.get('profit', 0):.2f})" if s.get("loser_limit_filled") else "Y(nofill)"

        print(f"  {start_str} {winner:>4s}  "
              f"0.85/60={sw('s85_c60'):>10s}  0.85/30={sw('s85_c30'):>10s}  0.85/0={sw('s85_c0'):>10s}  "
              f"0.90/60={sw('s90_c60'):>10s}  0.90/0={sw('s90_c0'):>10s}  "
              f"maker={maker.get('pair_cost', '—')}")

    await sdk.close()

    # ── Summary ──
    print(f"\n{'='*130}")
    print(f"  STRATEGY COMPARISON SUMMARY")
    print(f"{'='*130}")

    for label, key in [
        ("0.85, cutoff=60s (current)", "s85_c60"),
        ("0.85, cutoff=30s          ", "s85_c30"),
        ("0.85, no cutoff           ", "s85_c0"),
        ("0.90, cutoff=60s          ", "s90_c60"),
        ("0.90, no cutoff           ", "s90_c0"),
    ]:
        entered = [r for r in results if r["sweeps"][key].get("entered")]
        filled = [r for r in entered if r["sweeps"][key].get("loser_limit_filled")]
        print(f"\n  Sweep {label}:")
        print(f"    Triggered: {len(entered):>2d}/{len(results)} windows")
        if entered:
            avg_remain = sum(r["sweeps"][key]["remaining"] for r in entered) / len(entered)
            dipped = sum(1 for r in entered if r["sweeps"][key].get("dipped_after_entry"))
            reversed_ = sum(1 for r in entered if r["sweeps"][key].get("winner_lost"))
            print(f"    Avg remaining: {avg_remain:.0f}s")
            print(f"    Loser limit FILLED: {len(filled):>2d}/{len(entered):>2d} (profit locked)")
            print(f"    No fill (loser too expensive): {len(entered)-len(filled):>2d}/{len(entered):>2d}")
            print(f"    Dipped after entry: {dipped}/{len(entered)}")
            print(f"    Winner reversed: {reversed_}/{len(entered)}")
        else:
            print(f"    (no entries)")

    # Maker summary
    maker_ok = [r for r in results if r["maker"]["pair_cost"] > 0]
    if maker_ok:
        avg_pc = sum(r["maker"]["pair_cost"] for r in maker_ok) / len(maker_ok)
        total_pnl = sum(r["maker"]["pnl"] for r in maker_ok)
        print(f"\n  Maker-only during tail ({len(maker_ok)} windows):")
        print(f"    Avg pair cost: {avg_pc:.4f}")
        print(f"    Sum PnL: {total_pnl:.2f}")
        print(f"    Profitable windows: {sum(1 for r in maker_ok if r['maker']['pnl'] > 0)}/{len(maker_ok)}")

    # ── What happens if we just don't trade tail? ──
    print(f"\n{'='*130}")
    print(f"  BOTTOM LINE")
    print(f"{'='*130}")
    print(f"  Best variant: 0.85 threshold, 60s cutoff (current):")
    curr = [r for r in results if r["sweeps"]["s85_c60"].get("entered")]
    filled = [r for r in curr if r["sweeps"]["s85_c60"].get("loser_limit_filled")]
    print(f"    Enters: {len(curr)} windows")
    print(f"    Profit locked: {len(filled)} windows")
    print(f"    Unfilled (only winner bought → directional bet): {len(curr)-len(filled)} windows")
    print(f"    Dipped risk (winner dips below 0.85 after entry): {sum(1 for r in curr if r['sweeps']['s85_c60'].get('dipped_after_entry'))}/{len(curr)}")


if __name__ == "__main__":
    asyncio.run(main())
