"""Full-scale historical BTC-updown-15m analysis — all available windows."""
import asyncio, time, math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import httpx


TRADE_URL = "https://data-api.polymarket.com/trades"
WINDOW_MIN = 15
WINDOW_SEC = WINDOW_MIN * 60  # 900s
LOOKBACK_DAYS = 14
TOTAL_WINDOWS = (LOOKBACK_DAYS * 86400) // WINDOW_SEC  # 1344
RATE_LIMIT = 5  # concurrent requests

sem = asyncio.Semaphore(RATE_LIMIT)


async def fetch_trades(condition_id: str, start_ts: int, end_ts: int, limit: int = 2000):
    params = {"market": condition_id, "start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    async with sem, httpx.AsyncClient() as client:
        try:
            resp = await client.get(TRADE_URL, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return []


def analyze_window(ws_start: int, ws_end: int,
                   up_token_id: str, down_token_id: str,
                   trades: list) -> Optional[dict]:
    """Analyze a single window's trades. Returns None if insufficient data."""
    up_series, down_series = [], []
    for t in trades:
        ts = int(t.get("timestamp", 0))
        price = float(t.get("price", 0))
        if t.get("asset", "") == up_token_id or t.get("outcome") == "Up":
            up_series.append((ts, price))
        elif t.get("asset", "") == down_token_id or t.get("outcome") == "Down":
            down_series.append((ts, price))

    up_series.sort(key=lambda x: x[0])
    down_series.sort(key=lambda x: x[0])

    if len(up_series) < 3 or len(down_series) < 3:
        return None

    up_end = up_series[-1][1]
    down_end = down_series[-1][1]
    winner_side = "Up" if up_end > down_end else "Down"
    win_series = up_series if winner_side == "Up" else down_series
    lose_series = down_series if winner_side == "Up" else up_series

    # First time winner crosses thresholds
    def first_cross(series, thresh):
        for ts, p in series:
            if p >= thresh:
                return ts
        return None

    t_85 = first_cross(win_series, 0.85)
    t_90 = first_cross(win_series, 0.90)
    t_95 = first_cross(win_series, 0.95)

    # Max prices
    max_win_price = max(p for _, p in win_series)
    max_win_ts = next(ts for ts, p in win_series if p == max_win_price)
    max_lose_price = max(p for _, p in lose_series)
    max_lose_ts = next(ts for ts, p in lose_series if p == max_lose_price)

    tail_start = ws_end - 180

    # Did price hit threshold during tail?
    tail_85 = None
    if t_85:
        tail_85 = t_85 - tail_start  # positive = during tail, negative = before tail

    # Tail phase price details
    tail_win = [(ts, p) for ts, p in win_series if ts >= tail_start]
    tail_lose = [(ts, p) for ts, p in lose_series if ts >= tail_start]

    # Winner price at tail start
    entry_win = next((p for ts, p in tail_win if abs(ts - tail_start) < 15), None)
    entry_lose = next((p for ts, p in tail_lose if abs(ts - tail_start) < 15), None)

    # Did loser lead at any point during tail?
    loser_led = False
    if tail_win and tail_lose:
        min_ts = min(tail_win[0][0], tail_lose[0][0])
        for t_sample in range(int(min_ts), int(ws_end), 10):
            wp = next((p for ts, p in tail_win if abs(ts - t_sample) < 10), None)
            lp = next((p for ts, p in tail_lose if abs(ts - t_sample) < 10), None)
            if wp and lp and lp > wp:
                loser_led = True
                break

    # Simulate sweep: does loser ever reach target price after winner entry?
    sweep_ok = False
    sweep_entry_remaining = 0
    if t_85 and tail_85 and tail_85 > 0:  # hit during tail
        remaining = ws_end - t_85
        if remaining > 60:
            sweep_entry_remaining = remaining
            max_loser_target = 1.0 - (next(p for ts, p in win_series if ts == t_85)) - 0.05
            if max_loser_target > 0:
                future_loser = [lp for lt, lp in lose_series if lt > t_85 and lt <= ws_end]
                if future_loser and min(future_loser) <= max_loser_target:
                    sweep_ok = True

    # Dip after crossing 0.85
    dipped = False
    if t_85:
        dipped = any(p < 0.85 for ts, p in win_series if ts > t_85)

    return {
        "ts": ws_start,
        "time": datetime.fromtimestamp(ws_start, tz=timezone.utc).strftime('%m/%d %H:%M'),
        "winner": winner_side,
        "t_85": t_85,
        "t_90": t_90,
        "t_95": t_95,
        "tail_85": tail_85,
        "max_win_price": max_win_price,
        "max_win_ts": max_win_ts - ws_start,
        "max_lose_price": max_lose_price,
        "max_lose_ts": max_lose_ts - ws_start,
        "entry_win": entry_win,
        "entry_lose": entry_lose,
        "dipped": dipped,
        "loser_led": loser_led,
        "sweep_ok": sweep_ok,
        "sweep_remaining": sweep_entry_remaining,
        "trade_count": len(trades),
        "up_trades": len(up_series),
        "down_trades": len(down_series),
    }


async def main():
    import sys
    sys.path.insert(0, "/home/ubuntu/code")
    from poly_trader.tools.polymarket.client import SdkClient
    from poly_trader.platform.config import Config
    from poly_trader.platform.main import parse_market_spec

    cfg = Config()
    spec = parse_market_spec("btc-updown-15m")
    sdk = SdkClient(cfg)

    duration = spec.duration_min * 60
    now = int(time.time())
    current_ws = (now // duration) * duration

    print(f"Scanning {TOTAL_WINDOWS} windows over {LOOKBACK_DAYS} days...")
    print(f"Current window start: {datetime.fromtimestamp(current_ws, tz=timezone.utc)}\n")

    all_windows = []
    found_count = 0
    skip_count = 0

    for offset in range(TOTAL_WINDOWS):
        ws_start = current_ws - (offset * duration)
        ws_end = ws_start + duration
        slug = f"{spec.slug_pattern}-{ws_start}"

        market = await sdk.get_market_by_slug(slug)
        await asyncio.sleep(0.1)

        if not market:
            skip_count += 1
            continue

        trades = await fetch_trades(market.condition_id, ws_start, ws_end)
        await asyncio.sleep(0.15)

        result = analyze_window(ws_start, ws_end,
                                market.up_token_id, market.down_token_id,
                                trades)
        if result:
            all_windows.append(result)
            found_count += 1

        if found_count > 0 and found_count % 50 == 0:
            print(f"  ... {found_count} windows analyzed (skipped {skip_count})")

    await sdk.close()

    if not all_windows:
        print("NO DATA FOUND")
        return

    # ── Comprehensive Analysis ──
    n = len(all_windows)

    # Basic stats
    winners = [w for w in all_windows if w["t_85"] is not None]
    during_tail = [w for w in winners if w["tail_85"] is not None and w["tail_85"] > 0]
    before_tail = [w for w in winners if w["tail_85"] is not None and w["tail_85"] <= 0]
    up_wins = [w for w in all_windows if w["winner"] == "Up"]
    down_wins = [w for w in all_windows if w["winner"] == "Down"]

    print(f"\n{'='*100}")
    print(f"  BTC-UPDOWN-15M COMPREHENSIVE ANALYSIS — {n} windows ({LOOKBACK_DAYS} days)")
    print(f"{'='*100}")

    print(f"\n  ── GENERAL STATS ──")
    print(f"  Total windows: {n}")
    print(f"  Hit 0.85+: {len(winners)}/{n} ({len(winners)/n*100:.1f}%)")
    print(f"  Hit 0.95+: {len([w for w in winners if w['t_95']])}/{n}")
    print(f"  Winner split: Up={len(up_wins)} ({len(up_wins)/n*100:.1f}%) "
          f"Down={len(down_wins)} ({len(down_wins)/n*100:.1f}%)")

    # ── Threshold timing ──
    print(f"\n  ── THRESHOLD TIMING (seconds into window) ──")
    if winners:
        avg_85 = sum(w["t_85"] - w["ts"] for w in winners) / len(winners)
        t95_list = [w for w in winners if w["t_95"]]
        avg_95 = sum(w["t_95"] - w["ts"] for w in t95_list) / len(t95_list) if t95_list else 0
        print(f"  Avg 0.85 time: {avg_85:.0f}s (of {WINDOW_SEC}s window)")
        print(f"  Avg 0.95 time: {avg_95:.0f}s")
        print(f"  Avg 0.85→0.95: {avg_95 - avg_85:.0f}s")

        # Distribution of 0.85 timing
        bins = [(0, 180, "0-3min"), (180, 360, "3-6min"), (360, 540, "6-9min"),
                (540, 720, "9-12min"), (720, 900, "12-15min")]
        for lo, hi, label in bins:
            cnt = sum(1 for w in winners if lo < (w["t_85"] - w["ts"]) <= hi)
            print(f"    {label}: {cnt} ({cnt/len(winners)*100:.1f}%)")

    # ── Tail phase ──
    print(f"\n  ── TAIL PHASE (last 3 min) ──")
    print(f"  Hit 0.85 DURING tail: {len(during_tail)}/{len(winners)} "
          f"({len(during_tail)/len(winners)*100:.1f}% of winners)")
    print(f"  Hit 0.85 BEFORE tail (already decided): {len(before_tail)}/{len(winners)} "
          f"({len(before_tail)/len(winners)*100:.1f}% of winners)")

    if during_tail:
        delays = [w["tail_85"] for w in during_tail]
        print(f"  Tail 0.85 delay: avg={sum(delays)/len(delays):.0f}s "
              f"min={min(delays)}s max={max(delays)}s")

        # Cutoff analysis
        for cutoff_label, cutoff_sec in [("60s", 60), ("30s", 30), ("10s", 10), ("0s (none)", 0)]:
            viable = sum(1 for w in during_tail if 180 - w["tail_85"] > cutoff_sec)
            blocked = len(during_tail) - viable
            print(f"    Cutoff {cutoff_label}: {viable} viable, {blocked} blocked "
                  f"({viable/len(during_tail)*100:.0f}% pass)")

        # Sweep simulation
        sweeps_ok = sum(1 for w in during_tail if w["sweep_ok"])
        print(f"  Sweep profit lockable: {sweeps_ok}/{len(during_tail)} "
              f"({sweeps_ok/len(during_tail)*100:.0f}%)")

    # ── Max price analysis ──
    print(f"\n  ── MAX WINNER PRICE TIMING ──")
    if winners:
        max_times = [w["max_win_ts"] for w in winners]
        print(f"  Avg max price time: {sum(max_times)/len(max_times):.0f}s into window")
        early_max = sum(1 for w in winners if w["max_win_ts"] < 720)
        tail_max = sum(1 for w in winners if w["max_win_ts"] >= 720)
        print(f"  Max price in tail (last 3min): {tail_max}/{len(winners)} "
              f"({tail_max/len(winners)*100:.0f}%)")
        print(f"  Max price before tail: {early_max}/{len(winners)} "
              f"({early_max/len(winners)*100:.0f}%)")

    # ── Risk analysis ──
    print(f"\n  ── RISK METRICS ──")
    if winners:
        dipped = sum(1 for w in winners if w["dipped"])
        print(f"  Dipped below 0.85 after crossing: {dipped}/{len(winners)} "
              f"({dipped/len(winners)*100:.0f}%)")

    if during_tail:
        loser_led = sum(1 for w in during_tail if w["loser_led"])
        print(f"  Loser led during tail: {loser_led}/{len(during_tail)} "
              f"({loser_led/len(during_tail)*100:.0f}%)")

    # ── Price at tail entry distribution ──
    print(f"\n  ── PRICE AT TAIL START (last 3 min) ──")
    entries = [w for w in all_windows if w["entry_win"] is not None]
    if entries:
        resolved = sum(1 for w in entries if w["entry_win"] >= 0.95)
        sweep_range = sum(1 for w in entries if 0.85 <= w["entry_win"] < 0.95)
        far = sum(1 for w in entries if w["entry_win"] < 0.85)
        print(f"  Resolved (>0.95): {resolved}/{len(entries)} "
              f"({resolved/len(entries)*100:.1f}%)")
        print(f"  Sweep range (0.85-0.95): {sweep_range}/{len(entries)} "
              f"({sweep_range/len(entries)*100:.1f}%)")
        print(f"  Not resolved (<0.85): {far}/{len(entries)} "
              f"({far/len(entries)*100:.1f}%)")

    # ── Hourly pattern ──
    print(f"\n  ── HOURLY PATTERN (UTC) ──")
    hourly = defaultdict(lambda: {"total": 0, "up": 0, "down": 0, "tail_hit": 0, "tail_entry_price": []})
    for w in all_windows:
        hour = datetime.fromtimestamp(w["ts"], tz=timezone.utc).hour
        hourly[hour]["total"] += 1
        if w["winner"] == "Up":
            hourly[hour]["up"] += 1
        else:
            hourly[hour]["down"] += 1
        if w["tail_85"] is not None and w["tail_85"] > 0:
            hourly[hour]["tail_hit"] += 1
        if w["entry_win"] is not None:
            hourly[hour]["tail_entry_price"].append(w["entry_win"])

    for h in sorted(hourly):
        hr = hourly[h]
        bias = "↑" if hr["up"] > hr["down"] else "↓"
        avg_entry = (sum(hr["tail_entry_price"]) / len(hr["tail_entry_price"])
                     if hr["tail_entry_price"] else 0)
        print(f"    {h:02d}:00  total={hr['total']:>3d}  "
              f"Up={hr['up']:>2d}/{hr['down']:>2d} {bias}  "
              f"tail_hit={hr['tail_hit']:>2d}  "
              f"avg_entry={avg_entry:.3f}")

    # ── Ultimate summary ──
    print(f"\n{'='*100}")
    print(f"  BOTTOM LINE")
    print(f"{'='*100}")

    if during_tail:
        viable_60 = sum(1 for w in during_tail if 180 - w["tail_85"] > 60)
        sweeps_ok = sum(1 for w in during_tail if w["sweep_ok"])

        print(f"\n  Current strategy (0.85 threshold, 60s cutoff):")
        print(f"    Total viable windows: {viable_60}/{n} "
              f"({viable_60/n*100:.1f}% of all windows)")
        print(f"    Profit lockable: {min(sweeps_ok, viable_60)}/{viable_60}")

        # Calculate per-window stats
        total_profit = min(sweeps_ok, viable_60) * 0.05 * 5  # assume 5 contracts
        total_risk = (viable_60 - min(sweeps_ok, viable_60)) * 0  # no-fill = no loss
        print(f"    Est profit ({LOOKBACK_DAYS}d, 5ct/trade): ${total_profit:.2f}")

    print(f"\n  Best windows for sweep (ideal pattern):")
    ideal = sorted([w for w in during_tail if w["sweep_ok"] and w["tail_85"] < 60],
                   key=lambda x: x["tail_85"])[:5]
    for w in ideal:
        print(f"    {w['time']} {w['winner']:>4s} "
              f"0.85@tail+{w['tail_85']}s  remaining={w['sweep_remaining']}s")

    print(f"\n  Worst windows (dipped / loser-led):")
    risky = sorted([w for w in during_tail if w["dipped"] or w["loser_led"]],
                   key=lambda x: x["tail_85"])[:5]
    for w in risky:
        issues = []
        if w["dipped"]: issues.append("dip")
        if w["loser_led"]: issues.append("rev")
        print(f"    {w['time']} {w['winner']:>4s} "
              f"0.85@tail+{w['tail_85']}s  {'+'.join(issues)}")


if __name__ == "__main__":
    asyncio.run(main())
