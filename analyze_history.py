"""Deep analysis of Polymarket BTC-updown-15m historical patterns."""
import asyncio, time
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


async def main():
    import sys
    sys.path.insert(0, "/home/ubuntu/code")
    from poly_trader.client import SdkClient
    from poly_trader.config import Config
    from poly_trader.main import parse_market_spec

    cfg = Config()
    spec = parse_market_spec("btc-updown-15m")
    sdk = SdkClient(cfg)

    duration = spec.duration_min * 60
    now = int(time.time())
    current_ws = (now // duration) * duration

    all_windows = []

    for offset in range(24):
        ws_start = current_ws - (offset * duration)
        ws_end = ws_start + duration
        slug = f"{spec.slug_pattern}-{ws_start}"

        market = await sdk.get_market_by_slug(slug)
        if not market:
            all_windows.append({"ws_start": ws_start, "found": False})
            await asyncio.sleep(0.2)
            continue

        up_tid = market.up_token_id
        down_tid = market.down_token_id
        trades = await fetch_trades(market.condition_id, ws_start, ws_end)
        await asyncio.sleep(0.2)

        if not trades:
            all_windows.append({"ws_start": ws_start, "found": True, "trades_count": 0})
            continue

        # Parse trades
        up_series, down_series = [], []
        for t in trades:
            ts = int(t.get("timestamp", 0))
            price = float(t.get("price", 0))
            tid = t.get("asset", "")
            if tid == up_tid or t.get("outcome") == "Up":
                up_series.append((ts, price))
            elif tid == down_tid or t.get("outcome") == "Down":
                down_series.append((ts, price))
        up_series.sort(key=lambda x: x[0])
        down_series.sort(key=lambda x: x[0])

        # Sample every 5s
        def sample(series, interval=5):
            result, last_ts = [], 0
            for ts, p in series:
                if ts - last_ts >= interval:
                    result.append((ts, p))
                    last_ts = ts
            return result

        up_s = sample(up_series)
        down_s = sample(down_series)

        # Winner
        up_end = up_s[-1][1] if up_s else 0
        down_end = down_s[-1][1] if down_s else 0
        winner = "Up" if up_end > down_end else "Down"
        win_series = up_s if winner == "Up" else down_s
        lose_series = down_s if winner == "Up" else up_s

        # Threshold crossing times
        def first_cross(series, threshold):
            for ts, p in series:
                if p >= threshold:
                    return ts
            return None

        t_85 = first_cross(win_series, 0.85)
        t_90 = first_cross(win_series, 0.90)
        t_95 = first_cross(win_series, 0.95)

        tail_start = ws_end - 180
        tail_entry_win = next((p for ts, p in win_series if abs(ts - tail_start) < 30), None)

        # Did loser lead during tail
        tail_loser = [(ts, p) for ts, p in lose_series if ts >= tail_start]
        tail_winner = [(ts, p) for ts, p in win_series if ts >= tail_start]
        loser_led = False
        if tail_loser and tail_winner:
            min_ts = min(tail_loser[0][0], tail_winner[0][0])
            max_ts = ws_end
            for t in range(int(min_ts), int(max_ts), 10):
                wp = next((p for ts, p in tail_winner if abs(ts - t) < 10), None)
                lp = next((p for ts, p in tail_loser if abs(ts - t) < 10), None)
                if wp and lp and lp > wp:
                    loser_led = True
                    break

        # Dip after 85
        dipped = False
        if t_85:
            for ts, p in win_series:
                if ts > t_85 and p < 0.85:
                    dipped = True
                    break

        # 85->95 time
        t85_95 = (t_95 - t_85) if (t_85 and t_95 and t_95 > t_85) else None

        # Tail-relative times
        tail_85 = (t_85 - tail_start) if t_85 else None

        all_windows.append({
            "ws_start": ws_start,
            "found": True,
            "trades_count": len(trades),
            "winner": winner,
            "t_85": t_85,
            "t_90": t_90,
            "t_95": t_95,
            "tail_entry_win": tail_entry_win,
            "tail_85": tail_85,
            "time_85_to_95": t85_95,
            "dipped": dipped,
            "loser_led": loser_led,
        })

        start_str = datetime.fromtimestamp(ws_start, tz=timezone.utc).strftime('%H:%M')
        if t_85:
            t85_str = f"+{t_85-ws_start}s"
            t95_str = f"+{t_95-ws_start}s" if t_95 else "N/A"
            tail_label = f"(tail+{tail_85}s)" if tail_85 and tail_85 > 0 else "(before tail)"
            print(f"  {start_str} {winner:>4s}  85@ {t85_str:>6s} {tail_label:>16s}  95@ {t95_str:>6s}  dip={'Y' if dipped else 'N'}  entry={tail_entry_win:.4f}" if tail_entry_win else "")
        else:
            print(f"  {start_str} {winner:>4s}  never hit 0.85  entry={tail_entry_win:.4f}" if tail_entry_win else f"  {start_str} {winner:>4s}  never hit 0.85")

    print(f"\n{'='*90}")
    print(f"  SUMMARY")
    print(f"{'='*90}")

    found = [w for w in all_windows if w.get("found") and w.get("trades_count", 0) > 0]
    print(f"  Windows with data: {len(found)}")

    sweep_ok = [w for w in found if w.get("t_85")]
    print(f"  Windows hitting 0.85+: {len(sweep_ok)}/{len(found)}")

    during_tail = [w for w in sweep_ok if w.get("tail_85") and w["tail_85"] > 0]
    before_tail = [w for w in sweep_ok if w.get("tail_85") and w["tail_85"] <= 0]
    print(f"  Hit 0.85 DURING tail: {len(during_tail)}")
    print(f"  Hit 0.85 BEFORE tail: {len(before_tail)}")

    if during_tail:
        avg_tail = sum(w["tail_85"] for w in during_tail) / len(during_tail)
        print(f"  Avg tail delay to 0.85: {avg_tail:.0f}s")
        print(f"  Range: {min(w['tail_85'] for w in during_tail)}s - {max(w['tail_85'] for w in during_tail)}s")

    t85_95_list = [w["time_85_to_95"] for w in sweep_ok if w.get("time_85_to_95")]
    if t85_95_list:
        print(f"  Avg 0.85→0.95 time: {sum(t85_95_list)/len(t85_95_list):.0f}s")
        print(f"  Range: {min(t85_95_list)}s - {max(t85_95_list)}s")

    dips = [w for w in sweep_ok if w.get("dipped")]
    print(f"  Dipped below 0.85 after crossing: {len(dips)}/{len(sweep_ok)}")

    revs = [w for w in during_tail if w.get("loser_led")]
    print(f"  Loser led during tail: {len(revs)}/{len(during_tail)}")

    up_wins = [w for w in found if w["winner"] == "Up"]
    down_wins = [w for w in found if w["winner"] == "Down"]
    print(f"  Winner split: Up={len(up_wins)} Down={len(down_wins)}")

    entries = [w["tail_entry_win"] for w in found if w.get("tail_entry_win") is not None]
    if entries:
        resolved = sum(1 for p in entries if p >= 0.95)
        sweep_range = sum(1 for p in entries if 0.85 <= p < 0.95)
        far = sum(1 for p in entries if p < 0.85)
        print(f"  At tail start: resolved(>0.95)={resolved} sweep(0.85-0.95)={sweep_range} far(<0.85)={far}")

    if during_tail:
        print(f"\n  60s CUTOFF IMPACT:")
        for w in during_tail:
            remaining = 180 - w["tail_85"]
            ts = datetime.fromtimestamp(w["ws_start"], tz=timezone.utc).strftime('%H:%M')
            label = "OK" if remaining > 60 else "BLOCKED"
            print(f"    {ts}: 0.85 at tail+{w['tail_85']}s ({remaining}s left) -> {label}")
        viable = sum(1 for w in during_tail if 180 - w["tail_85"] > 60)
        print(f"    Would succeed: {viable}/{len(during_tail)}")

    await sdk.close()


if __name__ == "__main__":
    asyncio.run(main())
