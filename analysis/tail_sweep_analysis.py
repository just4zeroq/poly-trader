#!/usr/bin/env python3
"""
Tail-End Sweep Backtest — analyze timing, pricing, and success rate.

Reads Polymarket BTC 15m trade data from /tmp/polymarket_cache.
Simulates the tail sweep strategy: last 3 minutes, winner 0.85-0.95,
dynamic profit (4¢ / 2¢), winner-first then loser, multi-round.

Usage:
  python3 analysis/tail_sweep_analysis.py
  python3 analysis/tail_sweep_analysis.py --month 2026-03
  python3 analysis/tail_sweep_analysis.py --month 2026-03 --limit 200
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from statistics import mean, median, stdev
from typing import Optional

# ── Constants ──

CACHE_DIR = "/tmp/polymarket_cache"
WINDOW_SECS = 15 * 60  # 900
TICK_SIZE = 0.01

# Tail sweep params
MIN_BID = 0.85
MAX_BID = 0.95
PROFIT_HIGH = 0.04   # 0.85-0.90 → 4¢
PROFIT_LOW = 0.02    # 0.90-0.95 → 2¢
TAIL_WINDOW = 180    # last 3 minutes


@dataclass
class WindowData:
    slug: str
    win_start: int
    time_series: dict[int, dict[str, list[float]]] = field(default_factory=dict)
    up_token_id: str = ""
    down_token_id: str = ""
    winner_side: str = ""
    # Cached per-minute summaries
    minute_avg: dict[int, dict[str, float]] = field(default_factory=dict)
    minute_vol: dict[int, dict[str, int]] = field(default_factory=dict)


@dataclass
class SweepRound:
    """One simulated sweep round within a window."""
    round_num: int
    trigger_offset: int        # seconds into window when triggered
    winner_side: str
    winner_bid: float          # estimated best_bid at trigger
    profit_target: float       # 0.04 or 0.02
    winner_price: float        # our buy price for winner
    loser_price: float         # our buy price for loser
    winner_filled: bool        # would winner fill?
    loser_filled: bool         # would loser fill?
    loser_available_sec: int   # seconds after trigger that loser price was available
    note: str = ""


@dataclass
class WindowResult:
    slug: str
    win_start: int
    tail_triggered: bool
    rounds: list[SweepRound] = field(default_factory=list)

    @property
    def any_winner_filled(self) -> bool:
        return any(r.winner_filled for r in self.rounds)

    @property
    def any_both_filled(self) -> bool:
        return any(r.winner_filled and r.loser_filled for r in self.rounds)

    @property
    def best_round(self) -> Optional[SweepRound]:
        complete = [r for r in self.rounds if r.winner_filled and r.loser_filled]
        return complete[0] if complete else (self.rounds[0] if self.rounds else None)


# ── Data Loading ──

def iter_windows(month_filter: Optional[str] = None, limit: Optional[int] = None):
    """Yield WindowData for each trade file."""
    files = sorted(os.listdir(CACHE_DIR))
    files = [f for f in files if f.endswith(".jsonl.gz")]

    if month_filter:
        files = [f for f in files if _extract_date(f).startswith(month_filter)]

    print(f"Files matching: {len(files)}", file=sys.stderr)
    count = 0
    for fname in files:
        if limit and count >= limit:
            break
        path = os.path.join(CACHE_DIR, fname)
        try:
            w = _load_window(path)
            if w:
                yield w
                count += 1
        except Exception as e:
            print(f"  Skip {fname}: {e}", file=sys.stderr)

    print(f"Loaded {count} windows", file=sys.stderr)


def _extract_date(fname: str) -> str:
    m = re.search(r"_(\d{2})-(\d{2})-(\d{4})_", fname)
    if m:
        return f"{m.group(3)}-{m.group(2)}"
    return ""


def _load_window(path: str) -> Optional[WindowData]:
    trades = []
    token_ids: set[str] = set()
    with gzip.open(path, "rt") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            trades.append(t)
            if t.get("makerAssetId") != "0":
                token_ids.add(t["makerAssetId"])
            if t.get("takerAssetId") != "0":
                token_ids.add(t["takerAssetId"])

    if not trades or len(token_ids) < 2:
        return None

    trades.sort(key=lambda t: int(t["timestamp"]))
    win_start = (int(trades[0]["timestamp"]) // WINDOW_SECS) * WINDOW_SECS
    non_zero = [tid for tid in token_ids if tid != "0"]

    ts: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for t in trades:
        ts_val = int(t["timestamp"])
        if ts_val < win_start or ts_val >= win_start + WINDOW_SECS:
            continue
        offset = ts_val - win_start
        taker_id = t["takerAssetId"]
        maker_amt = int(t["makerAmountFilled"])
        taker_amt = int(t["takerAmountFilled"])
        if taker_id == "0" or maker_amt <= 0 or taker_amt <= 0:
            continue
        price = maker_amt / taker_amt
        ts[offset][taker_id].append(price)

    if len(ts) < 10:
        return None

    id0, id1 = non_zero[0], non_zero[1]
    up_id, down_id, winner_side = _resolve_tokens(ts, id0, id1)
    if not up_id:
        return None

    # Build minute aggregates
    minute_avg: dict[int, dict[str, float]] = {}
    minute_vol: dict[int, dict[str, int]] = {}
    for minute in range(15):
        avg_map: dict[str, list[float]] = defaultdict(list)
        vol_map: dict[str, int] = defaultdict(int)
        for offset in range(minute * 60, (minute + 1) * 60):
            if offset in ts:
                for tid, prices in ts[offset].items():
                    avg_map[tid].extend(prices)
                    vol_map[tid] += len(prices)
        if avg_map:
            minute_avg[minute] = {tid: mean(prices) for tid, prices in avg_map.items()}
            minute_vol[minute] = dict(vol_map)

    return WindowData(
        slug=os.path.basename(path).replace("data_raw_trades_btc_15m_", "").replace("_fills.jsonl.gz", ""),
        win_start=win_start,
        time_series=ts,
        up_token_id=up_id,
        down_token_id=down_id,
        winner_side=winner_side,
        minute_avg=minute_avg,
        minute_vol=minute_vol,
    )


def _resolve_tokens(ts: dict[int, dict[str, list[float]]], id0: str, id1: str) -> tuple[str, str, str]:
    late_0, late_1 = [], []
    for offset, tick in ts.items():
        if offset >= 13 * 60:
            if id0 in tick:
                late_0.extend(tick[id0])
            if id1 in tick:
                late_1.extend(tick[id1])
    if not late_0 or not late_1:
        return "", "", ""
    if mean(late_0) > mean(late_1):
        return id0, id1, "Up"
    else:
        return id1, id0, "Down"


# ── Price Estimation ──

def _estimate_price(ts: dict[int, dict[str, list[float]]], token_id: str,
                    offset: int, window: int = 5) -> Optional[float]:
    """Best estimate of current market price = last trade price near offset."""
    for look in range(max(0, offset - window), min(WINDOW_SECS, offset + window + 1)):
        if look in ts and token_id in ts[look]:
            return ts[look][token_id][-1]
    return None


def _estimate_bid_ask(ts: dict[int, dict[str, list[float]]], token_id: str,
                       offset: int, window: int = 5) -> tuple[Optional[float], Optional[float]]:
    """Estimate bid/ask from recent trades.

    We approximate:
      - best_bid ≈ recent buy trades (last trade where someone bought)
      - best_ask ≈ last trade price × 1.01 (typical Polymarket spread)

    Since we only have trade data (no order book), we use:
      - bid ≈ last trade price at or before offset
      - ask ≈ bid × 1.01 (conservative 1% spread estimate)
    """
    p = _estimate_price(ts, token_id, offset, window)
    if p is None:
        return None, None
    # In the tail, spread tightens. Conservative 1% spread.
    spread = max(0.005, p * 0.01)
    return p - spread / 2, p + spread / 2


def _check_price_available(ts: dict[int, dict[str, list[float]]], token_id: str,
                           target_price: float, from_offset: int, max_lookahead: int = 60) -> Optional[int]:
    """Check if there's a trade at or below target_price in the lookahead window.

    For a BUY limit order at target_price to fill, someone must SELL at that price.
    We check if any trade happens at price ≤ target_price (meaning there were sellers).
    Returns the offset where it first becomes available, or None.
    """
    for look in range(from_offset, min(WINDOW_SECS, from_offset + max_lookahead)):
        if look in ts and token_id in ts[look]:
            for p in ts[look][token_id]:
                if p <= target_price:
                    return look
    return None


# ── Core Simulation ──

def simulate_window(w: WindowData) -> WindowResult:
    """Run tail sweep simulation on a single window. Multi-round: after both sides
    fill, try another round if still in tail window."""
    result = WindowResult(slug=w.slug, win_start=w.win_start, tail_triggered=False)
    ts = w.time_series
    up_id = w.up_token_id
    down_id = w.down_token_id

    # We scan the last 3 minutes: offsets 720-899
    # Sample every 5 seconds for opportunities
    start_offset = WINDOW_SECS - TAIL_WINDOW  # 720

    # Map side names to token IDs
    side_token = {"Up": up_id, "Down": down_id}

    round_num = 0
    offset = start_offset

    while offset < WINDOW_SECS:
        # ── Check if one side is in tail-sweep range ──
        up_price = _estimate_price(ts, up_id, offset)
        down_price = _estimate_price(ts, down_id, offset)

        if up_price is None or down_price is None:
            offset += 5
            continue

        # Determine winner: which side's price is in [0.85, 0.95]
        winner_side: Optional[str] = None
        winner_bid: Optional[float] = None

        if MIN_BID <= up_price <= MAX_BID:
            winner_side = "Up"
            winner_bid = up_price
        elif MIN_BID <= down_price <= MAX_BID:
            winner_side = "Down"
            winner_bid = down_price

        if winner_side is None or winner_bid is None:
            offset += 5
            continue

        # Check if winner has been consistently above 0.85 (not a flash spike)
        # Verify by looking back 10 seconds
        lookback_consistent = True
        for look_off in range(max(start_offset, offset - 10), offset + 1, 2):
            chk = _estimate_price(ts, side_token[winner_side], look_off)
            if chk is None or chk < 0.82:  # allow small dip
                lookback_consistent = False
                break
        if not lookback_consistent:
            offset += 5
            continue

        result.tail_triggered = True
        round_num += 1

        # ── Calculate profit target ──
        if winner_bid < 0.90:
            profit_target = PROFIT_HIGH  # 4¢
        else:
            profit_target = PROFIT_LOW   # 2¢

        # ── Price the winner and loser ──
        # Winner: buy at market (approx last trade price) or a bit higher
        # We need: winner_price + loser_price + profit_target <= 1.0
        # With tick size constraints

        # If winner is the hot side, we need to pay at least the last trade price
        winner_token = side_token[winner_side]
        loser_side = "Down" if winner_side == "Up" else "Up"
        loser_token = side_token[loser_side]

        # Winner price = estimate of best_ask for winner (what we pay to buy)
        _, win_ask = _estimate_bid_ask(ts, winner_token, offset)
        if win_ask is None:
            win_ask = winner_bid * 1.01

        # We must buy winner at ≤ max_win to leave room for profit
        max_win = 1.0 - profit_target - TICK_SIZE
        winner_price = min(win_ask, max_win)
        winner_price = round(winner_price / TICK_SIZE) * TICK_SIZE

        # Derive loser price
        raw_loser = 1.0 - winner_price - profit_target
        loser_price = round(raw_loser / TICK_SIZE) * TICK_SIZE
        if loser_price < TICK_SIZE:
            loser_price = TICK_SIZE
            winner_price = 1.0 - loser_price - profit_target
            winner_price = round(winner_price / TICK_SIZE) * TICK_SIZE

        total = winner_price + loser_price
        if winner_price <= 0 or loser_price <= 0 or total >= 1.0:
            offset += 5
            continue

        # ── Can we fill the winner? ──
        # Winner should fill if there are recent trades at this price or higher
        # (someone selling at our buy price)
        # Check: is there a trade at winner_price or below in ±5 sec?
        winner_fill_offset = _check_price_available(ts, winner_token, winner_price, offset - 3, 10)
        winner_filled = winner_fill_offset is not None

        # ── Can we fill the loser? ──
        # Loser is a limit buy at loser_price. Need sellers at or below that price.
        loser_fill_offset = _check_price_available(ts, loser_token, loser_price, offset + 1, 120)
        loser_filled = loser_fill_offset is not None
        loser_available_sec = (loser_fill_offset - offset) if loser_fill_offset else 0

        # Build note
        notes = []
        if not winner_filled:
            notes.append("winner_no_fill")
        if winner_filled and not loser_filled:
            notes.append("loser_no_fill_within_120s")

        round_result = SweepRound(
            round_num=round_num,
            trigger_offset=offset,
            winner_side=winner_side,
            winner_bid=winner_bid,
            profit_target=profit_target,
            winner_price=winner_price,
            loser_price=loser_price,
            winner_filled=winner_filled,
            loser_filled=loser_filled,
            loser_available_sec=loser_available_sec,
            note="; ".join(notes),
        )
        result.rounds.append(round_result)

        # If both filled, advance past this round and try next
        if winner_filled and loser_filled:
            offset += 15  # skip ahead a bit for next round
        else:
            offset += 5  # keep scanning

    return result


# ── Reporting ──

def _pct(a: int, b: int) -> str:
    if b == 0:
        return "N/A"
    return f"{a / b * 100:.1f}%"


def print_separator(title: str):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main():
    parser = argparse.ArgumentParser(description="Tail sweep backtest")
    parser.add_argument("--month", type=str, default=None, help="Filter: 2026-03")
    parser.add_argument("--limit", type=int, default=None, help="Max windows")
    args = parser.parse_args()

    month_label = args.month or "all"
    print(f"Loading {month_label} data...", file=sys.stderr)
    windows = list(iter_windows(month_filter=args.month, limit=args.limit))
    if not windows:
        print("No windows loaded!")
        sys.exit(1)

    total = len(windows)
    print(f"\nLoaded {total} windows\n")

    # ── Run simulation ──
    print("Simulating tail sweep...", file=sys.stderr)
    results = [simulate_window(w) for w in windows]

    # ── 1. Market overview ──
    print_separator("1. Market Overview — Last 3 Minutes Price Distribution")

    # Collect minute-13 and minute-14 prices
    tail_prices_up: list[float] = []
    tail_prices_down: list[float] = []
    tail_winners = 0
    tail_winner_side_up = 0

    for w in windows:
        # Minute 13 (offset 780-839) and 14 (offset 840-899)
        for minute in [13, 14]:
            avg = w.minute_avg.get(minute, {})
            if w.up_token_id in avg and w.down_token_id in avg:
                tail_prices_up.append(avg[w.up_token_id])
                tail_prices_down.append(avg[w.down_token_id])

        # Check if winner determined by minute 12-14
        if w.winner_side == "Up":
            tail_winner_side_up += 1
        up_late = _estimate_price(w.time_series, w.up_token_id, 870)
        down_late = _estimate_price(w.time_series, w.down_token_id, 870)
        if up_late and down_late and (up_late >= 0.85 or down_late >= 0.85):
            tail_winners += 1

    if tail_prices_up:
        p75_up = sorted(tail_prices_up)[int(len(tail_prices_up) * 0.75)]
        p25_up = sorted(tail_prices_up)[int(len(tail_prices_up) * 0.25)]
        p75_down = sorted(tail_prices_down)[int(len(tail_prices_down) * 0.75)]
        p25_down = sorted(tail_prices_down)[int(len(tail_prices_down) * 0.25)]
        print(f"  {'':>12s}  {'Up price':>10s}  {'Down price':>10s}")
        print(f"  {'─'*12}  {'─'*10}  {'─'*10}")
        print(f"  {'Mean':>12s}  {mean(tail_prices_up):>10.4f}  {mean(tail_prices_down):>10.4f}")
        print(f"  {'Median':>12s}  {median(tail_prices_up):>10.4f}  {median(tail_prices_down):>10.4f}")
        print(f"  {'P25':>12s}  {p25_up:>10.4f}  {p25_down:>10.4f}")
        print(f"  {'P75':>12s}  {p75_up:>10.4f}  {p75_down:>10.4f}")
        print()
        print(f"  Winner is Up: {_pct(tail_winner_side_up, total)}  ({tail_winner_side_up}/{total})")
        print(f"  Winner clear by min 12-14: {_pct(tail_winners, total)}")
        print(f"  (one side reaches ≥0.85 in last 3 min)")

    # ── 2. Tail sweep opportunity ──
    triggered = [r for r in results if r.tail_triggered]
    triggered_pct = len(triggered) / total * 100 if total else 0

    print_separator("2. Tail Sweep Opportunity Detection")
    print(f"  Windows with tail opportunity:  {len(triggered)} / {total}  ({triggered_pct:.1f}%)")
    print()

    if triggered:
        # Trigger timing distribution
        trigger_offsets = [r.rounds[0].trigger_offset for r in triggered if r.rounds]
        avg_trigger = mean(trigger_offsets) if trigger_offsets else 0
        print(f"  Average first trigger offset:  {avg_trigger:.0f}s  "
              f"(min {min(trigger_offsets):.0f}s, max {max(trigger_offsets):.0f}s)")

        # Which side
        up_wins = sum(1 for r in triggered if r.rounds and r.rounds[0].winner_side == "Up")
        print(f"  Winner = Up:   {_pct(up_wins, len(triggered))}  ({up_wins}/{len(triggered)})")
        print(f"  Winner = Down: {_pct(len(triggered) - up_wins, len(triggered))}")

        # Profit distribution
        high_profit = sum(1 for r in triggered if r.rounds and r.rounds[0].profit_target == PROFIT_HIGH)
        print(f"  4¢ profit target: {_pct(high_profit, len(triggered))}  "
              f"(winner bid < 0.90)")
        print(f"  2¢ profit target: {_pct(len(triggered) - high_profit, len(triggered))}")

    # ── 3. Fill success rate ──
    print_separator("3. Fill Success Rate")

    # First round analysis
    first_rounds = [r.rounds[0] for r in triggered if r.rounds]
    winner_filled = sum(1 for r in first_rounds if r.winner_filled)
    both_filled = sum(1 for r in first_rounds if r.winner_filled and r.loser_filled)

    print(f"  First round winner fill rate:  {_pct(winner_filled, len(first_rounds))}  "
          f"({winner_filled}/{len(first_rounds)})")
    print(f"  First round both fill rate:    {_pct(both_filled, len(first_rounds))}  "
          f"({both_filled}/{len(first_rounds)})")
    print()

    if first_rounds:
        only_winner_no_loser = sum(1 for r in first_rounds if r.winner_filled and not r.loser_filled)
        neither = sum(1 for r in first_rounds if not r.winner_filled)
        print(f"  Breakdown:")
        print(f"    Both filled:          {_pct(both_filled, len(first_rounds))}")
        print(f"    Only winner filled:   {_pct(only_winner_no_loser, len(first_rounds))}")
        print(f"    Neither filled:       {_pct(neither, len(first_rounds))}")
        print()

        # Multi-round opportunities
        multiple_rounds = [r for r in triggered if len(r.rounds) > 1]
        if multiple_rounds:
            print(f"  Multi-round windows:  {len(multiple_rounds)} / {len(triggered)}")
            for r in multiple_rounds:
                complete = sum(1 for rr in r.rounds if rr.winner_filled and rr.loser_filled)
                print(f"    {r.slug}: {len(r.rounds)} rounds ({complete} complete)")

    # ── 4. Pricing analysis ──
    print_separator("4. Pricing Analysis")

    if first_rounds:
        filled = [r for r in first_rounds if r.winner_filled and r.loser_filled]
        if filled:
            winner_bids = [r.winner_bid for r in filled]
            w_prices = [r.winner_price for r in filled]
            l_prices = [r.loser_price for r in filled]
            totals = [r.winner_price + r.loser_price for r in filled]

            print(f"  {'':>15s}  {'Winner Bid':>10s}  {'Win Price':>10s}  {'Loser Price':>10s}  {'Sum':>7s}")
            print(f"  {'─'*15}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*7}")
            print(f"  {'Mean':>15s}  {mean(winner_bids):>10.4f}  {mean(w_prices):>10.4f}  "
                  f"{mean(l_prices):>10.4f}  {mean(totals):>7.4f}")
            print(f"  {'Median':>15s}  {median(winner_bids):>10.4f}  {median(w_prices):>10.4f}  "
                  f"{median(l_prices):>10.4f}  {median(totals):>7.4f}")

            # Avg seconds for loser to become available
            avail_secs = [r.loser_available_sec for r in filled]
            print(f"\n  Avg loser availability delay: {mean(avail_secs):.0f}s  "
                  f"(median {median(avail_secs):.0f}s, max {max(avail_secs):.0f}s)")

            # Unfilled analysis
            unfilled = [r for r in first_rounds if r.winner_filled and not r.loser_filled]
            if unfilled:
                print(f"\n  Unfilled loser breakdown ({len(unfilled)} rounds):")
                up_loser = sum(1 for r in unfilled if r.winner_side == "Up")
                print(f"    Winner=Up (need Down to fill):   {up_loser}")
                print(f"    Winner=Down (need Up to fill): {len(unfilled) - up_loser}")
                avg_loser_price = mean([r.loser_price for r in unfilled])
                print(f"    Avg loser price: {avg_loser_price:.4f}")
        else:
            print("  No completed rounds to analyze.")

    # ── 5. Window-by-window failure reasons ──
    print_separator("5. Failure Mode Breakdown")

    no_trigger = total - len(triggered)
    fail_no_trigger = no_trigger
    fail_winner = sum(1 for r in triggered if r.rounds and not r.rounds[0].winner_filled)
    fail_loser = sum(1 for r in triggered if r.rounds and r.rounds[0].winner_filled and not r.rounds[0].loser_filled)
    success = sum(1 for r in triggered if r.rounds and r.rounds[0].winner_filled and r.rounds[0].loser_filled)

    print(f"  {'Category':<35s}  {'Count':>7s}  {'% of Total':>12s}")
    print(f"  {'─'*35}  {'─'*7}  {'─'*12}")
    print(f"  {'No tail opportunity':<35s}  {fail_no_trigger:>7d}  {_pct(fail_no_trigger, total):>12s}")
    print(f"  {'Winner order didn\'t fill':<35s}  {fail_winner:>7d}  {_pct(fail_winner, total):>12s}")
    print(f"  {'Loser order didn\'t fill':<35s}  {fail_loser:>7d}  {_pct(fail_loser, total):>12s}")
    print(f"  {'SUCCESS (both filled)':<35s}  {success:>7d}  {_pct(success, total):>12s}")

    # ── 6. Multi-round analysis ──
    print_separator("6. Multi-Round Analysis")
    multi = [r for r in triggered if len(r.rounds) > 1]
    if multi:
        total_rounds = sum(len(r.rounds) for r in multi)
        print(f"  Windows with multiple rounds:  {len(multi)}  (total rounds: {total_rounds})")
        complete_rounds = sum(sum(1 for rr in r.rounds if rr.winner_filled and rr.loser_filled) for r in multi)
        print(f"  Complete rounds:  {complete_rounds} / {total_rounds}  ({complete_rounds/total_rounds*100:.1f}%)")

        # Average rounds per window
        avg_rnd = mean(len(r.rounds) for r in multi)
        print(f"  Average rounds per window:  {avg_rnd:.1f}")

        # Sequential round analysis - compare first vs second round
        first_rnd = [r.rounds[0] for r in multi]
        second_rnd = [r.rounds[1] for r in multi if len(r.rounds) > 1]

        if second_rnd:
            first_success = sum(1 for r in first_rnd if r.winner_filled and r.loser_filled)
            second_success = sum(1 for r in second_rnd if r.winner_filled and r.loser_filled)
            print(f"\n  Round 1 success rate:  {_pct(first_success, len(first_rnd))}")
            print(f"  Round 2 success rate:  {_pct(second_success, len(second_rnd))}")
    else:
        print("  No multi-round opportunities found.")

    # ── 7. Summary ──
    print_separator("7. P&L Estimation (per contract pair, assuming $1 payout)")

    if filled_rounds := [r for r in first_rounds if r.winner_filled and r.loser_filled]:
        profits = [r.profit_target for r in filled_rounds]
        total_profit = sum(profits)
        avg_profit = mean(profits)
        max_profit = max(profits)
        print(f"  Total windows analyzed:            {total}")
        print(f"  Windows with tail opportunity:     {len(triggered)} ({triggered_pct:.1f}%)")
        print(f"  Successful tail sweeps:            {len(filled_rounds)} ({len(filled_rounds)/total*100:.1f}%)")
        print()
        print(f"  Per successful pair:")
        print(f"    Average profit:                  {avg_profit * 100:.1f}¢ per contract")
        print(f"    Total profit (all pairs):        ${total_profit:.2f}")
        print(f"    Max profit:                      {max_profit * 100:.0f}¢")
        print()

        # Simplified PnL: per_tick=5, sum all profits
        if len(filled_rounds) > 0:
            per_tick = 5
            total_pnl = sum(r.profit_target for r in filled_rounds) * per_tick
            print(f"  Simulated PnL (per_tick={per_tick}):")
            print(f"    Total pairs executed:            {len(filled_rounds)}")
            print(f"    Total contracts:                 {len(filled_rounds) * per_tick}")
            print(f"    Gross profit:                    ${total_pnl:.2f}")
    else:
        print("  No successful sweeps to calculate P&L.")

    print()
    print(f"{'═' * 72}")
    print(f"  End of analysis — {total} windows, {month_label}")
    print(f"{'═' * 72}")


if __name__ == "__main__":
    main()
