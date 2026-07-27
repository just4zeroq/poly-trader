#!/usr/bin/env python3
"""
Tick-level performance analysis for the main strategy engine.

Parses the ``⏱ tick#...`` timing log lines emitted by the instrumented tick loop
in ``engine.py`` and produces latency statistics, phase breakdowns, and
distribution summaries.

Usage::

    # Analyze a log file (most recent run)
    python3 analysis/perf_analysis.py paper_trading.log

    # Analyze with per-window breakdown
    python3 analysis/perf_analysis.py --by-window paper_trading.log

    # Live tail — monitor tick latency in real time
    python3 analysis/perf_analysis.py --live paper_trading.log

    # Flag only slow ticks (> 200ms total)
    python3 analysis/perf_analysis.py --slow-threshold 200 paper_trading.log

    # Show P99 / slowest 10 ticks
    python3 analysis/perf_analysis.py --top-slow 10 paper_trading.log

Output columns::

    phase      min    max   mean  median    p95    p99
    ───────────────────────────────────────────────────
    price     0.8ms  5.2ms  1.3ms   1.1ms  2.1ms  3.8ms
    decide    0.3ms  1.8ms  0.6ms   0.5ms  0.9ms  1.4ms
    emit      0.1ms  0.9ms  0.2ms   0.1ms  0.3ms  0.5ms
    place*    80ms  450ms  150ms   130ms  320ms  420ms
    total     82ms  455ms  153ms   133ms  324ms  425ms
    *place = idle ticks excluded (0ms skew)

Each phase maps to:
  - price   — get_tick_size ×2 + _resolve_pair_prices (pure compute)
  - decide  — kill-switch check + cancel-replace + strategy.decide()
  - emit    — event dispatch + logging (I/O bound)
  - place   — order placement API call (network I/O)
  - total   — wall-clock duration of the full tick
"""
from __future__ import annotations

import argparse
import collections
import math
import os
import re
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

# ── Regex for tick timing log line ──
# Example:
#   08:15:23.456 [INFO]   ⏱ tick#42 total=152.3ms (price=3.1ms decide=0.8ms emit=2.4ms place=146.0ms) decisions=2 pending=2 inv=U150/D148
_TICK_RE = re.compile(
    r"⏱\s+tick#(\d+)\s+"
    r"total=([\d.]+)ms\s+"
    r"\(price=([\d.]+)ms\s+"
    r"decide=([\d.]+)ms\s+"
    r"emit=([\d.]+)ms\s+"
    r"place=([\d.]+)ms\)\s+"
    r"decisions=(\d+)\s+"
    r"pending=(\d+)\s+"
    r"inv=U(\d+)/D(\d+)"
)

# Window boundary regex (for --by-window grouping)
_WINDOW_START_RE = re.compile(r"Window #(\d+):")


@dataclass
class TickRecord:
    """Parsed timing data for a single tick."""
    tick_num: int
    total_ms: float
    price_ms: float
    decide_ms: float
    emit_ms: float
    place_ms: float
    decisions: int
    pending: int
    inv_up: int
    inv_down: int
    window_num: int = 0     # populated when --by-window

    @property
    def is_idle(self) -> bool:
        return self.decisions == 0

    @property
    def is_active(self) -> bool:
        return self.decisions > 0


@dataclass
class PhaseStats:
    """Aggregate statistics for a single timing phase."""
    name: str
    values: list[float] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def min(self) -> float:
        return min(self.values) if self.values else 0.0

    @property
    def max(self) -> float:
        return max(self.values) if self.values else 0.0

    @property
    def mean(self) -> float:
        return statistics.mean(self.values) if self.values else 0.0

    @property
    def median(self) -> float:
        return statistics.median(self.values) if self.values else 0.0

    @property
    def p95(self) -> float:
        return _percentile(self.values, 95)

    @property
    def p99(self) -> float:
        return _percentile(self.values, 99)

    @property
    def std(self) -> float:
        return statistics.stdev(self.values) if len(self.values) >= 2 else 0.0


def _percentile(sorted_or_not: list[float], pct: float) -> float:
    """Return the pct-th percentile of values (linear interpolation)."""
    if not sorted_or_not:
        return 0.0
    vals = sorted(sorted_or_not)
    k = (pct / 100.0) * (len(vals) - 1)
    f = int(k)
    c = k - f
    if f + 1 < len(vals):
        return vals[f] + c * (vals[f + 1] - vals[f])
    return vals[f]


# ── Parsing ──


def parse_log_file(path: str) -> list[TickRecord]:
    """Read a log file and return all parsed tick timing records."""
    records: list[TickRecord] = []
    current_window = 0

    with open(path, "r") as f:
        for line in f:
            # Track window boundaries
            m = _WINDOW_START_RE.search(line)
            if m:
                current_window = int(m.group(1))

            # Parse tick timing
            m = _TICK_RE.search(line)
            if m:
                r = TickRecord(
                    tick_num=int(m.group(1)),
                    total_ms=float(m.group(2)),
                    price_ms=float(m.group(3)),
                    decide_ms=float(m.group(4)),
                    emit_ms=float(m.group(5)),
                    place_ms=float(m.group(6)),
                    decisions=int(m.group(7)),
                    pending=int(m.group(8)),
                    inv_up=int(m.group(9)),
                    inv_down=int(m.group(10)),
                    window_num=current_window,
                )
                records.append(r)

    return records


def parse_live_stream(line: str) -> Optional[TickRecord]:
    """Parse a single line from a live stream. Returns None if not a tick line."""
    m = _TICK_RE.search(line)
    if not m:
        return None
    return TickRecord(
        tick_num=int(m.group(1)),
        total_ms=float(m.group(2)),
        price_ms=float(m.group(3)),
        decide_ms=float(m.group(4)),
        emit_ms=float(m.group(5)),
        place_ms=float(m.group(6)),
        decisions=int(m.group(7)),
        pending=int(m.group(8)),
        inv_up=int(m.group(9)),
        inv_down=int(m.group(10)),
    )


# ── Analysis ──


def build_stats(records: list[TickRecord]) -> dict[str, PhaseStats]:
    """Compute per-phase statistics across all records."""
    phases = {
        "price": PhaseStats("price"),
        "decide": PhaseStats("decide"),
        "emit": PhaseStats("emit"),
        "place": PhaseStats("place (active only)"),
        "total": PhaseStats("total"),
    }

    for r in records:
        phases["price"].values.append(r.price_ms)
        phases["decide"].values.append(r.decide_ms)
        phases["emit"].values.append(r.emit_ms)
        phases["total"].values.append(r.total_ms)
        if r.is_active:
            phases["place"].values.append(r.place_ms)

    return phases


def build_window_stats(records: list[TickRecord]) -> dict[int, dict[str, PhaseStats]]:
    """Group records by window_num and compute per-window statistics."""
    by_window: dict[int, list[TickRecord]] = collections.defaultdict(list)
    for r in records:
        by_window[r.window_num].append(r)
    return {wn: build_stats(recs) for wn, recs in by_window.items()}


# ── Output ──


def print_header(title: str):
    print()
    print("═" * 70)
    print(f"  {title}")
    print("═" * 70)


def print_stats_table(phases: dict[str, PhaseStats]):
    """Print a formatted statistics table."""
    header = f"{'phase':<22s} {'count':>6s} {'min':>7s} {'max':>7s} {'mean':>7s} {'median':>7s} {'p95':>7s} {'p99':>7s}"
    print(header)
    print("─" * len(header))

    for name, st in phases.items():
        if st.count == 0:
            continue
        print(
            f"{name:<22s} {st.count:>6d} "
            f"{st.min:>6.1f}ms {st.max:>6.1f}ms {st.mean:>6.1f}ms "
            f"{st.median:>6.1f}ms {st.p95:>6.1f}ms {st.p99:>6.1f}ms"
        )


def print_distribution(records: list[TickRecord], phase: str = "total",
                       bins: int = 10):
    """Print a text-based histogram of a timing phase."""
    values = [getattr(r, f"{phase}_ms") for r in records]
    if not values:
        return

    vmin, vmax = min(values), max(values)
    if vmin == vmax:
        print(f"  All {phase}: {vmin:.1f}ms")
        return

    bin_width = (vmax - vmin) / bins
    hist = [0] * bins
    for v in values:
        idx = min(int((v - vmin) / bin_width), bins - 1)
        hist[idx] += 1

    max_count = max(hist)
    bar_width = 40

    print(f"\n  {phase} distribution ({len(values)} ticks, "
          f"range {vmin:.1f}-{vmax:.1f}ms):")
    for i in range(bins):
        lo = vmin + i * bin_width
        hi = lo + bin_width
        bar = "█" * int(hist[i] / max_count * bar_width) if max_count > 0 else ""
        print(f"  {lo:>6.1f}-{hi:>6.1f}ms  {hist[i]:>5d}  {bar}")


def print_summary(records: list[TickRecord]):
    """Print a high-level summary of the tick data."""
    total = len(records)
    active = sum(1 for r in records if r.is_active)
    idle = total - active
    windows = len(set(r.window_num for r in records if r.window_num > 0))

    print_header("Summary")
    print(f"  Total ticks:  {total}")
    print(f"  Active:       {active}  ({100*active/max(total,1):.0f}%)")
    print(f"  Idle:         {idle}  ({100*idle/max(total,1):.0f}%)")
    print(f"  Windows:      {windows or 'N/A'}")

    # Slow tick breakdown
    if active > 0:
        place_vals = [r.place_ms for r in records if r.is_active]
        total_vals = [r.total_ms for r in records]
        print(f"\n  Place phase (API call) dominates total tick time:")
        place_pct = sum(place_vals) / max(sum(total_vals), 0.001) * 100
        print(f"    place / total = {place_pct:.0f}% of total wall-clock")

    # Ticks per window
    if windows and windows > 0:
        by_win = collections.defaultdict(int)
        for r in records:
            if r.window_num > 0:
                by_win[r.window_num] += 1
        ticks_per_win = list(by_win.values())
        if ticks_per_win:
            print(f"\n  Ticks per window:  min={min(ticks_per_win)}  "
                  f"max={max(ticks_per_win)}  "
                  f"mean={statistics.mean(ticks_per_win):.0f}  "
                  f"median={statistics.median(ticks_per_win):.0f}")


def print_slowest(records: list[TickRecord], top_n: int = 10):
    """Print the N slowest ticks."""
    sorted_recs = sorted(records, key=lambda r: r.total_ms, reverse=True)
    print_header(f"Slowest {min(top_n, len(sorted_recs))} Ticks")
    print(f"  {'tick#':>6s}  {'total':>7s}  {'price':>7s}  {'decide':>7s}  "
          f"{'emit':>7s}  {'place':>7s}  {'decisions':>9s}  inv(U/D)")
    print("  " + "─" * 70)
    for r in sorted_recs[:top_n]:
        print(
            f"  {r.tick_num:>6d}  {r.total_ms:>6.1f}ms  "
            f"{r.price_ms:>6.1f}ms  {r.decide_ms:>6.1f}ms  "
            f"{r.emit_ms:>6.1f}ms  {r.place_ms:>6.1f}ms  "
            f"{'active' if r.is_active else 'idle':>9s}  "
            f"U{r.inv_up}/D{r.inv_down}"
        )


def print_per_window(records: list[TickRecord], top_slow_threshold: float = 200.0):
    """Print per-window breakdown with slow-tick counts."""
    by_win = build_window_stats(records)
    if not by_win:
        print("  No window data — run with logs containing window boundaries.")
        return

    print_header("Per-Window Breakdown")
    header = (f"  {'Win#':>5s}  {'ticks':>6s}  {'active':>6s}  "
              f"{'mean':>7s}  {'p95':>7s}  {'p99':>7s}  "
              f"{'slow(>' + str(int(top_slow_threshold)) + 'ms)':>12s}")
    print(header)
    print("  " + "─" * len(header))

    for wn in sorted(by_win):
        st = by_win[wn]
        total_st = st["total"]
        if total_st.count == 0:
            continue
        place_st = st["place"]
        slow_count = sum(1 for v in total_st.values if v > top_slow_threshold)
        print(
            f"  {wn:>5d}  {total_st.count:>6d}  {place_st.count:>6d}  "
            f"{total_st.mean:>6.1f}ms  {total_st.p95:>6.1f}ms  "
            f"{total_st.p99:>6.1f}ms  {slow_count:>12d}"
        )


# ── Live monitoring ──


def live_monitor(path: str, slow_threshold: float = 200.0):
    """Tail a log file and print tick latency in real time.

    Waits for the file to appear, then follows it like ``tail -f``.
    Prints one compact line per tick:
      ✓ tick#42  152ms  (place=146ms)  decisions=2
      ⚠ tick#58  520ms  (place=510ms)  decisions=2   SLOW total > 200ms
    """
    print(f"  Live monitoring: {path}")
    print(f"  Slow threshold: {slow_threshold}ms")
    print(f"  Waiting for log output... (Ctrl-C to stop)")
    print()

    # Wait for file
    while not os.path.exists(path):
        time.sleep(1)

    try:
        with open(path, "r") as f:
            # Seek to end (like tail -f)
            f.seek(0, 2)

            while True:
                line = f.readline()
                if line:
                    r = parse_live_stream(line)
                    if r is None:
                        continue

                    # Compact one-line output
                    slow_flag = "⚠" if r.total_ms > slow_threshold else "✓"
                    detail = f"place={r.place_ms:.0f}ms" if r.is_active else "idle"
                    extra = f"  SLOW total > {slow_threshold}ms" if r.total_ms > slow_threshold else ""
                    print(
                        f"  {slow_flag} tick#{r.tick_num:<5d}  "
                        f"{r.total_ms:>6.0f}ms  ({detail})  "
                        f"decisions={r.decisions}{extra}"
                    )
                else:
                    time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n  Stopped.")


# ── Main ──


def main():
    parser = argparse.ArgumentParser(
        description="Tick-level performance analysis for poly_trader engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "logfile", type=str, nargs="?", default="paper_trading.log",
        help="Path to the log file (default: paper_trading.log)",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Tail log file and print tick latency in real time",
    )
    parser.add_argument(
        "--by-window", action="store_true",
        help="Show per-window breakdown",
    )
    parser.add_argument(
        "--top-slow", type=int, default=10, metavar="N",
        help="Show the N slowest ticks (default: 10)",
    )
    parser.add_argument(
        "--slow-threshold", type=float, default=200.0, metavar="MS",
        help="Highlight ticks slower than this threshold in ms (default: 200)",
    )
    parser.add_argument(
        "--dist", action="store_true",
        help="Show text-based distribution histogram",
    )
    args = parser.parse_args()

    # Live mode
    if args.live:
        live_monitor(args.logfile, args.slow_threshold)
        return

    # Batch mode
    if not os.path.exists(args.logfile):
        print(f"ERROR: Log file not found: {args.logfile}")
        print(f"  Usage: python3 analysis/perf_analysis.py <logfile>")
        # Search for recent log files
        candidates = []
        for f in os.listdir("."):
            if f.endswith(".log") and os.path.isfile(f):
                candidates.append(f)
        if candidates:
            print(f"\n  Found log files in current directory:")
            for c in sorted(candidates):
                size_kb = os.path.getsize(c) / 1024
                print(f"    {c}  ({size_kb:.0f} KB)")
        sys.exit(1)

    print(f"  Parsing: {args.logfile}  ({os.path.getsize(args.logfile)/1024:.0f} KB)")

    records = parse_log_file(args.logfile)

    if not records:
        print("  No tick timing data found. Make sure the log contains '⏱ tick#' lines.")
        print("  These are emitted by the instrumented tick loop in engine.py.")
        sys.exit(1)

    # ── Summary ──
    print_summary(records)

    # ── Phase statistics ──
    phases = build_stats(records)
    print_header("Phase Latency Statistics")
    print_stats_table(phases)

    # ── Slowest ticks ──
    print_slowest(records, args.top_slow)

    # ── Per-window breakdown ──
    if args.by_window:
        print_per_window(records, args.slow_threshold)

    # ── Distribution ──
    if args.dist:
        print_header("Latency Distribution")
        print_distribution(records, "total")
        active_recs = [r for r in records if r.is_active]
        if active_recs:
            print_distribution(active_recs, "place")

    # ── Final advice ──
    place_st = phases.get("place")
    if place_st and place_st.count > 0:
        print()
        if place_st.p95 > 500:
            print("  ⚠  P95 place latency > 500ms — order placement is the bottleneck.")
            print("     Consider: network proximity, CLOB API rate limits, connection pooling.")
        elif place_st.p95 > 200:
            print("  ⚡ P95 place latency {:.0f}ms — acceptable but worth monitoring.".format(place_st.p95))
        else:
            print("  ✅ P95 place latency {:.0f}ms — healthy.".format(place_st.p95))

        total_st = phases["total"]
        compute_total = (phases["price"].p95 + phases["decide"].p95 +
                         phases["emit"].p95)
        print(f"     Compute phases (price+decide+emit) P95: {compute_total:.1f}ms")
        print(f"     Place phase P95: {place_st.p95:.1f}ms")
        if total_st.p95 > 0:
            compute_pct = compute_total / total_st.p95 * 100
            place_pct = place_st.p95 / total_st.p95 * 100
            print(f"     → {compute_pct:.0f}% compute overhead, {place_pct:.0f}% network I/O")


if __name__ == "__main__":
    main()
