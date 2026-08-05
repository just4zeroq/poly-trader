"""Tests for fill dedup hardening in _handle_trade.

Polymarket's User WS matched_amount semantics are ambiguous: an event whose
matched_amount is ≤ the cumulative we already recorded could be a true replay
(no new fill) OR an incremental-style event (real new fill progress).  The
fix resolves the ambiguity against CLOB ground truth (get_order_filled)
instead of silently dropping, which previously lost real fills.
"""
import asyncio
import time
from types import SimpleNamespace

from poly_trader.platforms.executors import LiveExecutor
from poly_trader.platforms.models import PendingOrder, WindowState

SLUG = "btc-updown-15m-1700000000"


def make_executor(clob_cum, seen_filled=0):
    """Executor wired to a fake engine whose sdk.get_order_filled returns *clob_cum*.

    *seen_filled* seeds both the pending order's local fill counter and
    ``_fill_seen``, which stay in sync during normal operation.
    """
    ex = LiveExecutor()
    ws = WindowState(slug=SLUG, start_time=0.0, window_num=1)
    ws.pending_orders["ord_1"] = PendingOrder(
        order_id="ord_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=8, filled=seen_filled)

    calls = {"get_order_filled": 0}

    async def emit(name, event):
        pass

    async def get_order_filled(order_id):
        calls["get_order_filled"] += 1
        return clob_cum

    ex.engine = SimpleNamespace(
        _windows={SLUG: ws},
        _emit=emit,
        sdk=SimpleNamespace(get_order_filled=get_order_filled),
    )
    if seen_filled > 0:
        ex._fill_seen["ord_1"] = seen_filled
    return ex, ws, calls


def payload(matched_amount, price=0.55):
    """A UserTradeEvent payload with a single maker order."""
    return SimpleNamespace(maker_orders=[
        SimpleNamespace(order_id="ord_1",
                        matched_amount=str(matched_amount),
                        price=str(price)),
    ])


def test_ambiguous_event_resolved_via_clob():
    """matched_amount ≤ seen cumulative but CLOB shows more fill → apply the diff.

    This is the bug class that was silently dropping real fills: an
    incremental-style event (2 new shares) arrives as matched_amount=2 while
    _fill_seen already holds 4.  CLOB truth (cum=6) must recover the +2.
    """
    ex, ws, calls = make_executor(clob_cum=6, seen_filled=4)

    asyncio.run(ex._handle_trade(payload(4)))

    assert ws.inventory["Up"] == 2, "CLOB-confirmed fill must be applied"
    assert ws.pending_orders["ord_1"].filled == 6
    assert ex._fill_seen["ord_1"] == 6
    assert calls["get_order_filled"] == 1


def test_true_replay_still_dropped():
    """matched_amount ≤ seen and CLOB agrees → genuine replay, no inventory change."""
    ex, ws, calls = make_executor(clob_cum=4, seen_filled=4)

    asyncio.run(ex._handle_trade(payload(4)))

    assert ws.inventory["Up"] == 0
    assert ex._fill_seen["ord_1"] == 4
    assert calls["get_order_filled"] == 1  # queried once, then confirmed replay


def test_ambiguous_query_throttled_per_order():
    """At most one CLOB query per order per throttle window."""
    ex, ws, calls = make_executor(clob_cum=6, seen_filled=4)

    # First ambiguous event → queries CLOB, applies +2
    asyncio.run(ex._handle_trade(payload(4)))
    assert calls["get_order_filled"] == 1
    assert ws.inventory["Up"] == 2

    # Second ambiguous event inside the throttle window → dropped, no query
    asyncio.run(ex._handle_trade(payload(5)))
    assert calls["get_order_filled"] == 1
    assert ws.inventory["Up"] == 2


def test_normal_incremental_fill_unchanged():
    """fill_size > seen cumulative still takes the fast path, no CLOB query."""
    ex, ws, calls = make_executor(clob_cum=0)
    ex._fill_seen["ord_1"] = 2

    asyncio.run(ex._handle_trade(payload(5)))

    assert ws.inventory["Up"] == 3
    assert ex._fill_seen["ord_1"] == 5
    assert calls["get_order_filled"] == 0
