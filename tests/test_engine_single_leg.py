"""V4 single-leg order placement via TradingEngine._place_decisions."""

import asyncio
import time

from poly_trader.platforms.config import Config
from poly_trader.platforms.engine import PriceCache, TradingEngine
from poly_trader.platforms.models import (
    Decision, MarketInfo, PendingOrder, WindowState,
)


def make_place_engine(oid="oid"):
    """Engine whose SDK place_limit_order returns a fixed oid, with a live
    window registered so LiveExecutor.place has state to write into."""
    cfg = Config()
    engine = TradingEngine(cfg)
    engine._windows["btc-updown-15m-1700000000"] = WindowState(
        slug="btc-updown-15m-1700000000", window_num=1)

    async def fake_place_limit_order(**kwargs):
        return oid
    engine.sdk.place_limit_order = fake_place_limit_order
    return engine


def make_engine():
    cfg = Config()
    engine = TradingEngine(cfg)
    engine._markets["btc-updown-15m-1700000000"] = MarketInfo(
        slug="btc-updown-15m-1700000000",
        condition_id="cid",
        tokens={"Up": {"token_id": "up", "outcome": "Up"},
                "Down": {"token_id": "down", "outcome": "Down"}},
    )
    return engine


def run_place_decisions(engine, market, ws, decisions, results=(True,)):
    """Call _place_decisions with a recording fake _place_order.

    ``results`` is an iterable of return values consumed one per call.
    """
    calls = []
    results = list(results)

    async def fake_place_order(slug, token_id, outcome, price, amount,
                               is_favorite=False):
        calls.append((slug, token_id, outcome, price, amount))
        return results.pop(0) if results else True
    engine._place_order = fake_place_order

    ok = asyncio.run(engine._place_decisions(market, ws, decisions))
    return ok, calls


def test_place_decisions_single_leg_routes_token():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    d = Decision(side="Up", amount=5, price=0.55)

    ok, calls = run_place_decisions(engine, market, ws, [d])
    assert ok is True
    assert calls == [(market.slug, "up", "Up", 0.55, 5)]


def test_place_decisions_both_sides():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    decisions = [
        Decision(side="Up", amount=5, price=0.55),
        Decision(side="Down", amount=5, price=0.43),
    ]

    ok, calls = run_place_decisions(engine, market, ws, decisions)
    assert ok is True
    assert calls == [
        (market.slug, "up", "Up", 0.55, 5),
        (market.slug, "down", "Down", 0.43, 5),
    ]


def test_place_decisions_any_ok_true_on_partial_failure():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    decisions = [
        Decision(side="Up", amount=5, price=0.55),
        Decision(side="Down", amount=5, price=0.43),
    ]

    ok, calls = run_place_decisions(engine, market, ws, decisions, results=(False, True))
    assert ok is True  # at least one placed
    assert len(calls) == 2


def test_place_decisions_all_failed():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    d = Decision(side="Up", amount=5, price=0.55)

    ok, calls = run_place_decisions(engine, market, ws, [d], results=(False,))
    assert ok is False


def test_place_decisions_routes_cancel_prior_to_cancel():
    """A re-price Decision cancels the stale pending order instead of placing.

    The return value feeds the consecutive-placement-failure idle-shutdown
    counter, so a cancel — success or failure — must NOT count as a placed
    order: a cancelled favorite says nothing about the account balance.
    """
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    cancelled = []
    placed = []

    async def fake_cancel(order_id):
        cancelled.append(order_id)
        return True, "Up"
    engine.executor.cancel = fake_cancel
    engine._place_order = lambda *a, **k: placed.append(a) or True

    d = Decision(side="Cancel", amount=0, price=0.0, cancel_prior="stale_oid")
    ok = asyncio.run(engine._place_decisions(market, ws, [d]))

    assert ok is False  # no order placed — cancels don't count
    assert cancelled == ["stale_oid"]
    assert placed == []


def test_place_order_proxies_executor():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    calls = []

    async def fake_place(slug, token_id, outcome, price, amount,
                         is_favorite=False):
        calls.append((slug, token_id, outcome, price, amount))
        return "oid"
    engine.executor.place = fake_place

    ok = asyncio.run(engine._place_order(
        market.slug, market.up_token_id, "Up", 0.55, 5))
    assert ok is True
    assert calls == [(market.slug, market.up_token_id, "Up", 0.55, 5)]


def test_place_favorite_records_hedge_plan_on_success():
    """The bound hedge plan is recorded ONLY once the favorite order is
    successfully placed — opposite side, full size, max = hedge_price_bound
    − favorite price, bound to the real order_id."""
    engine = make_place_engine(oid="fav_oid")
    slug = "btc-updown-15m-1700000000"
    ws = engine._windows[slug]

    oid = asyncio.run(engine.executor.place(
        slug, "tid_up", "Up", 0.54, 5, is_favorite=True))

    assert oid == "fav_oid"
    assert ws.pending_orders["fav_oid"].side == "Up"
    plan = ws.hedge_plan
    assert plan is not None
    assert plan.order_id == "fav_oid"
    assert plan.side == "Down"                          # opposite of favorite
    assert plan.amount == 5
    assert plan.fav_price == 0.54
    assert plan.max_price == round(0.998 - 0.54, 4)     # 0.458
    assert plan.filled == 0.0
    assert plan.placed is False


def test_place_hedge_does_not_record_plan():
    """A hedge placement (not a favorite) must NOT create a bound hedge plan."""
    engine = make_place_engine(oid="hedge_oid")
    slug = "btc-updown-15m-1700000000"
    ws = engine._windows[slug]

    oid = asyncio.run(engine.executor.place(
        slug, "tid_down", "Down", 0.45, 5, is_favorite=False))

    assert oid == "hedge_oid"
    assert "hedge_oid" in ws.pending_orders
    assert ws.hedge_plan is None


def test_place_favorite_failure_leaves_no_plan():
    """SDK place failure → no pending order and no hedge plan (a plan never
    precedes a real order)."""
    engine = make_place_engine(oid=None)
    slug = "btc-updown-15m-1700000000"
    ws = engine._windows[slug]

    oid = asyncio.run(engine.executor.place(
        slug, "tid_up", "Up", 0.54, 5, is_favorite=True))

    assert oid is None
    assert ws.pending_orders == {}
    assert ws.hedge_plan is None


def test_position_poll_keeps_optimistic_fill_during_fetch():
    """A fill landing while get_positions is in flight must not be clobbered.

    The poll's snapshot predates the fill; overwriting would drop a real
    position from auth_inv.  The seq guard skips that round.
    """
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    engine._windows[market.slug] = ws

    async def fake_get_positions(cid, up_tid, down_tid):
        # Simulate a fill landing mid-fetch (optimistic write-through already
        # applied via _process_fill, which also bumps fill_seq).
        ws.auth_inv["Up"] += 5
        engine.executor.fill_seq += 1
        return {"Up": 0, "Down": 0}  # stale snapshot predating the fill

    engine.sdk.get_positions = fake_get_positions

    updated = asyncio.run(engine._poll_positions(ws, market))

    assert updated is False
    assert ws.auth_inv == {"Up": 5, "Down": 0}  # optimistic fill preserved


def test_position_poll_stale_snapshot_never_erases_auth_inv():
    """data-api is eventually consistent: a poll returning {0,0} *after* real
    fills must not erase the optimistic auth_inv.  Erasing it was the root
    cause of the wrong-direction second-leg order in live window 1785881700
    (Down 5 @ 0.64 filled, then a stale poll → strategy thought it was flat →
    bought Up 5 @ 0.50 → pair cost 1.14 > 1.00)."""
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    ws.inventory = {"Up": 0, "Down": 5}  # real Down fills recorded via WS
    ws.auth_inv = {"Up": 0, "Down": 5}
    engine._windows[market.slug] = ws

    async def fake_get_positions(cid, up_tid, down_tid):
        return {"Up": 0, "Down": 0}  # stale snapshot predating the fills

    engine.sdk.get_positions = fake_get_positions

    updated = asyncio.run(engine._poll_positions(ws, market))

    assert updated is False
    assert ws.auth_inv == {"Up": 0, "Down": 5}  # never erased
    assert ws.inventory == {"Up": 0, "Down": 5}


def test_position_poll_absorbs_missed_fill_into_pending():
    """CLOB shows Up=5 but the WS only recorded 4 → the poll completes the
    pending order at its limit price, so inventory / auth_inv / pending all
    converge.  This clears the phantom-pending anti-stack block and makes the
    log match the page (which showed the order filled)."""
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    ws.inventory = {"Up": 4, "Down": 5}
    ws.auth_inv = {"Up": 4, "Down": 5}
    ws.cost = {"Up": 4 * 0.50, "Down": 5 * 0.64}
    ws.total_spent = 4 * 0.50 + 5 * 0.64
    po = PendingOrder(order_id="up_oid", token_id=market.up_token_id,
                      side="Up", buy_sell="BUY", price=0.50,
                      amount=5, filled=4)
    ws.pending_orders["up_oid"] = po
    engine._windows[market.slug] = ws

    async def fake_get_positions(cid, up_tid, down_tid):
        return {"Up": 5, "Down": 5}

    engine.sdk.get_positions = fake_get_positions

    updated = asyncio.run(engine._poll_positions(ws, market))

    assert updated is True
    assert ws.inventory == {"Up": 5, "Down": 5}
    assert ws.auth_inv == {"Up": 5, "Down": 5}
    assert po.remaining == 0          # phantom pending cleared
    assert ws.cost["Up"] == 5 * 0.50  # missed fill recorded at limit price
    assert ws.trades == 1


# ── In-flight order reconciliation (_reconcile_orders) ──


def clob_order(oid, token_id, price, size, filled=0, created_at=None):
    """A raw SDK open-order object as returned by list_open_orders."""
    from types import SimpleNamespace
    return SimpleNamespace(
        id=oid, token_id=token_id, price=price,
        original_size=size, size_matched=filled,
        created_at=created_at or time.time(),
    )


def make_reconcile_engine():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    engine._windows[market.slug] = ws
    engine._ghost_polls.clear()
    return engine, market, ws


def set_clob(engine, orders):
    async def fake(condition_id):
        return orders
    engine.sdk.get_open_orders_for_market = fake


def test_reconcile_closes_ghost_after_consecutive_misses():
    """An order gone from CLOB for two consecutive polls is a ghost — cancelled
    out from under us.  Closing it (cancelled_at) unblocks the anti-stack gate
    that was stalling favorites for the whole window."""
    engine, market, ws = make_reconcile_engine()
    ws.pending_orders["g1"] = PendingOrder(
        order_id="g1", token_id=market.up_token_id,
        side="Up", buy_sell="BUY", price=0.55, amount=5)
    set_clob(engine, [])

    changed = asyncio.run(engine._reconcile_orders(ws, market))
    assert changed is False
    assert ws.pending_orders["g1"].cancelled_at == 0  # held for a second look

    changed = asyncio.run(engine._reconcile_orders(ws, market))
    assert changed is True
    assert ws.pending_orders["g1"].cancelled_at > 0   # ghost closed
    assert not any(po.cancelled_at == 0 and po.remaining > 0
                   for po in ws.pending_orders.values())  # anti-stack unblocked


def test_reconcile_keeps_order_still_on_clob():
    """An order the CLOB still lists is left working, poll after poll."""
    engine, market, ws = make_reconcile_engine()
    po = PendingOrder(order_id="live1", token_id=market.up_token_id,
                      side="Up", buy_sell="BUY", price=0.55, amount=5)
    ws.pending_orders["live1"] = po
    set_clob(engine, [clob_order("live1", market.up_token_id, 0.55, 5)])

    for _ in range(3):
        changed = asyncio.run(engine._reconcile_orders(ws, market))
        assert changed is False

    assert po.cancelled_at == 0
    assert "live1" in ws.pending_orders


def test_reconcile_purges_filled_phantom_immediately():
    """A fully-filled order that left CLOB is stale bookkeeping — purge it on
    the first poll (no confirmation window needed: it can't move anything)."""
    engine, market, ws = make_reconcile_engine()
    ws.pending_orders["p1"] = PendingOrder(
        order_id="p1", token_id=market.up_token_id,
        side="Up", buy_sell="BUY", price=0.55, amount=5, filled=5)
    set_clob(engine, [])

    changed = asyncio.run(engine._reconcile_orders(ws, market))
    assert changed is True
    assert "p1" not in ws.pending_orders


def test_reconcile_skips_already_cancelled():
    """Our own soft-cancelled orders are owned by flush_cancelled — the
    reconcile never double-closes or purges them."""
    engine, market, ws = make_reconcile_engine()
    po = PendingOrder(order_id="c1", token_id=market.up_token_id,
                      side="Up", buy_sell="BUY", price=0.55, amount=5)
    po.cancelled_at = time.time()
    ws.pending_orders["c1"] = po
    set_clob(engine, [])

    changed = asyncio.run(engine._reconcile_orders(ws, market))
    assert changed is False
    assert "c1" in ws.pending_orders
    assert po.cancelled_at > 0


def test_reconcile_query_failure_is_noop():
    """An API failure (None) must NOT be read as 'no open orders' — pending
    stays untouched, ghosts never get closed."""
    engine, market, ws = make_reconcile_engine()
    ws.pending_orders["g1"] = PendingOrder(
        order_id="g1", token_id=market.up_token_id,
        side="Up", buy_sell="BUY", price=0.55, amount=5)

    async def fail(condition_id):
        return None
    engine.sdk.get_open_orders_for_market = fail

    changed = asyncio.run(engine._reconcile_orders(ws, market))
    assert changed is False
    assert ws.pending_orders["g1"].cancelled_at == 0


# ── PriceCache.wait_update ──


def test_wait_update_reacts_promptly_to_update_during_processing():
    """An update that lands while the previous tick is still processing must
    wake the next wait_update immediately — not be cleared into a full
    timeout sleep (the lost-wakeup bug)."""
    pc = PriceCache()

    async def scenario():
        await pc.wait_update(timeout=0.05)    # nothing yet → times out
        pc.update_from_ws("tok", 0.50, 0.51)  # update "during processing"
        start = time.monotonic()
        await pc.wait_update(timeout=5.0)     # must not sleep the full 5s
        return time.monotonic() - start

    elapsed = asyncio.run(scenario())
    assert elapsed < 1.0, f"wait_update slept {elapsed:.2f}s despite fresh data"


def test_wait_update_sleeps_timeout_with_no_new_update():
    """No new update → wait_update sleeps the timeout (idle keep-alive)."""
    pc = PriceCache()

    async def scenario():
        start = time.monotonic()
        await pc.wait_update(timeout=0.1)
        return time.monotonic() - start

    elapsed = asyncio.run(scenario())
    assert elapsed >= 0.05, f"wait_update returned early: {elapsed:.3f}s"
