"""V4 strategy state machine tests.

The V4 state machine per tick (see platforms/strategy.py):

  1. remaining < min_remaining_time            → []        (window wind-down)
  2. auth_inv imbalanced                        → hedge the light side
  3. elapsed < pred_start_elapsed               → []        (before first judgment point)
  4. auth_inv flat + active pending             → []        (anti-stack)
  5. auth_inv flat + confident favorite         → buy favorite

The strategy gates ONLY on ws.auth_inv (authoritative positions).
"""

import time

from poly_trader.platforms.config import Config
from poly_trader.platforms.models import HedgePlan, PendingOrder, WindowState
from poly_trader.platforms.strategy import V4Strategy
from poly_trader.platforms.predictor import Predictor


def make_strategy(btc_price=100.3):
    """btc=100.3 → confident Up favorite (P_fair ≈ 0.76)."""
    cfg = Config()
    predictor = Predictor(cfg)
    predictor.set_window(1700000000, 100.0, 0.1, 0.2, 0.2)
    predictor.set_btc(btc_price)
    return V4Strategy(cfg, predictor), cfg


def make_ws(cfg, slug="btc-updown-15m-1700000000",
            auth_inv=None, inventory=None):
    """WindowState with auth_inv/inventory defaulting to flat."""
    ws = WindowState(slug=slug, start_time=0.0, window_num=1)
    if auth_inv is not None:
        ws.auth_inv = dict(auth_inv)
    if inventory is not None:
        ws.inventory = dict(inventory)
    return ws


# ── Step 1: near window end ──


def test_near_end_none():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=100.0) == []


# ── Step 2: before first judgment point ──


def test_before_60s_no_decision():
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg)
    # remaining 890 → elapsed 10s < 60s
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=890.0) == []


# ── Step 3: imbalanced → hedge the light side ──


def test_no_orders_in_first_60s_even_when_imbalanced():
    """Window start: NO orders at all for the first 60s — the hedge leg is
    gated by pred_start_elapsed just like the favorite leg."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})  # imbalance, but < 60s
    assert strat.decide(ws, up_price=0.55, down_price=0.43,
                        remaining_time=890.0) == []  # elapsed 10 < 60


def test_imbalanced_places_hedge():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5  # size = imbalance
    assert decisions[0].price == 0.43


def test_hedge_locks_in_filled_favorite_on_opposite_side():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 0, "Down": 7})
    decisions = strat.decide(ws, up_price=0.56, down_price=0.44,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].amount == 7
    assert decisions[0].price == 0.56


def test_uses_auth_inv_not_inventory():
    """Gate on authoritative positions: inventory flat but auth_inv imbalanced → hedge."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg,
                 auth_inv={"Up": 5, "Down": 0},
                 inventory={"Up": 5, "Down": 5})  # ws.inventory misleadingly flat
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5


def test_pending_hedge_blocks_second_hedge():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.pending_orders["ord_d1"] = PendingOrder(
        order_id="ord_d1", token_id="t_d", side="Down",
        buy_sell="BUY", price=0.43, amount=5)
    assert strat.decide(ws, up_price=0.55, down_price=0.43,
                        remaining_time=800.0) == []


def test_hedge_allowed_in_last_3_min():
    """The hedge leg is NOT subject to min_remaining_time — it keeps monitoring
    and placing until window end, so an imbalance is locked even in the last
    3 minutes."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43,
                             remaining_time=100.0)  # last 3 min
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5


def test_no_favorite_in_last_3_min():
    """No NEW favorite in the last 3 minutes — it wouldn't fill in time to be
    hedged at an affordable price.  Flat + confident → []."""
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.54, down_price=0.45,
                        remaining_time=100.0) == []


def test_hedge_cost_guard_skips_when_too_expensive():
    """avg_cost_heavy + hedge_price > pair_cost_target_extreme → skip."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 10, "Down": 0},
                 inventory={"Up": 10, "Down": 0})
    ws.cost["Up"] = 10 * 0.60  # avg_cost_up = 0.60
    # diff = 10 > min_order_size + 1 → generic hedge; 0.60 + 0.45 = 1.05 > 0.99 → skip
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_hedge_cost_guard_allows_cheap_lock():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 10, "Down": 0},
                 inventory={"Up": 10, "Down": 0})
    ws.cost["Up"] = 10 * 0.50  # avg_cost_up = 0.50
    # diff = 10 > min_order_size + 1 → generic hedge; 0.50 + 0.43 = 0.93 <= 0.99 → place
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 10


def test_hedge_cost_guard_blends_cheap_light_inventory():
    """The light side already holds contracts bought cheaper than the current
    maker price → the guard evaluates the BLENDED light average after the
    hedge, not the raw hedge price.  Old guard (0.55 + 0.45 = 1.00 > 0.99)
    would skip; blending 3@0.40 with 7@0.45 → 0.435, pair cost 0.985 ≤ 0.99."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 10, "Down": 3},
                 inventory={"Up": 10, "Down": 3})
    ws.cost["Up"] = 10 * 0.55    # avg_up = 0.55
    ws.cost["Down"] = 3 * 0.40   # avg_down = 0.40, light holds 3 already
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 7          # imbalance 10 − 3
    assert decisions[0].price == 0.45


def test_hedge_cost_guard_blends_expensive_light_inventory():
    """The reverse direction: a light side filled pricier than the hedge price
    averages UP, so the guard now skips where the old raw-price check would
    have placed (0.50 + 0.45 = 0.95 ≤ 0.99; blended 0.525 → 1.025 > 0.99)."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 10, "Down": 3},
                 inventory={"Up": 10, "Down": 3})
    ws.cost["Up"] = 10 * 0.50    # avg_up = 0.50
    ws.cost["Down"] = 3 * 0.70   # avg_down = 0.70
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_hedge_size_capped_to_exposure_room():
    """Over-positioned heavy side (over max_per_side) → hedge capped to light-side room."""
    strat, cfg = make_strategy(btc_price=100.3)
    max_ = cfg.max_per_side
    ws = make_ws(cfg, auth_inv={"Up": max_ + 5, "Down": max_ - 10})
    # diff = (max+5) - (max-10) = 15 → hedge Down; room on Down = max - (max-10) = 10
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 10


def test_hedge_skips_at_max_exposure():
    """Light side at max_per_side while heavy is over it → no room to hedge."""
    strat, cfg = make_strategy(btc_price=100.3)
    max_ = cfg.max_per_side
    ws = make_ws(cfg, auth_inv={"Up": max_ + 10, "Down": max_})
    assert strat.decide(ws, up_price=0.55, down_price=0.43,
                        remaining_time=800.0) == []


def test_submin_imbalance_folds_into_next_favorite():
    """|imbalance| < min_order_size → no sub-min hedge order.  The residual
    folds into the next favorite round (model confident Up, residual is Up=3
    → the new favorite stacks on the heavy side and gets locked in one
    ≥ min_order_size hedge later)."""
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg, auth_inv={"Up": 3, "Down": 0})  # partial 3/5 residual
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].amount == 5
    assert decisions[0].creates_hedge_plan is True


def test_submin_imbalance_no_order_when_model_quiet():
    """Sub-min residual + model not confident → nothing placed: the residual
    rides naked to settlement (≤ 4 contracts, bounded risk)."""
    strat, cfg = make_strategy(btc_price=100.0)  # P_fair ≈ 0.48 → not confident
    ws = make_ws(cfg, auth_inv={"Up": 3, "Down": 0})
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_bound_hedge_uses_live_imbalance():
    """The bound hedge amount is the LIVE imbalance, not the plan's original
    favorite size: a 1-contract residual folded into the next favorite (1 + 5
    = Up 6) is still within single-favorite scope (≤ min_order_size + 1), so
    the bound hedge locks all 6.  plan.amount is 5."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 6, "Down": 0})
    ws.hedge_plan = _plan(filled=5.0)  # side=Down, plan.amount=5
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 6          # live imbalance, not plan.amount
    assert decisions[0].price == 0.45
    assert ws.hedge_plan.placed is True


def test_accumulated_plan_dropped_to_generic_hedge():
    """A favorite whose position outgrew min_order_size + 1 (residual folded
    in, Up=8 = 3 residual + 5 new favorite) drops the live plan and routes to
    the generic blended-cost hedge — the plan's placement-time bound is
    anchored to the wrong cost once the position spans multiple fills."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 8, "Down": 0})
    ws.hedge_plan = _plan(filled=5.0)  # side=Down, plan.amount=5
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 8          # live imbalance
    assert decisions[0].price == 0.45
    assert ws.hedge_plan is None             # plan dropped, generic owns the lock


def test_single_unit_imbalance_uses_synthetic_bound():
    """No live plan + single-unit imbalance (≤ min_order_size + 1): the hedge
    is bound-style, anchored to the heavy avg cost.  max_price = 0.998 − 0.50
    = 0.498; the light maker 0.495 is within it, so it places — where the
    generic blended guard (0.50 + 0.495 = 0.995 > 0.99) would have skipped."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0},
                 inventory={"Up": 5, "Down": 0})
    ws.cost["Up"] = 5 * 0.50  # avg_cost_up = 0.50
    decisions = strat.decide(ws, up_price=0.55, down_price=0.495,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5
    assert decisions[0].price == 0.495


def test_accumulated_imbalance_uses_generic_guard():
    """No live plan + accumulated imbalance (> min_order_size + 1): the
    generic blended-cost guard applies.  The same light maker 0.495 that the
    synthetic bound would accept (≤ 0.498) is rejected: 0.50 + 0.495 = 0.995
    > 0.99 → skip."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 10, "Down": 0},
                 inventory={"Up": 10, "Down": 0})
    ws.cost["Up"] = 10 * 0.50  # avg_cost_up = 0.50
    assert strat.decide(ws, up_price=0.55, down_price=0.495,
                        remaining_time=800.0) == []


def test_synthetic_bound_waits_when_light_above_bound():
    """Synthetic bound (no live plan) waits when the light maker is above
    max_price = hedge_price_bound − avg_cost_heavy, same as a live plan."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0},
                 inventory={"Up": 5, "Down": 0})
    ws.cost["Up"] = 5 * 0.40  # avg_cost_up = 0.40 → bound = 0.598
    assert strat.decide(ws, up_price=0.55, down_price=0.60,
                        remaining_time=800.0) == []


# ── Step 4: flat + active pending → anti-stack ──


def test_pending_favorite_blocks_new_favorite():
    """A fresh (younger than stale threshold) pending favorite blocks a new
    favorite — anti-stack.  Only stale ones get cancelled proactively."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5, placed_at=time.time())
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_cancelled_pending_does_not_block():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.54, amount=5, cancelled_at=100.0)
    decisions = strat.decide(ws, up_price=0.54, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1  # cancelled order doesn't count as active
    assert decisions[0].side == "Up"


def test_order_filled_min_minus_1_does_not_block():
    """Pending boundary: an order filled min_order_size − 1 (4/5) is NOT
    'pending' (filled < min − 1 is the gate) — it no longer blocks new orders.
    A 4-contract sub-min residual folds into the next favorite round."""
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg, auth_inv={"Up": 4, "Down": 0})  # 4/5 favorite residual
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5, filled=4,
        placed_at=time.time())
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"          # new favorite, not a hedge
    assert decisions[0].amount == cfg.min_order_size
    assert decisions[0].creates_hedge_plan is True


def test_partial_fill_at_min_minus_1_folds_into_next_favorite():
    """A live bound plan whose favorite filled only min_order_size − 1 (4/5) is
    NOT a hedgeable unit (diff < min_order_size) — 满单才补腿.  The plan is
    dropped and the residual folds into the next favorite round, which gets
    locked in one ≥ min_order_size hedge later."""
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg, auth_inv={"Up": 4, "Down": 0})
    ws.hedge_plan = _plan(filled=4.0)  # favorite Up filled 4/5, plan live
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5, filled=4,
        placed_at=time.time())
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"          # new favorite, not a hedge
    assert decisions[0].amount == cfg.min_order_size
    assert ws.hedge_plan is None              # plan dropped — residual folds


# ── Step 4b: stale favorite re-pricing ──


def _pending(side, price, age, oid="ord_1"):
    return PendingOrder(
        order_id=oid, token_id=f"t_{side.lower()}", side=side,
        buy_sell="BUY", price=price, amount=5,
        placed_at=time.time() - age)


def test_stale_favorite_not_repriced_before_threshold():
    """A favorite younger than favorite_stale_seconds is left working, even if
    it is already priced out."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=10)
    assert strat.decide(ws, up_price=0.70, down_price=0.45,
                        remaining_time=800.0) == []


def test_stale_favorite_repriced_when_priced_out():
    """age > favorite_stale_seconds AND the current maker moved more than
    stale_price_diff ABOVE the limit → the bid will not fill → cancel; the next
    tick re-places at the fresh book price."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=200, oid="ord_up_1")
    decisions = strat.decide(ws, up_price=0.65, down_price=0.45,
                             remaining_time=800.0)  # 0.65 > 0.54 + 0.10
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_up_1"


def test_stale_favorite_not_repriced_when_not_priced_out():
    """age > favorite_stale_seconds but the current maker is still within
    stale_price_diff of the limit → NOT cancelled (churn guard: cancel →
    re-place at nearly the same price buys nothing)."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=200, oid="ord_up_1")
    assert strat.decide(ws, up_price=0.54, down_price=0.45,
                        remaining_time=800.0) == []


def test_stale_favorite_not_repriced_when_price_moved_below():
    """Current maker BELOW the limit → our bid is now the competitive side of
    the book (likely to fill) → never cancel it.  Only a market that ran ABOVE
    our bid (priced out) triggers the reprice."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=200, oid="ord_up_1")
    assert strat.decide(ws, up_price=0.40, down_price=0.45,
                        remaining_time=800.0) == []


def test_stale_favorite_repriced_on_heavy_side():
    """A stale, priced-out pending on the heavy side (a favorite) is cancelled
    regardless of which side the model favors."""
    strat, cfg = make_strategy(btc_price=100.3)  # model favors Up
    ws = make_ws(cfg)
    ws.pending_orders["ord_down_1"] = _pending("Down", 0.43, age=200, oid="ord_down_1")
    decisions = strat.decide(ws, up_price=0.54, down_price=0.60,
                             remaining_time=800.0)  # 0.60 > 0.43 + 0.10
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_down_1"


def test_reprice_skips_stale_hedge_on_light_side():
    """A stale pending hedge on the light side (locking a sub-min imbalance) is
    never cancelled — only the stale favorite (先行脚) is.  Cancelling the hedge
    would throw away a committed bound hedge (plan already consumed) and leave
    the filled contracts naked."""
    strat, cfg = make_strategy(btc_price=100.3)
    # Sub-min imbalance (diff 4 < min_order_size 5): favorite Up filled 4,
    # bound hedge Down resting to lock it.  Plan already consumed.
    ws = make_ws(cfg, auth_inv={"Up": 4, "Down": 0})
    ws.hedge_plan = _plan(side="Down", fav_price=0.54, filled=5.0, placed=True)
    # Stale, priced-out favorite on the heavy side — should cancel.
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.50, age=200, oid="ord_up_1")
    # Stale, priced-out hedge on the light side — must NOT be cancelled.
    ws.pending_orders["ord_down_hedge"] = PendingOrder(
        order_id="ord_down_hedge", token_id="t_down", side="Down",
        buy_sell="BUY", price=0.44, amount=4, filled=0,
        placed_at=time.time() - 200)
    decisions = strat.decide(ws, up_price=0.65, down_price=0.60,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_up_1"
    assert decisions[0].cancel_prior != "ord_down_hedge"


def test_reprice_skips_stale_hedge_on_flat_consumed_plan():
    """Flat auth_inv + a consumed plan: a stale pending on the plan's hedge
    side is still protected (对冲脚), even with no live imbalance to point at
    the light side — age and price-out are irrelevant for the hedge leg."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)  # flat auth_inv
    ws.hedge_plan = _plan(side="Down", fav_price=0.54, filled=5.0, placed=True)
    ws.pending_orders["ord_down_hedge"] = PendingOrder(
        order_id="ord_down_hedge", token_id="t_down", side="Down",
        buy_sell="BUY", price=0.44, amount=4, filled=0,
        placed_at=time.time() - 200)
    assert strat.decide(ws, up_price=0.60, down_price=0.60,
                        remaining_time=800.0) == []


# ── Step 5: flat + confident favorite → buy ──


def test_flat_confident_places_favorite():
    strat, cfg = make_strategy(btc_price=100.3)  # P_fair ≈ 0.76 → Up
    ws = make_ws(cfg)
    decisions = strat.decide(ws, up_price=0.54, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].amount == cfg.min_order_size
    assert decisions[0].price == 0.54  # maker price on the favorite side


def test_flat_not_confident_none():
    strat, cfg = make_strategy(btc_price=100.0)  # P_fair ≈ 0.48 → not confident
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_cycle_after_flat_places_new_favorite():
    """After the hedge fills, positions are flat again → new round favorite."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 5})
    decisions = strat.decide(ws, up_price=0.54, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"


# ── Guards inside _place_favorite ──


def test_exposure_guard_blocks_favorite():
    """Flat at max_per_side on both legs → no room for a new favorite."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg,
                 auth_inv={"Up": cfg.max_per_side, "Down": cfg.max_per_side})
    assert strat.decide(ws, up_price=0.54, down_price=0.45,
                        remaining_time=800.0) == []


# ── _place_favorite pricing guards ──


def test_favorite_uses_maker_price_above_p_fair():
    """Model fair only picks direction — no P_fair price cap.  A book maker
    price above fair is still taken; the favorite fills at the book's maker
    price (pair-cost is checked on the hedge leg, not here)."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite, P_fair ≈ 0.76
    ws = make_ws(cfg)
    decisions = strat.decide(ws, up_price=0.80, down_price=0.18,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].price == 0.80  # book maker, uncapped by P_fair


def test_favorite_uses_book_price_when_below_p_fair():
    """Book maker already under fair − margin → keep the cheaper book price."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    decisions = strat.decide(ws, up_price=0.60, down_price=0.30,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].price == 0.60


def test_favorite_places_even_when_pair_cost_high():
    """No forward pair-cost guard on the favorite: committing the heavy leg
    does not require the light side to be affordable now.  Pair-cost is
    enforced later on the hedge leg (avg_heavy + hedge_price ≤ 0.99)."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    # 0.60 (favorite) + 0.42 (light) = 1.02 > 0.99 → still places the favorite
    decisions = strat.decide(ws, up_price=0.60, down_price=0.42,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].price == 0.60


def test_favorite_skipped_when_price_at_or_above_extreme():
    """Never chase the favorite above max_extreme_price (0.90)."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.91, down_price=0.08,
                        remaining_time=800.0) == []


# ── Bound hedge plan (favorite → filled ≥4 → hedge at ≤ bound) ──


def _plan(side="Down", fav_price=0.54, filled=0.0, placed=False,
          order_id="ord_up_1"):
    return HedgePlan(
        order_id=order_id, side=side, amount=5,
        fav_price=fav_price,
        max_price=round(0.998 - fav_price, 4),
        filled=filled, placed=placed,
    )


def test_favorite_tags_creates_hedge_plan_but_defers_record():
    """A favorite Decision is tagged creates_hedge_plan=True — but the plan
    itself is NOT recorded at decision time.  executor.place records it only
    once the order is successfully placed, so a plan never precedes a real
    order."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    decisions = strat.decide(ws, up_price=0.54, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].creates_hedge_plan is True
    assert ws.hedge_plan is None  # record deferred to executor.place success


def test_hedge_waits_while_favorite_pending():
    """A partially-filled favorite (3/5) is 'pending' (filled < min_order_size
    − 1) → it blocks ALL orders (favorite, bound hedge, generic hedge) until
    it fills to min_order_size − 1 or is cancelled.  The generic imbalance
    hedge must NOT touch its partial fill."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 3, "Down": 0})  # partial 3/5 favorite
    ws.hedge_plan = _plan(filled=3.0)
    # The favorite order is live (fresh, partially filled 3/5) — pending gate
    # blocks both the bound hedge and the generic hedge.
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5, filled=3,
        placed_at=time.time())
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_bound_hedge_placed_when_filled():
    """favorite filled ≥ 4 → bound hedge on the opposite side at the light
    maker price (≤ bound), full order size."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(filled=5.0)  # side=Down, bound=0.458
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5
    assert decisions[0].price == 0.45
    assert ws.hedge_plan.placed is True  # consumed exactly once


def test_bound_hedge_fires_on_fully_filled_favorite():
    """A fully-filled favorite (live imbalance = min_order_size) fires the
    bound hedge.  plan.filled is now informational only — the pending gate +
    live imbalance route the hedge, not a plan.filled threshold."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(filled=0.0)  # informational — no longer the trigger
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].amount == 5


def test_bound_hedge_waits_when_light_above_bound():
    """Light maker price above the pre-determined bound → wait, don't chase.
    The generic imbalance hedge must not fire either."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(fav_price=0.60, filled=5.0)  # bound = 0.398
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []
    assert ws.hedge_plan.placed is False  # still waiting


def test_bound_hedge_consumed_after_place():
    """Once the bound hedge Decision is emitted (placed=True), it does not
    re-hedge the same favorite on later ticks."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(filled=5.0, placed=True)  # already hedged
    # The bound hedge is now a live pending order — it blocks the generic
    # imbalance hedge on the same side (mirrors the real flow after place()).
    ws.pending_orders["ord_down_1"] = PendingOrder(
        order_id="ord_down_1", token_id="t_d", side="Down",
        buy_sell="BUY", price=0.45, amount=5)
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_bound_hedge_pending_blocks_second():
    """A live hedge already on the plan's side blocks a second bound hedge."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(filled=5.0)
    ws.pending_orders["ord_down_1"] = PendingOrder(
        order_id="ord_down_1", token_id="t_d", side="Down",
        buy_sell="BUY", price=0.45, amount=5)
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_abandoned_plan_when_favorite_cancelled():
    """A cancelled favorite abandons the bound plan so the window re-enters
    the normal cycle (no dead-plan block)."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.hedge_plan = _plan(filled=2.0)
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.54, amount=5, filled=2, cancelled_at=100.0)
    # favourite cancelled, plan abandoned → flat + confident → new favorite.
    # decide() no longer records plans — the dead plan is dropped (None) and
    # the fresh favorite is tagged creates_hedge_plan for executor.place.
    decisions = strat.decide(ws, up_price=0.54, down_price=0.45,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].creates_hedge_plan is True
    assert ws.hedge_plan is None


def test_no_new_favorite_while_plan_awaits_hedge():
    """Anti-stack: a live (unconsumed) plan — a favorite already awaiting its
    bound hedge — blocks any NEW favorite order."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.hedge_plan = _plan(filled=3.0)  # favorite filled 3/5, hedge not yet due
    assert strat._place_favorite(ws, up_price=0.54, down_price=0.45,
                                 elapsed=100.0, remaining_time=800.0) == []
    # And the plan is left untouched for the bound hedge to fire later.
    assert ws.hedge_plan.filled == 3.0
    assert ws.hedge_plan.placed is False
