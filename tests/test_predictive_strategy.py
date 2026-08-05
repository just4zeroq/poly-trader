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
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5)
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


# ── Step 4b: stale favorite re-pricing ──


def _pending(side, price, age, oid="ord_1"):
    return PendingOrder(
        order_id=oid, token_id=f"t_{side.lower()}", side=side,
        buy_sell="BUY", price=price, amount=5,
        placed_at=time.time() - age)


def test_stale_favorite_not_repriced_before_threshold():
    """A favorite younger than favorite_stale_seconds is left working."""
    strat, cfg = make_strategy(btc_price=100.3)  # Up favorite
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=10)
    assert strat.decide(ws, up_price=0.54, down_price=0.45,
                        remaining_time=800.0) == []


def test_stale_favorite_not_repriced_when_price_same():
    """Old but still at the fresh placement price → no pointless cancel/churn."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.54, age=40)
    assert strat.decide(ws, up_price=0.54, down_price=0.45,
                        remaining_time=800.0) == []


def test_stale_favorite_repriced_when_model_flips():
    """Model flipped away from the resting favorite → cancel it."""
    strat, cfg = make_strategy(btc_price=100.3)  # model favors Up
    ws = make_ws(cfg)
    ws.pending_orders["ord_down_1"] = _pending("Down", 0.43, age=40, oid="ord_down_1")
    decisions = strat.decide(ws, up_price=0.54, down_price=0.43,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_down_1"


def test_stale_favorite_repriced_when_rebid_higher():
    """Model fair moved up → re-bid at the higher price instead of holding the dead low-ball."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.50, age=40, oid="ord_up_1")
    decisions = strat.decide(ws, up_price=0.60, down_price=0.30,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_up_1"


def test_stale_favorite_kept_when_model_quiet():
    """Model quiet (or a guard blocking a fresh placement) → nothing better to
    place, so leave the resting favorite working rather than churn-cancelling."""
    strat, cfg = make_strategy(btc_price=100.0)  # P_fair ≈ 0.48 → not confident
    ws = make_ws(cfg)
    ws.pending_orders["ord_up_1"] = _pending("Up", 0.55, age=40, oid="ord_up_1")
    assert strat.decide(ws, up_price=0.55, down_price=0.45,
                        remaining_time=800.0) == []


def test_reprice_skips_stale_hedge_on_light_side():
    """A stale hedge on the light side (locking a sub-min imbalance) is never
    re-priced — cancelling it would throw away a committed bound hedge (plan
    already consumed) and leave the filled favorite naked.  Only the stale
    favorite itself gets the re-bid cancel."""
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    # Sub-min imbalance (diff 4 < min_order_size 5): favorite Up filled 4,
    # bound hedge Down resting to lock it.  Plan already consumed.
    ws = make_ws(cfg, auth_inv={"Up": 4, "Down": 0})
    ws.hedge_plan = _plan(side="Down", fav_price=0.54, filled=5.0, placed=True)
    # Stale favorite: re-bid 0.50 → 0.60 is materially better → should cancel.
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.50, amount=5, filled=4,
        placed_at=time.time() - 40)
    # Stale hedge on the light side — must NOT be cancelled.
    ws.pending_orders["ord_down_hedge"] = PendingOrder(
        order_id="ord_down_hedge", token_id="t_down", side="Down",
        buy_sell="BUY", price=0.44, amount=4, filled=0,
        placed_at=time.time() - 40)
    decisions = strat.decide(ws, up_price=0.60, down_price=0.44,
                             remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Cancel"
    assert decisions[0].cancel_prior == "ord_up_1"
    assert decisions[0].cancel_prior != "ord_down_hedge"


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


def test_hedge_waits_until_favorite_fills_4():
    """A plan whose favorite has filled < 4 does not hedge — and the generic
    imbalance hedge must not touch the partial fill either."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 3, "Down": 0})  # partial 3/5 favorite
    ws.hedge_plan = _plan(filled=3.0)
    # The favorite order is live (fresh, partially filled 3/5) — keeps the
    # plan alive so neither the bound hedge nor the generic hedge fires.
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


def test_4p992_fill_triggers_hedge():
    """A 4.992 fill counts as ≥ 4 — the fee-artifact truncation case."""
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg, auth_inv={"Up": 5, "Down": 0})
    ws.hedge_plan = _plan(filled=4.992)
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
