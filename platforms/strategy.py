"""
V4 simplified predictive strategy — favorite → hedge → flat.

Each tick the window state machine runs:
  1. elapsed < pred_start_elapsed (60s)               → []          (no orders at all — window not established)
  2. any pending order (filled < min_order_size − 1)  → []          (anti-stack; stale priced-out favorites cancelled)
  3. bound hedge owns its fully-filled favorite       → hedge at ≤ bound; a position that outgrew min_order_size + 1
                                                          drops the plan and goes generic (blended-cost guard); a
                                                          sub-min residual (partial fill) folds into the next favorite
  4. auth_inv imbalanced                              → hedge the light side; single-unit imbalance (≤ min_order_size
                                                          + 1) uses a bound-style hedge anchored to the heavy avg cost,
                                                          larger goes generic (blended-cost guard)
  5. remaining_time < min_remaining_time (180s)       → []          (last 3 min: no NEW favorite; hedges above keep running)
  6. auth_inv flat + confident favorite               → buy favorite

The favorite (step 6) is offered at the book's maker price — the model's
P_fair only picks the direction, never intervenes on price.  Once the order
is successfully placed, the executor records a bound hedge plan (opposite
side, full order size, max hedge price = hedge_price_bound − favorite_price)
— never at decision time, so a plan always corresponds to a real live order.

The pending gate (step 2) is the anti-stack: an order whose fill has NOT
reached min_order_size − 1 blocks ALL new orders (favorite, bound hedge,
generic hedge).  So a favorite must fully fill before it is hedged — a
partial fill (e.g. 4/5) no longer blocks (it is not "pending"), and its
sub-min residual folds into the next favorite round, which then gets locked
in one ≥ min_order_size hedge.  A favorite that sits pending too long and
gets priced out is cancelled proactively so the window isn't idle.

The strategy gates ONLY on ws.auth_inv (authoritative positions): WS fills
write it through optimistically and the engine's 2s position poll overwrites
it with CLOB truth.  ws.inventory is reporting-only and never gates a decision.
"""

from __future__ import annotations
import logging
import time

from .config import Config
from .models import Decision, HedgePlan, WindowState, _parse_duration_from_slug
from .predictor import Predictor

logger = logging.getLogger(__name__)


class V4Strategy:
    """Favorite→hedge→flat predictive market making, gated on ws.auth_inv."""

    def __init__(self, cfg: Config, predictor: Predictor):
        self.cfg = cfg
        self.predictor = predictor

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def decide(
        self,
        ws: WindowState,
        up_price: float,
        down_price: float,
        remaining_time: float = 999,
    ) -> list[Decision]:
        cfg = self.cfg

        # First 60s of the window: no orders at all — neither leg.  The book
        # isn't established yet and P_fair has no path to judge on.
        elapsed = _parse_duration_from_slug(ws.slug) - remaining_time
        if elapsed < cfg.pred_start_elapsed:
            return []

        # Pending gate — the anti-stack.  An order whose fill has NOT reached
        # min_order_size − 1 blocks ALL new orders (favorite, bound hedge,
        # generic hedge): a partially-filled favorite is still owned, and a
        # resting hedge is still locking its position.  Only stale, priced-out
        # lead-leg favorites may be cancelled (re-priced against the fresh
        # book).  Runs regardless of min_remaining_time — hedges monitor and
        # place until the very end of the window.
        if self._has_active_pending(ws):
            return self._reprice_stale_favorites(ws, up_price, down_price)

        # No pending → every tracked order has filled ≥ min_order_size − 1 or
        # is cancelled.  A live bound plan owns its (now substantially-filled)
        # favorite: a single favorite's worth of imbalance (∈ [min_order_size,
        # min_order_size + 1]) is locked against the placement-time price
        # bound; anything larger (residuals folded in) spans mixed costs →
        # drop the plan and let the generic blended-cost guard own the lock.
        # A sub-min residual (partial favorite fill) is not a hedgeable unit →
        # drop the plan; the residual folds into the next favorite round.
        plan = ws.hedge_plan
        plan_live = plan is not None and not plan.placed
        if plan_live:
            heavy = "Down" if plan.side == "Up" else "Up"
            amount = ws.auth_inv[heavy] - ws.auth_inv[plan.side]
            if amount > cfg.min_order_size + 1:
                logger.info(
                    "  [hedge] accumulated %d > %d → generic, drop plan %s",
                    amount, cfg.min_order_size + 1,
                    plan.order_id[:10] if plan.order_id else "?")
                ws.hedge_plan = None
                price = down_price if plan.side == "Down" else up_price
                return self._place_hedge(ws, plan.side, price, amount)
            if amount >= cfg.min_order_size:
                return self._place_bound_hedge(ws, plan, up_price, down_price)
            # amount < min_order_size — the favorite only partially filled and
            # its sub-min residual folds into the next favorite round.
            ws.hedge_plan = None

        # Generic imbalance hedge — no bound plan owns the position (a dropped
        # plan means nothing is left to protect).  Route by imbalance size: a
        # single-unit imbalance (≤ min_order_size + 1) is one favorite's worth
        # and is locked with bound-style price discipline anchored to the heavy
        # side's average cost; anything larger spans mixed costs and uses the
        # blended-cost guard.  Also runs regardless of min_remaining_time
        # (hedge leg never quits).  |imbalance| < min_order_size is below the
        # strategy's unit: no sub-min hedge order.  Fall through so the
        # residual folds into the next favorite round, which gets locked in
        # one ≥ min_order_size hedge.
        diff = ws.auth_inv["Up"] - ws.auth_inv["Down"]
        if diff >= cfg.min_order_size:
            side, amount = "Down", diff
        elif diff <= -cfg.min_order_size:
            side, amount = "Up", -diff
        else:
            side, amount = None, 0
        if side is not None:
            if amount <= cfg.min_order_size + 1:
                return self._place_synthetic_bound_hedge(
                    ws, side, up_price, down_price)
            return self._place_hedge(
                ws, side, down_price if side == "Down" else up_price, amount)

        # Last 3 minutes: no NEW favorite — it wouldn't fill in time to be
        # hedged at an affordable price.  (Hedges above already took priority.)
        if remaining_time < cfg.min_remaining_time:
            return []

        # Flat + no pending + no live plan → place the fresh favorite (model
        # direction, maker price, min_order_size contracts).
        return self._place_favorite(ws, up_price, down_price, elapsed, remaining_time)

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    def _has_active_pending(self, ws: WindowState) -> bool:
        """True if any live (uncancelled) pending order with filled < min − 1 exists.

        "Pending" = an order whose fill has NOT reached min_order_size − 1.
        Such an order still owns its position and blocks all new orders
        (anti-stack).  An order filled ≥ min_order_size − 1 no longer blocks:
        it is routed instead (hedge if it cleared min_order_size, else its
        sub-min residual folds into the next favorite round).
        """
        return any(
            po.cancelled_at == 0 and po.filled < self.cfg.min_order_size - 1
            for po in ws.pending_orders.values()
        )

    def _place_hedge(
        self, ws: WindowState, side: str, price: float, amount: int,
    ) -> list[Decision]:
        """Lock in the filled favorite with an opposite-leg order (size = imbalance)."""
        cfg = self.cfg
        heavy = "Down" if side == "Up" else "Up"

        # Don't stack a second hedge while one is already working.  Only live
        # orders count — a filled-but-unpurged phantom (remaining ≤ 0) must
        # not block the hedge forever.
        if any(po.side == side and po.cancelled_at == 0 and po.remaining > 0
               for po in ws.pending_orders.values()):
            logger.info("  [hedge] %s already pending → skip", side)
            return []

        # Exposure guard — cap hedge size to remaining room on the light side
        room = cfg.max_per_side - ws.auth_inv[side]
        if room <= 0:
            logger.info("  [hedge] %s exposure at max (%d) → skip",
                        side, cfg.max_per_side)
            return []
        amount = min(amount, room)

        # Cost guard: the expected POST-hedge pair cost ≤ pair_cost_target_extreme.
        # The light side may already hold contracts, so its average after the
        # hedge is the blended mean of the existing light position and the new
        # hedge at `price` — NOT the raw hedge price.  Hedging into a light
        # side already filled cheaper averages the pair down (a pricier light
        # position averages it up); the guard reflects the true locked-in pair
        # cost.  An empty light side degenerates to avg_heavy + price.
        avg_heavy = ws.avg_cost_up if heavy == "Up" else ws.avg_cost_down
        if avg_heavy > 0:
            avg_light = ws.avg_cost_up if side == "Up" else ws.avg_cost_down
            light_inv = ws.inventory[side]
            blended = ((avg_light * light_inv + amount * price)
                       / (light_inv + amount))
            pair_cost = avg_heavy + blended
            if pair_cost > cfg.pair_cost_target_extreme:
                logger.info(
                    "  [hedge] %s avg %.4f + light %.4f = %.4f > %.2f → skip",
                    heavy, avg_heavy, blended, pair_cost,
                    cfg.pair_cost_target_extreme,
                )
                return []

        logger.info("  [hedge] %s %d @ %.4f  (imbalance=%d)",
                    side, amount, price,
                    ws.auth_inv[heavy] - ws.auth_inv[side])
        return [Decision(side=side, amount=amount, price=price)]

    def _place_bound_hedge(
        self, ws: WindowState, plan: HedgePlan,
        up_price: float, down_price: float,
    ) -> list[Decision]:
        """Fire the bound hedge once its favorite no longer blocks the window.

        Called only when no pending order blocks (every tracked order has
        filled ≥ min_order_size − 1 or is cancelled) and the favorite's live
        imbalance is a single favorite's worth (∈ [min_order_size,
        min_order_size + 1]).  The hedge side and max price were decided when
        the favorite was placed.
        The AMOUNT is the current live imbalance: a sub-min residual folded
        into the next favorite accumulates on the heavy side, and only the
        live imbalance fully locks it (for a plain favorite this equals the
        plan's original size).  Here we only need the current light-side maker
        price: if it is within the bound, place; otherwise wait — chasing
        above the bound would make the pair cost more than it is worth.
        Returns [] and retries next tick until affordable.
        """
        cfg = self.cfg
        price = down_price if plan.side == "Down" else up_price

        # Don't stack a second hedge while one is already working
        if any(po.side == plan.side and po.cancelled_at == 0 and po.remaining > 0
               for po in ws.pending_orders.values()):
            logger.info("  [hedge] %s already pending → skip", plan.side)
            return []

        # Price bound decided at favorite placement: never pay above this.
        if price > plan.max_price:
            logger.info(
                "  [hedge] %s maker %.4f > bound %.4f (fav %s @ %.4f) → wait",
                plan.side, price, plan.max_price,
                plan.order_id[:10] if plan.order_id else "?",
                plan.fav_price,
            )
            return []

        # Exposure guard — cap hedge size to remaining room on the light side
        room = cfg.max_per_side - ws.auth_inv[plan.side]
        if room <= 0:
            logger.info("  [hedge] %s exposure at max (%d) → skip",
                        plan.side, cfg.max_per_side)
            return []

        # Amount = live imbalance (light brought up to heavy), not the plan's
        # original favorite size — accumulated residuals must be fully locked.
        heavy = "Down" if plan.side == "Up" else "Up"
        amount = ws.auth_inv[heavy] - ws.auth_inv[plan.side]
        if amount <= 0:
            logger.info("  [hedge] %s no imbalance to hedge → skip", plan.side)
            return []
        amount = min(amount, room)

        plan.placed = True  # consume the plan exactly once
        logger.info("  [hedge] %s %d @ %.4f  (bound=%.4f fav=%.4f filled=%.1f)",
                    plan.side, amount, price, plan.max_price,
                    plan.fav_price, plan.filled)
        return [Decision(side=plan.side, amount=amount, price=price)]

    def _place_synthetic_bound_hedge(
        self, ws: WindowState, side: str,
        up_price: float, down_price: float,
    ) -> list[Decision]:
        """Bound-style hedge for a single-unit imbalance with no live plan.

        Restart recovery loses the in-memory HedgePlan, and a consumed or
        abandoned plan leaves none behind either.  For a position still a single
        favorite (≤ min_order_size + 1 net) the bound hedge's price discipline
        still applies: anchor the max hedge price to the heavy side's average
        cost — for one fully-filled favorite that is exactly the price the plan
        would have locked at placement time — so the pair stays
        ≤ hedge_price_bound.  A position with no cost basis falls back to the
        generic blended-cost guard (cheaper pair, never riskier).
        """
        cfg = self.cfg
        heavy = "Down" if side == "Up" else "Up"
        avg_heavy = ws.avg_cost_up if heavy == "Up" else ws.avg_cost_down
        if avg_heavy <= 0:
            diff = ws.auth_inv["Up"] - ws.auth_inv["Down"]
            amount = -diff if side == "Up" else diff
            return self._place_hedge(
                ws, side, down_price if side == "Down" else up_price, amount)
        plan = HedgePlan(
            order_id="",
            side=side,
            amount=0,
            fav_price=avg_heavy,
            max_price=round(cfg.hedge_price_bound - avg_heavy, 4),
        )
        return self._place_bound_hedge(ws, plan, up_price, down_price)

    def _favorite_decision(
        self, ws: WindowState,
        up_price: float, down_price: float,
        remaining_time: float, log: bool = True,
    ) -> "Decision | None":
        """The fresh favorite Decision (single leg), or None when gated.

        The model's P_fair picks the direction only — it never intervenes on
        price.  The favorite is offered at the book's maker price and fills
        when the market trades through it.  Pair-cost is checked entirely on
        the hedge leg (avg_heavy + hedge_price ≤ pair_cost_target_extreme).
        """
        cfg = self.cfg

        fav = self.predictor.favorite(remaining_time)
        if fav is None:
            return None  # model not confident, or BTC/features stale
        side, p_fair_up = fav   # p_fair_up is always P(Up) — see Predictor.favorite
        fav_pf = p_fair_up if side == "Up" else 1.0 - p_fair_up
        price = up_price if side == "Up" else down_price

        # Exposure guard.  auth_inv is flat here (step-4 gate) and the
        # anti-stack gate blocks any active pending, so the only way to be
        # at max is both legs filled to max_per_side → skip the new favorite.
        if ws.auth_inv[side] >= cfg.max_per_side:
            logger.info("  [favorite] %s exposure %d >= %d → skip",
                        side, ws.auth_inv[side], cfg.max_per_side)
            return None

        # Price cap: never chase the favorite above max_extreme_price (0.90).
        # Near settlement the light-side hedge can't clear the cost guard, so
        # a favorite this high is effectively a naked bet — cap it.
        if price > cfg.max_extreme_price:
            logger.info("  [favorite] %s price %.4f > %.2f → skip",
                        side, price, cfg.max_extreme_price)
            return None

        if log:
            logger.info("  [favorite] %s %d @ %.4f  P_fair=%.4f",
                        side, cfg.min_order_size, price, fav_pf)
        return Decision(side=side, amount=cfg.min_order_size, price=price)

    def _place_favorite(
        self, ws: WindowState,
        up_price: float, down_price: float,
        elapsed: float, remaining_time: float,
    ) -> list[Decision]:
        """Single Decision on the model favorite, or [] when gated.

        The Decision is tagged ``creates_hedge_plan`` — the bound hedge plan
        (opposite side, full order size, max hedge price = hedge_price_bound
        − favorite price) is recorded by executor.place ONLY once the favorite
        order is successfully placed, so a plan never precedes a real order.

        Anti-stack: if a favorite is already awaiting hedge (an active,
        unplaced bound-hedge plan owns the position), no NEW favorite is
        placed — at most one favorite in play per window.
        """
        # Anti-stack guard — a live (unconsumed) plan means the previous
        # favorite is still owned and awaits its bound hedge.  Placing another
        # favorite would stack two unhedged positions.  (decide() structurally
        # blocks this too; the guard makes the invariant explicit.)
        plan = ws.hedge_plan
        if plan is not None and not plan.placed:
            logger.info("  [favorite] favorite awaiting hedge (%s) → no new favorite",
                        plan.order_id[:10] if plan.order_id else "?")
            return []

        d = self._favorite_decision(ws, up_price, down_price, remaining_time)
        if d is None:
            return []
        return [Decision(
            side=d.side, amount=d.amount, price=d.price,
            creates_hedge_plan=True,
        )]

    def _reprice_stale_favorites(
        self, ws: WindowState,
        up_price: float, down_price: float,
    ) -> list[Decision]:
        """Proactively cancel long-unfilled, priced-out favorites.

        A favorite that never fills blocks every later tick of the window under
        the pending gate (filled < min_order_size − 1 → all new orders
        blocked).  It is cancelled only when BOTH hold:

          - it has rested favorite_stale_seconds without filling, AND
          - the current maker price has moved MORE than stale_price_diff ABOVE
            its limit — a bid the market has run past will not fill.  A bid the
            market still sits at (or has fallen below) can still fill, so
            cancelling it would be pure churn (cancel → re-place at nearly the
            same price).

        The next tick then places fresh at the current book maker price.

        Only the lead leg (先行脚 / favorite) is ever cancelled.  The hedge leg
        (对冲脚) is never touched: cancelling it would throw away a committed
        bound hedge and leave the filled contracts naked.  A pending order is a
        hedge when it sits on the light side (fewer auth_inv contracts), or
        when a consumed plan (placed=True) recorded it as the hedge side — the
        latter also covers the flat case where the hedge is still resting while
        auth_inv reads equal.
        """
        cfg = self.cfg
        now = time.time()

        # 对冲脚: the light side of the live imbalance, or the side of a
        # consumed bound-hedge plan (the hedge leg already fired).
        diff = ws.auth_inv["Up"] - ws.auth_inv["Down"]
        hedge_sides = {"Down"} if diff > 0 else ({"Up"} if diff < 0 else set())
        plan = ws.hedge_plan
        if plan is not None and plan.placed:
            hedge_sides.add(plan.side)

        cancels: list[Decision] = []
        for po in ws.pending_orders.values():
            if po.side in hedge_sides:
                continue  # 对冲脚 — never cancel a working hedge
            if po.cancelled_at != 0 or po.remaining <= 0:
                continue
            if po.filled >= cfg.min_order_size - 1:
                continue  # filled enough to route — not a blocking pending
            age = now - po.placed_at
            if age < cfg.favorite_stale_seconds:
                continue  # still a reasonable shot — leave it working
            cur = up_price if po.side == "Up" else down_price
            if cur <= po.price + cfg.stale_price_diff:
                continue  # not priced out — cancelling would just churn
            logger.info("  [reprice] %s stale (age=%.0fs, %.4f → %.4f) → cancel %s…",
                        po.side, age, po.price, cur, po.order_id[:8])
            cancels.append(Decision(
                side="Cancel", amount=0, price=0.0,
                cancel_prior=po.order_id))
        return cancels
