"""Predictive single-leg-first strategy — poly_predict P_fair favorite buy.

Per window the loop is:
  1. From +pred_start_elapsed s, compute P_fair → buy the favorite side
     (single-leg order) when |P_fair - 0.5| >= pred_conf_threshold.
  2. When the favorite fills → place the pairing order (opposite leg) via
     the inherited step-2 repair, if pair cost stays under
     pair_cost_target_extreme.
  3. When the pair completes → repeat from step 1.

Serialization: max_single_leg_pairs=1 (default) allows only one unpaired
favorite at a time, so the cycle strictly alternates buy-favorite →
pair → buy-favorite → pair.  The min_remaining_time guard stops the cycle
near settlement (same risk posture as the maker strategy).
"""
from __future__ import annotations

import logging

from .config import Config
from .models import Decision, WindowState, _parse_duration_from_slug
from .predictor import Predictor
from .strategy import MakerStrategy

logger = logging.getLogger(__name__)


class PredictiveMakerStrategy(MakerStrategy):
    """Pair-after-favorite strategy driven by the poly_predict model."""

    def __init__(self, cfg: Config, predictor: Predictor):
        super().__init__(cfg)
        self.predictor = predictor

    def decide(
        self,
        ws: WindowState,
        up_price: float,
        down_price: float,
        remaining_time: float = 999,
    ) -> list[Decision]:
        """Favorite-first: pair any filled favorite, else buy a new favorite."""
        cfg = self.cfg

        # End-of-window guard (same risk posture as maker)
        if remaining_time < cfg.min_remaining_time:
            return []

        # +pred_start_elapsed gate: no favorite before the first judgment point
        elapsed = _parse_duration_from_slug(ws.slug) - remaining_time
        if elapsed < cfg.pred_start_elapsed:
            return []

        # Step 1: fuse single-leg pairs (safety net — inherits maker fusion)
        self._fuse_pairs(ws)

        # Step 2: pair only after the favorite fills (default).  A
        # still-pending favorite is left unpaired until it actually fills,
        # so we never accumulate a naked non-favorite leg (the single-leg
        # risk is accepted only on the favorite side).  Set
        # POLY_PRED_REQUIRE_FILLED=false to pair immediately instead.
        decisions = self._step2_repair(
            ws, up_price, down_price, require_filled=cfg.pred_require_filled)
        if decisions:
            return decisions  # awaiting pairing — hold off on a new favorite

        # Serialize the loop: one unpaired favorite at a time
        if self._count_single_legs(ws) >= cfg.max_single_leg_pairs:
            return []

        # Step 3: new favorite buy (single-leg order on the model favorite)
        return self._place_favorite(ws, up_price, down_price, elapsed, remaining_time)

    def _place_favorite(
        self, ws: WindowState,
        up_price: float, down_price: float,
        elapsed: float, remaining_time: float,
    ) -> list[Decision]:
        """Single Decision on the model favorite, or [] when gated."""
        decisions: list[Decision] = []
        cfg = self.cfg

        fav = self.predictor.favorite(remaining_time)
        if fav is None:
            return decisions  # model not confident, or BTC/features stale
        side, p_fair = fav
        price = up_price if side == "Up" else down_price

        # Exposure guard per side (inventory + pending)
        pending_side = sum(
            po.remaining for po in ws.pending_orders.values()
            if po.side == side and po.cancelled_at == 0
        )
        if ws.inventory[side] + pending_side >= cfg.max_per_side:
            logger.info("  [favorite] %s exposure %d >= %d → skip",
                        side, ws.inventory[side] + pending_side, cfg.max_per_side)
            return decisions

        # Directional drift guard: don't stack too many on one side
        pending_up = sum(po.remaining for po in ws.pending_orders.values()
                         if po.side == "Up" and po.cancelled_at == 0)
        pending_down = sum(po.remaining for po in ws.pending_orders.values()
                           if po.side == "Down" and po.cancelled_at == 0)
        if abs((ws.inventory["Up"] + pending_up)
               - (ws.inventory["Down"] + pending_down)) >= cfg.max_imbalance:
            logger.info("  [favorite] directional imbalance >= %d → skip",
                        cfg.max_imbalance)
            return decisions

        # Pending order cap (same as maker step3)
        pending_cnt = sum(1 for po in ws.pending_orders.values()
                          if po.cancelled_at == 0 and po.filled == 0)
        if pending_cnt >= cfg.max_pending_orders:
            return decisions

        # Min price gap — don't stack at the same price
        if any(abs(po.price - price) < cfg.min_price_gap
               for po in ws.pending_orders.values()
               if po.side == side and po.cancelled_at == 0):
            logger.info("  [favorite] %s price %.4f too close to pending → skip",
                        side, price)
            return decisions

        decisions.append(Decision(side=side, amount=cfg.min_order_size, price=price))
        logger.info("  [favorite] %s %d @ %.4f  P_fair=%.4f  elapsed=%ds",
                    side, cfg.min_order_size, price, p_fair, int(elapsed))
        return decisions
