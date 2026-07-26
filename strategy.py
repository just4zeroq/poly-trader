"""
Simple maker strategy: independently buy Up and Down each tick.

Each tick:
  1. Skip if remaining_time too short.
  2. Per-side: filled inventory + pending shares >= max_per_side → skip tick.
  3. Place per_tick contracts on each side (capped by max_per_side - side_exposure).

Fills are generically paired in the executor — no directed patch logic needed.
Engine's _resolve_pair_prices already validates that up_price + down_price < max_pair_sum.
"""

from __future__ import annotations
import logging

from .config import Config
from .models import Decision, WindowState

logger = logging.getLogger(__name__)


class MakerStrategy:
    """Simple maker: independently buy Up and Down each tick."""

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def decide(
        self,
        ws: WindowState,
        up_price: float,
        down_price: float,
        remaining_time: float = 999,
    ) -> list[Decision]:
        """Produce 0-2 buy decisions (Up + Down) for this tick.

        Uses engine-validated prices directly.  No recomputation needed.
        """
        decisions: list[Decision] = []
        cfg = self.cfg

        # ── Guard: stop near window end ──
        if remaining_time < cfg.min_remaining_time:
            return decisions

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        # ── Per-side pending shares ──
        pending_up = sum(
            po.remaining for po in ws.pending_orders.values()
            if po.side == "Up" and po.cancelled_at == 0
        )
        pending_down = sum(
            po.remaining for po in ws.pending_orders.values()
            if po.side == "Down" and po.cancelled_at == 0
        )

        # ── Per-side decisions (each side independent) ──
        exposure_up = inv_up + pending_up
        exposure_down = inv_down + pending_down

        # ── Price proximity check (global: any side too close → skip tick) ──
        # If either side's price is too close to an existing pending order on
        # that side, skip the entire tick.  Prevents single-sided accumulation
        # when one side's orders won't fill while the other keeps buying.
        up_too_close = any(
            abs(po.price - up_price) < cfg.min_price_gap
            for po in ws.pending_orders.values()
            if po.side == "Up" and po.cancelled_at == 0
        )
        down_too_close = any(
            abs(po.price - down_price) < cfg.min_price_gap
            for po in ws.pending_orders.values()
            if po.side == "Down" and po.cancelled_at == 0
        )
        if up_too_close or down_too_close:
            logger.info(
                "  [maker] Skip tick: price too close to pending order "
                "(min_gap=%.4f)  Up_close=%s Down_close=%s  "
                "Up=%.4f Down=%.4f  pending_U=%d D=%d",
                cfg.min_price_gap, up_too_close, down_too_close,
                up_price, down_price, pending_up, pending_down,
            )
            return decisions

        for side, price, exposure in [
            ("Up", up_price, exposure_up),
            ("Down", down_price, exposure_down),
        ]:
            room = cfg.max_per_side - exposure
            if room <= 0:
                logger.info("  [maker] %s %d/%d at limit → skip", side, exposure, cfg.max_per_side)
                continue

            # ── Imbalance guard: don't add to the heavy side ──
            # In coupled pricing mode, the derived side (pair-completing order)
            # would be blocked incorrectly — skip the guard.
            if self.cfg.profit_target <= 0:
                imbalance = abs(exposure_up - exposure_down)
                if imbalance >= cfg.max_imbalance:
                    heavy = "Up" if exposure_up > exposure_down else "Down"
                    if side == heavy:
                        logger.info("  [maker] %s skip: imbalance %d >= %d (expo U=%d D=%d)",
                                    side, imbalance, cfg.max_imbalance,
                                    exposure_up, exposure_down)
                        continue
                    # Light side: cap price to ensure pair cost ≤ max_pair_sum
                    heavy_avg = ws.avg_cost_up if heavy == "Up" else ws.avg_cost_down
                    max_light = cfg.max_pair_sum - heavy_avg
                    if max_light > 0 and price > max_light:
                        logger.info("  [maker] %s rebalance: price %.4f → %.4f (heavy_avg=%.4f, max_pair=%.4f)",
                                    side, price, max_light, heavy_avg, cfg.max_pair_sum)
                        price = max_light

            # ── Marginal pair cost check ──
            # Would filling this order push the average pair cost above
            # max_pair_sum?  Skip if so — otherwise we lock in a guaranteed loss.
            # In coupled pricing mode (profit_target > 0), every NEW pair
            # costs exactly lead + derived = 1.0 - profit_target, independent
            # of existing avg_cost.  Skip marginal check to avoid blocking
            # the lead side when existing inventory happens to have a very
            # different average cost (e.g. Up filled cheap, Down won't fill).
            if self.cfg.profit_target > 0:
                pass  # coupled pricing: new pairs always cost 1.0 - target
            elif side == "Up" and ws.inventory["Down"] > 0:
                marginal = price + ws.avg_cost_down
                if marginal > cfg.max_pair_sum:
                    logger.info(
                        "  [maker] %s skip: marginal pair %.4f > %.4f "
                        "(up=%.4f + avg_down=%.4f  inv Down=%d)",
                        side, marginal, cfg.max_pair_sum,
                        price, ws.avg_cost_down, ws.inventory["Down"],
                    )
                    continue
            elif side == "Down" and ws.inventory["Up"] > 0:
                marginal = ws.avg_cost_up + price
                if marginal > cfg.max_pair_sum:
                    logger.info(
                        "  [maker] %s skip: marginal pair %.4f > %.4f "
                        "(avg_up=%.4f + down=%.4f  inv Up=%d)",
                        side, marginal, cfg.max_pair_sum,
                        ws.avg_cost_up, price, ws.inventory["Up"],
                    )
                    continue

            qty = min(cfg.per_tick, room)
            if qty >= cfg.min_order_size:
                decisions.append(Decision(side=side, amount=qty, price=price))

        return decisions
