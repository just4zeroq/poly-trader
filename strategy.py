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

        # ── Atomic pre-check: if either side fails price proximity, skip both ──
        # Prevents deadlock where one side keeps placing while the other is stuck
        # because its price is too close to an existing pending order.
        for side, price in [("Up", up_price), ("Down", down_price)]:
            if price <= 0:
                continue
            too_close = any(
                abs(po.price - price) < cfg.min_price_gap
                for po in ws.pending_orders.values()
                if po.side == side and po.cancelled_at == 0
            )
            if too_close:
                logger.info(
                    "  [maker] Atomic skip: %s price %.4f too close to pending "
                    "(min_gap=%.4f) \u2192 skip both sides",
                    side, price, cfg.min_price_gap,
                )
                return decisions  # empty \u2192 skip both Up and Down

        for side, price, exposure in [
            ("Up", up_price, exposure_up),
            ("Down", down_price, exposure_down),
        ]:
            if price <= 0:
                continue
            room = cfg.max_per_side - exposure
            if room <= 0:
                logger.info("  [maker] %s %d/%d at limit → skip", side, exposure, cfg.max_per_side)
                continue

            # ── Imbalance guard: don't add to the heavy side ──
            # Tier 1: Filled-only — real directional risk
            inv_imbalance = abs(inv_up - inv_down)
            if inv_imbalance >= cfg.max_imbalance:
                heavy = "Up" if inv_up > inv_down else "Down"
                if side == heavy:
                    logger.info("  [maker] %s skip: filled imbalance %d >= %d (inv U=%d D=%d)",
                                side, inv_imbalance, cfg.max_imbalance,
                                inv_up, inv_down)
                    continue
                # Light side: cap price to ensure pair cost ≤ max_pair_sum
                heavy_avg = ws.avg_cost_up if heavy == "Up" else ws.avg_cost_down
                max_light = cfg.max_pair_sum - heavy_avg
                if max_light > 0 and price > max_light:
                    logger.info("  [maker] %s rebalance: price %.4f → %.4f (heavy_avg=%.4f, max_pair=%.4f)",
                                side, price, max_light, heavy_avg, cfg.max_pair_sum)
                    price = max_light
            # Tier 2: Exposure (filled+pending) — prevent pending from masking risk
            exp_imbalance = abs(exposure_up - exposure_down)
            if exp_imbalance >= cfg.max_imbalance:
                heavy = "Up" if exposure_up > exposure_down else "Down"
                if side == heavy:
                    logger.info("  [maker] %s skip: exposure imbalance %d >= %d (expo U=%d D=%d)",
                                side, exp_imbalance, cfg.max_imbalance,
                                exposure_up, exposure_down)
                    continue
                # Light side: cap price to ensure pair cost ≤ max_pair_sum
                heavy_avg = ws.avg_cost_up if heavy == "Up" else ws.avg_cost_down
                max_light = cfg.max_pair_sum - heavy_avg
                if max_light > 0 and price > max_light:
                    logger.info("  [maker] %s rebalance: price %.4f → %.4f (heavy_avg=%.4f, max_pair=%.4f)",
                                side, price, max_light, heavy_avg, cfg.max_pair_sum)
                    price = max_light


            # ── Price proximity check (per-side, after all price mods) ──
            # Check against the FINAL price (post rebalance cap), not the
            # original engine price.  Prevents stacking at the same level.
            too_close = any(
                abs(po.price - price) < cfg.min_price_gap
                for po in ws.pending_orders.values()
                if po.side == side and po.cancelled_at == 0
            )
            if too_close:
                logger.info(
                    "  [maker] %s skip: price %.4f too close to pending (min_gap=%.4f)",
                    side, price, cfg.min_price_gap,
                )
                continue

            qty = min(cfg.per_tick, room)
            if qty >= cfg.min_order_size:
                decisions.append(Decision(side=side, amount=qty, price=price))

        return decisions
