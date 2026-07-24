"""
Temporal Arbitrage Strategy — cost-aware proportional allocation.

Core idea:
  Buy both Up and Down each tick. Cheap side gets more (proportional).
  Sides where current price is below holding average cost get bonus
  allocation to actively improve pair_cost toward < $1.

  Over a window, accumulate both sides.
  If average cost pair < $1, profit.
"""

from __future__ import annotations
from typing import Optional

from .config import Config
from .models import OrderBookSnapshot, WindowState


class TemporalArbStrategy:
    """Strategy: proportional allocation weighted by cost improvement."""

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def decide(
        self,
        ws: WindowState,
        up_price: float,
        down_price: float,
        remaining_time: float = 999,
        up_snap: Optional[OrderBookSnapshot] = None,
        down_snap: Optional[OrderBookSnapshot] = None,
    ):
        """Decide how many contracts to buy on each side this tick.

        Three layers:
          1. Base — proportional: cheap side gets more (backtest-proven)
          2. Cost bonus — if price < avg_cost, bonus allocation to improve position
          3. Floor & cap — each side ≥ 1, total ≤ per_tick, respect max_per_side

        Returns:
            (up_buy: int, down_buy: int)
        """
        # Stop placing new orders when the window is nearly over —
        # gives pending orders time to fill before settlement.
        if remaining_time < self.cfg.min_remaining_time:
            return (0, 0)

        per_tick = self.cfg.per_tick
        max_side = self.cfg.max_per_side
        min_size = self.cfg.min_order_size

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        # Skip side with an active pending order (one pending per side)
        pending_up = any(po.side == "Up" for po in ws.pending_orders.values())
        pending_down = any(po.side == "Down" for po in ws.pending_orders.values())

        # Capacity check
        up_cap = max_side - inv_up if not pending_up else 0
        down_cap = max_side - inv_down if not pending_down else 0

        if up_cap < min_size and down_cap < min_size:
            return (0, 0)

        total = up_price + down_price
        if total <= 0:
            return (0, 0)

        # ── Layer 1: base proportional allocation ──
        # cheap side gets more: up_share = 1 - up_price/total
        up_share = 1.0 - up_price / total
        down_share = 1.0 - down_price / total

        # ── Layer 2: cost-improvement bonus ──
        # side where current price < holding avg_cost gets bonus multiplier
        avg_up = ws.avg_cost_up
        avg_down = ws.avg_cost_down

        if avg_up > 0 and up_price < avg_up:
            discount = (avg_up - up_price) / up_price
            up_share *= (1.0 + min(discount, 2.0))  # cap bonus at 3x

        if avg_down > 0 and down_price < avg_down:
            discount = (avg_down - down_price) / down_price
            down_share *= (1.0 + min(discount, 2.0))

        # ── Layer 3: normalize → contracts ──
        # floor: each side ≥ 1 if capacity available
        total_share = up_share + down_share
        if total_share <= 0:
            return (0, 0)

        up_buy = max(1, int(per_tick * up_share / total_share + 0.5))
        down_buy = max(1, int(per_tick * down_share / total_share + 0.5))

        # Ensure total doesn't exceed per_tick by more than 1
        if up_buy + down_buy > per_tick + 1:
            scale = per_tick / (up_buy + down_buy)
            up_buy = max(1, int(up_buy * scale))
            down_buy = max(1, int(down_buy * scale))

        # Apply capacity limits
        up_buy = min(up_buy, up_cap)
        down_buy = min(down_buy, down_cap)

        return (up_buy, down_buy)
