"""
Cheap-Side-Only Strategy with Imbalance Cap.

Core idea:
  Always buy the cheaper side each tick (maker only).  When the price
  oscillates (range-bound), the cheap side alternates naturally and pairs
  accumulate with average cost < $1.

  When the market trends (one-sided), the same side keeps being cheap —
  imbalance grows.  Once |N_up − N_down| hits the hard cap K, we stop
  buying the overweight side and only buy the underweight side to
  rebalance, or wait for settlement.

  This is NOT risk-free arbitrage.  It is a range-bound market-making
  strategy that profits from mean-reverting oscillations and caps trend
  losses at K contracts of directional exposure.
"""

from __future__ import annotations
from typing import Optional

from .config import Config
from .models import OrderBookSnapshot, WindowState


class TemporalArbStrategy:
    """Cheap-side-only maker with imbalance cap."""

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
        """Decide which side to buy this tick.

        Rules:
          1. Target the cheap side.
          2. If cheap side == overweight side AND imbalance >= K:
             trend mode — only buy the underweight side (or skip).
          3. One pending order per side max.

        Returns:
            (up_buy: int, down_buy: int)
        """
        if remaining_time < self.cfg.min_remaining_time:
            return (0, 0)

        per_tick = self.cfg.per_tick
        max_side = self.cfg.max_per_side
        max_imb = self.cfg.max_imbalance

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        # ── Determine cheap side ──
        cheap = "Up" if up_price < down_price else "Down"

        # ── Determine overweight / underweight ──
        if inv_up > inv_down:
            overweight, underweight = "Up", "Down"
        elif inv_down > inv_up:
            overweight, underweight = "Down", "Up"
        else:
            overweight, underweight = None, None

        # ── Imbalance gate: trend protection ──
        imbalance = abs(inv_up - inv_down)
        if (overweight is not None
                and cheap == overweight
                and imbalance >= max_imb):
            # Trend mode: only buy the underweight side,
            # even if it's more expensive right now.
            target = underweight
        else:
            target = cheap

        # ── Pending order check (one per side) ──
        pending_up = any(po.side == "Up" for po in ws.pending_orders.values())
        pending_down = any(po.side == "Down" for po in ws.pending_orders.values())

        up_cap = max_side - inv_up if not pending_up else 0
        down_cap = max_side - inv_down if not pending_down else 0

        # ── Allocate ──
        if target == "Up":
            up_buy = min(per_tick, up_cap) if up_cap > 0 else 0
            down_buy = 0
        else:
            down_buy = min(per_tick, down_cap) if down_cap > 0 else 0
            up_buy = 0

        return (up_buy, down_buy)
