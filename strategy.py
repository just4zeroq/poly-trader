"""
Two-role strategy: Pairer (close exposure) + Cheap-seeker (open new positions).

Core idea:
  - **Pairer**: Every tick, find the cheapest unpaired lot and try to pair it
    at a slightly more aggressive maker price (aggressiveness=0.5).
    Constraint: lot.cost + maker_price < max_pair_cost (0.9999).
    Always runs — unconstrained by remaining_time or imbalance K.

  - **Cheap-seeker**: Buy the cheaper side each tick at a conservative maker
    price (aggressiveness=0.2).  Pauses when imbalance >= K AND cheap side
    equals the overweight side (trend protection).

Conflict: if both roles want the same side, pairing wins.
"""

from __future__ import annotations
from typing import Optional

from .config import Config
from .models import Decision, OrderBookSnapshot, WindowState


class TemporalArbStrategy:
    """Two-role maker: pairer (close) + cheap-seeker (open)."""

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
    ) -> list[Decision]:
        """Produce 0-2 buy decisions for this tick.

        Returns a list of Decision objects.  Pairer runs first so it claims
        its side before the cheap-seeker; natural conflict resolution without
        an explicit priority pass.
        """
        decisions: list[Decision] = []

        if not up_snap or not down_snap:
            return decisions

        per_tick = self.cfg.per_tick
        max_side = self.cfg.max_per_side
        max_imb = self.cfg.max_imbalance
        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        # ── Determine cheap / overweight ──
        cheap = "Up" if up_price < down_price else "Down"
        if inv_up > inv_down:
            overweight, underweight = "Up", "Down"
        elif inv_down > inv_up:
            overweight, underweight = "Down", "Up"
        else:
            overweight, underweight = None, None

        imbalance = abs(inv_up - inv_down)

        # ── Pending order presence per side (exclude soft-deleted) ──
        def _has_pending(side: str) -> bool:
            return any(
                po.side == side and po.cancelled_at == 0
                for po in ws.pending_orders.values()
            )

        pending_up = _has_pending("Up")
        pending_down = _has_pending("Down")

        # ══════════════════════════════════════════════════════════
        # Role 1 — Pairer (always runs; unconstrained)
        # ══════════════════════════════════════════════════════════

        # Collect unpaired lots sorted by cost ascending (cheapest first)
        unpaired = sorted(
            (l for l in ws.lots if l.unpaired_qty > 0),
            key=lambda l: l.price,
        )

        for lot in unpaired:
            pair_side = "Up" if lot.side == "Down" else "Down"

            # Side taken by existing pending order?
            if pair_side == "Up" and pending_up:
                continue
            if pair_side == "Down" and pending_down:
                continue

            # max_per_side check
            if pair_side == "Up" and inv_up >= max_side:
                continue
            if pair_side == "Down" and inv_down >= max_side:
                continue

            snap = up_snap if pair_side == "Up" else down_snap
            if not snap.best_bid or not snap.best_ask:
                continue

            # Compute pairing maker price
            raw = snap.best_bid + snap.spread * self.cfg.pairing_aggressiveness
            cap = self.cfg.max_pair_cost - lot.price
            price = round(min(raw, cap), 4)

            # Hard condition: lot.cost + price < max_pair_cost
            if lot.price + price >= self.cfg.max_pair_cost:
                continue

            # Price must be within the book
            if price < snap.best_bid or price > snap.best_ask:
                continue

            qty = min(per_tick, lot.unpaired_qty, max_side - (inv_up if pair_side == "Up" else inv_down))

            decisions.append(Decision(
                side=pair_side,
                amount=qty,
                price=price,
                role="pairing",
                lot_id=lot.lot_id,
            ))

            # Claim the side so cheap-seeker doesn't collide
            if pair_side == "Up":
                pending_up = True
            else:
                pending_down = True
            break  # One pairing decision per tick

        # ══════════════════════════════════════════════════════════
        # Role 2 — Cheap-seeker
        # ══════════════════════════════════════════════════════════

        # Guard 1: stop opening new positions in the final minutes
        if remaining_time >= self.cfg.min_remaining_time:
            # Guard 2: trend protection — don't add to the overweight side
            trend_stopped = (
                imbalance >= max_imb
                and overweight is not None
                and cheap == overweight
            )
            if not trend_stopped:
                # Guard 3: side already claimed (by pairer or existing pending)
                side_blocked = (cheap == "Up" and pending_up) or (cheap == "Down" and pending_down)
                side_full = (cheap == "Up" and inv_up >= max_side) or (cheap == "Down" and inv_down >= max_side)

                if not side_blocked and not side_full:
                    snap = up_snap if cheap == "Up" else down_snap
                    if snap.best_bid and snap.best_ask:
                        price = round(
                            snap.best_bid + snap.spread * self.cfg.aggressiveness, 4,
                        )
                        qty = min(per_tick, max_side - (inv_up if cheap == "Up" else inv_down))

                        decisions.append(Decision(
                            side=cheap,
                            amount=qty,
                            price=price,
                            role="cheap",
                            lot_id=None,
                        ))

        return decisions
