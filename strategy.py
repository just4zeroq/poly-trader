"""
Two-role strategy: Pairer (close exposure) + Cheap-seeker (open new positions).

Core idea:
  - **Pairer**: Every tick, find the cheapest unpaired lot and try to pair it
    at a slightly more aggressive maker price (aggressiveness=0.5).
    Constraint: lot.cost + maker_price < max_pair_cost (0.98).
    Qty is NOT limited by lot.unpaired_qty — the lot only provides the
    cost-basis for the price cap.  Always runs.

  - **Cheap-seeker**: Buy the cheaper side each tick at a conservative maker
    price (aggressiveness=0.2).  Pauses when imbalance >= K AND cheap side
    equals the overweight side (trend protection).

Conflict: pairer > cheap-seeker.
"""

from __future__ import annotations
import logging
from typing import Optional

from .config import Config
from .models import Decision, OrderBookSnapshot, WindowState

logger = logging.getLogger(__name__)


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
        # Role 1 — Pairer (lot-driven; always runs)
        # ══════════════════════════════════════════════════════════

        # Collect unpaired lots sorted by cost ascending (cheapest first)
        unpaired = sorted(
            (l for l in ws.lots if l.unpaired_qty > 0),
            key=lambda l: l.price,
        )

        for lot in unpaired:
            # Don't pair if the lot's side still has pending orders
            # (position still building — wait for full fill first)
            if any(
                po.side == lot.side and po.cancelled_at == 0
                for po in ws.pending_orders.values()
            ):
                continue

            pair_side = "Up" if lot.side == "Down" else "Down"

            # max_per_side check
            if pair_side == "Up" and inv_up >= max_side:
                continue
            if pair_side == "Down" and inv_down >= max_side:
                continue

            snap = up_snap if pair_side == "Up" else down_snap
            if not snap.best_bid or not snap.best_ask:
                continue

            # Compute pairing maker price — capped by lot's cost basis
            raw = snap.best_bid + snap.spread * self.cfg.pairing_aggressiveness
            cap = self.cfg.max_pair_cost - lot.price
            price = round(min(raw, cap), 4)

            # Hard condition: lot.cost + price <= max_pair_cost (allow boundary)
            if lot.price + price > self.cfg.max_pair_cost:
                continue

            # Price must be within the book
            if price < snap.best_bid or price > snap.best_ask:
                logger.info(
                    "  [pairer] price %.4f outside book (bid=%.4f ask=%.4f) "
                    "for lot %s @ %.4f — skip",
                    price, snap.best_bid, snap.best_ask,
                    lot.lot_id, lot.price,
                )
                continue

            # Pair qty logic: if the lot is small (< 2× per_tick), pair at
            # per_tick so the exchange has room to fill.  Otherwise pair the
            # full unpaired amount to close exposure quickly.
            raw_qty = per_tick if lot.unpaired_qty < 2 * per_tick else lot.unpaired_qty
            qty = min(raw_qty, max_side - (inv_up if pair_side == "Up" else inv_down))
            if qty < self.cfg.min_order_size:
                logger.info("  [pairer] qty %d < min %d for lot %s — skip",
                            qty, self.cfg.min_order_size, lot.lot_id)
                continue

            # ── Side-taken check (after price/qty checks pass) ──
            # If target side is blocked by a pairing order, skip.
            # If blocked by a cheap order, cancel it and replace.
            cancel_cheap_oid: str | None = None
            target_blocked = False
            for oid, po in ws.pending_orders.items():
                if po.side == pair_side and po.cancelled_at == 0:
                    if po.pairing_lot_id is not None:
                        target_blocked = True
                        break
                    cancel_cheap_oid = oid
            if target_blocked:
                logger.info("  [pairer] skip lot %s: %s side taken by pairing order",
                            lot.lot_id, pair_side)
                continue
            if cancel_cheap_oid:
                logger.info("  [pairer] overriding cheap %s order %s for lot %s",
                            pair_side, cancel_cheap_oid, lot.lot_id)

            decisions.append(Decision(
                side=pair_side,
                amount=qty,
                price=price,
                role="pairing",
                lot_id=lot.lot_id,
                cancel_order_id=cancel_cheap_oid,
            ))

            # Claim the side so cheap-seeker doesn't collide
            if pair_side == "Up":
                pending_up = True
            else:
                pending_down = True
            break  # One pairing decision per tick

        # ══════════════════════════════════════════════════════════
        # Role 3 — Cheap-seeker
        # ══════════════════════════════════════════════════════════

        # Pair-before-open: when imbalance exists, buy underweight side
        # instead of the cheapest side, to reduce exposure.
        if imbalance > 0 and underweight is not None:
            cheap = underweight

        # Guard 1: stop opening new positions in the final minutes
        if remaining_time >= self.cfg.min_remaining_time:
            # Guard 2: minimum price edge — skip near-50/50 prices
            if abs(up_price - down_price) < self.cfg.min_edge:
                return decisions

            # Guard 3: trend protection + early brake at K/2
            trend_stopped = (
                overweight is not None
                and cheap == overweight
                and imbalance >= max_imb // 2
            )
            if not trend_stopped:
                # Guard 4: side already claimed (by pairer or existing pending)
                side_blocked = (cheap == "Up" and pending_up) or (cheap == "Down" and pending_down)
                side_full = (cheap == "Up" and inv_up >= max_side) or (cheap == "Down" and inv_down >= max_side)

                if not side_blocked and not side_full:
                    snap = up_snap if cheap == "Up" else down_snap
                    if snap.best_bid and snap.best_ask:
                        price = round(
                            snap.best_bid + snap.spread * self.cfg.aggressiveness, 4,
                        )
                        # Clamp: post-only BUY must not cross best_ask
                        if price >= snap.best_ask:
                            price = snap.best_bid

                        qty = min(per_tick, max_side - (inv_up if cheap == "Up" else inv_down))

                        # Auto-pair key links cheap + expensive sides for paired fill
                        pair_key = f"ap_{ws.window_num}_{len(decisions)}"

                        decisions.append(Decision(
                            side=cheap,
                            amount=qty,
                            price=price,
                            role="cheap",
                            lot_id=None,
                            auto_pair_key=pair_key,
                        ))

                        # Plan 2: also buy the expensive side at pairing_aggressiveness
                        # (no max_pair_cost cap — both orders on book simultaneously)
                        expensive = "Down" if cheap == "Up" else "Up"
                        # Check if side was claimed by pairer after cheap decision
                        exp_claimed = (expensive == "Up" and pending_up) or (expensive == "Down" and pending_down)
                        if not exp_claimed:
                            exp_snap = up_snap if expensive == "Up" else down_snap
                            if exp_snap.best_bid and exp_snap.best_ask:
                                exp_price = round(
                                    exp_snap.best_bid + exp_snap.spread * self.cfg.pairing_aggressiveness,
                                    4,
                                )
                                if exp_price >= exp_snap.best_ask:
                                    exp_price = exp_snap.best_bid
                                decisions.append(Decision(
                                    side=expensive,
                                    amount=qty,
                                    price=exp_price,
                                    role="auto_pair",
                                    lot_id=None,
                                    auto_pair_key=pair_key,
                                ))

        return decisions
