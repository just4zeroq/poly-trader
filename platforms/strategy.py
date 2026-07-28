"""
Pair-first maker strategy for V3.

Each tick:
  1. Free-pair existing unpaired lots (cost <= pair_cost_max, no orders)
  2. If still unbalanced, place a new pair order (cost <= pair_cost_max)
  3. Normal independent logic (only if step 2 didn't place)
"""

from __future__ import annotations
import logging

from .config import Config
from .models import Decision, Pair, WindowState

logger = logging.getLogger(__name__)

class MakerStrategy:
    """Pair-first market making strategy."""

    def __init__(self, cfg: Config):
        self.cfg = cfg

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
        """Produce 0-2 buy decisions for this tick."""
        decisions: list[Decision] = []
        cfg = self.cfg

        # ── Guard: stop near window end ──
        if remaining_time < cfg.min_remaining_time:
            return decisions

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        pending_up = sum(
            po.remaining for po in ws.pending_orders.values()
            if po.side == "Up" and po.cancelled_at == 0
        )
        pending_down = sum(
            po.remaining for po in ws.pending_orders.values()
            if po.side == "Down" and po.cancelled_at == 0
        )

        exposure_up = inv_up + pending_up
        exposure_down = inv_down + pending_down

        # ════════════════════════════════════════════
        # Step 1: Free pairing — match existing lots
        # ════════════════════════════════════════════
        self._free_pair(ws)

        # ── Count remaining unpaired ──
        unpaired_up_lots = [l for l in ws.lots if l.side == "Up" and l.unpaired_qty > 0]
        unpaired_down_lots = [l for l in ws.lots if l.side == "Down" and l.unpaired_qty > 0]
        unpaired_up = sum(l.unpaired_qty for l in unpaired_up_lots)
        unpaired_down = sum(l.unpaired_qty for l in unpaired_down_lots)

        # ════════════════════════════════════════════
        # Step 2: New pair order to cover imbalance
        #         (unpaired lots only — existing pairs left alone)
        # ════════════════════════════════════════════
        placed_pair = self._step2_pair_order(
            ws, up_price, down_price,
            unpaired_up, unpaired_down,
            unpaired_up_lots, unpaired_down_lots,
            exposure_up, exposure_down,
        )
        if placed_pair:
            return placed_pair

        # ════════════════════════════════════════════
        # Step 3: New pair — normal market making
        # ════════════════════════════════════════════
        return self._step3_normal(
            ws, up_price, down_price,
            exposure_up, exposure_down,
        )

    # ──────────────────────────────────────────────
    # Step 1: Free pairing
    # ──────────────────────────────────────────────

    def _free_pair(self, ws: WindowState):
        """Match unpaired Up and Down lots where cost <= pair_cost_max.

        Strategy: sort Up ascending (cheapest first), Down descending (most
        expensive first).  Each cheap Up pairs with the most expensive Down
        that still fits the budget, preserving cheap Down lots for expensive
        Up lots that can't pair otherwise.

        This is a two-pointer maximum-bipartite-matching under a monotonic
        cost constraint — pairing cheap+expensive maximizes total matched
        contracts compared to cheap+cheap greedy.
        """
        cfg = self.cfg
        up_lots = [l for l in ws.lots if l.side == "Up" and l.unpaired_qty > 0]
        down_lots = [l for l in ws.lots if l.side == "Down" and l.unpaired_qty > 0]

        if not up_lots or not down_lots:
            return

        up_lots.sort(key=lambda l: l.price)               # cheapest Up first
        down_lots.sort(key=lambda l: l.price, reverse=True)  # most expensive Down first

        paired = 0
        for ul in up_lots:
            if ul.is_fully_paired:
                continue
            for dl in down_lots:
                if dl.unpaired_qty <= 0:
                    continue
                if ul.unpaired_qty <= 0:
                    break

                # Cost check: too expensive → cheaper Down next (descending), try next
                if ul.price + dl.price > cfg.pair_cost_max:
                    continue

                # Found a match
                pair_qty = min(ul.unpaired_qty, dl.unpaired_qty)
                ul.paired_qty += pair_qty
                dl.paired_qty += pair_qty
                paired += pair_qty

        if paired:
            logger.info(
                "  [free_pair] %d contracts paired  unpaired remaining: Up=%d Down=%d",
                paired,
                sum(l.unpaired_qty for l in ws.lots if l.side == "Up"),
                sum(l.unpaired_qty for l in ws.lots if l.side == "Down"),
            )

    # ──────────────────────────────────────────────
    # Step 2: Re-pair + new pair order
    # ──────────────────────────────────────────────

    def _step2_pair_order(
        self, ws: WindowState,
        up_price: float, down_price: float,
        unpaired_up: int, unpaired_down: int,
        unpaired_up_lots: list, unpaired_down_lots: list,
        exposure_up: int, exposure_down: int,
    ) -> list[Decision] | None:
        """Place a new pair order for completely unpaired lots only.

        Scans unpaired heavy lots individually and finds the first affordable
        one to pair against a new light-side order.  Per-lot cost check (not
        average) avoids skipping good lots just because another expensive lot
        drags the average up.

        Does NOT handle re-pairing — pairs with partial fills are left for
        future ticks (free_pair will re-match them when both sides fill).

        Returns a decision list or None to fall through to step 3.
        """
        cfg = self.cfg

        # ── New pair order for remaining unpaired lots ──
        if unpaired_up == 0 and unpaired_down == 0:
            return None

        # Determine which side to buy, then find first affordable heavy lot
        if unpaired_up > unpaired_down:
            pair_side = "Down"
            heavy_lots = [l for l in unpaired_up_lots if l.unpaired_qty > 0]
            heavy_label = "Up"
            price = down_price
        elif unpaired_down > unpaired_up:
            pair_side = "Up"
            heavy_lots = [l for l in unpaired_down_lots if l.unpaired_qty > 0]
            heavy_label = "Down"
            price = up_price
        else:
            # Equal unpaired qty — find the lot with higher price on either side
            candidates: list[tuple] = []
            for l in unpaired_up_lots:
                if l.unpaired_qty > 0:
                    candidates.append((l, "Down", down_price))  # if buying Down
            for l in unpaired_down_lots:
                if l.unpaired_qty > 0:
                    candidates.append((l, "Up", up_price))      # if buying Up
            if not candidates:
                return None
            # Sort by heavy price descending: try the most expensive heavy lot first
            candidates.sort(key=lambda x: x[0].price, reverse=True)
            for heavy_lot, p_side, p_price in candidates:
                if heavy_lot.price + p_price <= cfg.pair_cost_max:
                    pair_side = p_side
                    heavy_lot_selected = heavy_lot
                    heavy_label = "Down" if p_side == "Up" else "Up"
                    price = p_price
                    break
            else:
                logger.info(
                    "  [pair_order] No affordable pair at current prices → skip",
                )
                return None

            exposure = exposure_up if pair_side == "Up" else exposure_down
            room = cfg.max_per_side - exposure
            heavy_lots = [heavy_lot_selected]

        # Find first affordable heavy lot (per-lot cost check)
        heavy_lot = None
        for lot in heavy_lots:
            if lot.unpaired_qty <= 0:
                continue
            if lot.price + price > cfg.pair_cost_max:
                continue
            heavy_lot = lot
            break

        if heavy_lot is None:
            logger.info(
                "  [pair_order] %s cost check: all heavy lots too expensive "
                "(price=%.4f, first heavy=%.4f) → skip",
                pair_side, price,
                heavy_lots[0].price if heavy_lots else 0,
            )
            return None

        if price <= 0:
            return None
        exposure = exposure_up if pair_side == "Up" else exposure_down
        room = cfg.max_per_side - exposure

        if room < cfg.min_order_size:
            logger.info(
                "  [pair_order] %s at limit %d/%d → skip",
                pair_side, exposure, cfg.max_per_side,
            )
            return None

        # Min gap
        too_close = any(
            abs(po.price - price) < cfg.min_price_gap
            for po in ws.pending_orders.values()
            if po.side == pair_side and po.cancelled_at == 0
        )
        if too_close:
            logger.info(
                "  [pair_order] %s price %.4f too close to pending (min_gap=%.4f) → skip",
                pair_side, price, cfg.min_price_gap,
            )
            return None

        # Place order
        qty = min(cfg.min_order_size, room)
        if qty >= cfg.min_order_size:
            logger.info(
                "  [pair_order] %s %d @ %.4f (heavy=%s lot=%.4f need=%d/%d)",
                pair_side, qty, price, heavy_label,
                heavy_lot.price, heavy_lot.unpaired_qty, heavy_lot.amount,
            )

            # Create Pair object for fill tracking
            pair = Pair(
                pair_id=f"pair_{ws.window_num}_{len(ws.pairs)}",
                up_price=up_price if pair_side == "Up" else heavy_lot.price,
                down_price=down_price if pair_side == "Down" else heavy_lot.price,
                qty=cfg.min_order_size,
            )
            ws.pairs.append(pair)

            # Pre-fill the heavy side — this specific lot
            if pair_side == "Down":
                pair.up_filled = qty
            else:
                pair.down_filled = qty
            match = min(heavy_lot.unpaired_qty, qty)
            heavy_lot.paired_qty += match
            heavy_lot.pair_id = pair.pair_id

            return [Decision(side=pair_side, amount=qty, price=price, pair_id=pair.pair_id)]

        logger.info(
            "  [pair_order] %s qty %d < min_order_size %d → skip",
            pair_side, qty, cfg.min_order_size,
        )
        return None

    # ──────────────────────────────────────────────
    # Step 3: Normal logic — per-side independent orders
    # ──────────────────────────────────────────────

    def _step3_normal(
        self, ws: WindowState,
        up_price: float, down_price: float,
        exposure_up: int, exposure_down: int,
    ) -> list[Decision]:
        """Per-side independent Up/Down orders — normal market making.

        Only runs when step 2 did NOT place a pair order.
        Unlike step 2 (pair order to fix imbalance), step 3 places Up and
        Down independently, subject to per-side guards.

        Guards:
          - Room: both sides have space for min_order_size (preserve room for pairing)
          - Atomic pre-check: either side price too close to pending → skip both
          - Per-side: room check, imbalance guard (based on filled inventory)
          - Pair cost cap (both sides only): up_price + down_price <= pair_cost_max
        """
        decisions: list[Decision] = []
        cfg = self.cfg

        # ── Room check: need space on both sides to preserve room for future pairing ──
        room_up = cfg.max_per_side - exposure_up
        room_down = cfg.max_per_side - exposure_down
        if room_up < cfg.min_order_size or room_down < cfg.min_order_size:
            logger.info(
                "  [step3] Room insufficient: Up=%d/%d Down=%d/%d → skip",
                exposure_up, cfg.max_per_side, exposure_down, cfg.max_per_side,
            )
            return decisions

        # ── Atomic pre-check: either side too close to pending → skip both ──
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
                "  [step3] Atomic pre-check: price too close  "
                "Up_close=%s Down_close=%s  Up=%.4f Down=%.4f",
                up_too_close, down_too_close, up_price, down_price,
            )
            return decisions

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]

        # ── Per-side independent decisions ──
        for side, price, exposure in [
            ("Up", up_price, exposure_up),
            ("Down", down_price, exposure_down),
        ]:
            # Room check (per side)
            room = cfg.max_per_side - exposure
            if room < cfg.min_order_size:
                continue

            # Imbalance guard: two independent checks — either can block.
            #
            # 1. Total exposure imbalance (inv + pending):
            #    if one side has way more total exposure, it's adding risk.
            exp_imbalance = abs(exposure_up - exposure_down)
            if exp_imbalance >= cfg.max_imbalance:
                heavy_exp = "Up" if exposure_up > exposure_down else "Down"
                if side == heavy_exp:
                    logger.info(
                        "  [step3] %s skip: exp imbalance %d >= %d (U=%d D=%d)",
                        side, exp_imbalance, cfg.max_imbalance,
                        exposure_up, exposure_down,
                    )
                    continue

            # 2. Filled inventory imbalance:
            #    pending may never fill — exposure gives false balance when
            #    one side has heavy fills and the other heavy pending.
            #    Example: inv_up=15, pending_down=15 → exposure balanced,
            #    but real filled imbalance = 15.
            inv_imbalance = abs(inv_up - inv_down)
            if inv_imbalance >= cfg.max_imbalance:
                heavy_inv = "Up" if inv_up > inv_down else "Down"
                if side == heavy_inv:
                    logger.info(
                        "  [step3] %s skip: inv imbalance %d >= %d (inv U=%d D=%d  exp U=%d D=%d)",
                        side, inv_imbalance, cfg.max_imbalance,
                        inv_up, inv_down, exposure_up, exposure_down,
                    )
                    continue

            qty = min(cfg.min_order_size, room)
            decisions.append(Decision(side=side, amount=qty, price=price))

        # ── Pair cost cap: only when both sides would be placed ──
        if len(decisions) == 2 and up_price + down_price > cfg.pair_cost_max:
            logger.info(
                "  [step3] Pair cost %.4f > %.2f → skip (would lock loss)",
                up_price + down_price, cfg.pair_cost_max,
            )
            return []

        if decisions:
            parts = [f"{d.side}={d.amount}@{d.price:.4f}" for d in decisions]
            logger.info("  [step3] %s", "  ".join(parts))
        return decisions
