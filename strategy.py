"""
Pair-first maker strategy for V3.

Each tick:
  1. Free-pair existing unpaired lots (cost <= pair_cost_max, no orders)
  2. If still unbalanced, place a new pair order (cost <= pair_cost_max)
  3. Normal independent logic (only if step 2 didn't place)
"""

from __future__ import annotations
import logging
import time

from .config import Config
from .models import Decision, Pair, WindowState

logger = logging.getLogger(__name__)

PAIR_SIZE = 5  # contracts per side per pair


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
        # Step 2: Re-pair existing incomplete pairs,
        #         then new pair order to cover imbalance
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
        # Step 3: Normal independent logic
        # ════════════════════════════════════════════
        return self._step3_normal(
            ws, up_price, down_price,
            unpaired_up, unpaired_down,
            unpaired_up_lots, unpaired_down_lots,
            inv_up, inv_down,
            exposure_up, exposure_down,
        )

    # ──────────────────────────────────────────────
    # Step 1: Free pairing
    # ──────────────────────────────────────────────

    def _free_pair(self, ws: WindowState):
        """Match unpaired Up and Down lots where cost <= pair_cost_max.

        Greedy matching: for each unpaired Up lot, find the cheapest
        unpaired Down lot that keeps the pair cost within budget.
        Updates lot paired_qty in-place — no orders placed.
        """
        cfg = self.cfg
        up_lots = [l for l in ws.lots if l.side == "Up" and l.unpaired_qty > 0]
        down_lots = [l for l in ws.lots if l.side == "Down" and l.unpaired_qty > 0]

        if not up_lots or not down_lots:
            return

        # Sort by price (lowest first) to pair cheaper lots first
        up_lots.sort(key=lambda l: l.price)
        down_lots.sort(key=lambda l: l.price)

        paired = 0
        for ul in up_lots:
            if ul.is_fully_paired:
                continue
            for dl in down_lots:
                if dl.is_fully_paired:
                    continue
                if dl.unpaired_qty <= 0:
                    continue
                if ul.unpaired_qty <= 0:
                    break  # this Up lot is fully paired

                # Cost check
                if ul.price + dl.price > cfg.pair_cost_max:
                    continue

                # Pair by minimum unpaired qty
                pair_qty = min(ul.unpaired_qty, dl.unpaired_qty)
                ul.paired_qty += pair_qty
                dl.paired_qty += pair_qty
                pair_id = f"fp_{ul.lot_id}_{dl.lot_id}"
                ul.pair_id = pair_id
                dl.pair_id = pair_id
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
        """First try re-pairing existing pairs, then new pair for imbalance.

        Re-pair: existing pair has one side filled, place the missing side.
        New pair: no re-pair needed, place to cover unpaired lots.

        Returns a decision list or None to fall through to step 3.
        """
        cfg = self.cfg

        # ── Phase A: Re-pair existing incomplete pairs ──
        for pair in ws.pairs:
            if pair.is_complete:
                continue
            pending_side = pair.pending_side
            if pending_side is None:
                continue  # both sides still pending

            # Price for missing side = current market price
            if pending_side == "Up":
                price = up_price
                filled_price = pair.down_price
                remaining = pair.qty - pair.up_filled
            else:
                price = down_price
                filled_price = pair.up_price
                remaining = pair.qty - pair.down_filled

            if price <= 0:
                continue

            # Cost check: filled_side_price + current_price <= pair_cost_max
            if filled_price + price > cfg.pair_cost_max:
                logger.info(
                    "  [re-pair] %s cost %.4f + %.4f = %.4f > %.2f → skip",
                    pending_side, filled_price, price,
                    filled_price + price, cfg.pair_cost_max,
                )
                continue

            # Room check
            exposure = exposure_up if pending_side == "Up" else exposure_down
            room = cfg.max_per_side - exposure
            if room < cfg.min_order_size:
                logger.info(
                    "  [re-pair] %s at limit %d/%d → skip",
                    pending_side, exposure, cfg.max_per_side,
                )
                continue

            # Min gap
            too_close = any(
                abs(po.price - price) < cfg.min_price_gap
                for po in ws.pending_orders.values()
                if po.side == pending_side and po.cancelled_at == 0
            )
            if too_close:
                logger.info(
                    "  [re-pair] %s price %.4f too close to pending (min_gap=%.4f) → skip",
                    pending_side, price, cfg.min_price_gap,
                )
                continue

            qty = min(remaining, PAIR_SIZE, room)
            if qty >= cfg.min_order_size:
                logger.info(
                    "  [re-pair] %s %d @ %.4f (pair=%s, remaining=%d)",
                    pending_side, qty, price, pair.pair_id, remaining,
                )
                return [Decision(side=pending_side, amount=qty, price=price, pair_id=pair.pair_id)]

            logger.info(
                "  [re-pair] %s qty %d < min_order_size %d → skip",
                pending_side, qty, cfg.min_order_size,
            )
            return None  # one re-pair per tick max

        # ── Phase B: New pair order for remaining unpaired lots ──
        if unpaired_up == 0 and unpaired_down == 0:
            return None

        # Determine which side to buy
        if unpaired_up > unpaired_down:
            pair_side = "Down"
            heavy_lots = [l for l in unpaired_up_lots if l.unpaired_qty > 0]
            heavy_label = "Up"
            price_raw = down_price
        elif unpaired_down > unpaired_up:
            pair_side = "Up"
            heavy_lots = [l for l in unpaired_down_lots if l.unpaired_qty > 0]
            heavy_label = "Down"
            price_raw = up_price
        else:
            avg_up = (
                sum(l.price * l.unpaired_qty for l in unpaired_up_lots) / unpaired_up
                if unpaired_up > 0 else 0
            )
            avg_down = (
                sum(l.price * l.unpaired_qty for l in unpaired_down_lots) / unpaired_down
                if unpaired_down > 0 else 0
            )
            if avg_up >= avg_down:
                pair_side = "Down"
                heavy_lots = [l for l in unpaired_up_lots if l.unpaired_qty > 0]
                heavy_label = "Up"
                price_raw = down_price
            else:
                pair_side = "Up"
                heavy_lots = [l for l in unpaired_down_lots if l.unpaired_qty > 0]
                heavy_label = "Down"
                price_raw = up_price

        heavy_qty = sum(l.unpaired_qty for l in heavy_lots)
        if heavy_qty <= 0:
            return None
        avg_heavy = sum(l.price * l.unpaired_qty for l in heavy_lots) / heavy_qty

        price = price_raw
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

        # Cost check
        if avg_heavy + price > cfg.pair_cost_max:
            logger.info(
                "  [pair_order] %s cost %.4f + %.4f = %.4f > %.2f → skip",
                pair_side, avg_heavy, price, avg_heavy + price, cfg.pair_cost_max,
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
        qty = min(PAIR_SIZE, room)
        if qty >= cfg.min_order_size:
            logger.info(
                "  [pair_order] %s %d @ %.4f (heavy=%s avg=%.4f need=%d)",
                pair_side, qty, price, heavy_label, avg_heavy, heavy_qty,
            )

            # Create Pair object for fill tracking
            pair = Pair(
                pair_id=f"pair_{ws.window_num}_{len(ws.pairs)}",
                up_price=up_price if pair_side == "Up" else avg_heavy,
                down_price=down_price if pair_side == "Down" else avg_heavy,
                qty=PAIR_SIZE,
            )
            ws.pairs.append(pair)

            return [Decision(side=pair_side, amount=qty, price=price, pair_id=pair.pair_id)]

        logger.info(
            "  [pair_order] %s qty %d < min_order_size %d → skip",
            pair_side, qty, cfg.min_order_size,
        )
        return None

    # ──────────────────────────────────────────────
    # Step 3: Normal independent logic
    # ──────────────────────────────────────────────

    def _step3_normal(
        self, ws: WindowState,
        up_price: float, down_price: float,
        unpaired_up: int, unpaired_down: int,
        unpaired_up_lots: list, unpaired_down_lots: list,
        inv_up: int, inv_down: int,
        exposure_up: int, exposure_down: int,
    ) -> list[Decision]:
        """Normal independent Up/Down orders, with guards.

        Only runs when step 2 did NOT place a pair order.
        Room check: reserve space for future pairing before normal logic.
        """
        decisions: list[Decision] = []
        cfg = self.cfg

        # ── Room check: reserve space for future pairing ──
        needed_pairs = max(
            len([l for l in unpaired_up_lots if l.unpaired_qty > 0]),
            len([l for l in unpaired_down_lots if l.unpaired_qty > 0]),
        )
        if needed_pairs > 0:
            up_needed = exposure_up + needed_pairs * PAIR_SIZE
            down_needed = exposure_down + needed_pairs * PAIR_SIZE
            if up_needed > cfg.max_per_side or down_needed > cfg.max_per_side:
                logger.info(
                    "  [step3] Room check: need %d pairs → Up=%d/%d Down=%d/%d  skip tick",
                    needed_pairs, up_needed, cfg.max_per_side,
                    down_needed, cfg.max_per_side,
                )
                return decisions

        # ── Atomic pre-check: either side too close → skip both ──
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
                    "  [step3] Atomic skip: %s price %.4f too close to pending "
                    "(min_gap=%.4f) → skip both sides",
                    side, price, cfg.min_price_gap,
                )
                return decisions

        # ── Imbalance: tier 1 (filled) ──
        inv_imbalance = abs(inv_up - inv_down)
        inv_heavy = "Up" if inv_up > inv_down else "Down"

        # ── Imbalance: tier 2 (filled + pending) ──
        exp_imbalance = abs(exposure_up - exposure_down)
        exp_heavy = "Up" if exposure_up > exposure_down else "Down"

        for side, price, exposure in [
            ("Up", up_price, exposure_up),
            ("Down", down_price, exposure_down),
        ]:
            if price <= 0:
                continue

            # Room check
            room = cfg.max_per_side - exposure
            if room <= 0:
                logger.info("  [step3] %s %d/%d at limit → skip", side, exposure, cfg.max_per_side)
                continue

            # Imbalance guard tier 1 (filled)
            if inv_imbalance >= cfg.max_imbalance and side == inv_heavy:
                logger.info(
                    "  [step3] %s skip: filled imbalance %d >= %d (inv U=%d D=%d)",
                    side, inv_imbalance, cfg.max_imbalance, inv_up, inv_down,
                )
                continue

            # Imbalance guard tier 2 (filled + pending)
            if exp_imbalance >= cfg.max_imbalance and side == exp_heavy:
                logger.info(
                    "  [step3] %s skip: exposure imbalance %d >= %d (expo U=%d D=%d)",
                    side, exp_imbalance, cfg.max_imbalance, exposure_up, exposure_down,
                )
                continue

            # Per-side min_gap
            too_close = any(
                abs(po.price - price) < cfg.min_price_gap
                for po in ws.pending_orders.values()
                if po.side == side and po.cancelled_at == 0
            )
            if too_close:
                logger.info(
                    "  [step3] %s skip: price %.4f too close to pending (min_gap=%.4f)",
                    side, price, cfg.min_price_gap,
                )
                continue

            qty = min(PAIR_SIZE, room)
            if qty >= cfg.min_order_size:
                decisions.append(Decision(side=side, amount=qty, price=price))

        return decisions
