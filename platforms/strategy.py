"""
Pair-first maker strategy for V3.

Each tick:
  1. Fuse single-leg Pairs into two-leg Pairs
  2. If inventory imbalance, place a pair order to re-balance
  3. Place new Up+Down pair (always 0 or 2 decisions, engine creates Pair)
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
        # Step 1: Fuse single-leg Pairs into two-leg Pairs
        # ════════════════════════════════════════════
        self._fuse_pairs(ws)

        # ════════════════════════════════════════════
        # Step 2: Pair order to cover inventory imbalance
        # ════════════════════════════════════════════
        placed_pair = self._step2_pair_order(ws, up_price, down_price)
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
    # Step 1: Fuse single-leg Pairs
    # ──────────────────────────────────────────────

    def _fuse_pairs(self, ws: WindowState):
        """Fuse single-leg Pairs into two-leg Pairs — maximize matching.

        All single-leg Pairs participate regardless of fill status:
          - Fresh (0 fill, 1 pending)
          - Partial fill (e.g. up_filled=3, down_filled=0)
          - Fully filled one side (e.g. up_filled=5, down_filled=0)

        Fusion condition: up_price + down_price <= pair_cost_max (1.0)
        Must be one Up-leg + one Down-leg.  Maximizes matches by pairing
        expensive Up-legs with cheap Down-legs (greedy).

        Transfers fills, pending order, and price from dissolved Pair
        to survivor.  Releases dissolved Pair's qty from accumulate.
        """
        cfg = self.cfg

        up_legs: list[Pair] = []    # has active Up, missing Down
        down_legs: list[Pair] = []  # has active Down, missing Up

        for pair in list(ws.pairs):
            has_up = (
                pair.up_filled > 0
                or (pair.up_order_id
                    and pair.up_order_id in ws.pending_orders
                    and ws.pending_orders[pair.up_order_id].cancelled_at == 0)
            )
            has_down = (
                pair.down_filled > 0
                or (pair.down_order_id
                    and pair.down_order_id in ws.pending_orders
                    and ws.pending_orders[pair.down_order_id].cancelled_at == 0)
            )

            if has_up and not has_down:
                up_legs.append(pair)
            elif has_down and not has_up:
                down_legs.append(pair)
            # else: both legs or neither → skip

        if not up_legs or not down_legs:
            return

        # Maximize matches: pair expensive Up with cheap Down
        up_legs.sort(key=lambda p: p.up_price, reverse=True)  # most expensive first
        down_legs.sort(key=lambda p: p.down_price)  # cheapest first

        used_down: set[int] = set()
        matched = 0

        for up_pair in up_legs:
            for di, down_pair in enumerate(down_legs):
                if di in used_down:
                    continue
                if up_pair.up_price + down_pair.down_price > cfg.pair_cost_max:
                    continue

                # ── Match: dissolve down_pair into up_pair ──
                down_oid = down_pair.down_order_id
                up_pair.down_order_id = down_oid
                up_pair.down_filled += down_pair.down_filled
                up_pair.down_price = down_pair.down_price

                # Relink pending order to survivor
                if down_oid and down_oid in ws.pending_orders:
                    ws.pending_orders[down_oid].pair_id = up_pair.pair_id

                # Release dissolved Pair's accumulate
                ws.accumulate = max(0, ws.accumulate - down_pair.qty)

                ws.pairs.remove(down_pair)
                used_down.add(di)
                matched += 1

                logger.info(
                    "  [fuse] %s + %s → %s  cost=%.4f  "
                    "fills=(U=%d/D=%d) accumulate=%d",
                    up_pair.pair_id, down_pair.pair_id, up_pair.pair_id,
                    up_pair.up_price + down_pair.down_price,
                    up_pair.up_filled, up_pair.down_filled,
                    ws.accumulate,
                )
                break  # up_pair matched, try next

        if matched:
            logger.info("  [fuse] Matched %d pairs  accumulate=%d", matched, ws.accumulate)

    # ──────────────────────────────────────────────
    # Step 2: Pair order to re-balance inventory
    # ──────────────────────────────────────────────

    def _step2_pair_order(
        self, ws: WindowState,
        up_price: float, down_price: float,
    ) -> list[Decision] | None:
        """Place a pair order to re-balance inventory.

        Uses inventory-level imbalance (inv_up - paired_up vs inv_down - paired_down)
        instead of lot-level tracking.  Cost check uses heavy side avg cost.

        Returns a decision list or None to fall through to step 3.
        """
        cfg = self.cfg

        unpaired_up = ws.unpaired_up
        unpaired_down = ws.unpaired_down

        if unpaired_up == 0 and unpaired_down == 0:
            return None

        # Determine which side to buy
        if unpaired_up > unpaired_down:
            pair_side = "Down"
            price = down_price
            heavy_avg = ws.avg_cost_up
            prefill_side = "Up"
        elif unpaired_down > unpaired_up:
            pair_side = "Up"
            price = up_price
            heavy_avg = ws.avg_cost_down
            prefill_side = "Down"
        else:
            # Balanced — nothing to do
            return None

        if price <= 0:
            return None

        # Cost check: heavy side avg + current price <= pair_cost_max
        if heavy_avg + price > cfg.pair_cost_max:
            logger.info(
                "  [pair_order] Cost too high: %s avg=%.4f + %s price=%.4f > %.2f → skip",
                prefill_side, heavy_avg, pair_side, price, cfg.pair_cost_max,
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

        # Place order: prefill heavy side with unpaired excess
        qty = cfg.min_order_size
        excess = abs(unpaired_up - unpaired_down)
        prefill_qty = min(excess, qty)

        logger.info(
            "  [pair_order] %s %d @ %.4f  (heavy=%s unpaired=%d avg=%.4f  prefill=%d)",
            pair_side, qty, price, prefill_side,
            excess, heavy_avg, prefill_qty,
        )

        # Create Pair, prefill heavy side from existing inventory
        pair = Pair(
            pair_id=f"pair_{ws.window_num}_{len(ws.pairs)}",
            up_price=up_price if pair_side == "Up" else price,
            down_price=down_price if pair_side == "Down" else price,
            qty=qty,
        )
        if prefill_side == "Up":
            pair.up_filled = prefill_qty
        else:
            pair.down_filled = prefill_qty
        ws.pairs.append(pair)

        return [Decision(side=pair_side, amount=qty, price=price, pair_id=pair.pair_id)]

    # ──────────────────────────────────────────────
    # Step 3: Normal logic — per-side independent orders
    # ──────────────────────────────────────────────

    def _step3_normal(
        self, ws: WindowState,
        up_price: float, down_price: float,
        exposure_up: int, exposure_down: int,
    ) -> list[Decision]:
        """Place a new Up+Down pair (always 0 or 2 decisions).

        Step 3 is the only source of new positions.  Always places Up+Down
        together as a unit.  The engine creates the Pair object when both
        sides are successfully placed.

        Room is shared via ws.accumulate (one pool for both sides).  Guards
        block the entire tick (not per-side):

          - Shared room: max_per_side - accumulate < min_order_size → skip
          - Atomic pre-check: Up or Down price too close to pending → skip both
          - Imbalance: exposure or filled inventory gap too large → skip both
          - Pair cost cap: up_price + down_price > pair_cost_max → skip
        """
        decisions: list[Decision] = []
        cfg = self.cfg

        # ── Shared room check: max_per_side - accumulate (both sides share one pool) ──
        shared_room = cfg.max_per_side - ws.accumulate
        if shared_room < cfg.min_order_size:
            logger.info(
                "  [step3] accumulate=%d >= %d → skip",
                ws.accumulate, cfg.max_per_side,
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

        # ── Imbalance guards: block the whole tick (not per-side) ──
        exp_imbalance = abs(exposure_up - exposure_down)
        if exp_imbalance >= cfg.max_imbalance:
            logger.info(
                "  [step3] Exp imbalance %d >= %d (U=%d D=%d) → skip",
                exp_imbalance, cfg.max_imbalance,
                exposure_up, exposure_down,
            )
            return decisions

        inv_up = ws.inventory["Up"]
        inv_down = ws.inventory["Down"]
        inv_imbalance = abs(inv_up - inv_down)
        if inv_imbalance >= cfg.max_imbalance:
            logger.info(
                "  [step3] Inv imbalance %d >= %d (inv U=%d D=%d  exp U=%d D=%d) → skip",
                inv_imbalance, cfg.max_imbalance,
                inv_up, inv_down, exposure_up, exposure_down,
            )
            return decisions

        # ── Pair cost cap ──
        if up_price + down_price > cfg.pair_cost_max:
            logger.info(
                "  [step3] Pair cost %.4f > %.2f → skip (would lock loss)",
                up_price + down_price, cfg.pair_cost_max,
            )
            return decisions

        # ── Both sides pass all guards → place 5+5 ──
        qty = cfg.min_order_size
        decisions.append(Decision(side="Up", amount=qty, price=up_price))
        decisions.append(Decision(side="Down", amount=qty, price=down_price))
        logger.info("  [step3] Up=%d@%.4f  Down=%d@%.4f", qty, up_price, qty, down_price)
        return decisions
