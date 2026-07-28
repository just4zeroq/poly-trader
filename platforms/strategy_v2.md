# Strategy V2 — Pair-First Market Making

## Core Concept

Pair is a first-class entity. Each tick flow: (1) free-pair existing unpaired lots, (2) place new pair orders to cover remaining imbalance, (3) normal independent market making.

**1 pair = 5 contracts Up + 5 contracts Down** — matches `MIN_ORDER_SIZE=5`.

## Data Model

```python
class Pair:
    pair_id: str
    qty: int = 5                  # Both sides same qty
    up_order_id: str | None
    down_order_id: str | None
    up_price: float               # Locked at creation
    down_price: float             # Locked at creation
    up_filled: int = 0
    down_filled: int = 0

    @property
    def cost(self) -> float:
        return self.up_price + self.down_price

    @property
    def is_complete(self) -> bool:
        return self.up_filled >= self.qty and self.down_filled >= self.qty

    @property
    def pending_side(self) -> str | None:
        """Return the side that still needs filling, if any."""
        if self.up_filled < self.qty and self.down_filled >= self.qty:
            return "Up"
        if self.down_filled < self.qty and self.up_filled >= self.qty:
            return "Down"
        return None
```

Add to `WindowState`:
- `pairs: list[Pair]` — all active pairs for this window
- Each `Lot` gets a `pair_id` field linking it back to its pair

## Config Changes

| Variable | Value | Notes |
|----------|-------|-------|
| `POLY_PAIR_COST_MAX` | 1.0 | Pair cost cap (break-even) |
| `POLY_MAX_PER_SIDE` | 20 | Unchanged — max exposure per side |
| `POLY_MIN_PRICE_GAP` | 0.02 | Unchanged — atomic pre-check + step 2 min_gap |
| `POLY_MIN_ORDER_SIZE` | 5 | Unchanged — default order qty per side |
| `POLY_MAX_IMBALANCE` | 10 | Both exp + inv imbalance guards in step 3 |

Removed: `POLY_PER_TICK` — replaced by `MIN_ORDER_SIZE` (default 5).
Removed: `POLY_PAIR_FALLTHROUGH_THRESHOLD` — normal logic always runs after pairing.
Removed: `POLY_REPAIR_COST_MAX` — unified under `POLY_PAIR_COST_MAX`.

## Per-Tick Flow

```
decide():

1. Free pairing (no orders, just match existing lots)
   └─ Scan unpaired Up lots and unpaired Down lots
   └─ up_price + down_price <= 1.0 → pair them
   └─ Update paired_qty, assign pair_id
   └─ Continue scanning until no more pair-able lots

2. Order to fill remaining imbalance (new pair)
   └─ Priority:
        ├─ 1st: pair the heavy side (more unpaired qty) → reduce directional risk
        └─ 2nd: if equal unpaired qty, pair the higher-priced side → reduce cost risk
   └─ Place the missing side as a new pair order
   └─ Price: current _maker_price
   └─ Cost check: filled_side_price + current_price <= 1.0
   └─ Guard: min_gap, room

3. Normal logic (only if step 2 didn't place a new pair)
   └─ Room check: both sides need min_order_size room (preserve capacity for pairing)
   │    不通 → skip tick
   │
   └─ Atomic pre-check: Up or Down price too close to pending → skip both
   │
   └─ Per-side independent Up/Down orders:
        ├─ room check
        ├─ exp imbalance guard — total exposure too large → block heavy side
        ├─ inv imbalance guard — filled inventory too heavy → block heavy side
        ├─ min_order_size
        └─ place order
   │
   └─ Pair cost cap: both sides placed AND up+down > pair_cost_max → cancel both


Guard matrix:
                 Step 2 pair   Step 3 normal
  room             ✓            ✓
  atomic pre-check N/A          ✓
  exp imbalance   N/A           ✓
  inv imbalance   N/A           ✓
  pair cost cap   1.0          1.0 (both sides only)
  min_gap          ✓            covered by atomic pre-check
  min_order_size   ✓            ✓
```

## Cancel-Replace

| Variable | Value | Notes |
|----------|-------|-------|
| `POLY_CANCEL_REPLACE_THRESHOLD` | 10.0 | Price deviation % to trigger cancel (unchanged) |
| `POLY_CANCEL_MIN_AGE` | 180s | Min age before cancel (unchanged) |

### Rules

- Pair pending and normal pending are treated the same
- Skip cancel if `order.remaining < MIN_ORDER_SIZE` (can't re-place)
- Pair pending cancelled → pair dissolves, filled side lot returns to unpaired pool
- Next tick's free pairing will re-match dissolved pair lots

- **Free pairing is free**: No orders placed, just bookkeeping. Matches lots whose prices sum <= 1.0.
- **Normal logic always runs**: Its own guards (imbalance, min_gap, atomic pre-check) prevent it from making bad situations worse.
- **New pair uses current market price**: Not the locked price from the original lot.
- **Partial fill**: Orders with `filled > 0` are excluded from cancel-replace (Rule 2). Let them ride until fill or window end.

## Pending: Kill-Switch

Kill-switch discussion deferred. Need to decide:
- What metrics trigger kill (pair cost? unpaired lots?)
- Per-window vs per-session kill
- Relationship with stop-on-window-loss
