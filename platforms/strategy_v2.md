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

Add to `PendingOrder`:
- `pair_id: str = ""` — links order to a `Pair` for fill tracking

Add to `WindowState`:
- `pairs: list[Pair]` — all active pairs for this window
- `accumulate: int = 0` — cumulative step3 order qty, shared constraint across both sides

### Room Constraint

Step 3 uses a **shared** room constraint across Up and Down:

```
shared_room = max_per_side - accumulate
```

Both sides count against one pool. Example (max_per_side=20):

| accumulate | shared_room | can place 5+5? |
|------------|-------------|----------------|
| 0          | 20          | yes            |
| 5          | 15          | yes            |
| 15         | 5           | yes            |
| 20         | 0           | no             |

Step 1 and Step 2 **do not** use this constraint — they operate on `exposure` only.

## Config Changes

| Variable | Value | Notes |
|----------|-------|-------|
| `POLY_PAIR_COST_MAX` | 1.0 | Pair cost cap (break-even) |
| `POLY_MAX_PER_SIDE` | 20 | Shared room bound via `accumulate` |
| `POLY_MIN_PRICE_GAP` | 0.02 | Atomic pre-check for step 3; step 2 min_gap |
| `POLY_MIN_ORDER_SIZE` | 5 | Fixed pair unit — always placed as 5+5 when active |
| `POLY_MAX_IMBALANCE` | 10 | Exp + inv imbalance guards in step 3 |

Removed: `POLY_PER_TICK` — replaced by `MIN_ORDER_SIZE` (default 5).
Removed: `POLY_PAIR_FALLTHROUGH_THRESHOLD` — normal logic always runs after pairing.
Removed: `POLY_REPAIR_COST_MAX` — unified under `POLY_PAIR_COST_MAX`.

## Per-Tick Flow

```
decide():

1. Fuse single-leg Pairs
   └─ Scan ws.pairs for fresh (0-fill) Pairs with only one pending side
   └─ Match Up-leg Pairs ↔ Down-leg Pairs
   └─ Merge: transfer the missing order_id into the survivor Pair,
      update PendingOrder.pair_id, remove the dissolved Pair
   └─ Release dissolved Pair's qty from accumulate (frees room for step3)

2. Pair order to re-balance inventory
   └─ Check inventory imbalance (ws.unpaired_up vs ws.unpaired_down)
   └─ Heavy side avg cost + current price <= pair_cost_max → proceed
   └─ Create Pair, prefill heavy side from existing inventory
   └─ Guards: min_gap
   └─ No room check (step1/2 don't use accumulate)

3. New pair placement (the only source of new positions)
   └─ Always places Up+Down together: 0 or 2 decisions
   └─ Shared room check: max_per_side - accumulate >= min_order_size
   └─ Atomic pre-check: Up or Down price too close to pending → skip both
   └─ Imbalance guard — exposure gap too large → skip
   └─ Pair cost cap: up_price + down_price > pair_cost_max → skip
   └─ Engine processes results:
        ├─ Both sides placed → two-leg Pair kept, accumulate += min_order_size
        ├─ One side placed  → single-leg Pair kept, accumulate += min_order_size
        └─ Neither placed   → Pair dissolved, accumulate unchanged
        └─ Single-leg Pairs cleaned up by cancel logic (> 180s → cancel + free accumulate)

Guard matrix:
                 Step 2 pair   Step 3 normal
  shared_room     N/A           ✓ (max_per_side - accumulate)
  atomic pre-check N/A          ✓
  exp imbalance   N/A           ✓
  inv imbalance   N/A           ✓
  pair cost cap   1.0          1.0
  min_gap          ✓            covered by atomic pre-check
  min_order_size   ✓            ✓
```

## Cancel-Replace

| Variable | Value | Notes |
|----------|-------|-------|
| `POLY_CANCEL_MIN_AGE` | 120s | Cancel threshold — single-leg: time only; two-leg: time or price |
| `POLY_CANCEL_REPLACE_THRESHOLD` | 0.10 | Absolute price deviation for early cancel of two-leg Pairs |
| `POLY_CANCEL_MAX_AGE` | 600s | Hard force-cancel safety net |

### Rules

- Cancel is Pair-based: only two-leg Pairs with both sides placed are eligible
- Two-leg Pair: age >= `cancel_min_age` (120s) OR `abs(price - current_price)` >= `cancel_replace_threshold` (0.10) → cancel
- Single-leg Pairs are NOT cancelled by this logic — they are handled by fusion instead
- Accumulate freed when BOTH sides of a fresh Pair are done (cancelled)
- Skip cancel if `order.remaining < MIN_ORDER_SIZE` (can't re-place)
- Skip cancel if `order.filled > 0` (partial fill — let it ride)
- Cancel does NOT mutate Lot records (Lots are pure cost records)

- **Step 2 is recovery**: Re-balances inventory with a pair order when sides are unequal.
- **Step 3 is the source**: Always places Up+Down together (0 or 2 decisions). Creates new positions. Room is shared via `accumulate`.
- **`accumulate` is sticky**: Once incremented, never released within the window — except when both sides of a fresh Pair are done (cancelled or never placed). Single-leg frees immediately; two-leg frees on the second cancel.
- **Pair created at engine level**: When both sides place, engine creates the Pair and links order IDs through `PendingOrder.pair_id`.
- **Partial fill**: Orders with `filled > 0` are excluded from cancel-replace. Let them ride until fill or window end.
- **Single-leg cancel**: Step3 Pairs with only one pending side and no fills on either side are cancelled after `cancel_unpaired_max_age` (180s). On cancel, `accumulate` is reduced by `qty`, freeing room for new step3 placements.

## Pending: Kill-Switch

Kill-switch discussion deferred. Need to decide:
- What metrics trigger kill (pair cost? unpaired lots?)
- Per-window vs per-session kill
- Relationship with stop-on-window-loss
