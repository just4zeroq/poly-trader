# Pairing Strategy — Design & Invariants

## Core Concept

A **Pair** = 1 Up contract + 1 Down contract, equal quantity, placed together.
The unit of profit: settlement pays $1 for the winner, so a complete pair locks in `$1 - (up_price + down_price)`.

**The source of a pair is the act of placing both sides intentionally.** The `Pair` object is a tracker for that dual-side order — its lifecycle is tied to the orders, not to the resulting inventory.

---

## Three Placement Outcomes

| Outcome | Up | Down | Pair Status |
|---------|----|------|-------------|
| **1** | Placed, fully filled | Placed, fully filled | Complete — no action needed |
| **2** | Placed, unfilled/partial | Placed, unfilled/partial | Pair tracks both sides; fills resolve naturally |
| **3** | Placed (any state) | ❌ Rejected | **Pair dissolution trigger** |

### Scenario 3 — Pair Missing

One side placed successfully, the other rejected at submission. The successful side's order is orphaned — its intended pair never materialized.

**Rule**: If a pair order is cancelled before receiving any fills, dissolve the Pair and free the other side's lots.

---

## Cancel Rules

### Rule 1: Unfilled cancel → dissolve pair

When cancelling an order that belongs to a Pair and `filled == 0`:

1. Clear the cancelled order's ID from the Pair
2. Clear `pair_id` from all lots belonging to the other side
3. Remove the Pair

The freed lots re-enter the free-pairing pool (`_free_pair`), where they can be matched by cost with any available counterpart.

### Rule 2: No cancel of partial fills

Orders with `filled > 0` (partial fills) are excluded from cancel-replace logic. A working order that's already getting fills should be left alone — the filled portion is already in inventory, and the pending portion will fill when price permits.

### Rule 3: Both sides unfilled → dissolve pair

If both sides of a Pair are cancelled before either fills, the Pair is removed. Orders expire at window end anyway.

---

## Pair Lifecycle

```
Pair created (strategy._step2_pair_order Phase B)
  │
  ├── Heavy side pre-filled (existing inventory)
  │
  ├── Light side placed successfully
  │     ├── Fills → Pair complete, both sides matched
  │     └── Pending → pending_side = light side, re-pair fills gap
  │
  └── Light side rejected (Scenario 3)
        └── Cancel unfilled → dissolve Pair, free heavy lots back to pool
```

### Phase B Pre-fill

When Phase B creates a pair to match existing inventory (e.g., buying Down to pair against existing Up lots), the heavy side is **pre-filled** on the Pair object:

- `pair.up_filled = qty` (when buying Down)
- `pair.down_filled = qty` (when buying Up)

The heavy lots' `paired_qty` and `pair_id` are updated immediately. This prevents `pending_side` from falsely reporting the heavy side as missing after the light side fills, which would otherwise trigger an unnecessary re-pair that opens **new** net position instead of pairing existing inventory.

---

## Free Pairing (Fallback)

`_free_pair` matches unpaired lots greedily by lowest price, subject to `pair_cost_max`. It operates on any lot with `unpaired_qty > 0`, regardless of `pair_id`. This is the safety net:

- Lots freed by pair dissolution re-enter here
- Independent fills (Step 3 non-pair orders) enter here
- Partial fills from dissolved pairs enter here

---

## Invariants

1. **A Pair only exists while at least one side has an active order or fills.** If the cancelled side never filled → dissolve.
2. **Partial fills are never cancelled preemptively.** They ride until fill or window end.
3. **No lot is held hostage by a dead Pair.** When a pair dissolves, all affected lots' `pair_id` is cleared.
4. **Free pairing by cost, not by origin.** Any unpaired lot can match with any counterpart — price efficiency is the only criterion.
