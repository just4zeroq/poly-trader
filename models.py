"""
Data models for the trading system.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


# ── Market Spec ──


@dataclass
class MarketSpec:
    """Describes a Polymarket market to discover and trade."""
    symbol: str          # "BTC", "SOL", "ETH"
    duration_min: int    # 5 or 15
    slug_pattern: str    # "btc-updown-5m"

    def __str__(self) -> str:
        return f"{self.symbol}-{self.duration_min}m"


# ── Market Model ──


def _parse_duration_from_slug(slug: str) -> int:
    """Extract window duration in seconds from slug like 'btc-updown-5m-1234567890'."""
    for part in slug.split("-"):
        if part.endswith("m") and part[:-1].isdigit():
            return int(part[:-1]) * 60
    return 900  # default


@dataclass
class MarketInfo:
    """Represents a Polymarket binary market (Up / Down)."""
    slug: str
    condition_id: str
    tokens: dict[str, dict]  # {"Up": {"token_id": str, "outcome": "Up"}, "Down": ...}
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    minimum_tick_size: float = 0.01  # minimum price increment

    @property
    def up_token_id(self) -> str:
        return self.tokens["Up"]["token_id"]

    @property
    def down_token_id(self) -> str:
        return self.tokens["Down"]["token_id"]

    @property
    def window_start(self) -> int:
        """Extract window start timestamp from slug."""
        try:
            return int(self.slug.split("-")[-1])
        except (ValueError, IndexError):
            return 0

    @property
    def window_duration(self) -> int:
        """Window duration in seconds, parsed from slug."""
        return _parse_duration_from_slug(self.slug)

    @property
    def window_end(self) -> int:
        return self.window_start + self.window_duration


# ── Order Book ──


@dataclass
class PriceLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    """Cached L2 order book for one token."""
    token_id: str
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    updated_at: float = 0.0

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        return (b + a) / 2 if b and a else None

    @property
    def spread(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        return a - b if b and a else None


# ── Orders & Fills ──


@dataclass
class Lot:
    """A single fill record — the fundamental unit of cost tracking and pairing."""
    lot_id: str
    side: str         # "Up" / "Down"
    amount: int       # original fill size
    price: float      # fill price
    paired_qty: int = 0
    created_at: float = 0.0

    @property
    def unpaired_qty(self) -> int:
        return self.amount - self.paired_qty

    @property
    def is_fully_paired(self) -> bool:
        return self.paired_qty >= self.amount


@dataclass
class Decision:
    """Output of strategy.decide() — one order instruction per tick per role."""
    side: str            # "Up" / "Down"
    amount: int          # order size
    price: float         # maker limit price (role-specific aggressiveness applied)
    role: str            # "cheap" / "pairing"
    lot_id: str | None = None  # pairing role: the lot being paired; cheap role: None
    cancel_order_id: str | None = None  # pairer: cancel this cheap order before placing


@dataclass
class PendingOrder:
    """An order placed but not yet fully filled."""
    order_id: str
    token_id: str
    side: str          # "Up" or "Down"
    buy_sell: str      # "BUY" or "SELL"
    price: float
    amount: int
    filled: int = 0
    placed_at: float = 0.0
    cancelled_at: float = 0.0  # soft-delete timestamp; > 0 means cancel requested
    pairing_lot_id: str | None = None  # pairing role: lot being paired; cheap: None

    @property
    def remaining(self) -> int:
        return self.amount - self.filled


@dataclass
class FillData:
    """A fill notification from WebSocket or poll."""
    order_id: str
    token_id: str
    outcome: str       # "Up" or "Down"
    side: str          # "BUY" or "SELL"
    price: float
    size: int
    remaining: int = 0


# ── Window State ──


@dataclass
class WindowState:
    """Mutable state for the current trading window."""
    slug: str
    start_time: float = 0.0
    window_num: int = 0
    inventory: dict = field(default_factory=lambda: {"Up": 0, "Down": 0})
    cost: dict = field(default_factory=lambda: {"Up": 0.0, "Down": 0.0})
    total_spent: float = 0.0
    trades: int = 0
    pending_orders: dict[str, PendingOrder] = field(default_factory=dict)
    lots: list = field(default_factory=list)  # list[Lot] — per-fill records for pairing

    @property
    def avg_cost_up(self) -> float:
        return self.cost["Up"] / self.inventory["Up"] if self.inventory["Up"] else 0.0

    @property
    def avg_cost_down(self) -> float:
        return self.cost["Down"] / self.inventory["Down"] if self.inventory["Down"] else 0.0

    @property
    def pair_cost(self) -> float:
        return self.avg_cost_up + self.avg_cost_down

    @property
    def guaranteed_pairs(self) -> int:
        """Number of completed (Up, Down) pairs — the real unit of profit."""
        return min(self.inventory["Up"], self.inventory["Down"])

    @property
    def imbalance(self) -> int:
        """Unpaired exposure: |N_up − N_down|."""
        return abs(self.inventory["Up"] - self.inventory["Down"])

    @property
    def guaranteed_pnl(self) -> float:
        """PnL from completed pairs only: paired contracts' settlement value minus their cost.

        Uses lot-level paired_qty tracking so unpaired contracts' costs are
        excluded — total_spent includes the full inventory (both paired and
        unpaired), which would unfairly make guaranteed_pnl always negative
        whenever there is any imbalance.
        """
        paired_cost = sum(l.price * l.paired_qty for l in self.lots)
        return self.guaranteed_pairs - paired_cost

    @property
    def total_contracts(self) -> int:
        return self.inventory["Up"] + self.inventory["Down"]

    def is_full(self, max_per_side: int) -> bool:
        return (self.inventory["Up"] >= max_per_side
                and self.inventory["Down"] >= max_per_side)

    def report(self, winner: Optional[str] = None) -> dict:
        payout = (self.inventory[winner] if winner else 0)
        pnl = payout - self.total_spent if winner else None
        return {
            "inv_up": self.inventory["Up"],
            "inv_down": self.inventory["Down"],
            "avg_up": round(self.avg_cost_up, 4),
            "avg_down": round(self.avg_cost_down, 4),
            "pairs": self.guaranteed_pairs,
            "imbalance": self.imbalance,
            "guaranteed_pnl": round(self.guaranteed_pnl, 2),
            "pair_cost": round(self.pair_cost, 4),
            "total_spent": round(self.total_spent, 2),
            "pnl": round(pnl, 2) if pnl is not None else None,
            "trades": self.trades,
            "winner": winner,
        }


# ── Engine Events ──

# Event types used as keys for on()/emit():
#   "order_placed"    → OrderPlaced
#   "order_filled"    → OrderFilled
#   "order_cancelled" → OrderCancelled
#   "order_failed"    → OrderFailed
#   "tick"            → TickEvent
#   "window_start"    → WindowStart
#   "window_end"      → WindowEnd


@dataclass
class OrderPlaced:
    window_num: int
    outcome: str           # "Up" / "Down"
    side: str              # "BUY"
    price: float
    amount: int
    order_id: str = ""


@dataclass
class OrderFilled:
    window_num: int
    outcome: str
    price: float
    amount: int
    order_id: str = ""
    total_inv_up: int = 0
    total_inv_down: int = 0


@dataclass
class OrderCancelled:
    window_num: int
    outcome: str
    amount: int
    price: float
    order_id: str = ""


@dataclass
class OrderFailed:
    window_num: int
    outcome: str
    price: float
    amount: int
    reason: str = ""


@dataclass
class TickEvent:
    window_num: int
    slug: str               # market slug for multi-market disambiguation
    elapsed: int            # seconds into window
    up_price: float
    down_price: float
    price_sum: float
    up_buy: int
    down_buy: int
    roles: str = ""         # e.g. "pairing/cheap" or "cheap" or "idle"


@dataclass
class WindowStart:
    window_num: int
    slug: str
    up_token_id: str
    down_token_id: str


@dataclass
class WindowEnd:
    window_num: int
    slug: str
    report: dict            # from WindowState.report()
    cum_pnl: float


