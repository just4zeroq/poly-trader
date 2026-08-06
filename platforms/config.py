"""Configuration — all parameters loaded from environment / .env with sensible defaults."""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from .models import MarketSpec

# Load .env beside this file (also tries CWD and parents)
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)


# ── typed env helpers ──

def _env(key: str, default: str = "") -> str:
    """Read POLY_{KEY}, fall back to POLYMARKET_{KEY} alias."""
    val = os.environ.get(f"POLY_{key.upper()}")
    if val:
        return val
    alias = _ENV_ALIASES.get(key)
    return os.environ.get(alias, default) if alias else default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(f"POLY_{key.upper()}", str(default)))
    except (ValueError, TypeError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(f"POLY_{key.upper()}", str(default)))
    except (ValueError, TypeError):
        return default


# Maps config field names to alternative POLYMARKET_* env var names.
_ENV_ALIASES: dict[str, str] = {
    "api_key": "POLYMARKET_API_KEY",
    "api_secret": "POLYMARKET_API_SECRET",
    "api_passphrase": "POLYMARKET_PASSPHRASE",
    "private_key": "POLYMARKET_PK",
    "wallet_address": "POLYMARKET_FUNDER",
}


@dataclass
class Config:
    # ── Credentials (from env) ──
    api_key: str = field(default_factory=lambda: _env("api_key"))
    api_secret: str = field(default_factory=lambda: _env("api_secret"))
    api_passphrase: str = field(default_factory=lambda: _env("api_passphrase"))
    private_key: str = field(default_factory=lambda: _env("private_key"))
    wallet_address: str = field(default_factory=lambda: _env("wallet_address"))

    # ── Strategy ──
    pair_cost_target_extreme: float = field(default_factory=lambda: _env_float("pair_cost_target_extreme", 0.99))
    """Hedge cost guard: skip locking in the light side when the heavy leg's
    average cost + the hedge price would exceed this (a fresh pair costs
    ~1.0, so overpaying to close is not worth it)."""
    hedge_price_bound: float = field(default_factory=lambda: _env_float("hedge_price_bound", 0.998))
    """Bound-hedge pair-cost ceiling, decided at favorite placement: the hedge
    is only placed at a price ≤ hedge_price_bound − favorite_price.  Tighter
    than pair_cost_target_extreme (0.99) because it's set up-front, before the
    favorite fills and any drift happens."""
    max_per_side: int = field(default_factory=lambda: _env_int("max_per_side", 20))
    """Max filled exposure per side.  Caps the hedge size and blocks a new
    favorite when the favorite side is already at max."""
    aggressiveness: float = field(default_factory=lambda: _env_float("aggressiveness", 0.3))
    """Aggressiveness 0-1 — higher = more aggressive pricing (closer to best_ask)."""

    # ── Markets ──
    market_slug: str = field(default_factory=lambda: _env("market", "btc-updown-15m"))
    market_specs: list[MarketSpec] = field(default_factory=list)

    # ── Risk ──
    min_order_size: int = field(default_factory=lambda: _env_int("min_order_size", 5))
    max_extreme_price: float = field(default_factory=lambda: _env_float("max_extreme_price", 0.90))
    """Skip tick if either side's best_bid exceeds this (market is already settled)."""

    min_remaining_time: float = field(default_factory=lambda: _env_float("min_remaining_time", 180.0))
    """Stop placing new orders when fewer than this many seconds remain in the window."""
    max_consecutive_failures: int = field(default_factory=lambda: _env_int("max_consecutive_failures", 15))
    """Stop trying after this many consecutive ticks where all orders are rejected (balance likely depleted)."""

    # ── Predictive integration (poly_predict P_fair model) ──
    pred_conf_threshold: float = field(default_factory=lambda: _env_float("pred_conf_threshold", 0.05))
    """Min |P_fair - 0.5| to place a favorite order."""
    pred_start_elapsed: float = field(default_factory=lambda: _env_float("pred_start_elapsed", 60.0))
    """Seconds into the window before the first favorite order."""
    pred_btc_max_age: float = field(default_factory=lambda: _env_float("pred_btc_max_age", 8.0))
    """Skip predictive decisions when the cached BTC price is older than this."""
    favorite_stale_seconds: float = field(default_factory=lambda: _env_float("favorite_stale_seconds", 120.0))
    """A favorite order left unfilled (filled < min_order_size − 1) for this
    many seconds AND priced out by more than stale_price_diff is cancelled and
    re-priced against the fresh book.  Without this a stale low-ball favorite
    blocks the whole window via the pending gate (an order with filled <
    min_order_size − 1 blocks all new orders; one that never fills = an idle
    window)."""
    stale_price_diff: float = field(default_factory=lambda: _env_float("stale_price_diff", 0.10))
    """Churn guard for stale-cancel: a resting favorite is cancelled only when
    the current maker price has moved more than this ABOVE its limit — a bid
    the market ran past will not fill.  A bid still at/near its limit (or
    below) can still fill, so cancelling it would just churn (cancel →
    re-place at nearly the same price)."""

    # ── Position reconciliation ──
    positions_interval: float = field(default_factory=lambda: _env_float("positions_interval", 2.0))
    """Seconds between CLOB position polls that refresh ws.auth_inv. 0 disables."""

    # ── Connection ──
    ws_reconnect_delay: float = field(default_factory=lambda: _env_float("ws_reconnect_delay", 3.0))
    min_tick_interval: float = field(default_factory=lambda: _env_float("min_tick_interval", 0.0))
    """Min seconds between ticks.  0 = no throttle — the tick loop reacts to
    every price event so a bound hedge fires the instant its favorite fills."""

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.private_key)
