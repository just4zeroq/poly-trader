"""Configuration — all parameters loaded from environment / .env with sensible defaults."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

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


def _env_bool(key: str, default: bool) -> bool:
    val = os.environ.get(f"POLY_{key.upper()}")
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes", "on")


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
    # ── Polymarket API ──
    clob_api_url: str = field(default_factory=lambda: _env("clob_api_url", "https://clob.polymarket.com"))
    ws_market_url: str = field(default_factory=lambda: _env("ws_market_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"))
    ws_user_url: str = field(default_factory=lambda: _env("ws_user_url", "wss://ws-subscriptions-clob.polymarket.com/ws/user"))
    gamma_api_url: str = field(default_factory=lambda: _env("gamma_api_url", "https://gamma-api.polymarket.com"))

    # ── Credentials (from env) ──
    api_key: str = field(default_factory=lambda: _env("api_key"))
    api_secret: str = field(default_factory=lambda: _env("api_secret"))
    api_passphrase: str = field(default_factory=lambda: _env("api_passphrase"))
    private_key: str = field(default_factory=lambda: _env("private_key"))
    wallet_address: str = field(default_factory=lambda: _env("wallet_address"))

    # ── Strategy ──
    pair_cost_max: float = field(default_factory=lambda: _env_float("pair_cost_max", 1.0))
    """Max cost for a paired order (up_price + down_price). New pairs and re-pairs
    skip when cost exceeds this threshold.  1.0 = break-even."""
    pair_cost_mid: float = field(default_factory=lambda: _env_float("pair_cost_mid", 0.97))
    """Tighter cost cap when both prices are in the 0.40–0.60 mid-range,
    where the edge is thinner.  Applies to step3 new pairs only."""
    max_per_side: int = field(default_factory=lambda: _env_int("max_per_side", 20))
    aggressiveness: float = field(default_factory=lambda: _env_float("aggressiveness", 0.3))
    """Aggressiveness 0-1 — higher = more aggressive pricing (closer to best_ask)."""
    min_price_gap: float = field(default_factory=lambda: _env_float("min_price_gap", 0.02))
    """Minimum price gap to place another order on the same side. If the current tick's
    maker price is within this distance of any existing pending order, skip that side.
    Prevents stacking multiple orders at nearly identical prices."""
    max_single_leg_pairs: int = field(default_factory=lambda: _env_int("max_single_leg_pairs", 1))
    """Max single-leg Pairs allowed before step 3 is blocked.
    After step 2 fusion+repair, if remaining single-leg Pairs >= this,
    skip step 3 to avoid accumulating more unpaired positions."""

    # ── Markets ──
    market_slug: str = field(default_factory=lambda: _env("market", "btc-updown-15m"))
    market_specs: list[MarketSpec] = field(default_factory=list)

    # ── Window ──
    settle_buffer: int = field(default_factory=lambda: _env_int("settle_buffer", 5))

    # ── Risk ──
    min_order_size: int = field(default_factory=lambda: _env_int("min_order_size", 5))
    max_price_dev: float = field(default_factory=lambda: _env_float("max_price_dev", 0.20))
    max_extreme_price: float = field(default_factory=lambda: _env_float("max_extreme_price", 0.90))
    """Skip tick if either side's best_bid exceeds this (market is already settled)."""
    max_imbalance: int = field(default_factory=lambda: _env_int("max_imbalance", 10))
    """When the difference between Up and Down inventory exceeds this, stop adding to the heavy side."""
    max_drawdown: float = field(default_factory=lambda: _env_float("max_drawdown", -5.0))
    stop_on_window_loss: bool = field(default_factory=lambda: _env_bool("stop_on_window_loss", True))

    # ── Cancel-replace ──
    cancel_min_age: float = field(default_factory=lambda: _env_float("cancel_min_age", 120.0))
    """Age threshold (seconds) for cancel. Single-leg: cancel + free accumulate.
    Two-leg: cancel by time, or early if price deviates >= cancel_replace_threshold."""
    cancel_replace_threshold: float = field(default_factory=lambda: _env_float("cancel_replace_threshold", 0.10))
    """Absolute price deviation threshold for early cancel of two-leg Pairs.
    E.g. 0.10 means cancel if current price differs from order price by >= 0.10."""
    cancel_max_age: float = field(default_factory=lambda: _env_float("cancel_max_age", 600.0))
    """Maximum seconds a pending order can live — force-cancel regardless of price."""
    min_remaining_time: float = field(default_factory=lambda: _env_float("min_remaining_time", 180.0))
    """Stop placing new orders when fewer than this many seconds remain in the window."""
    max_consecutive_failures: int = field(default_factory=lambda: _env_int("max_consecutive_failures", 15))
    """Stop trying after this many consecutive ticks where all orders are rejected (balance likely depleted)."""

    # ── Connection ──
    ws_reconnect_delay: float = field(default_factory=lambda: _env_float("ws_reconnect_delay", 3.0))
    min_tick_interval: float = field(default_factory=lambda: _env_float("min_tick_interval", 1.0))

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.private_key)
