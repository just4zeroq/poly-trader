"""Configuration — all parameters loaded from environment / .env with sensible defaults."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .models import MarketSpec

# Load .env beside this file (also tries CWD and parents)
_env_path = Path(__file__).parent / ".env"
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
    per_tick: int = field(default_factory=lambda: _env_int("per_tick", 5))
    max_per_side: int = field(default_factory=lambda: _env_int("max_per_side", 500))
    aggressiveness: float = field(default_factory=lambda: _env_float("aggressiveness", 0.3))
    pairing_aggressiveness: float = field(default_factory=lambda: _env_float("pairing_aggressiveness", 0.5))
    """Aggressiveness for the pairing role — higher to increase fill probability."""
    maker_fee: float = field(default_factory=lambda: _env_float("maker_fee", 0.0))
    cancel_replace_threshold: float = field(default_factory=lambda: _env_float("cancel_replace_threshold", 0.10))
    max_pair_sum: float = field(default_factory=lambda: _env_float("max_pair_sum", 1.0))

    # ── Markets ──
    market_specs: list[MarketSpec] = field(default_factory=list)

    # ── Window ──
    settle_buffer: int = field(default_factory=lambda: _env_int("settle_buffer", 5))

    # ── Risk ──
    min_order_size: int = field(default_factory=lambda: _env_int("min_order_size", 5))
    max_imbalance: int = field(default_factory=lambda: _env_int("max_imbalance", 10))
    """Hard cap on |N_up − N_down| — unpaired exposure limit."""
    min_edge: float = field(default_factory=lambda: _env_float("min_edge", 0.05))
    """Skip cheap-seeker buy when |Up − Down| price difference is below this threshold."""
    max_price_dev: float = field(default_factory=lambda: _env_float("max_price_dev", 0.20))
    max_spread: float = field(default_factory=lambda: _env_float("max_spread", 0.05))
    max_extreme_price: float = field(default_factory=lambda: _env_float("max_extreme_price", 0.90))
    """Skip tick if either side's best_bid exceeds this (market is already settled)."""
    max_pair_cost: float = field(default_factory=lambda: _env_float("max_pair_cost", 0.9999))
    min_pair_cost_fills: int = field(default_factory=lambda: _env_int("min_pair_cost_fills", 2))
    kill_pnl_per_pair: float = field(default_factory=lambda: _env_float("kill_pnl_per_pair", 0.03))
    """Stop adding when guaranteed_pnl < -pairs × this (imbalance damage threshold)."""
    max_drawdown: float = field(default_factory=lambda: _env_float("max_drawdown", -10.0))
    stop_on_window_loss: bool = field(default_factory=lambda: _env_bool("stop_on_window_loss", True))

    # ── Cancel-replace ──
    cancel_min_age: float = field(default_factory=lambda: _env_float("cancel_min_age", 30.0))
    """Minimum seconds a pending order must live before cancel-replace considers it."""
    min_remaining_time: float = field(default_factory=lambda: _env_float("min_remaining_time", 300.0))
    """Stop placing new orders when fewer than this many seconds remain in the window."""

    # ── Connection ──
    ws_reconnect_delay: float = field(default_factory=lambda: _env_float("ws_reconnect_delay", 3.0))
    min_tick_interval: float = field(default_factory=lambda: _env_float("min_tick_interval", 1.0))

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.private_key)
