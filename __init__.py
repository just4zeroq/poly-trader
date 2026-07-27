"""
Polymarket WebSocket-driven Temporal Arbitrage Trading Bot

Message-driven architecture:
  WebSocket events → price/order updates → strategy decisions → order placement
"""

# Re-export key classes for backward compatibility
from .platform.config import Config
from .platform.models import (
    MarketSpec,
    MarketInfo,
    OrderBookSnapshot,
    PriceLevel,
    Lot,
    Decision,
    Pair,
    PendingOrder,
    FillData,
    WindowState,
    OrderPlaced,
    OrderFilled,
    OrderCancelled,
    OrderFailed,
    TickEvent,
    WindowStart,
    WindowEnd,
)
from .platform.engine import TradingEngine
from .platform.executors import LiveExecutor, OrderExecutor
from .platform.strategy import MakerStrategy
