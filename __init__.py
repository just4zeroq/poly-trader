"""
Polymarket WebSocket-driven Temporal Arbitrage Trading Bot

Message-driven architecture:
  WebSocket events → price/order updates → strategy decisions → order placement
"""

# Re-export key classes for backward compatibility
from .platforms.config import Config
from .platforms.models import (
    MarketSpec,
    MarketInfo,
    OrderBookSnapshot,
    PriceLevel,
    Lot,
    Decision,
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
from .platforms.engine import TradingEngine
from .platforms.executors import LiveExecutor, OrderExecutor
from .platforms.strategy import V4Strategy
