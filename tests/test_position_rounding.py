"""Position size parsing must round, not truncate, near-integer sizes.

Polymarket reports maker-filled positions as e.g. Decimal('4.9992') for a
5-contract fill (fee/accounting artifact).  int() truncation turns that into
4, so the reconcile poll never sees the hedge complete and the strategy freezes
on a phantom pending order.
"""

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from poly_trader.platforms.config import Config
from poly_trader.platforms.poly_client import SdkClient


class _Page:
    def __init__(self, items):
        self.items = items


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def __aiter__(self):
        self._it = iter(self._pages)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


def _pos(token_id, size):
    return SimpleNamespace(token_id=token_id, size=Decimal(str(size)))


def _sdk_with_positions(rows):
    sdk = SdkClient(Config())
    sdk._secure = SimpleNamespace(
        list_positions=lambda market: _Paginator([_Page(rows)])
    )
    return sdk


def test_get_positions_rounds_near_integer_size():
    # 5-contract fill reports as 4.9992 — must count as 5, not truncate to 4.
    sdk = _sdk_with_positions([_pos("up", "5"), _pos("down", "4.9992")])
    result = asyncio.run(sdk.get_positions("cid", "up", "down"))
    assert result == {"Up": 5, "Down": 5}


def test_get_positions_keeps_integer_sizes():
    sdk = _sdk_with_positions([_pos("up", "3"), _pos("down", "0")])
    result = asyncio.run(sdk.get_positions("cid", "up", "down"))
    assert result == {"Up": 3, "Down": 0}
