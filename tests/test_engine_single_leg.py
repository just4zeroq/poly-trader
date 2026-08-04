import asyncio

from poly_trader.platforms.config import Config
from poly_trader.platforms.engine import TradingEngine
from poly_trader.platforms.models import Decision, MarketInfo, WindowState


def make_engine():
    cfg = Config()
    engine = TradingEngine(cfg)
    engine._markets["btc-updown-15m-1700000000"] = MarketInfo(
        slug="btc-updown-15m-1700000000",
        condition_id="cid",
        tokens={"Up": {"token_id": "up", "outcome": "Up"},
                "Down": {"token_id": "down", "outcome": "Down"}},
    )
    return engine


def test_place_step3_single_creates_single_leg_pair():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    d = Decision(side="Up", amount=5, price=0.55)

    async def fake_place_order(slug, token_id, outcome, price, amount, pair_id=""):
        return True
    engine._place_order = fake_place_order

    ok = asyncio.run(engine._place_step3_single(market, ws, d))
    assert ok is True
    assert len(ws.pairs) == 1
    pair = ws.pairs[0]
    assert pair.up_price == 0.55 and pair.down_price == 0.0
    assert pair.qty == 5
    assert d.pair_id == pair.pair_id
    assert ws.accumulate == 5


def test_place_step3_pair_routes_single_leg():
    engine = make_engine()
    market = engine._markets["btc-updown-15m-1700000000"]
    ws = WindowState(slug=market.slug, window_num=1)
    d = Decision(side="Down", amount=5, price=0.43)

    async def fake_place_order(slug, token_id, outcome, price, amount, pair_id=""):
        return True
    engine._place_order = fake_place_order

    ok = asyncio.run(engine._place_step3_pair(market, ws, [d]))
    assert ok is True
    assert len(ws.pairs) == 1
    assert ws.pairs[0].down_price == 0.43 and ws.pairs[0].up_price == 0.0
    assert d.pair_id == ws.pairs[0].pair_id
