import asyncio
import time

import pytest

from poly_trader.platforms.config import Config
from poly_trader.platforms.predictor import BinanceFeed, Predictor


def make_predictor(open_=100.0, prior15=0.1, prior1h=0.2, sigma5=0.2):
    cfg = Config()
    p = Predictor(cfg)
    p.set_window(1700000000, open_, prior15, prior1h, sigma5)
    p.set_btc(open_)
    return p


def test_fair_prob_stays_in_unit_interval():
    p = make_predictor()
    for price in (99.0, 99.9, 100.0, 100.1, 101.0):
        p.set_btc(price)
        assert 0.0 < p.fair_prob(t_rem=300.0) < 1.0


def test_higher_btc_price_raises_up_probability():
    p = make_predictor()
    p.set_btc(99.0)
    lo = p.fair_prob(t_rem=300.0)
    p.set_btc(101.0)
    hi = p.fair_prob(t_rem=300.0)
    assert hi > lo


def test_time_scaling_magnifies_late_move():
    p = make_predictor()
    p.set_btc(100.5)
    early = p.fair_prob(t_rem=600.0)
    late = p.fair_prob(t_rem=120.0)
    assert abs(late - 0.5) > abs(early - 0.5)


def test_favorite_gate_confidence_threshold():
    p = make_predictor()
    p.set_btc(100.0)  # P_fair ≈ 0.48 → not confident
    assert p.favorite(t_rem=300.0) is None
    p.set_btc(100.3)  # confident favorite
    fav = p.favorite(t_rem=300.0)
    assert fav is not None and fav[0] in ("Up", "Down")


def test_favorite_requires_fresh_btc_and_window():
    p = make_predictor()
    p.set_btc(100.0)
    p.btc_ts = time.time() - 999.0  # stale
    assert p.fair_prob(300.0) is None
    assert p.favorite(300.0) is None


def test_config_defaults(monkeypatch):
    # Hermetic: clear POLY_POSITIONS_INTERVAL so the default assertion doesn't
    # depend on a live .env overriding it.
    monkeypatch.delenv("POLY_POSITIONS_INTERVAL", raising=False)
    cfg = Config()
    assert cfg.pred_conf_threshold == 0.05
    assert cfg.pred_start_elapsed == 60.0
    assert cfg.pred_btc_max_age == 8.0
    assert cfg.positions_interval == 2.0


def test_fair_prob_matches_calibrated_reference():
    # 钉住校准系数：canonical 配置（open=100, prior15=0.1, prior1h=0.2,
    # sigma5=0.2, btc=open, t_rem=300）→ P_fair ≈ 0.4776。任一系数回归即红。
    p = make_predictor()
    assert p.fair_prob(t_rem=300.0) == pytest.approx(0.4776, abs=1e-4)


def test_fair_prob_requires_window():
    cfg = Config()
    p = Predictor(cfg)
    p.set_btc(100.0)  # 有币价但无窗口特征 → None
    assert p.fair_prob(300.0) is None
    assert p.favorite(300.0) is None


# ── BinanceFeed 解析（mock HTTP，不联网）──


class _FakeResp:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class _FakeClient:
    def __init__(self, data):
        self._data = data

    async def get(self, *args, **kwargs):
        return _FakeResp(self._data)


def _kline(open_time_ms, open_, close):
    # Binance kline: [open_time, open, high, low, close, volume, ...]
    return [open_time_ms, open_, open_ + 1.0, open_ - 1.0, close, 100.0, 0, 0, 0, 0, 0, 0]


def test_window_features_parses_klines(monkeypatch):
    T = 1700000000000  # epoch 毫秒
    base = T - 3600000
    step = 300000  # 5m
    # 16 根 5m K 线覆盖 T−60m..T，open 逐根 +0.1
    candles = [_kline(base + step * i, 100.0 + i * 0.1, 100.0 + i * 0.1 + 0.05)
               for i in range(16)]

    feed = BinanceFeed()

    async def _fake_get():
        return _FakeClient(candles)

    monkeypatch.setattr(feed, "_get", _fake_get)

    feat = asyncio.run(feed.window_features(T // 1000))
    assert feat is not None
    open_, prior15, prior1h, sigma5 = feat
    assert open_ == 101.2  # 窗口起点（open_time == T 的 K 线，i=12）开盘价
    assert prior15 == pytest.approx((101.2 / 100.95 - 1.0) * 100.0)  # 相对 T−15m 收盘（i=9）
    assert prior1h == pytest.approx((101.2 / 100.05 - 1.0) * 100.0)  # 相对 T−1h 收盘（i=0）
    assert sigma5 >= 0.0 and sigma5 < 1.0  # 近线性序列 → 波动率很小但有限
