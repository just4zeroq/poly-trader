import time

from poly_trader.platforms.config import Config
from poly_trader.platforms.predictor import Predictor


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


def test_config_defaults():
    cfg = Config()
    assert cfg.predictive_enabled is False
    assert cfg.pred_conf_threshold == 0.05
    assert cfg.pred_start_elapsed == 60.0
    assert cfg.pred_btc_max_age == 8.0
