from poly_trader.platforms.config import Config
from poly_trader.platforms.models import Pair, PendingOrder, WindowState
from poly_trader.platforms.predictive import PredictiveMakerStrategy
from poly_trader.platforms.predictor import Predictor


def make_strategy(btc_price=100.3):
    cfg = Config()
    predictor = Predictor(cfg)
    predictor.set_window(1700000000, 100.0, 0.1, 0.2, 0.2)
    predictor.set_btc(btc_price)
    return PredictiveMakerStrategy(cfg, predictor), cfg


def make_ws(cfg, slug="btc-updown-15m-1700000000"):
    return WindowState(slug=slug, start_time=0.0, window_num=1)


def test_no_favorite_before_pred_start_elapsed():
    strat, cfg = make_strategy(btc_price=100.3)  # confident Up
    ws = make_ws(cfg)
    # remaining 890 → elapsed 10s < 60s
    assert strat.decide(ws, up_price=0.55, down_price=0.45, remaining_time=890.0) == []


def test_favorite_placed_on_confident_side():
    strat, cfg = make_strategy(btc_price=100.3)  # P_fair ≈ 0.76 → Up
    ws = make_ws(cfg)
    decisions = strat.decide(ws, up_price=0.55, down_price=0.45, remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"
    assert decisions[0].amount == cfg.min_order_size
    assert decisions[0].price == 0.55
    assert decisions[0].pair_id == ""


def test_no_favorite_when_not_confident():
    strat, cfg = make_strategy(btc_price=100.0)  # P_fair ≈ 0.48
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.55, down_price=0.45, remaining_time=800.0) == []


def test_repair_pairs_filled_favorite():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    # Favorite already bought & filled → single-leg Up pair missing Down
    ws.pairs.append(Pair(pair_id="pair_1_0", up_price=0.55, down_price=0.0,
                         qty=5, up_filled=5, down_filled=0))
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43, remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].price == 0.43
    assert decisions[0].pair_id == "pair_1_0"


def test_pending_favorite_not_paired_until_filled():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    # Favorite placed but NOT yet filled (pending order only, no fills) →
    # the pairing must wait until the favorite actually fills.
    ws.pairs.append(Pair(pair_id="pair_1_0", up_price=0.55, down_price=0.0,
                         qty=5, up_filled=0, down_filled=0,
                         up_order_id="ord_up_1"))
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5)
    assert strat.decide(ws, up_price=0.55, down_price=0.43, remaining_time=800.0) == []


def test_pending_favorite_paired_when_require_filled_disabled():
    cfg = Config()
    cfg.pred_require_filled = False
    predictor = Predictor(cfg)
    predictor.set_window(1700000000, 100.0, 0.1, 0.2, 0.2)
    predictor.set_btc(100.3)
    strat = PredictiveMakerStrategy(cfg, predictor)
    ws = make_ws(cfg)
    # Favorite placed, NOT yet filled — with the gate off, pair immediately
    ws.pairs.append(Pair(pair_id="pair_1_0", up_price=0.55, down_price=0.0,
                         qty=5, up_filled=0, down_filled=0,
                         up_order_id="ord_up_1"))
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5)
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43, remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].pair_id == "pair_1_0"


def test_partially_filled_favorite_is_paired():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    # Favorite partially filled (3/5) → committed to the side → pair now
    ws.pairs.append(Pair(pair_id="pair_1_0", up_price=0.55, down_price=0.0,
                         qty=5, up_filled=3, down_filled=0,
                         up_order_id="ord_up_1"))
    ws.pending_orders["ord_up_1"] = PendingOrder(
        order_id="ord_up_1", token_id="t_up", side="Up",
        buy_sell="BUY", price=0.55, amount=5, filled=3)
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43, remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Down"
    assert decisions[0].pair_id == "pair_1_0"


def test_cycle_after_pair_completes_places_new_favorite():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    # Completed pair (both legs filled) is not single-leg → new favorite allowed
    ws.pairs.append(Pair(pair_id="pair_1_0", up_price=0.55, down_price=0.43,
                         qty=5, up_filled=5, down_filled=5))
    decisions = strat.decide(ws, up_price=0.55, down_price=0.43, remaining_time=800.0)
    assert len(decisions) == 1
    assert decisions[0].side == "Up"


def test_exposure_guard_blocks_favorite():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    ws.inventory = {"Up": cfg.max_per_side, "Down": 0}
    assert strat.decide(ws, up_price=0.55, down_price=0.45, remaining_time=800.0) == []


def test_stops_near_settlement():
    strat, cfg = make_strategy(btc_price=100.3)
    ws = make_ws(cfg)
    assert strat.decide(ws, up_price=0.55, down_price=0.45, remaining_time=100.0) == []
