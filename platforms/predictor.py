"""Poly_predict P_fair model port + Binance feed for the live engine.

Self-contained copy of the calibrated model from poly_predict/config.py
(a=-0.0312, b15=-0.3313, b1h=-0.1268, k=11.1227, beta=1, gamma=1, sigma refs).
Keep coefficients in sync with poly_predict/config.py when re-calibrated.
"""
from __future__ import annotations

import json
import logging
import math
import time
from typing import Optional

import httpx
import websockets

from .config import Config

logger = logging.getLogger(__name__)

BINANCE_API = "https://api.binance.com/api/v3"


class Predictor:
    """Computes P_fair from cached BTC price + window features. Pure math, no I/O."""

    # Calibrated coefficients (poly_predict/config.py, 2026-08-04)
    A = -0.0312
    B15 = -0.3313
    B1H = -0.1268
    K_COEFF = 11.1227
    T_REF_S = 300.0
    T_EXP = 1.0
    SIGMA_REF = 0.20
    SIGMA_FLOOR = 0.05
    SIGMA_EXP = 1.0

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.btc_price: Optional[float] = None
        self.btc_ts: float = 0.0
        self.window: Optional[dict] = None  # {"epoch", "open", "prior15", "prior1h", "sigma5"}

    def set_window(self, epoch: int, open_: float, prior15: float,
                   prior1h: float, sigma5: float) -> None:
        self.window = {"epoch": epoch, "open": open_, "prior15": prior15,
                       "prior1h": prior1h, "sigma5": sigma5}

    def set_btc(self, price: float) -> None:
        self.btc_price = price
        self.btc_ts = time.time()

    @property
    def btc_fresh(self) -> bool:
        return (self.btc_price is not None
                and time.time() - self.btc_ts <= self.cfg.pred_btc_max_age)

    def fair_prob(self, t_rem: float) -> Optional[float]:
        """P(close>open) at remaining seconds t_rem, or None if data missing."""
        if self.window is None or not self.btc_fresh:
            return None
        w = self.window
        move = (self.btc_price / w["open"] - 1.0) * 100.0
        k_eff = (self.K_COEFF
                 * (self.T_REF_S / max(t_rem, 1.0)) ** self.T_EXP
                 * (self.SIGMA_REF / max(w["sigma5"], self.SIGMA_FLOOR)) ** self.SIGMA_EXP)
        z = (self.A + self.B15 * w["prior15"] + self.B1H * w["prior1h"]
             + k_eff * move)
        return 1.0 / (1.0 + math.exp(-max(min(z, 40.0), -40.0)))

    def favorite(self, t_rem: float) -> Optional[tuple[str, float]]:
        """(side, P_fair) for the model favorite if confident enough, else None."""
        pf = self.fair_prob(t_rem)
        if pf is None or abs(pf - 0.5) < self.cfg.pred_conf_threshold:
            return None
        return ("Up" if pf >= 0.5 else "Down", pf)


class BinanceFeed:
    """Async Binance REST feed: background BTC ticker + per-window features."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def ticker_price(self) -> float:
        c = await self._get()
        r = await c.get(f"{BINANCE_API}/ticker/price?symbol=BTCUSDT")
        r.raise_for_status()
        return float(r.json()["price"])

    async def stream_btc_price(self):
        """Yield the latest BTC price via Binance public WS (miniTicker, ~1s push).

        Reconnects are the caller's job: a disconnect surfaces as an exception
        out of this generator, which the engine background task catches.
        """
        url = "wss://stream.binance.com:9443/ws/btcusdt@miniTicker"
        async with websockets.connect(url) as ws:
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    yield float(data["c"])  # close/last price
                except Exception:
                    continue  # malformed frame — keep the stream alive

    async def window_features(self, epoch: int) -> Optional[tuple]:
        """(open, prior15, prior1h, sigma5) — 口径同 poly_predict fetch_window_start."""
        c = await self._get()
        r = await c.get(f"{BINANCE_API}/klines?symbol=BTCUSDT&interval=5m"
                        f"&startTime={epoch * 1000 - 3600000}&limit=16")
        r.raise_for_status()
        d = {int(cd[0]): (float(cd[1]), float(cd[4])) for cd in r.json()}
        if epoch * 1000 not in d:
            return None
        open_ = d[epoch * 1000][0]
        if (epoch * 1000 - 900000) not in d or (epoch * 1000 - 3600000) not in d:
            return None
        prior15 = (open_ / d[epoch * 1000 - 900000][1] - 1.0) * 100.0
        prior1h = (open_ / d[epoch * 1000 - 3600000][1] - 1.0) * 100.0
        # σ5 needs the 12 pre-window 5m open prices (T−60m..T−5m). Sparse
        # candles → None (safety over silence): a bogus sigma would distort k_eff.
        prices5 = []
        for t in range(300000, 3600001, 300000):
            if (epoch * 1000 - t) not in d:
                return None
            prices5.append(d[epoch * 1000 - t][0])
        if len(prices5) < 2:
            return None
        rets = [math.log(prices5[i] / prices5[i - 1]) for i in range(1, len(prices5))]
        mean = sum(rets) / len(rets)
        sigma5 = (sum((x - mean) ** 2 for x in rets) / len(rets)) ** 0.5 * 100.0
        return open_, prior15, prior1h, sigma5
