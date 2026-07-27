# Poly Trader — Polymarket Temporal Arbitrage Bot

## Project Overview

WebSocket-based market making bot trading Polymarket BTC 15m binary options (Up/Down). Pair-first strategy: free pair existing lots, re-pair incomplete pairs, place new pairs, then normal independent logic.

## Code Structure

```
poly_trader/
├── platform/                  # Core trading (strategy + engine)
│   ├── main.py                CLI entry: info, run, check
│   ├── config.py              All config from .env (POLY_* / POLYMARKET_*)
│   ├── engine.py              Core engine — WS lifecycle, tick loop, settlement
│   ├── strategy.py            Pair-first maker strategy (V3)
│   ├── executors.py           LiveExecutor — real Polymarket orders + user WS fill tracking
│   ├── models.py              Data models — MarketInfo, WindowState, OrderBookSnapshot, Pair
├── tools/
│   ├── polymarket/            Polymarket-specific tooling
│   │   ├── client.py          SDK wrapper — market discovery, WS subscribe, order placement
│   │   ├── balance.py         Account balance & portfolio snapshot
│   │   ├── deposit.py         USDC.e deposit to CLOB
│   │   ├── gamma_discovery.py Gamma API market discovery (programmatic)
│   │   └── run_info.py        Market info query tool (Gamma API CLI)
│   └── onchain/               On-chain operations
│       ├── settle.py          Position settlement & redemption
│       └── check_balance.py   Quick USDC balance check
├── analysis/                  Analysis & backtesting
│   ├── entry_timing.py        Entry timing analysis
│   ├── perf_analysis.py       Performance analysis
│   ├── analyze_all_btc.py     BTC backtest analysis
│   ├── analyze_history.py     Historical analysis
│   └── simulate_tail.py       Tail sweep simulation
├── scripts/
│   ├── check.sh               Credential + balance check
│   └── run_live.sh            Live trading launcher
├── __init__.py                Package init, re-exports key classes
├── __main__.py                python -m poly_trader entry point
├── .env                       Environment configuration
├── .gitignore
└── CLAUDE.md
```

## CLI Commands

```bash
python -m poly_trader info                               # query current market
python -m poly_trader run --market btc-updown-15m         # live trading (maker strategy)
python -m poly_trader check                               # verify credentials
```

## Key Parameters (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POLY_PAIR_COST_MAX` | 1.0 | Max cost for paired order (up_price + down_price) |
| `POLY_MAX_PER_SIDE` | 20 | Max exposure per side (filled + pending shares) |
| `POLY_AGGRESSIVENESS` | 0.3 | Maker price = bid + spread × aggressiveness (0-1) |
| `POLY_MIN_PRICE_GAP` | 0.02 | Min price gap to place another order on same side |
| `POLY_CANCEL_MIN_AGE` | 120.0 | Min seconds before pending order can be cancelled |
| `POLY_CANCEL_REPLACE_THRESHOLD` | 10.0 | Fractional price deviation to trigger cancel-replace |
| `POLY_MIN_REMAINING_TIME` | 180.0 | Stop new orders when < this many seconds left in window |
| `POLY_MIN_ORDER_SIZE` | 5 | Minimum order size in contracts |
| `POLY_MAX_IMBALANCE` | 10 | Stop adding to heavy side when filled+pending difference exceeds this |
| `POLY_MAX_DRAWDOWN` | -5.0 | Session PnL stop (USDC) |
| `POLY_STOP_ON_WINDOW_LOSS` | true | Skip next window after loss |
| `POLY_MAX_PRICE_DEV` | 0.20 | Max deviation from 1.0 for pair sum validation |
| `POLY_MAX_EXTREME_PRICE` | 0.90 | Skip tick if either side's best_bid exceeds this |
| `POLY_MAX_CONSECUTIVE_FAILURES` | 15 | Stop placing after N consecutive rejected ticks |
| `POLY_MIN_TICK_INTERVAL` | 1.0 | Min seconds between ticks |
| `POLY_WS_RECONNECT_DELAY` | 3.0 | WS reconnect delay on disconnect |
