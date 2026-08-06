# Poly Trader — Polymarket Temporal Arbitrage Bot

## Project Overview

WebSocket-based market making bot trading Polymarket BTC 15m binary options (Up/Down). V4 predictive strategy: buy the P_fair favorite → hedge the light side on fill → repeat when flat. Reconciliation = WS fills write-through `auth_inv` + 2s CLOB position poll.

## Code Structure

```
poly_trader/
├── platform/                  # Core trading (strategy + engine)
│   ├── main.py                CLI entry: info, run, check
│   ├── config.py              All config from .env (POLY_* / POLYMARKET_*)
│   ├── engine.py              Core engine — WS lifecycle, tick loop, position poll, settlement
│   ├── strategy.py            V4 predictive strategy (favorite → hedge → flat)
│   ├── executors.py           LiveExecutor — real Polymarket orders + user WS fill tracking
│   ├── models.py              Data models — MarketInfo, WindowState, OrderBookSnapshot
├── tools/
│   ├── polymarket/            Polymarket-specific tooling
│   │   ├── client.py          SDK wrapper — market discovery, WS subscribe, order placement
│   │   ├── balance.py         Account balance & portfolio snapshot
│   │   ├── deposit.py         USDC.e deposit to CLOB
│   │   ├── gamma_discovery.py Gamma API market discovery (programmatic)
│   │   └── run_info.py        Market info query tool (Gamma API CLI)
│   └── onchain/               On-chain operations
│       ├── settle_window.py   Auto-settle previous window at new-window start (resolve + redeem)
│       ├── settle.py          Manual position settlement & redemption
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
| `POLY_MAX_PER_SIDE` | 20 (fallback) | Max filled exposure per side; caps hedge size and favorite guard. Set in `.env` (live: 100) |
| `POLY_AGGRESSIVENESS` | 0.3 | Maker price = bid + spread × aggressiveness (0-1) |
| `POLY_PAIR_COST_TARGET_EXTREME` | 0.99 | Hedge cost guard: skip when heavy avg cost + hedge price > this |
| `POLY_MIN_ORDER_SIZE` | 5 | Favorite order size (contracts) |
| `POLY_MIN_REMAINING_TIME` | 180.0 | Stop new orders when < this many seconds left in window |
| `POLY_MAX_EXTREME_PRICE` | 0.90 | Skip tick if either side's best_bid exceeds this |
| `POLY_MAX_CONSECUTIVE_FAILURES` | 15 | Stop placing after N consecutive rejected ticks |
| `POLY_PRED_CONF_THRESHOLD` | 0.05 | Min \|P_fair − 0.5\| to place a favorite |
| `POLY_PRED_START_ELAPSED` | 60.0 | Seconds into window before first favorite order |
| `POLY_PRED_BTC_MAX_AGE` | 8.0 | Skip predictive decisions when cached BTC price older than this |
| `POLY_FAVORITE_STALE_SECONDS` | 120.0 | A pending favorite (`filled < min_order_size − 1`) resting this many seconds AND priced out by > `stale_price_diff` is cancelled (lead leg only — never the hedge leg) so the next tick re-places at the current book price — prevents an unfilled bid from blocking the whole window |
| `POLY_STALE_PRICE_DIFF` | 0.10 | Churn guard for stale-cancel: cancel only when the current maker has moved > this ABOVE the resting limit (a bid the market ran past won't fill). A bid still at/near its limit can still fill — cancelling it would just churn |
| `POLY_HEDGE_PRICE_BOUND` | 0.998 | Bound-hedge price ceiling decided at favorite placement: `max_price = hedge_price_bound − fav_price` |
| `POLY_POSITIONS_INTERVAL` | 2.0 | CLOB position poll interval (0 disables) — refreshes `auth_inv` |
| `POLY_MIN_TICK_INTERVAL` | 0.0 | Min seconds between ticks (0 = no throttle, so the bound hedge fires as soon as its favorite fills ≥ 4) |
| `POLY_WS_RECONNECT_DELAY` | 3.0 | WS reconnect delay on disconnect |
