# Poly Trader — Polymarket Temporal Arbitrage Bot

## Project Overview

WebSocket-based market making bot trading Polymarket BTC 15m binary options (Up/Down). Strategy: independently buy Up and Down at maker prices each tick. Fills are generically paired. Profit when average pair cost < $1.0.

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | CLI entry: `info`, `run`, `check` |
| `config.py` | All config from `.env` (`POLY_*` / `POLYMARKET_*`) |
| `client.py` | SDK wrapper — market discovery, WS subscribe, order placement |
| `engine.py` | Core engine — WS lifecycle, tick loop, settlement |
| `executors.py` | `LiveExecutor` — real Polymarket orders + user WS fill tracking |
| `strategy.py` | Maker strategy — independent Up/Down buys, per-side exposure cap, generic pairing |
| `tail_sweep.py` | Tail-end sweep strategy — last 3 min, buys winner at market + loser with 5¢ profit |
| `models.py` | Data models — `MarketInfo`, `WindowState`, `OrderBookSnapshot`, `PendingOrder` |

## CLI Commands

```bash
python -m poly_trader info                               # query current market
python -m poly_trader run --market btc-updown-15m         # live trading (maker strategy)
python -m poly_trader.tail_sweep --market btc-updown-15m  # standalone tail-end sweep
python -m poly_trader check                               # verify credentials
```

## Architecture

- **Multi-market capable**: One loop per `MarketSpec`, sharing WS connection + PriceCache
- **Event-driven**: Typed dataclass events (`OrderFilled`, `TickEvent`, `WindowEnd`, etc.)
- **Pluggable executors**: `LiveExecutor` — real Polymarket orders via CLOB REST API + WS user channel for fill tracking
- **WS subscriptions**: Public market data (BestBidAsk, trades) + optional authenticated user channel (fills)

## Strategies

### Maker Strategy (`strategy.py`)
Continuous market-making: independently buy Up and Down each tick at maker prices. Fills are generically paired. Profit when average pair cost < $1.0. Supports coupled pricing mode (`POLY_PROFIT_TARGET > 0`) where each new pair costs `1.0 - profit_target`.

```bash
python -m poly_trader run --market btc-updown-15m
```

### Tail-End Sweep (`tail_sweep.py`)
Standalone strategy for the last 3 minutes of a window. When one side's best_bid reaches >= 0.90 (market clearly resolved), it buys the winner at market price and the loser at a price that locks in 5¢ profit per pair. Runs independently, does not interfere with the maker strategy.

```bash
python -m poly_trader.tail_sweep --market btc-updown-15m
```

Options: `--per-tick N` `--max-side N` `-v` (verbose logging)

## Key Parameters (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POLY_PER_TICK` | 5 | Contracts per tick per side |
| `POLY_MAX_PER_SIDE` | 500 | Max exposure per side (filled + pending shares) |
| `POLY_AGGRESSIVENESS` | 0.3 | Maker price = bid + spread × aggressiveness (0-1) |
| `POLY_MAX_PAIR_SUM` | 0.98 | Skip tick if up_price + down_price ≥ this |
| `POLY_MIN_PRICE_GAP` | 0.02 | Min price gap to place another order on same side (avoids stacking) |
| `POLY_CANCEL_MIN_AGE` | 30.0 | Min seconds before pending order can be cancelled |
| `POLY_CANCEL_REPLACE_THRESHOLD` | 0.10 | Fractional price deviation to trigger cancel-replace |
| `POLY_MIN_REMAINING_TIME` | 180.0 | Stop new orders when < this many seconds left in window |
| `POLY_MIN_ORDER_SIZE` | 5 | Minimum order size in contracts |
| `POLY_KILL_PNL_PER_PAIR` | 0.03 | Per-pair loss threshold for kill-switch |
| `POLY_MAX_IMBALANCE` | 100 | Stop adding to heavy side when filled+pending difference exceeds this |
| `POLY_MAX_DRAWDOWN` | -10.0 | Session PnL stop (USDC) |
| `POLY_STOP_ON_WINDOW_LOSS` | true | Skip next window after loss |
| `POLY_MAX_PRICE_DEV` | 0.20 | Max deviation from 1.0 for pair sum validation |
| `POLY_MAX_EXTREME_PRICE` | 0.90 | Skip tick if either side's best_bid exceeds this |
| `POLY_MAX_CONSECUTIVE_FAILURES` | 15 | Stop placing after N consecutive rejected ticks (balance likely depleted) |
| `POLY_MIN_TICK_INTERVAL` | 1.0 | Min seconds between ticks |
| `POLY_WS_RECONNECT_DELAY` | 3.0 | WS reconnect delay on disconnect |

## Live Mode

Orders are placed as `post_only` limit orders via the Polymarket CLOB SDK. Fill tracking uses the authenticated user WebSocket channel (`UserTradeEvent` → `handle_user_event`). Each fill creates a `Lot` and pairs generically with any unpaired opposite-side lot via `_create_lot_and_pair()`.

## Credential Resolution

Config reads `POLY_*` first, falls back to `POLYMARKET_*` aliases for credentials:
- `POLY_PRIVATE_KEY` ← `POLYMARKET_PK`
- `POLY_WALLET_ADDRESS` ← `POLYMARKET_FUNDER`
- `POLY_API_KEY` ← `POLYMARKET_API_KEY`
- etc.

## Scripts

```bash
./run_live.sh     # live trading (with credential + 5s safety delay)
./check.sh        # credential + balance check
```

## CLOB Deposit (USDC.e to Deposit Wallet)

Polymarket CLOB uses **deposit wallet** (proxy contract, deployed per-user by SDK). Key facts:

- **Deposit wallet address**: Shown in logs as `Secure client created (wallet=0x...)` — e.g., `0x6685150fF6DAc46Dfbc0F9ac2F789B307f31C498`
- **CLOB balance = deposit wallet's on-chain USDC.e balance** — no separate `deposit()` call needed
- **To add funds**: Simply transfer USDC.e from EOA to the deposit wallet address via ERC20 `transfer()`
- **USDC.e token**: `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB` (Polygon, bridged USDC)
- **Do NOT call `collateral_adapter.deposit()`** — it reverts for deposit wallets (the adapter requires gasless relay which needs a Builder API key)
- **Native USDC** (`0x3c499c...`) won't work — must swap to USDC.e first via DEX

Quick deposit (replace with actual deposit wallet address):
```python
from web3 import Web3
# ERC20 transfer to deposit_wallet address
token = w3.eth.contract(address=USDC_E, abi=erc20_abi)
tx = token.functions.transfer(deposit_wallet, amount_wei).build_transaction(...)
# sign and send
```
