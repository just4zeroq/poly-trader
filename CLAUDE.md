# Poly Trader — Polymarket Temporal Arbitrage Bot

## Project Overview

WebSocket-based market making bot trading Polymarket BTC 15m binary options (Up/Down). Strategy: buy both sides equally at maker prices each tick, profit when average pair cost < $1.0.

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | CLI entry: `info`, `paper`, `run`, `check` |
| `config.py` | All config from `.env` (`POLY_*` / `POLYMARKET_*`) |
| `client.py` | SDK wrapper — market discovery, WS subscribe, order placement |
| `engine.py` | Core engine — WS lifecycle, tick loop, settlement |
| `executors.py` | `PaperExecutor` (simulated fills) / `LiveExecutor` (real orders + user WS fills) |
| `strategy.py` | Pure allocation logic — equal sizing per tick |
| `models.py` | Data models — `MarketInfo`, `WindowState`, `OrderBookSnapshot`, `PendingOrder` |

## CLI Commands

```bash
python -m poly_trader info                               # query current market
python -m poly_trader paper --market btc-updown-15m       # paper test
python -m poly_trader run --market btc-updown-15m         # live trading
python -m poly_trader check                               # verify credentials
python -m poly_trader.check_balance                       # check USDC balance
```

## Architecture

- **Multi-market capable**: One loop per `MarketSpec`, sharing WS connection + PriceCache
- **Event-driven**: Typed dataclass events (`OrderFilled`, `TickEvent`, `WindowEnd`, etc.)
- **Pluggable executors**: `PaperExecutor` (time-based probabilistic fills) vs `LiveExecutor` (real Polymarket orders)
- **WS subscriptions**: Public market data (BestBidAsk, trades) + optional authenticated user channel (fills)

## Key Parameters (`.env`)

- `POLY_PER_TICK` — contracts per tick per side (default: 5)
- `POLY_MAX_PER_SIDE` — max position per side (default: 500)
- `POLY_AGGRESSIVENESS` — 0-1, maker price = bid + spread × aggressiveness (default: 0.3)
- `POLY_MAX_PAIR_COST` — stop adding if avg pair cost > this (default: 0.9999)
- `POLY_MAX_DRAWDOWN` — session PnL stop (default: -10.0)
- `POLY_STOP_ON_WINDOW_LOSS` — skip next window after loss (default: true)
- `POLY_CANCEL_MIN_AGE` — minimum seconds before a pending order can be cancelled (default: 30.0)
- `POLY_MIN_REMAINING_TIME` — stop new orders when < this many seconds left in window (default: 300.0)

## Live vs Paper Mode

Paper mode uses probabilistic fills based on order age + random chance, triggered by WS trade events (`on_trade`) and a periodic fallback (`try_fill_pending`). Live mode places real `post_only` limit orders via the Polymarket SDK and tracks fills via the authenticated user WS channel (`UserTradeEvent` → `handle_user_event`).

## Credential Resolution

Config reads `POLY_*` first, falls back to `POLYMARKET_*` aliases for credentials:
- `POLY_PRIVATE_KEY` ← `POLYMARKET_PK`
- `POLY_WALLET_ADDRESS` ← `POLYMARKET_FUNDER`
- `POLY_API_KEY` ← `POLYMARKET_API_KEY`
- etc.

## Scripts

```bash
./run_paper.sh    # paper test launcher
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
