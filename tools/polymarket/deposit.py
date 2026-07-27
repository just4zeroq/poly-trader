"""Deposit USDC.e to Polymarket CLOB.

Modes:
  check              — check balances, allowances, CLOB balance (default)
  approve            — approve the collateral adapter to spend USDC.e (via SDK)
  deposit AMOUNT     — deposit AMOUNT USDC.e to the CLOB (via SDK)
  swap-guide         — show swap instructions

If your wallet has native USDC (0x3c499c...) instead of USDC.e (0xC011a7...),
you need to swap via a DEX first (see `swap-guide`).
"""
import asyncio
import sys
from eth_abi.abi import encode as abi_encode
from web3 import Web3

from polymarket.calls import MAX_UINT256, TransactionCall
from polymarket.types import HexString

from ...platform.config import Config
from .client import SdkClient

# Polygon RPC (read-only checks only — writes go through SDK)
RPC = "https://polygon.drpc.org"

# Polymarket production contract addresses
COLLATERAL_TOKEN = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"     # USDC.e
NATIVE_USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"         # native USDC
COLLATERAL_ADAPTER = "0xAdA100Db00Ca00073811820692005400218FcE1f"

# ERC20 ABI (minimal, read-only)
ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
]

# deposit(uint256) selector = keccak256("deposit(uint256)")[:4]
DEPOSIT_SELECTOR = "0xe2bbb158"


def _fmt_usdc(val: int) -> str:
    return f"{val / 1_000_000:.2f}"


def _fmt_matic(val: int) -> str:
    return f"{val / 10**18:.4f}"


def _make_deposit_call(amount: int) -> TransactionCall:
    """Build a TransactionCall for collateral_adapter.deposit(amount)."""
    # DEPOSIT_SELECTOR already includes 0x, abi_encode returns raw hex
    data = DEPOSIT_SELECTOR + abi_encode(["uint256"], [amount]).hex()
    return TransactionCall(to=COLLATERAL_ADAPTER, data=HexString(data))


async def check_balances(w3: Web3, wallet: str) -> dict:
    """Return wallet token balances and allowances."""
    token = w3.eth.contract(address=Web3.to_checksum_address(COLLATERAL_TOKEN), abi=ERC20_ABI)
    sym = token.functions.symbol().call()
    decimals = token.functions.decimals().call()
    bal = token.functions.balanceOf(Web3.to_checksum_address(wallet)).call()

    native_token = w3.eth.contract(address=Web3.to_checksum_address(NATIVE_USDC), abi=ERC20_ABI)
    native_sym = native_token.functions.symbol().call()
    native_dec = native_token.functions.decimals().call()
    native_bal = native_token.functions.balanceOf(Web3.to_checksum_address(wallet)).call()

    # MATIC balance
    matic_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet))

    # Allowances
    adapter_allowance = token.functions.allowance(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(COLLATERAL_ADAPTER),
    ).call()

    return {
        "usdc_e": {"symbol": sym, "balance": bal, "decimals": decimals},
        "native_usdc": {"symbol": native_sym, "balance": native_bal, "decimals": native_dec},
        "matic": matic_wei,
        "adapter_allowance": adapter_allowance,
    }


async def get_clob_balance(cfg: Config) -> str:
    """Query CLOB USDC balance via SDK."""
    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            return "N/A (secure client unavailable)"
        result = await sdk._secure.get_balance_allowance(asset_type="COLLATERAL")
        bal = int(result.balance) / 1_000_000
        return f"${bal:.2f} USDC"
    except Exception as e:
        return f"Error: {e}"
    finally:
        await sdk.close()


# ── Commands ──

async def cmd_check():
    """Check balances and allowances (default)."""
    cfg = Config()
    if not cfg.private_key:
        print("No private key configured — check .env")
        return

    w3 = Web3(Web3.HTTPProvider(RPC))
    if not w3.is_connected():
        print("Cannot connect to Polygon RPC")
        return

    acct = w3.eth.account.from_key(cfg.private_key)
    wallet = acct.address
    print(f"Wallet:   {wallet}")
    print()

    info = await check_balances(w3, wallet)
    ue = info["usdc_e"]
    nu = info["native_usdc"]

    print(f"{ue['symbol']:<10} {_fmt_usdc(ue['balance']):>10}   (Polymarket collateral)")
    print(f"{nu['symbol']:<10} {_fmt_usdc(nu['balance']):>10}   (native — needs swap)")
    print(f"{'MATIC':<10} {_fmt_matic(info['matic']):>10}   (gas)")
    print()

    # Allowance
    allowance_ok = info["adapter_allowance"] >= ue["balance"]
    print(f"Adapter allowance: {'OK' if allowance_ok else 'NEED APPROVE'}")
    print()

    # CLOB balance
    clob = await get_clob_balance(cfg)
    print(f"CLOB balance: {clob}")
    print()

    # Action summary
    if ue["balance"] > 0 and allowance_ok:
        print(f"You can deposit {_fmt_usdc(ue['balance'])} {ue['symbol']} to the CLOB:")
        print(f"  python -m poly_trader.deposit deposit {_fmt_usdc(ue['balance'])}")
    elif ue["balance"] > 0 and not allowance_ok:
        print("Step 1 (approve adapter):")
        print("  python -m poly_trader.deposit approve")
        print("Step 2 (deposit to CLOB):")
        print(f"  python -m poly_trader.deposit deposit <amount>")
    elif nu["balance"] > 0:
        print(f"You have {_fmt_usdc(nu['balance'])} native USDC but Polymarket uses {ue['symbol']}.")
        print()
        print("To swap native USDC -> USDC.e, use a DEX on Polygon:")
        print("  python -m poly_trader.deposit swap-guide")
    else:
        print("No USDC.e or native USDC found in wallet.")
        print("Send USDC to this wallet on Polygon first.")


async def cmd_approve():
    """Approve the collateral adapter for max USDC.e via SDK."""
    cfg = Config()
    if not cfg.private_key:
        print("No private key configured")
        return

    # Read-only check via web3
    w3 = Web3(Web3.HTTPProvider(RPC))
    if not w3.is_connected():
        print("Cannot connect to Polygon RPC")
        return
    acct = w3.eth.account.from_key(cfg.private_key)
    wallet = acct.address

    token = w3.eth.contract(address=Web3.to_checksum_address(COLLATERAL_TOKEN), abi=ERC20_ABI)
    current = token.functions.allowance(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(COLLATERAL_ADAPTER),
    ).call()
    bal = token.functions.balanceOf(Web3.to_checksum_address(wallet)).call()

    if current >= bal:
        print("Allowance already sufficient.")
        return

    print(f"Approving collateral adapter ({COLLATERAL_ADAPTER[:12]}...) for max USDC.e...")
    print("(sending transaction via SDK...)")

    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("Failed to create secure SDK client")
            return
        handle = await sdk._secure.approve_erc20(
            token_address=COLLATERAL_TOKEN,
            spender_address=COLLATERAL_ADAPTER,
            amount="max",
        )
        outcome = await handle.wait()
        print(f"Approval outcome: {outcome}")
        print("Done. Now deposit:")
        print(f"  python -m poly_trader.deposit deposit <amount>")
    except Exception as e:
        print(f"Approval failed: {e}")
    finally:
        await sdk.close()


async def cmd_deposit(amount_str: str):
    """Deposit USDC.e to the CLOB via SDK."""
    cfg = Config()
    if not cfg.private_key:
        print("No private key configured")
        return

    try:
        amount = float(amount_str)
    except ValueError:
        print(f"Invalid amount: {amount_str}")
        return

    if amount <= 0:
        print("Amount must be positive")
        return

    amount_wei = int(amount * 1_000_000)

    # Read-only checks via web3
    w3 = Web3(Web3.HTTPProvider(RPC))
    if not w3.is_connected():
        print("Cannot connect to Polygon RPC")
        return
    acct = w3.eth.account.from_key(cfg.private_key)
    wallet = acct.address

    token = w3.eth.contract(address=Web3.to_checksum_address(COLLATERAL_TOKEN), abi=ERC20_ABI)
    bal = token.functions.balanceOf(Web3.to_checksum_address(wallet)).call()
    if amount_wei > bal:
        print(f"Insufficient USDC.e: have {_fmt_usdc(bal)}, trying to deposit {amount}")
        return

    current = token.functions.allowance(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(COLLATERAL_ADAPTER),
    ).call()
    if current < amount_wei:
        print("Allowance insufficient. Run approve first:")
        print("  python -m poly_trader.deposit approve")
        return

    matic_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet))
    if matic_wei < 0.005 * 10**18:
        print(f"Low MATIC ({_fmt_matic(matic_wei)}) — need at least ~0.005 for gas")
        return

    print(f"Depositing {amount} USDC.e to Polymarket CLOB...")
    print("(sending transaction via SDK...)")

    deposit_call = _make_deposit_call(amount_wei)
    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("Failed to create secure SDK client")
            return
        handle = await sdk._secure.execute_transaction(
            calls=[deposit_call],
            metadata=f"Deposit {amount} USDC.e to CLOB",
        )
        outcome = await handle.wait()
        print(f"Deposit outcome: {outcome}")

        # Verify
        result = await sdk._secure.get_balance_allowance(asset_type="COLLATERAL")
        new_bal = int(result.balance) / 1_000_000
        print(f"CLOB balance: ${new_bal:.2f} USDC")
    except Exception as e:
        print(f"Deposit failed: {e}")
    finally:
        await sdk.close()


async def cmd_swap_guide():
    """Show instructions for swapping native USDC to USDC.e."""
    print("Native USDC -> USDC.e Swap Guide")
    print("=" * 40)
    print()
    print("Polymarket on Polygon uses USDC.e (bridged USDC, 0xC011a7...)")
    print("Your wallet has native USDC (0x3c499c...).")
    print()
    print("Options to convert:")
    print()
    print("1. Uniswap (recommended)")
    print("   - Go to app.uniswap.org")
    print("   - Connect wallet (Polygon network)")
    print("   - Swap USDC (native) -> USDC.e (bridged)")
    print()
    print("2. Odos")
    print("   - Go to odos.xyz")
    print("   - Same flow: native USDC -> USDC.e")
    print()
    print("3. KyberSwap")
    print("   - Go to kyberswap.com")
    print()
    print("After swapping, run:")
    print("  python -m poly_trader.deposit check")
    print("  python -m poly_trader.deposit approve")
    print("  python -m poly_trader.deposit deposit <amount>")


async def main():
    args = sys.argv[1:] if len(sys.argv) > 1 else ["check"]
    cmd = args[0]

    if cmd == "check":
        await cmd_check()
    elif cmd == "approve":
        await cmd_approve()
    elif cmd == "deposit":
        if len(args) >= 2:
            await cmd_deposit(args[1])
        else:
            print("Missing amount. Example: python -m poly_trader.deposit deposit 10")
    elif cmd == "swap-guide":
        await cmd_swap_guide()
    else:
        print("Usage:")
        print("  python -m poly_trader.deposit                    # check (default)")
        print("  python -m poly_trader.deposit check              # check balances")
        print("  python -m poly_trader.deposit approve            # approve adapter")
        print("  python -m poly_trader.deposit deposit 10         # deposit 10 USDC.e")
        print("  python -m poly_trader.deposit swap-guide         # swap instructions")


if __name__ == "__main__":
    asyncio.run(main())
