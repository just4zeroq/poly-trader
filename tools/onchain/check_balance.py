"""Quick balance check — prints USDC collateral balance."""
import asyncio
from ...platform.config import Config
from ..polymarket.client import SdkClient

async def main():
    cfg = Config()
    sdk = SdkClient(cfg)
    try:
        await sdk.create_secure(cfg)
        if not sdk.is_secure:
            print("Secure client unavailable")
            return
        result = await sdk._secure.get_balance_allowance(asset_type="COLLATERAL")
        bal = int(result.balance) / 1_000_000
        print(f"{cfg.wallet_address} | ${bal:.2f} USDC")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await sdk.close()

asyncio.run(main())
