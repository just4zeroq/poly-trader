#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"

echo "═══ Polymarket Wallet & Credential Check ═══"
echo ""

python3 -m poly_trader check
echo ""
python3 -m poly_trader.tools.onchain.check_balance
