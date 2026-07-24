#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"

echo "═══ Polymarket Temporal Arbitrage — Live Trading ═══"
echo ""

# Check credentials first
python3 -m poly_trader check

echo ""
echo "Starting live trading in 5s (Ctrl+C to abort)..."
sleep 5

exec python3 -m poly_trader run --market btc-updown-15m "$@"
