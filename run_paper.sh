#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"

echo "═══ Polymarket Temporal Arbitrage — Paper Test ═══"
echo ""

exec python3 -m poly_trader paper --market btc-updown-15m "$@"
