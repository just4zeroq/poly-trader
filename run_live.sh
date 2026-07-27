#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"

PIDFILE="/tmp/poly_trader_live.pid"

# ── Prevent multiple instances ──
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another instance is already running (PID=$OLD_PID)"
        echo "  Kill it first: kill $OLD_PID  or  rm $PIDFILE"
        exit 1
    fi
    # PID file exists but process is dead — clean up
    rm -f "$PIDFILE"
fi
echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT INT TERM

# ── Kill stray poly_trader processes (only run mode, not check) ──
STRAYS=$(pgrep -f "python.*poly_trader run" 2>/dev/null || true)
if [ -n "$STRAYS" ]; then
    echo "Killing stale poly_trader run processes: $STRAYS"
    kill $STRAYS 2>/dev/null || true
    sleep 1
fi

echo "═══ Polymarket Temporal Arbitrage — Live Trading ═══"
echo "  PID: $$"
echo ""

# Check credentials first — fail-fast if invalid
echo "Checking credentials..."
if ! python3 -m poly_trader check; then
    echo "ERROR: Credential check failed — aborting"
    exit 1
fi

echo ""
echo "Starting live trading in 5s (Ctrl+C to abort)..."
sleep 5

python3 -m poly_trader run --market btc-updown-15m "$@"

# Clean up PID file on exit
rm -f "$PIDFILE"
