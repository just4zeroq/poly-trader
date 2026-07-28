#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"
export PYTHONPATH="${DIR}/..:${PYTHONPATH:-}"

# ── Load .env into shell environment ──
set -a
source "${DIR}/.env" 2>/dev/null || true
set +a

# ── Config ──
PIDFILE="/tmp/poly_trader_live.pid"
LOGDIR="${DIR}/logs"
LOGFILE="${LOGDIR}/live_$(date +%Y%m%d_%H%M%S).log"
MAX_RESTARTS=10          # max auto-restarts per session
RESTART_COOLDOWN=5       # seconds between restarts

# ── Prevent multiple instances ──
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ERROR: Another instance is already running (PID=$OLD_PID)"
        echo "  Kill it first: kill $OLD_PID  or  rm $PIDFILE"
        exit 1
    fi
    rm -f "$PIDFILE"
fi
echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT INT TERM

# ── Validate env ──
MARKET="${POLY_MARKET:-}"
if [ -z "$MARKET" ]; then
    echo "ERROR: POLY_MARKET not set (should be in .env, e.g. btc-updown-15m)"
    exit 1
fi

# ── Logging ──
mkdir -p "$LOGDIR"
exec > >(tee -a "$LOGFILE") 2>&1
echo "Log: $LOGFILE"

# ── Cleanup stale processes ──
STRAYS=$(pgrep -f "python.*poly_trader run" 2>/dev/null || true)
if [ -n "$STRAYS" ]; then
    echo "Killing stale poly_trader run processes: $STRAYS"
    kill $STRAYS 2>/dev/null || true
    sleep 1
fi

echo "═══════════════════════════════════════════"
echo "  Polymarket Temporal Arbitrage — Live"
echo "  Market:  $MARKET"
echo "  PID:     $$"
echo "  Log:     $LOGFILE"
echo "  Started: $(date)"
echo "═══════════════════════════════════════════"
echo ""

# ── Credential check ──
echo "Checking credentials..."
if ! python3 -m poly_trader check; then
    echo "ERROR: Credential check failed — aborting"
    exit 1
fi
echo ""

# ── Run with auto-restart ──
restart_count=0
while true; do
    echo "─── Starting (restart #$restart_count) ─── $(date)"

    python3 -m poly_trader run "$@"

    rc=$?
    echo "─── Exited (rc=$rc) ─── $(date)"

    if [ $rc -eq 0 ] || [ $rc -eq 130 ]; then
        # 0 = clean exit, 130 = Ctrl+C
        echo "Clean shutdown — not restarting"
        break
    fi

    restart_count=$((restart_count + 1))
    if [ $restart_count -gt $MAX_RESTARTS ]; then
        echo "ERROR: $MAX_RESTARTS restarts reached — giving up"
        break
    fi

    echo "Restarting in ${RESTART_COOLDOWN}s (attempt $restart_count/$MAX_RESTARTS)..."
    sleep $RESTART_COOLDOWN
done

# Clean up
rm -f "$PIDFILE"
echo "Done: $(date)"
