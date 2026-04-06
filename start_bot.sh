#!/bin/bash
# Mother's Joy Bot — self-healing production watchdog
# Monitors health endpoint, auto-restarts on crash or unresponsive state.
# Designed to run under launchd (macOS) or as a standalone supervisor.

set -uo pipefail

BOT_DIR="/Users/bcode/mothersjoy-bot"
PYTHON="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
LOG_DIR="/Users/bcode/Library/Logs/mothersjoy"
PORT=5055
ACCOUNT="mothersjoyapp"
HEALTH_URL="http://127.0.0.1:${PORT}/api/health"
HEALTH_CHECK_INTERVAL=60
MAX_UNHEALTHY=3
RESTART_DELAY=10
STARTUP_GRACE=15

cd "$BOT_DIR"

# Ensure log directory exists
mkdir -p "$LOG_DIR"
mkdir -p logs

# Validate critical files exist
for f in dashboard.py bot/config.py bot/agents.py bot/scraper.py bot/poster.py bot/db.py; do
    if [ ! -f "$f" ]; then
        echo "$(date): FATAL: missing $f — bot cannot start" >&2
        exit 1
    fi
done

# Validate account .env exists
if [ ! -f "accounts/$ACCOUNT/.env" ]; then
    echo "$(date): FATAL: missing accounts/$ACCOUNT/.env — no credentials" >&2
    exit 1
fi

# Install/update Playwright browsers (prevents "Executable doesn't exist" crashes)
$PYTHON -m playwright install chromium 2>/dev/null || true

# Kill any existing dashboard on this port
lsof -ti:$PORT | xargs kill -9 2>/dev/null || true
sleep 2

cleanup() {
    echo "$(date): Watchdog shutting down..."
    if [ -n "${BOT_PID:-}" ] && kill -0 "$BOT_PID" 2>/dev/null; then
        kill -TERM "$BOT_PID" 2>/dev/null
        # Wait up to 60s for graceful shutdown
        for i in $(seq 1 12); do
            kill -0 "$BOT_PID" 2>/dev/null || break
            sleep 5
        done
        kill -9 "$BOT_PID" 2>/dev/null || true
    fi
    exit 0
}
trap cleanup SIGTERM SIGINT

echo "$(date): Watchdog starting (account: $ACCOUNT, port: $PORT)"

while true; do
    # Start the bot with autopilot
    echo "$(date): Starting Mother's Joy Bot..."
    $PYTHON dashboard.py --account "$ACCOUNT" --port "$PORT" --autopilot \
        >> "$LOG_DIR/bot.log" 2>&1 &
    BOT_PID=$!
    echo "$(date): Bot started (PID: $BOT_PID)"

    # Grace period for startup
    sleep "$STARTUP_GRACE"

    # Health monitoring loop
    unhealthy_count=0
    while kill -0 "$BOT_PID" 2>/dev/null; do
        sleep "$HEALTH_CHECK_INTERVAL"

        # Check if process is still alive
        if ! kill -0 "$BOT_PID" 2>/dev/null; then
            echo "$(date): Bot process died — restarting"
            break
        fi

        # Check health endpoint
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 --max-time 10 "$HEALTH_URL" 2>/dev/null || echo "000")

        if [ "$HTTP_CODE" = "200" ]; then
            if [ "$unhealthy_count" -gt 0 ]; then
                echo "$(date): Bot recovered (was unhealthy for $unhealthy_count checks)"
            fi
            unhealthy_count=0
        else
            unhealthy_count=$((unhealthy_count + 1))
            echo "$(date): Health check failed (HTTP $HTTP_CODE, count: $unhealthy_count/$MAX_UNHEALTHY)"

            if [ "$unhealthy_count" -ge "$MAX_UNHEALTHY" ]; then
                echo "$(date): Bot unresponsive after $MAX_UNHEALTHY checks — killing"
                kill -TERM "$BOT_PID" 2>/dev/null
                sleep 5
                kill -9 "$BOT_PID" 2>/dev/null || true
                break
            fi
        fi
    done

    # Wait for process to fully exit
    wait "$BOT_PID" 2>/dev/null || true

    echo "$(date): Bot stopped — restarting in ${RESTART_DELAY}s"
    sleep "$RESTART_DELAY"
done
