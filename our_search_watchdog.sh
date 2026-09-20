#!/data/data/com.termux/files/usr/bin/bash

PROJECT_ROOT="$HOME/our_search"
START_SCRIPT="$HOME/.termux/boot/our_search_start.sh"
LOG_FILE="$PROJECT_ROOT/watchdog.log"

mkdir -p "$PROJECT_ROOT"

echo "[$(date)] Watchdog check started" >> "$LOG_FILE"

if ! curl -fsS --max-time 5 http://127.0.0.1:8080/health >/dev/null 2>&1; then
    echo "[$(date)] Gateway unavailable. Starting OUR SEARCH stack." >> "$LOG_FILE"

    if [ -x "$START_SCRIPT" ]; then
        "$START_SCRIPT" >> "$LOG_FILE" 2>&1
    else
        echo "[$(date)] ERROR: startup script missing or not executable." >> "$LOG_FILE"
    fi
else
    echo "[$(date)] Gateway healthy." >> "$LOG_FILE"
fi
