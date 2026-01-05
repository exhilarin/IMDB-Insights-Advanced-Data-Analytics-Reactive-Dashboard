#!/usr/bin/env bash
set -euo pipefail

# scripts/stop_all.sh
# Stops background services started by run_all.sh and removes PID files.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Stopping services (if running)..."

stop_pid_file(){
  pidfile="$1"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
      echo "Killing PID $pid from $pidfile"
      kill "$pid" || true
      sleep 1
    else
      echo "No running process for $pidfile"
    fi
    rm -f "$pidfile"
  else
    echo "PID file $pidfile not found"
  fi
}

stop_pid_file "logs/data_processor.pid"
stop_pid_file "logs/react.pid"

echo "Stopped. You can inspect logs/ for output (data_processor.log, react.log)."
