#!/usr/bin/env bash
set -euo pipefail

# scripts/run_all.sh
# Sets up Python venv (if missing), installs Python deps, runs the data_processor in background
# and starts the React frontend in background. Writes logs and PID files to logs/.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Working from repo root: $ROOT_DIR"

# 1) Ensure logs dir
mkdir -p logs

# 2) Python venv
if [ ! -x "venv/bin/activate" ]; then
  echo "Creating Python virtualenv 'venv'..."
  python3 -m venv venv
fi

echo "Activating venv and installing Python dependencies (requirements.txt)..."
# shellcheck source=/dev/null
source venv/bin/activate
pip install --upgrade pip >/dev/null
pip install -r requirements.txt

# 3) Run data_processor in background
DP_LOG=logs/data_processor.log
DP_PID=logs/data_processor.pid
if [ -f "$DP_PID" ] && kill -0 "$(cat $DP_PID)" >/dev/null 2>&1; then
  echo "data_processor already running (PID $(cat $DP_PID)). Skipping start.";
else
  echo "Starting data_processor (limit=250 threads=24)..."
  ./venv/bin/python data_processor.py --limit 250 --threads 24 --autosave-every 25 > "$DP_LOG" 2>&1 &
  echo $! > "$DP_PID"
  echo "data_processor started, PID=$(cat $DP_PID), logs -> $DP_LOG"
fi

# 4) Start frontend (in background)
FR_LOG=logs/react.log
FR_PID=logs/react.pid
if [ -f "$FR_PID" ] && kill -0 "$(cat $FR_PID)" >/dev/null 2>&1; then
  echo "React dev server already running (PID $(cat $FR_PID)). Skipping start.";
else
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm not found. Please install Node.js/npm to run the frontend. Skipping frontend start.";
  else
    echo "Installing frontend npm deps (this may take a moment)..."
    (cd frontend && npm install --silent)
    echo "Starting React dev server on PORT=3001 (background)..."
    (cd frontend && PORT=3001 npm start > "$ROOT_DIR/$FR_LOG" 2>&1 & echo $! > "$ROOT_DIR/$FR_PID")
    echo "React dev server started, PID=$(cat $FR_PID), logs -> $FR_LOG"
  fi
fi

echo "All requested services have been started (where possible). Use scripts/stop_all.sh to stop them." 
