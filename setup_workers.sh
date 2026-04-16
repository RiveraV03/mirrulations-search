#!/bin/bash
# Sets up mirrulations-fetch and mirrulations-csv on the EC2 instance.
# Usage: ./setup_workers.sh [--check]
#   --check  Skip cloning if the directory already exists (default behavior).
#            Pass this flag explicitly to make the intent clear in scripts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

FETCH_DIR="$PARENT_DIR/mirrulations-fetch"
CSV_DIR="$PARENT_DIR/mirrulations-csv"

FETCH_URL="https://github.com/mirrulations/mirrulations-fetch.git"
CSV_URL="https://github.com/mirrulations/mirrulations-csv.git"

clone_if_missing() {
    local url="$1"
    local dest="$2"
    local name="$(basename "$dest")"

    if [ -d "$dest" ]; then
        echo "[skip] $name already exists at $dest"
    else
        echo "[clone] Cloning $name into $dest"
        git clone "$url" "$dest"
        echo "[done] $name cloned"
    fi
}

echo "=== mirrulations worker setup ==="
echo "Target directory: $PARENT_DIR"
echo

clone_if_missing "$FETCH_URL" "$FETCH_DIR"
clone_if_missing "$CSV_URL" "$CSV_DIR"

echo
echo "=== Installing dependencies ==="

if [ -f "$FETCH_DIR/requirements.txt" ]; then
    echo "[pip] Installing mirrulations-fetch dependencies"
    pip install -r "$FETCH_DIR/requirements.txt"
fi

if [ -f "$CSV_DIR/requirements.txt" ]; then
    echo "[pip] Installing mirrulations-csv dependencies"
    pip install -r "$CSV_DIR/requirements.txt"
fi

echo
echo "=== Setup complete ==="
echo "  mirrulations-fetch: $FETCH_DIR"
echo "  mirrulations-csv:   $CSV_DIR"
