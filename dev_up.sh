#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Install Postgres if missing (Mac/Homebrew)
if ! command -v psql &>/dev/null && command -v brew &>/dev/null; then
    echo "Installing PostgreSQL via Homebrew..."
    brew install postgresql
    brew services start postgresql
fi

# Setup Postgres DB if not already initialized
brew services start postgresql 2>/dev/null || true
if ! psql -lqt postgres 2>/dev/null | grep -qw mirrulations; then
    ./db/setup_postgres.sh
fi

# Build the React frontend
(cd frontend && npm install && npm run build)

# Load .env variables
[[ -f .env ]] && source .env

# Generate JWT_SECRET if not set
if [[ -z "${JWT_SECRET:-}" ]]; then
    JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    echo "JWT_SECRET=$JWT_SECRET" >> .env
    echo "Generated JWT_SECRET and saved to .env"
fi

# Stop existing Gunicorn if running (so we can start fresh)
if [[ -f gunicorn.pid ]]; then
    sudo kill -TERM "$(cat gunicorn.pid)" 2>/dev/null || true
    rm -f gunicorn.pid
fi

# Start the gunicorn server on port 80 using the configuration in conf/gunicorn.py
export PYTHONPATH="$PWD/src"
sudo OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES \
  PYTHONPATH="$PYTHONPATH" \
  USE_POSTGRES="$USE_POSTGRES" \
  DB_HOST="$DB_HOST" \
  DB_PORT="$DB_PORT" \
  DB_NAME="$DB_NAME" \
  DB_USER="$DB_USER" \
  DB_PASSWORD="$DB_PASSWORD" \
  BASE_URL="$BASE_URL" \
  GOOGLE_CLIENT_ID="$GOOGLE_CLIENT_ID" \
  GOOGLE_CLIENT_SECRET="$GOOGLE_CLIENT_SECRET" \
  JWT_SECRET="$JWT_SECRET" \
  .venv/bin/gunicorn -c conf/gunicorn.py mirrsearch.app:app
echo "Mirrulations search has been started"
