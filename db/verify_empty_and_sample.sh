#!/bin/bash
# Run create_empty_db and create_sample_db against mirrulations_test and verify.
# Use this before committing to ensure the scripts complete the task correctly.
# Usage: ./db/verify_empty_and_sample.sh

set -e

DB_NAME="mirrulations_test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Verification: empty DB and sample DB scripts ==="
echo "Using database: $DB_NAME"
echo ""

# Clean up test DB if it exists from a previous run
dropdb --if-exists "$DB_NAME" 2>/dev/null || true

echo "--- 1. Create empty DB (schema only) ---"
OVERWRITE_YES=1 DB_NAME="$DB_NAME" "$SCRIPT_DIR/create_empty_db.sh"

# Verify empty: tables exist, no rows in dockets/documents
DOCKETS=$(psql -d "$DB_NAME" -tAc "SELECT count(*) FROM dockets;")
DOCS=$(psql -d "$DB_NAME" -tAc "SELECT count(*) FROM documents;")
if [ "$DOCKETS" != "0" ] || [ "$DOCS" != "0" ]; then
    echo "Error: Empty DB should have 0 rows in dockets and documents (got dockets=$DOCKETS, documents=$DOCS)."
    dropdb --if-exists "$DB_NAME" 2>/dev/null || true
    exit 1
fi
echo "OK: Empty DB has 0 rows in dockets and documents."
echo ""

echo "--- 2. Drop test DB and create sample DB ---"
dropdb --if-exists "$DB_NAME" 2>/dev/null || true
OVERWRITE_YES=1 DB_NAME="$DB_NAME" "$SCRIPT_DIR/create_sample_db.sh"

# Verify sample: dockets and documents have expected minimum rows
DOCKETS=$(psql -d "$DB_NAME" -tAc "SELECT count(*) FROM dockets;")
DOCS=$(psql -d "$DB_NAME" -tAc "SELECT count(*) FROM documents;")
if [ "$DOCKETS" -lt 1 ] || [ "$DOCS" -lt 1 ]; then
    echo "Error: Sample DB should have at least 1 docket and 1 document (got dockets=$DOCKETS, documents=$DOCS)."
    dropdb --if-exists "$DB_NAME" 2>/dev/null || true
    exit 1
fi
echo "OK: Sample DB has $DOCKETS dockets and $DOCS documents."
echo ""

# Teardown
dropdb --if-exists "$DB_NAME" 2>/dev/null || true
echo "=== All checks passed. ==="
