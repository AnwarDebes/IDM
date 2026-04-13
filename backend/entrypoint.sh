#!/bin/bash
set -e

echo "=== EpiDW Backend Entrypoint ==="

# Step 1: Wait for databases and check if loading is needed
echo "Checking database readiness and data status..."
python /app/scripts/check_and_load.py

echo ""
echo "=== Starting API Server ==="
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
