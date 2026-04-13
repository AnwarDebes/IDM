#!/bin/bash
set -e

echo "============================================"
echo "  Epidemiological Data Warehouse - Setup"
echo "============================================"
echo ""

echo "[1/7] Starting all services..."
docker compose up -d --build
echo ""

echo "[2/7] Waiting for services to be healthy..."
sleep 30
# Verify health
for i in {1..10}; do
    STATUS=$(curl -sf http://localhost:8000/api/v1/health | python -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "unreachable")
    if [ "$STATUS" = "ok" ]; then
        echo "  All services healthy!"
        break
    fi
    echo "  Waiting... ($STATUS)"
    sleep 10
done
echo ""

echo "[3/7] Generating synthetic data (if needed)..."
if [ ! -f data/raw/tycho_level1.csv ]; then
    python etl/generate_synthetic_data.py
else
    ROWS=$(wc -l < data/raw/tycho_level1.csv)
    echo "  Data file exists: $ROWS lines"
fi
echo ""

echo "[4/7] Loading PostgreSQL..."
python etl/load_postgres.py
echo ""

echo "[5/7] Loading MongoDB..."
python etl/load_mongo.py
echo ""

echo "[6/7] Loading Neo4j..."
python etl/load_neo4j.py
echo ""

echo "[7/7] Applying kSQL statements..."
docker compose exec -T ksqldb-cli ksql http://ksqldb-server:8088 <<'KSQL'
CREATE STREAM IF NOT EXISTS disease_events_stream (
    epi_week INT, state VARCHAR, loc VARCHAR, loc_type VARCHAR,
    disease VARCHAR, cases INT, incidence_rate DOUBLE
) WITH (KAFKA_TOPIC='raw-disease-events', VALUE_FORMAT='JSON');

CREATE STREAM IF NOT EXISTS validated_disease_events AS
    SELECT * FROM disease_events_stream
    WHERE cases >= 0 AND disease IS NOT NULL EMIT CHANGES;

CREATE TABLE IF NOT EXISTS weekly_national_summary
    WITH (KEY_FORMAT='JSON') AS
    SELECT disease, epi_week,
           SUM(cases) AS national_cases,
           COUNT(*) AS states_reporting,
           AVG(incidence_rate) AS avg_national_incidence
    FROM validated_disease_events
    GROUP BY disease, epi_week EMIT CHANGES;

CREATE STREAM IF NOT EXISTS anomaly_alerts AS
    SELECT disease, state, epi_week, cases, incidence_rate,
           'HIGH_INCIDENCE' AS alert_type
    FROM validated_disease_events
    WHERE incidence_rate > 50.0 EMIT CHANGES;
KSQL
echo ""

echo "============================================"
echo "  Setup Complete!"
echo "============================================"
echo ""
echo "  Dashboard:  http://localhost:3000"
echo "  API Docs:   http://localhost:8000/docs"
echo "  Health:     http://localhost:8000/api/v1/health"
echo ""
echo "  To start streaming data:"
echo "    python etl/kafka_producer.py --limit 1000"
echo ""
