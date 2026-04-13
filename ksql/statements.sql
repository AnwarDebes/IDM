-- ============================================================
-- kSQL Stream Processing Statements
-- ============================================================

-- Create stream from raw topic
CREATE STREAM IF NOT EXISTS disease_events_stream (
    epi_week INT,
    state VARCHAR,
    loc VARCHAR,
    loc_type VARCHAR,
    disease VARCHAR,
    cases INT,
    incidence_rate DOUBLE
) WITH (
    KAFKA_TOPIC='raw-disease-events',
    VALUE_FORMAT='JSON'
);

-- Real-time validation: filter out records with negative cases or null disease
CREATE STREAM IF NOT EXISTS validated_disease_events AS
    SELECT *
    FROM disease_events_stream
    WHERE cases >= 0 AND disease IS NOT NULL
    EMIT CHANGES;

-- Rolling weekly national aggregation (windowed)
CREATE TABLE IF NOT EXISTS weekly_national_summary AS
    SELECT
        disease,
        epi_week,
        SUM(cases) AS national_cases,
        COUNT(*) AS states_reporting,
        AVG(incidence_rate) AS avg_national_incidence
    FROM validated_disease_events
    GROUP BY disease, epi_week
    EMIT CHANGES;

-- Anomaly detection stream: flag states with high incidence rate
CREATE STREAM IF NOT EXISTS anomaly_alerts AS
    SELECT
        disease,
        state,
        epi_week,
        cases,
        incidence_rate,
        'HIGH_INCIDENCE' AS alert_type
    FROM validated_disease_events
    WHERE incidence_rate > 50.0
    EMIT CHANGES;
