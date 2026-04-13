-- ============================================================
-- Epidemiological Data Warehouse — PostgreSQL Star Schema DDL
-- ============================================================

-- DIMENSION: Time
CREATE TABLE dim_time (
    time_key        SERIAL PRIMARY KEY,
    epi_week        INTEGER NOT NULL,
    week_number     SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(15) NOT NULL,
    quarter         SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    decade          SMALLINT NOT NULL,
    century         SMALLINT NOT NULL,
    is_summer       BOOLEAN NOT NULL,
    is_flu_season   BOOLEAN NOT NULL,
    UNIQUE(epi_week)
);

-- DIMENSION: Location
CREATE TABLE dim_location (
    location_key    SERIAL PRIMARY KEY,
    city_name       VARCHAR(100),
    state_code      CHAR(2) NOT NULL,
    state_name      VARCHAR(50) NOT NULL,
    census_region   VARCHAR(20) NOT NULL,
    census_division VARCHAR(30) NOT NULL,
    loc_type        VARCHAR(10) NOT NULL,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    UNIQUE(city_name, state_code, loc_type)
);

-- DIMENSION: Disease
CREATE TABLE dim_disease (
    disease_key         SERIAL PRIMARY KEY,
    disease_name        VARCHAR(50) NOT NULL UNIQUE,
    disease_category    VARCHAR(30) NOT NULL,
    transmission_type   VARCHAR(30) NOT NULL,
    is_vaccine_preventable BOOLEAN NOT NULL,
    vaccine_intro_year  SMALLINT
);

-- FACT: Disease Incidence
CREATE TABLE fact_disease_incidence (
    incidence_key   BIGSERIAL PRIMARY KEY,
    time_key        INTEGER NOT NULL REFERENCES dim_time(time_key),
    location_key    INTEGER NOT NULL REFERENCES dim_location(location_key),
    disease_key     INTEGER NOT NULL REFERENCES dim_disease(disease_key),
    case_count      INTEGER NOT NULL DEFAULT 0,
    incidence_rate  DECIMAL(12,4),
    population      INTEGER
);

-- Indexes for star schema query patterns
CREATE INDEX idx_fact_time ON fact_disease_incidence(time_key);
CREATE INDEX idx_fact_location ON fact_disease_incidence(location_key);
CREATE INDEX idx_fact_disease ON fact_disease_incidence(disease_key);
CREATE INDEX idx_fact_composite ON fact_disease_incidence(disease_key, time_key, location_key);

-- ============================================================
-- Pre-Aggregated Summary Tables (Materialized Views)
-- These will be empty until data is loaded and refreshed.
-- ============================================================

-- SUMMARY 1: Monthly cases by disease and state
CREATE MATERIALIZED VIEW mv_monthly_disease_state AS
SELECT
    d.disease_name,
    l.state_name,
    l.census_region,
    t.year,
    t.month,
    t.month_name,
    SUM(f.case_count) AS total_cases,
    AVG(f.incidence_rate) AS avg_incidence_rate,
    COUNT(*) AS record_count
FROM fact_disease_incidence f
JOIN dim_disease d ON f.disease_key = d.disease_key
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE l.loc_type = 'STATE'
GROUP BY d.disease_name, l.state_name, l.census_region, t.year, t.month, t.month_name
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_monthly ON mv_monthly_disease_state(disease_name, state_name, year, month);

-- SUMMARY 2: Yearly cases by disease and region
CREATE MATERIALIZED VIEW mv_yearly_disease_region AS
SELECT
    d.disease_name,
    d.disease_category,
    l.census_region,
    t.year,
    t.decade,
    SUM(f.case_count) AS total_cases,
    AVG(f.incidence_rate) AS avg_incidence_rate,
    MAX(f.case_count) AS peak_weekly_cases,
    COUNT(DISTINCT l.location_key) AS reporting_locations
FROM fact_disease_incidence f
JOIN dim_disease d ON f.disease_key = d.disease_key
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE l.loc_type = 'STATE'
GROUP BY d.disease_name, d.disease_category, l.census_region, t.year, t.decade
WITH NO DATA;

-- SUMMARY 3: Decade-level national totals (highest roll-up)
CREATE MATERIALIZED VIEW mv_decade_disease_national AS
SELECT
    d.disease_name,
    t.decade,
    SUM(f.case_count) AS total_cases,
    AVG(f.incidence_rate) AS avg_incidence_rate,
    COUNT(DISTINCT l.state_code) AS states_reporting
FROM fact_disease_incidence f
JOIN dim_disease d ON f.disease_key = d.disease_key
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_time t ON f.time_key = t.time_key
WHERE l.loc_type = 'STATE'
GROUP BY d.disease_name, t.decade
WITH NO DATA;
