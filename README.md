# Epidemiological Data Warehouse & Analytics Platform

A multi-backend data warehouse for epidemiological surveillance analytics built on real Project Tycho Level 1 disease incidence data (1916 to 2011, 8 diseases, 51 state-level and 120 city-level jurisdictions, 473{,}189 cleaned weekly observations). The system consolidates weekly disease reports across **three database backends** (PostgreSQL with a relational star schema, MongoDB with the document bucket pattern, and Neo4j with a labeled property graph) unified behind a single **FastAPI** REST layer with a **React** analytics dashboard. Data ingestion is powered by **Apache Kafka** running in KRaft mode (Confluent Platform 8.2) with **ksqlDB** for stream validation, windowed aggregation, and anomaly alerting. A controlled synthetic generator is retained for benchmarking and reproducibility.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Model](#data-model)
- [Database Backend Designs](#database-backend-designs)
- [ETL Pipeline](#etl-pipeline)
- [Streaming Pipeline](#streaming-pipeline)
- [Decision Support Queries](#decision-support-queries)
- [OLAP Operations](#olap-operations)
- [REST API Reference](#rest-api-reference)
- [Frontend Dashboard](#frontend-dashboard)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Running the Demo](#running-the-demo)

---

## Architecture Overview

The platform follows a layered architecture with clear separation between data ingestion, storage, API, and presentation layers.

```
                    +--------------------------------------------+
                    |          React Analytics Dashboard          |
                    |  Overview | Explorer | Compare | Stream | Graph  |
                    +----------------------+---------------------+
                                           |
                                      HTTP / SSE
                                           |
                    +----------------------v---------------------+
                    |              FastAPI REST API               |
                    |          (Unified Query Interface)          |
                    |                                            |
                    |  /query  /compare  /olap  /stream  /meta   |
                    +----+----------+----------+---------+------+
                         |          |          |         |
           +-------------+    +-----+-----+   |    +----+--------+
           |                  |           |   |    |             |
    +------v-------+   +------v-----+ +---v--v--+ | +-----------v---------+
    |  PostgreSQL   |   |  MongoDB   | |  Neo4j  | | |   Apache Kafka      |
    |              |   |            | |         | | |   (KRaft Mode)      |
    |  Star Schema |   |  Bucket   | |  Graph  | | +----------+----------+
    |  3 Dims +    |   |  Pattern  | |  Model  | |            |
    |  1 Fact +    |   |  Monthly  | |  Nodes: | | +----------v----------+
    |  3 Mat.Views |   |  Buckets  | |  Disease | | |   kSQL Processing   |
    |              |   |  + 2 Pre- | |  State   | | |  - Validation       |
    |  Port 15432  |   |  Agg Coll.| |  Time    | | |  - Aggregation      |
    +--------------+   |           | |  Obs.    | | |  - Anomaly Alerts   |
                       |  Port     | |  Monthly | | +---------------------+
                       |  27017    | |  Agg     | |
                       +-----------+ | Port     | |
                                     | 7687     | |
                                     +---------+  |
                                                   |
                    +------------------------------v---------+
                    |           ETL Pipeline (Python)         |
                    |  Extract -> Transform -> Load (per DB)  |
                    |  + Kafka Producer for streaming events  |
                    +-------------------+--------------------+
                                        |
                    +-------------------v--------------------+
                    |    Data Sources (real + synthetic)      |
                    |  Real: Project Tycho Level 1            |
                    |   473,189 clean rows (1916-2011),       |
                    |   8 diseases, 51 jurisdictions +        |
                    |   120 cities, weekly granularity        |
                    |  Synthetic generator for benchmarking   |
                    +----------------------------------------+
```

### Service Topology

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| PostgreSQL 18 | `epid-postgres` | 15432 | Star schema relational DW |
| MongoDB 8.0 | `epid-mongodb` | 27017 | Document-based DW (bucket pattern) |
| Neo4j 2026.03 | `epid-neo4j` | 7474 / 7687 | Graph-based DW with APOC |
| Kafka (KRaft, Confluent 8.2) | `epid-kafka` | 9092 | Event streaming broker |
| ksqlDB Server (Confluent 8.2) | `epid-ksqldb-server` | 8088 | Stream processing engine |
| ksqlDB CLI (Confluent 8.2) | `epid-ksqldb-cli` | (none) | Interactive kSQL client |
| FastAPI | `epid-backend` | 8000 | Unified REST API |
| React | `epid-frontend` | 3000 | Analytics dashboard |

All services run on a shared Docker bridge network (`dw-network`) with health checks and automatic restart policies. The backend container waits for all three databases to be healthy before starting, and automatically runs the ETL loaders if any backend is empty.

---

## Data Model

### Source Data

The primary dataset is **Project Tycho Level 1** disease surveillance data from the University of Pittsburgh, downloaded directly from the Tycho data portal. A controlled synthetic data generator is retained alongside it for benchmarking and reproducibility.

| Attribute | Real Project Tycho Level 1 | Synthetic Benchmark |
|-----------|----------------------------|---------------------|
| **Diseases** | 8 (Measles, Pertussis, Polio, Diphtheria, Hepatitis A, Mumps, Rubella, Smallpox) | Same 8 diseases |
| **Geographic Coverage** | 51 jurisdictions (50 states + District of Columbia) plus 120 city-grain (state, city) reporting locations | 51 jurisdictions |
| **Time Range** | 1916 to 2011 (96 years) | 1910 to 2010 (101 years) |
| **Granularity** | Weekly (epidemiological weeks) | Weekly (epidemiological weeks) |
| **Total Records** | 759,467 raw rows, 473,189 cleaned fact rows | ~1,109,360 fact rows |

### Disease Reference Data

```
+---------------+------------+---------------+---------------------+--------------+
|   Disease     | Category   | Transmission  | Vaccine Preventable | Vaccine Year |
+---------------+------------+---------------+---------------------+--------------+
| MEASLES       | Viral      | Airborne      | Yes                 | 1963         |
| PERTUSSIS     | Bacterial  | Airborne      | Yes                 | 1948         |
| POLIO         | Viral      | Fecal-oral    | Yes                 | 1955         |
| DIPHTHERIA    | Bacterial  | Airborne      | Yes                 | 1923         |
| SMALLPOX      | Viral      | Contact       | Yes                 | 1796         |
| HEPATITIS A   | Viral      | Fecal-oral    | Yes                 | 1995         |
| MUMPS         | Viral      | Airborne      | Yes                 | 1967         |
| RUBELLA       | Viral      | Airborne      | Yes                 | 1969         |
+---------------+------------+---------------+---------------------+--------------+
```

The real Project Tycho Level 1 feed exposes genuine epidemiological patterns such as sharp post-vaccine decline for MEASLES (1963), POLIO (1955), and RUBELLA (1969), as well as real reporting-era boundaries (HEPATITIS A begins in 1966, MUMPS in 1968, RUBELLA in 1966, and PERTUSSIS has a genuine 1956 to 1973 reporting gap in the Level 1 archive). The synthetic generator, used only for benchmarking, reproduces these behaviours through pre and post vaccine exponential decline, sinusoidal seasonal modulation with respiratory diseases peaking in winter, population-proportional geographic variation based on US Census data, and Poisson-distributed random noise for statistical realism.

---

## Database Backend Designs

### 1. PostgreSQL (Star Schema, Relational / ROLAP)

The relational backend implements a classic **Kimball-style star schema** optimized for OLAP slice, dice, drill-down, roll-up, and pivot operations.

```
                    +------------------+
                    |    dim_disease   |
                    +------------------+
                    | disease_key (PK) |
                    | disease_name     |
                    | disease_category |
                    | transmission_type|
                    | is_vaccine_prev. |
                    | vaccine_intro_yr |
                    +--------+---------+
                             |
+------------------+         |         +-------------------+
|    dim_time      +---------+---------+   dim_location    |
+------------------+         |         +-------------------+
| time_key (PK)    |  +------v-------+ | location_key (PK) |
| epi_week         |  | FACT TABLE   | | city_name         |
| week_number      |  | fact_disease | | state_code        |
| month / month_nm |  | _incidence   | | state_name        |
| quarter          |  +--------------+ | census_region     |
| year / decade    |  | incidence_key| | census_division   |
| century          |  | time_key(FK) | | loc_type          |
| is_summer        |  | location_(FK)| | latitude          |
| is_flu_season    |  | disease_(FK) | | longitude         |
+------------------+  | case_count   | +-------------------+
                      | incidence_rt |
                      | population   |
                      +--------------+
```

**Pre-aggregated materialized views** for common query patterns:

| Materialized View | Granularity | Rows (real Tycho load) | Purpose |
|-------------------|-------------|-----------------------|---------|
| `mv_monthly_disease_state` | Monthly | 126,113 | Disease cases by state per month |
| `mv_yearly_disease_region` | Yearly | 1,253 | Disease cases by region per year with peak tracking |
| `mv_decade_disease_national` | Decade | 46 | National totals by disease per decade |
| `mv_yearly_disease_city` | Yearly | 3,301 | City-grain yearly totals (DIPHTHERIA 1916 to 1947 etc.) |

**Indexes:** Composite index on `(disease_key, time_key, location_key)` plus individual FK indexes for flexible star-join performance.

---

### 2. MongoDB (Bucket Pattern, Document / DOLAP)

The document backend uses the **Bucket Pattern** to group weekly observations into monthly documents, reducing document count while preserving granular access.

```json
{
  "disease": {
    "name": "MEASLES",
    "category": "Viral",
    "transmission": "Airborne",
    "vaccine_preventable": true,
    "vaccine_year": 1963
  },
  "location": {
    "state_code": "CA",
    "state_name": "CALIFORNIA",
    "region": "West",
    "division": "Pacific",
    "loc_type": "STATE",
    "coordinates": { "lat": 36.7783, "lng": -119.4179 }
  },
  "time_bucket": {
    "year": 1960,
    "month": 3,
    "month_name": "March",
    "quarter": 1,
    "decade": 1960
  },
  "weekly_observations": [
    { "epi_week": 196009, "week": 9, "cases": 1247, "incidence_rate": 7.93 },
    { "epi_week": 196010, "week": 10, "cases": 1389, "incidence_rate": 8.83 },
    { "epi_week": 196011, "week": 11, "cases": 1456, "incidence_rate": 9.26 },
    { "epi_week": 196012, "week": 12, "cases": 1512, "incidence_rate": 9.62 }
  ],
  "monthly_summary": {
    "total_cases": 5604,
    "avg_incidence_rate": 8.91,
    "peak_weekly_cases": 1512,
    "observation_count": 4
  }
}
```

**Collections:**

| Collection | Documents (real Tycho load) | Purpose |
|------------|-----------------------------|---------|
| `disease_observations` | 155,063 buckets (avg 3.05 weekly observations per bucket) | Primary bucket documents (disease + location + month) |
| `summary_monthly_by_region` | 14,157 | Regional monthly summaries |
| `summary_decade_national` | 46 | National decade-level totals |

**Indexes:** Compound indexes on `(disease.name, time_bucket.year, location.state_code)` and `(location.region, time_bucket.decade)`, plus `(location.loc_type, location.loc)` to separate STATE-grain and CITY-grain documents in the same collection.

---

### 3. Neo4j (Graph Model, GOLAP)

The graph backend models the data as a network of interconnected nodes, enabling relationship-based queries impossible in relational or document stores.

```
                                 (:Country {name: "USA"})
                                         ^
                                    IN_COUNTRY
                                         |
     (:Disease)                  (:Region {name: "West"})
         ^                               ^
    SUMMARIZES                      IN_REGION
    _DISEASE                             |
         |                       (:State {code: "CA"})
  (:MonthlyAggregate)                    ^
    {total_cases,             SUMMARIZES_STATE
     avg_incidence,                      |
     obs_count}           (:MonthlyAggregate)-------SUMMARIZES_MONTH---->(:Month)
         |                                                                  |
    Also links:                                                        IN_QUARTER
    (:Observation)                                                          |
         |                                                             (:Quarter)
    AFFECTS -> (:Disease)                                                   |
    OBSERVED_IN -> (:State)                                            IN_YEAR
    OBSERVED_AT -> (:Week)                                                  |
                                                                       (:Year)
    (:State)-[:BORDERS]->(:State)                                           |
                                                                       IN_DECADE
                                                                            |
                                                                       (:Decade)
```

**Node Types:**

| Label | Count (real Tycho load) | Key Properties |
|-------|-------------------------|----------------|
| `Disease` | 8 | name, category, transmission, vaccine_year |
| `State` | 51 | code, name, division, lat/lng |
| `City` | 120 | city_name, state_code, loc_type |
| `Region` | 4 | name |
| `Observation` | 473,189 | case_count, incidence_rate |
| `MonthlyAggregate` | 155,063 | total_cases, avg_incidence, observation_count |
| `Week` | 4,947 | epi_week, week_number, year, month |
| `Month` | 1,152 | year, month, month_name |
| `Year` | 96 | year |
| `Decade` | 11 | decade |

**Key Relationships:**

| Relationship | Description |
|-------------|-------------|
| `BORDERS` | State adjacency (for geographic spread analysis) |
| `IN_REGION` | State to Region hierarchy |
| `LOCATED_IN_STATE` | City to State hierarchy (city-grain Tycho rows) |
| `SUMMARIZES_DISEASE` / `SUMMARIZES_STATE` / `SUMMARIZES_MONTH` | MonthlyAggregate to dimensions |
| `AFFECTS` / `OBSERVED_IN` / `OBSERVED_AT` | Observation to Disease / State / Week |
| `IN_MONTH`, `IN_QUARTER`, `IN_YEAR`, `IN_DECADE` | Time hierarchy chain |

---

## ETL Pipeline

The ETL pipeline runs in three stages. Each loader script reads the cleaned CSV and builds backend-specific data structures.

```
  data/raw/tycho_level1_real.csv  (or tycho_level1_synth.csv via TYCHO_DATA_FILE)
            |
     +------v-------+
     |   extract.py  |  Read CSV + reference data (regions, diseases, borders, populations)
     +--------------+
            |
     +------v--------+
     |  transform.py  |  Clean, derive time fields, build dims/facts/documents/graph data
     +----------------+
            |
     +------v---+------v---+-------v-----+
     | load_     | load_    | load_       |
     | postgres  | mongo    | neo4j       |
     | .py       | .py      | .py         |
     +-----------+----------+-------------+
     | Star      | Bucket   | Nodes +     |
     | schema    | docs     | Rels +      |
     | + mat.    | + pre-   | Monthly     |
     | views     | agg      | Aggregates  |
     +-----------+----------+-------------+
```

### Pipeline Details

| Stage | Script | Input | Output |
|-------|--------|-------|--------|
| **Fetch real data** | `fetch_tycho.py` | Project Tycho Level 1 portal | `tycho_level1_real.csv` (759,467 raw rows) |
| **Generate synthetic** | `generate_synthetic_data.py` | Disease parameters, Census populations | `tycho_level1_synth.csv` (~42 MB, 1.1M rows) |
| **Validate** | `validate_data.py` | Raw CSV + reference files | Quality report (column checks, range validation, data quality %) |
| **Extract** | `extract.py` | CSV file selected by `TYCHO_DATA_FILE` env var | Pandas DataFrames |
| **Transform** | `transform.py` | Raw DataFrames | Dimensions, fact table, Mongo docs, Neo4j nodes/rels |
| **Load PG** | `load_postgres.py` | Cleaned data | Dimensions + fact table via COPY + materialized view refresh |
| **Load Mongo** | `load_mongo.py` | Cleaned data | Bucket documents + pre-aggregated summary collections |
| **Load Neo4j** | `load_neo4j.py` | Cleaned data | Graph nodes + relationships + MonthlyAggregate nodes |

The backend container's `entrypoint.sh` automatically detects empty databases on startup and runs the appropriate loaders.

---

## Streaming Pipeline

Real-time data ingestion uses Apache Kafka (KRaft mode, no ZooKeeper) with kSQL for stream processing.

```
  etl/kafka_producer.py                            Frontend
         |                                        (SSE Client)
         | JSON events                                 ^
         v                                             |
  +------+------+                              +-------+------+
  | Kafka Topic  |                              | FastAPI SSE  |
  | raw-disease- |----> kSQL Processing --+     | /stream/     |
  | events       |                        |     | events       |
  +------+-------+      Streams:          |     +--------------+
         |               |                |            ^
         |               v                v            |
         |    validated_disease_events    anomaly_   Kafka
         |    (filtered: cases >= 0)     alerts     Consumer
         |                               (rate > 50)  |
         +-----> weekly_national_summary              |
                 (windowed aggregation)        backend/kafka/
                                               consumer.py
```

### kSQL Streams and Tables

| Name | Type | Description |
|------|------|-------------|
| `disease_events_stream` | Stream | Raw events from `raw-disease-events` topic |
| `validated_disease_events` | Derived Stream | Filtered: removes null diseases and negative case counts |
| `weekly_national_summary` | Table | Windowed aggregation: national cases by disease and epi_week |
| `anomaly_alerts` | Derived Stream | Flags events where `incidence_rate > 50.0` |

---

## Decision Support Queries

The platform implements **13 decision support queries** (10 that run across all three backends for performance comparison, plus 3 graph-exclusive queries that leverage Neo4j's relationship traversal).

### Cross-Backend Queries (Q1 to Q10)

| ID | Query | OLAP Operation | Description |
|----|-------|---------------|-------------|
| **Q1** | Cases by disease and decade | Roll-up | Aggregate weekly data to decade granularity |
| **Q2** | Measles incidence by state | Slice | Fix disease=MEASLES and year, show state breakdown |
| **Q3** | Top 10 states by cases | Dice | Filter on disease + year range, rank states |
| **Q4** | Seasonal patterns | Roll-up + Agg | Average weekly cases per calendar month |
| **Q5** | Year-over-year change | Window / Pivot | YoY % change using LAG / $setWindowFields |
| **Q6** | Disease co-occurrence | Dice + Correlation | Which diseases spike together in same state-years |
| **Q7** | Geographic spread ranking | Drill-down | Rank states by first reported year per disease |
| **Q8** | Vaccination impact | Slice + Agg | Compare avg annual cases 10yr before vs 10yr after vaccine |
| **Q9** | Anomaly detection | Statistical | Find state-years > 2 standard deviations above national mean |
| **Q10** | Normalized trends | Pivot + Norm | Each disease's annual cases as % of its historical peak |

### Graph-Exclusive Queries (Q11 to Q13)

| ID | Query | OLAP Operation | Description |
|----|-------|---------------|-------------|
| **Q11** | Border spread analysis | Graph Traversal | Detect disease spread between neighboring states using `BORDERS` relationships |
| **Q12** | State similarity profiles | Graph Pattern | Compute each state's disease composition as a percentage breakdown |
| **Q13** | Disease centrality | Graph Centrality | Measure each disease's presence across state-years (coverage %) |

### Implementation Comparison

Each query is implemented natively in all supported backends:

```
PostgreSQL:  SQL with JOINs, CTEs, window functions (LAG, RANK), LATERAL joins
MongoDB:     Aggregation pipelines with $group, $unwind, $setWindowFields, $lookup
Neo4j:       Cypher with MATCH patterns, UNWIND, COLLECT, CALL subqueries
```

The `/compare` endpoint runs the same logical query on all three backends concurrently (using a thread pool) and returns execution times for direct performance comparison.

---

## OLAP Operations

The API exposes five generic OLAP operations against the PostgreSQL star schema:

```
           +---------------------------------------------------+
           |              OLAP Operation Cube                   |
           |                                                   |
           |   Dimensions: disease, state, year, decade, region |
           |                                                   |
           |   SLICE   -->  Fix one dimension, view all others  |
           |   DICE    -->  Filter on multiple dimensions       |
           |   DRILL   -->  decade -> year -> quarter -> month  |
           |   ROLLUP  -->  week -> month -> quarter -> year    |
           |   PIVOT   -->  Diseases as columns, decades as rows|
           +---------------------------------------------------+

  Drill-Down Hierarchy:        Roll-Up Hierarchy:
  decade                       week
    |                            |
    v                            v
  year                         month
    |                            |
    v                            v
  quarter                      quarter
    |                            |
    v                            v
  month                        year
```

---

## REST API Reference

Base URL: `http://localhost:8000`

Interactive documentation available at `/docs` (Swagger UI).

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/health` | Service health (all backends + Kafka) |
| `GET` | `/api/v1/queries` | List all 13 queries with metadata and parameters |
| `POST` | `/api/v1/query` | Execute a query on a specific backend |
| `POST` | `/api/v1/compare` | Run same query across all backends concurrently |
| `POST` | `/api/v1/olap/slice` | OLAP slice operation |
| `POST` | `/api/v1/olap/dice` | OLAP dice operation |
| `POST` | `/api/v1/olap/drilldown` | OLAP drill-down |
| `POST` | `/api/v1/olap/rollup` | OLAP roll-up |
| `POST` | `/api/v1/olap/pivot` | OLAP pivot |
| `GET` | `/api/v1/metadata/{backend}` | Schema info (tables, collections, nodes, indexes) |
| `POST` | `/api/v1/batch/refresh-summaries` | Refresh materialized views and pre-aggregated collections |
| `GET` | `/api/v1/batch/status` | Last refresh timestamps |
| `GET` | `/api/v1/stream/events` | SSE endpoint for real-time Kafka events |
| `POST` | `/api/v1/stream/demo` | Start the Kafka producer for demo streaming |
| `GET` | `/api/v1/stream/demo/status` | Check if demo producer is running |

### Example: Execute a Query

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query_id": "Q3", "backend": "postgres", "params": {"disease": "MEASLES", "start_year": 1950, "end_year": 1970}}'
```

### Example: Compare Across Backends

```bash
curl -X POST http://localhost:8000/api/v1/compare \
  -H "Content-Type: application/json" \
  -d '{"query_id": "Q1"}'
```

---

## Frontend Dashboard

The React dashboard provides five pages for interacting with the data warehouse:

### 1. Overview (`/`)

System health dashboard showing:
- Backend connectivity status (PostgreSQL, MongoDB, Neo4j, Kafka)
- Row/document/node counts across all backends
- Disease distribution (bar chart + pie chart)
- Cases by decade (line chart)

### 2. OLAP Explorer (`/explorer`)

Interactive query execution interface:
- Select any of the 13 queries from a dropdown
- Choose the target backend (PostgreSQL, MongoDB, or Neo4j)
- Configure query parameters (disease, year range, etc.)
- View results as auto-generated charts and sortable data tables
- Inspect the actual SQL / aggregation pipeline / Cypher query

### 3. Comparison Dashboard (`/compare`)

Side-by-side backend performance comparison:
- Run a single query across all backends simultaneously
- "Run All Q1-Q10" button for full benchmark
- Execution time bar chart comparing PostgreSQL vs MongoDB vs Neo4j
- Per-backend result tables with row counts
- Cumulative timing chart across all queries

### 4. Live Stream (`/stream`)

Real-time event monitoring via Server-Sent Events (SSE):
- Connect/disconnect to the Kafka event stream
- "Stream Sample Data" button starts the Kafka producer from the backend
- Live events-per-second chart
- Anomaly alert panel (flags events with incidence rate > 50)
- Scrolling event table with disease, state, week, cases, and rate

### 5. Graph Explorer (`/graph`)

Neo4j-exclusive analytics:
- Q11: Border spread analysis with lag-month distribution chart
- Q12: State disease profile similarity table with colored percentage tags
- Q13: Disease centrality and coverage bar chart
- Raw results table and Cypher query viewer

---

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Python 3.10+** with pip (for running ETL scripts from the host, if needed)
- **~4 GB free RAM** for all containers running simultaneously

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/AnwarDebes/IDM.git
cd IDM

# 2. Create the environment file
cp .env.example .env

# 3. Start all services
docker compose up -d --build

# 4. Wait for health check (~60s for all databases to initialize)
curl http://localhost:8000/api/v1/health

# 5. Generate synthetic data (only needed on first run)
python etl/generate_synthetic_data.py

# 6. Load all three backends
python etl/load_postgres.py
python etl/load_mongo.py
python etl/load_neo4j.py

# 7. Open the dashboard
open http://localhost:3000
```

Alternatively, run the automated setup:

```bash
bash scripts/setup.sh
```

The backend container also auto-loads data on startup if it detects any empty databases.

---

## Project Structure

```
epidemiological-dw/
|
+-- backend/                        # FastAPI application
|   +-- app/
|   |   +-- main.py                 # App entry point, health check, CORS, router registration
|   |   +-- config.py               # Pydantic settings (env-based configuration)
|   |   +-- routers/
|   |   |   +-- query_router.py     # /query endpoint (single-backend execution)
|   |   |   +-- compare_router.py   # /compare endpoint (concurrent multi-backend)
|   |   |   +-- olap_router.py      # /olap/* endpoints (slice, dice, drilldown, rollup, pivot)
|   |   |   +-- batch_router.py     # /batch/* (refresh materialized views)
|   |   |   +-- metadata_router.py  # /metadata/{backend} (schema introspection)
|   |   |   +-- stream_router.py    # /stream/* (SSE events, demo producer control)
|   |   +-- services/
|   |   |   +-- postgres_service.py # 10 queries implemented in SQL
|   |   |   +-- mongo_service.py    # 10 queries as aggregation pipelines
|   |   |   +-- neo4j_service.py    # 13 queries in Cypher (10 + 3 graph-exclusive)
|   |   |   +-- query_registry.py   # Query metadata registry
|   |   +-- kafka/
|   |       +-- consumer.py         # Async Kafka consumer for SSE streaming
|   +-- scripts/
|   |   +-- check_and_load.py       # Startup: wait for DBs, auto-load if empty
|   +-- Dockerfile
|   +-- entrypoint.sh
|   +-- requirements.txt
|
+-- frontend/                       # React 18 dashboard
|   +-- src/
|   |   +-- App.jsx                 # Router setup with context providers
|   |   +-- AppContext.jsx          # Global state (queries, explorer, compare state)
|   |   +-- StreamContext.jsx       # SSE connection state and event buffer
|   |   +-- api.js                  # Axios API client
|   |   +-- styles.js               # Shared colors, layout, button styles
|   |   +-- pages/
|   |   |   +-- Home.jsx            # Overview dashboard with charts
|   |   |   +-- Explorer.jsx        # OLAP query explorer
|   |   |   +-- Compare.jsx         # Cross-backend comparison
|   |   |   +-- Stream.jsx          # Live Kafka event monitor
|   |   |   +-- Graph.jsx           # Neo4j graph analytics
|   |   +-- components/
|   |       +-- Layout.jsx          # Sidebar navigation and page layout
|   |       +-- Toast.jsx           # Notification toast system
|   +-- public/
|   |   +-- index.html
|   +-- Dockerfile
|   +-- package.json
|
+-- etl/                            # ETL pipeline
|   +-- generate_synthetic_data.py  # Realistic data generator (Poisson, seasonal, vaccine effects)
|   +-- extract.py                  # CSV reader (raw + reference data)
|   +-- transform.py                # Data cleaning + transformation for all 3 backends
|   +-- validate_data.py            # Data quality validation script
|   +-- load_postgres.py            # Bulk loader (COPY protocol + materialized view refresh)
|   +-- load_mongo.py               # Batch insert + pre-aggregation pipelines
|   +-- load_neo4j.py               # Node/relationship creation + MonthlyAggregate generation
|   +-- kafka_producer.py           # Streams CSV rows as JSON events to Kafka
|
+-- docker/                         # Database initialization scripts
|   +-- postgres/init.sql           # Star schema DDL + materialized views + indexes
|   +-- mongodb/init.js             # Collection creation + index definitions
|   +-- neo4j/init.cypher           # Constraints + indexes
|
+-- ksql/
|   +-- statements.sql              # kSQL stream/table definitions
|
+-- batch_jobs/
|   +-- refresh_mongo_summaries.py  # Standalone MongoDB summary refresh job
|
+-- scripts/
|   +-- setup.sh                    # Full automated setup (start, generate, load, ksql)
|   +-- run_demo_queries.sh         # Run all 13 queries and print comparison table
|
+-- data/
|   +-- raw/                        # Generated CSV (excluded from git)
|   +-- reference/                  # Static reference data
|       +-- disease_metadata.csv    # 8 diseases with categories and vaccine years
|       +-- us_regions.csv          # 51 jurisdictions (50 states + DC) with regions, divisions, coordinates
|       +-- state_populations.csv   # Jurisdiction populations by decade (1910-2010)
|       +-- state_borders.csv       # State adjacency for graph queries
|
+-- docker-compose.yml              # Full service orchestration
+-- .env.example                    # Environment variable template
+-- .gitignore
+-- README.md
```

---

## Running the Demo

### Full Cross-Backend Benchmark

```bash
bash scripts/run_demo_queries.sh
```

This runs all 13 queries and prints a comparison table:

```
Query  | PostgreSQL   | MongoDB      | Neo4j        | Name
-------+--------------+--------------+--------------+-----------------------------------------
Q1     | 45ms (64r)   | 78ms (64r)   | 120ms (64r)  | Total cases by disease and decade
Q2     | 12ms (50r)   | 34ms (50r)   | 89ms (50r)   | Measles incidence by state for a year
...
```

### Start the Streaming Demo

```bash
# From the host
python -m etl.kafka_producer --limit 5000

# Or trigger it from the dashboard's Live Stream page
# (click "Stream Sample Data")
```

### kSQL Interactive Session

```bash
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088

ksql> SHOW STREAMS;
ksql> SHOW TABLES;
ksql> SELECT * FROM anomaly_alerts EMIT CHANGES LIMIT 10;
```
