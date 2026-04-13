// Epidemiological Data Warehouse — Neo4j Constraints & Indexes

CREATE CONSTRAINT disease_name IF NOT EXISTS FOR (d:Disease) REQUIRE d.name IS UNIQUE;
CREATE CONSTRAINT state_code IF NOT EXISTS FOR (s:State) REQUIRE s.code IS UNIQUE;
CREATE CONSTRAINT region_name IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE;
CREATE INDEX week_epi IF NOT EXISTS FOR (w:Week) ON (w.epi_week);
CREATE INDEX month_year IF NOT EXISTS FOR (m:Month) ON (m.year, m.month);
CREATE INDEX year_val IF NOT EXISTS FOR (y:Year) ON (y.year);
