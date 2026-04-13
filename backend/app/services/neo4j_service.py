"""
Neo4j query service — implements all 10 standard queries + 3 graph-exclusive queries.
Uses MonthlyAggregate nodes for performance on analytical queries,
and raw Observation nodes for graph-specific queries.
"""

import time

from neo4j import GraphDatabase

from app.config import settings


class Neo4jService:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def execute(self, query: str, params: dict = None) -> tuple[list[dict], float, str]:
        start = time.time()
        with self.driver.session() as session:
            result = session.run(query, params or {})
            rows = [dict(r) for r in result]
        elapsed_ms = round((time.time() - start) * 1000, 2)
        return rows, elapsed_ms, query.strip()

    # Q1: Total cases by disease, by decade (via MonthlyAggregate)
    def q1(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)-[:IN_DECADE]->(dec:Decade)
            RETURN d.name AS disease_name, dec.decade AS decade,
                   SUM(a.total_cases) AS total_cases,
                   ROUND(AVG(a.avg_incidence), 2) AS avg_incidence
            ORDER BY d.name, dec.decade
        """
        return self.execute(query)

    # Q2: Measles incidence by state for a specific year
    def q2(self, year: int = 1960, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease {name: 'MEASLES'}),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year {year: $year})
            RETURN s.name AS state_name, SUM(a.total_cases) AS total_cases,
                   ROUND(AVG(a.avg_incidence), 2) AS avg_incidence
            ORDER BY total_cases DESC
        """
        return self.execute(query, {"year": year})

    # Q3: Top 10 states by total cases for disease + time range
    def q3(self, disease: str = "MEASLES", start_year: int = 1950, end_year: int = 1970, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease {name: $disease}),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WHERE y.year >= $startYear AND y.year <= $endYear
            RETURN s.name AS state_name, SUM(a.total_cases) AS total_cases
            ORDER BY total_cases DESC
            LIMIT 10
        """
        return self.execute(query, {"disease": disease, "startYear": start_year, "endYear": end_year})

    # Q4: Seasonal pattern — avg weekly cases per month
    def q4(self, disease: str = "MEASLES", **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease {name: $disease}),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)
            RETURN m.month AS month, m.month_name AS month_name,
                   ROUND(AVG(a.total_cases / CASE WHEN a.observation_count = 0 THEN 1 ELSE a.observation_count END), 2) AS avg_weekly_cases
            ORDER BY m.month
        """
        return self.execute(query, {"disease": disease})

    # Q5: Year-over-year change
    def q5(self, disease: str = "MEASLES", **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease {name: $disease}),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WITH s.name AS state, y.year AS year, SUM(a.total_cases) AS total_cases
            ORDER BY state, year
            WITH state, COLLECT({year: year, cases: total_cases}) AS yearly_data
            UNWIND RANGE(1, SIZE(yearly_data)-1) AS i
            WITH state, yearly_data[i] AS curr, yearly_data[i-1] AS prev
            RETURN state AS state_name, curr.year AS year, curr.cases AS total_cases,
                   prev.cases AS prev_year_cases,
                   ROUND(toFloat(curr.cases - prev.cases) /
                         CASE WHEN prev.cases = 0 THEN 1 ELSE prev.cases END * 100, 2) AS yoy_pct_change
            ORDER BY state, year
        """
        return self.execute(query, {"disease": disease})

    # Q6: Disease co-occurrence (simplified — uses aggregate nodes)
    def q6(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WITH d.name AS disease, s.code AS state, y.year AS year, SUM(a.total_cases) AS total
            WHERE total > 100
            WITH state, year, COLLECT(disease) AS diseases
            WHERE SIZE(diseases) >= 2
            UNWIND diseases AS d1
            UNWIND diseases AS d2
            WITH d1 AS disease_a, d2 AS disease_b
            WHERE disease_a < disease_b
            RETURN disease_a, disease_b, COUNT(*) AS co_occurrence_count
            ORDER BY co_occurrence_count DESC
            LIMIT 20
        """
        return self.execute(query)

    # Q7: Geographic spread — rank states by first report
    def q7(self, disease: str = "MEASLES", **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease {name: $disease}),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WHERE a.total_cases > 0
            WITH s.name AS state_name, MIN(y.year) AS first_reported_year
            RETURN state_name, first_reported_year
            ORDER BY first_reported_year
        """
        return self.execute(query, {"disease": disease})

    # Q8: Vaccination impact — before/after
    def q8(self, **kwargs):
        query = """
            MATCH (d:Disease)
            WHERE d.vaccine_year IS NOT NULL AND d.vaccine_year >= 1900
            CALL (d) {
                MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d),
                      (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
                WHERE y.year >= d.vaccine_year - 10 AND y.year < d.vaccine_year
                WITH y.year AS year, SUM(a.total_cases) AS yearly_total
                RETURN AVG(yearly_total) AS pre_avg
            }
            CALL (d) {
                MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d),
                      (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
                WHERE y.year > d.vaccine_year AND y.year <= d.vaccine_year + 10
                WITH y.year AS year, SUM(a.total_cases) AS yearly_total
                RETURN AVG(yearly_total) AS post_avg
            }
            RETURN d.name AS disease_name, d.vaccine_year AS vaccine_intro_year,
                   ROUND(pre_avg) AS pre_vaccine_avg_annual_cases,
                   ROUND(post_avg) AS post_vaccine_avg_annual_cases,
                   ROUND((post_avg - pre_avg) /
                         CASE WHEN pre_avg = 0 THEN 1 ELSE pre_avg END * 100, 2) AS pct_change
            ORDER BY pct_change
        """
        return self.execute(query)

    # Q9: Anomaly detection — states > 2 std dev above mean
    def q9(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WITH d.name AS disease, s.name AS state, y.year AS year,
                 SUM(a.total_cases) AS state_total
            WITH disease, year,
                 COLLECT({state: state, total: state_total}) AS states,
                 AVG(state_total) AS national_mean,
                 stDev(state_total) AS national_stddev
            WHERE national_stddev > 0
            UNWIND states AS s
            WITH disease, s.state AS state_name, year, s.total AS state_total,
                 national_mean, national_stddev,
                 (s.total - national_mean) / national_stddev AS z_score
            WHERE z_score > 2
            RETURN disease AS disease_name, state_name, year, state_total,
                   ROUND(national_mean) AS national_mean, ROUND(z_score, 2) AS z_score
            ORDER BY z_score DESC
            LIMIT 50
        """
        return self.execute(query)

    # Q10: Cross-disease normalized trend
    def q10(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WITH d.name AS disease, y.year AS year, SUM(a.total_cases) AS total_cases
            WITH disease, COLLECT({year: year, cases: total_cases}) AS data,
                 MAX(total_cases) AS max_cases
            UNWIND data AS d
            RETURN disease AS disease_name, d.year AS year, d.cases AS total_cases,
                   ROUND(toFloat(d.cases) /
                         CASE WHEN max_cases = 0 THEN 1 ELSE max_cases END * 100, 2) AS normalized_index
            ORDER BY disease, year
        """
        return self.execute(query)

    # ==================================================================
    # GRAPH-EXCLUSIVE QUERIES (Q11-Q13)
    # ==================================================================

    # Q11: Disease spread by state borders
    def q11(self, disease: str = "MEASLES", threshold: int = 50, **kwargs):
        query = """
            MATCH (s1:State)-[:BORDERS]->(s2:State),
                  (a1:MonthlyAggregate)-[:SUMMARIZES_STATE]->(s1),
                  (a1)-[:SUMMARIZES_DISEASE]->(d:Disease {name: $disease}),
                  (a1)-[:SUMMARIZES_MONTH]->(m1:Month),
                  (a2:MonthlyAggregate)-[:SUMMARIZES_STATE]->(s2),
                  (a2)-[:SUMMARIZES_DISEASE]->(d),
                  (a2)-[:SUMMARIZES_MONTH]->(m2:Month)
            WHERE a1.total_cases > $threshold AND a2.total_cases > $threshold
              AND (m1.year * 100 + m1.month) < (m2.year * 100 + m2.month)
            WITH s1.name AS origin, s2.name AS destination,
                 m1.year AS origin_year, m1.month AS origin_month,
                 m2.year AS dest_year, m2.month AS dest_month,
                 (m2.year * 12 + m2.month) - (m1.year * 12 + m1.month) AS lag_months
            WHERE lag_months > 0 AND lag_months <= 6
            RETURN origin, destination, origin_year, origin_month, dest_year, dest_month, lag_months
            ORDER BY lag_months ASC
            LIMIT 50
        """
        return self.execute(query, {"disease": disease, "threshold": threshold})

    # Q12: State similarity by disease pattern
    def q12(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_DISEASE]->(d:Disease)
            WITH s, d.name AS disease, SUM(a.total_cases) AS total
            WITH s, COLLECT({disease: disease, cases: total}) AS pattern
            WITH s, pattern, REDUCE(sum=0, p IN pattern | sum + p.cases) AS grand_total
            WITH s.name AS state, s.code AS code,
                 [p IN pattern | {disease: p.disease, pct: ROUND(toFloat(p.cases) / CASE WHEN grand_total=0 THEN 1 ELSE grand_total END * 100, 1)}] AS profile
            RETURN state, code, profile
            ORDER BY state
        """
        return self.execute(query)

    # Q13: Disease centrality in co-occurrence network
    def q13(self, **kwargs):
        query = """
            MATCH (a:MonthlyAggregate)-[:SUMMARIZES_DISEASE]->(d:Disease),
                  (a)-[:SUMMARIZES_STATE]->(s:State),
                  (a)-[:SUMMARIZES_MONTH]->(m:Month)-[:IN_QUARTER]->(:Quarter)-[:IN_YEAR]->(y:Year)
            WHERE a.total_cases > 100
            WITH d.name AS disease, COUNT(DISTINCT s.code + toString(y.year)) AS presence
            RETURN disease AS disease_name, presence AS state_year_presence,
                   ROUND(toFloat(presence) / (50 * 81) * 100, 2) AS coverage_pct
            ORDER BY presence DESC
        """
        return self.execute(query)
