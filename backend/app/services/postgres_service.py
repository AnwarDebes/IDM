"""
PostgreSQL query service — implements all 10 decision support queries
against the star schema.
"""

import time
from typing import Any

import psycopg2
import psycopg2.extras

from app.config import settings


class PostgresService:
    def __init__(self):
        self.conn_params = {
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "user": settings.postgres_user,
            "password": settings.postgres_password,
            "dbname": settings.postgres_db,
        }

    def _get_conn(self):
        return psycopg2.connect(**self.conn_params)

    def execute(self, query: str, params: tuple = ()) -> tuple[list[dict], float, str]:
        """Execute a query and return (results, execution_time_ms, query_text)."""
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        start = time.time()
        cur.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        elapsed_ms = round((time.time() - start) * 1000, 2)
        cur.close()
        conn.close()
        return rows, elapsed_ms, query.strip()

    # ------------------------------------------------------------------
    # Q1: Total cases by disease, by decade (Roll-up)
    # ------------------------------------------------------------------
    def q1(self, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT d.disease_name, t.decade,
                   SUM(f.case_count) AS total_cases,
                   ROUND(AVG(f.incidence_rate)::numeric, 2) AS avg_incidence
            FROM fact_disease_incidence f
            JOIN dim_disease d ON f.disease_key = d.disease_key
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE l.loc_type = 'STATE'
            GROUP BY d.disease_name, t.decade
            ORDER BY d.disease_name, t.decade
        """
        return self.execute(query)

    # ------------------------------------------------------------------
    # Q2: Measles incidence by state for a specific year (Slice)
    # ------------------------------------------------------------------
    def q2(self, year: int = 1960, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT l.state_name, SUM(f.case_count) AS total_cases,
                   ROUND(AVG(f.incidence_rate)::numeric, 2) AS avg_incidence
            FROM fact_disease_incidence f
            JOIN dim_disease d ON f.disease_key = d.disease_key
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE d.disease_name = 'MEASLES' AND t.year = %s AND l.loc_type = 'STATE'
            GROUP BY l.state_name
            ORDER BY total_cases DESC
        """
        return self.execute(query, (year,))

    # ------------------------------------------------------------------
    # Q3: Top 10 states by total cases for disease + time range (Dice)
    # ------------------------------------------------------------------
    def q3(self, disease: str = "MEASLES", start_year: int = 1950, end_year: int = 1970, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT l.state_name, SUM(f.case_count) AS total_cases
            FROM fact_disease_incidence f
            JOIN dim_disease d ON f.disease_key = d.disease_key
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE d.disease_name = %s AND t.year BETWEEN %s AND %s AND l.loc_type = 'STATE'
            GROUP BY l.state_name
            ORDER BY total_cases DESC
            LIMIT 10
        """
        return self.execute(query, (disease, start_year, end_year))

    # ------------------------------------------------------------------
    # Q4: Seasonal pattern — avg weekly cases per month (Roll-up + Agg)
    # ------------------------------------------------------------------
    def q4(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT t.month, t.month_name, ROUND(AVG(f.case_count)::numeric, 2) AS avg_weekly_cases
            FROM fact_disease_incidence f
            JOIN dim_disease d ON f.disease_key = d.disease_key
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE d.disease_name = %s AND l.loc_type = 'STATE'
            GROUP BY t.month, t.month_name
            ORDER BY t.month
        """
        return self.execute(query, (disease,))

    # ------------------------------------------------------------------
    # Q5: Year-over-year change in incidence by state (Window/Pivot)
    # ------------------------------------------------------------------
    def q5(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        query = """
            WITH yearly AS (
                SELECT d.disease_name, l.state_name, t.year,
                       SUM(f.case_count) AS total_cases
                FROM fact_disease_incidence f
                JOIN dim_disease d ON f.disease_key = d.disease_key
                JOIN dim_time t ON f.time_key = t.time_key
                JOIN dim_location l ON f.location_key = l.location_key
                WHERE d.disease_name = %s AND l.loc_type = 'STATE'
                GROUP BY d.disease_name, l.state_name, t.year
            )
            SELECT state_name, year, total_cases,
                   LAG(total_cases) OVER (PARTITION BY state_name ORDER BY year) AS prev_year_cases,
                   ROUND(((total_cases - LAG(total_cases) OVER (PARTITION BY state_name ORDER BY year))::numeric
                   / NULLIF(LAG(total_cases) OVER (PARTITION BY state_name ORDER BY year), 0)) * 100, 2) AS yoy_pct_change
            FROM yearly
            ORDER BY state_name, year
        """
        return self.execute(query, (disease,))

    # ------------------------------------------------------------------
    # Q6: Disease co-occurrence by state and time (Dice + Correlation)
    # ------------------------------------------------------------------
    def q6(self, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            WITH state_year_disease AS (
                SELECT l.state_name, t.year, d.disease_name, SUM(f.case_count) AS total_cases
                FROM fact_disease_incidence f
                JOIN dim_disease d ON f.disease_key = d.disease_key
                JOIN dim_time t ON f.time_key = t.time_key
                JOIN dim_location l ON f.location_key = l.location_key
                WHERE l.loc_type = 'STATE'
                GROUP BY l.state_name, t.year, d.disease_name
                HAVING SUM(f.case_count) > 100
            )
            SELECT a.disease_name AS disease_a, b.disease_name AS disease_b,
                   COUNT(*) AS co_occurrence_count,
                   ROUND(CORR(a.total_cases, b.total_cases)::numeric, 3) AS correlation
            FROM state_year_disease a
            JOIN state_year_disease b ON a.state_name = b.state_name AND a.year = b.year
                 AND a.disease_name < b.disease_name
            GROUP BY a.disease_name, b.disease_name
            HAVING COUNT(*) > 10
            ORDER BY correlation DESC
        """
        return self.execute(query)

    # ------------------------------------------------------------------
    # Q7: Geographic spread — rank states by first report (Drill-down)
    # ------------------------------------------------------------------
    def q7(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT l.state_name, MIN(t.year) AS first_reported_year,
                   RANK() OVER (ORDER BY MIN(t.year)) AS spread_rank
            FROM fact_disease_incidence f
            JOIN dim_disease d ON f.disease_key = d.disease_key
            JOIN dim_time t ON f.time_key = t.time_key
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE d.disease_name = %s AND f.case_count > 0 AND l.loc_type = 'STATE'
            GROUP BY l.state_name
            ORDER BY first_reported_year
        """
        return self.execute(query, (disease,))

    # ------------------------------------------------------------------
    # Q8: Vaccination impact — before/after comparison (Slice + Agg)
    # ------------------------------------------------------------------
    def q8(self, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            SELECT d.disease_name, d.vaccine_intro_year,
                   pre.avg_cases AS pre_vaccine_avg_annual_cases,
                   post.avg_cases AS post_vaccine_avg_annual_cases,
                   ROUND(((post.avg_cases - pre.avg_cases)::numeric / NULLIF(pre.avg_cases, 0)) * 100, 2) AS pct_change
            FROM dim_disease d
            JOIN LATERAL (
                SELECT AVG(yearly_total)::numeric AS avg_cases FROM (
                    SELECT t.year, SUM(f.case_count) AS yearly_total
                    FROM fact_disease_incidence f
                    JOIN dim_time t ON f.time_key = t.time_key
                    JOIN dim_location l ON f.location_key = l.location_key
                    WHERE f.disease_key = d.disease_key AND l.loc_type = 'STATE'
                      AND t.year BETWEEN d.vaccine_intro_year - 10 AND d.vaccine_intro_year - 1
                    GROUP BY t.year
                ) sub
            ) pre ON true
            JOIN LATERAL (
                SELECT AVG(yearly_total)::numeric AS avg_cases FROM (
                    SELECT t.year, SUM(f.case_count) AS yearly_total
                    FROM fact_disease_incidence f
                    JOIN dim_time t ON f.time_key = t.time_key
                    JOIN dim_location l ON f.location_key = l.location_key
                    WHERE f.disease_key = d.disease_key AND l.loc_type = 'STATE'
                      AND t.year BETWEEN d.vaccine_intro_year + 1 AND d.vaccine_intro_year + 10
                    GROUP BY t.year
                ) sub
            ) post ON true
            WHERE d.vaccine_intro_year IS NOT NULL
            ORDER BY pct_change
        """
        return self.execute(query)

    # ------------------------------------------------------------------
    # Q9: Anomaly detection — states > 2 std dev above national mean
    # ------------------------------------------------------------------
    def q9(self, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            WITH national_stats AS (
                SELECT disease_name, year,
                       AVG(state_total) AS national_mean,
                       STDDEV(state_total) AS national_stddev
                FROM (
                    SELECT d.disease_name, l.state_name, t.year, SUM(f.case_count) AS state_total
                    FROM fact_disease_incidence f
                    JOIN dim_disease d ON f.disease_key = d.disease_key
                    JOIN dim_time t ON f.time_key = t.time_key
                    JOIN dim_location l ON f.location_key = l.location_key
                    WHERE l.loc_type = 'STATE'
                    GROUP BY d.disease_name, l.state_name, t.year
                ) sub
                GROUP BY disease_name, year
            ),
            state_totals AS (
                SELECT d.disease_name, l.state_name, t.year, SUM(f.case_count) AS state_total
                FROM fact_disease_incidence f
                JOIN dim_disease d ON f.disease_key = d.disease_key
                JOIN dim_time t ON f.time_key = t.time_key
                JOIN dim_location l ON f.location_key = l.location_key
                WHERE l.loc_type = 'STATE'
                GROUP BY d.disease_name, l.state_name, t.year
            )
            SELECT s.disease_name, s.state_name, s.year, s.state_total,
                   ROUND(n.national_mean::numeric, 0) AS national_mean,
                   ROUND(((s.state_total - n.national_mean) / NULLIF(n.national_stddev, 0))::numeric, 2) AS z_score
            FROM state_totals s
            JOIN national_stats n ON s.disease_name = n.disease_name AND s.year = n.year
            WHERE n.national_stddev > 0
              AND (s.state_total - n.national_mean) / n.national_stddev > 2
            ORDER BY z_score DESC
            LIMIT 50
        """
        return self.execute(query)

    # ------------------------------------------------------------------
    # Q10: Cross-disease normalized trend comparison (Pivot + Norm)
    # ------------------------------------------------------------------
    def q10(self, **kwargs) -> tuple[list[dict], float, str]:
        query = """
            WITH disease_year AS (
                SELECT d.disease_name, t.year, SUM(f.case_count) AS total_cases
                FROM fact_disease_incidence f
                JOIN dim_disease d ON f.disease_key = d.disease_key
                JOIN dim_time t ON f.time_key = t.time_key
                JOIN dim_location l ON f.location_key = l.location_key
                WHERE l.loc_type = 'STATE'
                GROUP BY d.disease_name, t.year
            ),
            disease_max AS (
                SELECT disease_name, MAX(total_cases) AS max_cases
                FROM disease_year GROUP BY disease_name
            )
            SELECT dy.disease_name, dy.year, dy.total_cases,
                   ROUND((dy.total_cases::numeric / NULLIF(dm.max_cases, 0)) * 100, 2) AS normalized_index
            FROM disease_year dy
            JOIN disease_max dm ON dy.disease_name = dm.disease_name
            ORDER BY dy.disease_name, dy.year
        """
        return self.execute(query)
