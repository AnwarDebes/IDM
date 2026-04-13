"""
Load data into Neo4j graph data warehouse.
Creates dimension hierarchy nodes, Observation nodes, border relationships,
and pre-aggregated MonthlyAggregate nodes.
"""

import os
import sys
import time

import pandas as pd
from neo4j import GraphDatabase

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def get_driver():
    return GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "dw_secure_2024"),
        ),
    )


def run_query(session, query, params=None):
    """Run a Cypher query."""
    session.run(query, params or {})


def load_neo4j():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start = time.time()

    # Read source data
    print("Reading source data...")
    tycho = pd.read_csv(os.path.join(base, "data", "raw", "tycho_level1.csv"))
    regions = pd.read_csv(os.path.join(base, "data", "reference", "us_regions.csv"))
    diseases = pd.read_csv(os.path.join(base, "data", "reference", "disease_metadata.csv"))
    borders = pd.read_csv(os.path.join(base, "data", "reference", "state_borders.csv"))

    # Clean
    tycho = tycho.dropna(subset=["cases", "disease", "state", "epi_week"])
    tycho["cases"] = pd.to_numeric(tycho["cases"], errors="coerce")
    tycho = tycho[tycho["cases"] > 0].copy()
    tycho["cases"] = tycho["cases"].astype(int)
    tycho["disease"] = tycho["disease"].str.strip().str.upper()
    tycho["state"] = tycho["state"].str.strip().str.upper()
    tycho["epi_week"] = tycho["epi_week"].astype(int)
    tycho["incidence_per_100000"] = pd.to_numeric(tycho["incidence_per_100000"], errors="coerce").fillna(0.0)
    tycho["year"] = tycho["epi_week"] // 100
    tycho["week_number"] = tycho["epi_week"] % 100
    tycho["month"] = ((tycho["week_number"] - 1) * 12 // 52 + 1).clip(1, 12)
    print(f"  {len(tycho):,} rows after cleaning")

    driver = get_driver()

    with driver.session() as session:
        # Clear existing data
        print("Clearing existing graph data...")
        session.run("MATCH (n) DETACH DELETE n")

        # Recreate constraints
        print("Creating constraints and indexes...")
        for stmt in [
            "CREATE CONSTRAINT disease_name IF NOT EXISTS FOR (d:Disease) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT state_code IF NOT EXISTS FOR (s:State) REQUIRE s.code IS UNIQUE",
            "CREATE CONSTRAINT region_name IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE",
            "CREATE INDEX week_epi IF NOT EXISTS FOR (w:Week) ON (w.epi_week)",
            "CREATE INDEX month_year IF NOT EXISTS FOR (m:Month) ON (m.year, m.month)",
            "CREATE INDEX year_val IF NOT EXISTS FOR (y:Year) ON (y.year)",
        ]:
            session.run(stmt)

        # === Disease nodes ===
        print("Creating Disease nodes...")
        for _, d in diseases.iterrows():
            session.run(
                """CREATE (d:Disease {
                    name: $name, category: $cat, transmission: $trans,
                    vaccine_preventable: $vp, vaccine_year: $vy
                })""",
                {
                    "name": d["disease_name"],
                    "cat": d["disease_category"],
                    "trans": d["transmission_type"],
                    "vp": str(d["is_vaccine_preventable"]).lower() == "true",
                    "vy": int(d["vaccine_intro_year"]),
                },
            )
        print(f"  8 Disease nodes created")

        # === Region nodes ===
        print("Creating Region nodes...")
        region_names = sorted(regions["census_region"].unique())
        for r in region_names:
            session.run("CREATE (:Region {name: $name})", {"name": r})
        print(f"  {len(region_names)} Region nodes created")

        # === Country node ===
        session.run("CREATE (:Country {name: 'USA'})")
        for r in region_names:
            session.run(
                "MATCH (r:Region {name: $r}), (c:Country {name: 'USA'}) CREATE (r)-[:IN_COUNTRY]->(c)",
                {"r": r},
            )

        # === State nodes + IN_REGION rels ===
        print("Creating State nodes...")
        for _, r in regions.iterrows():
            session.run(
                """CREATE (s:State {
                    code: $code, name: $name, division: $div,
                    latitude: $lat, longitude: $lng
                })""",
                {
                    "code": r["state_code"],
                    "name": r["state_name"],
                    "div": r["census_division"],
                    "lat": float(r["latitude"]),
                    "lng": float(r["longitude"]),
                },
            )
            session.run(
                "MATCH (s:State {code: $code}), (r:Region {name: $region}) CREATE (s)-[:IN_REGION]->(r)",
                {"code": r["state_code"], "region": r["census_region"]},
            )
        print(f"  {len(regions)} State nodes created")

        # === Border relationships ===
        print("Creating border relationships...")
        border_count = 0
        for _, b in borders.iterrows():
            for neighbor in b["borders"].split():
                session.run(
                    "MATCH (s1:State {code: $s1}), (s2:State {code: $s2}) MERGE (s1)-[:BORDERS]->(s2)",
                    {"s1": b["state_code"], "s2": neighbor.strip()},
                )
                border_count += 1
        print(f"  {border_count} BORDERS relationships created")

        # === Time hierarchy: Decade -> Year -> Quarter -> Month -> Week ===
        print("Creating time hierarchy...")

        # Decades
        decades = sorted(tycho["year"].apply(lambda y: (y // 10) * 10).unique())
        for d in decades:
            session.run("CREATE (:Decade {decade: $d})", {"d": int(d)})

        # Years
        years = sorted(tycho["year"].unique())
        for y in years:
            decade = (int(y) // 10) * 10
            session.run("CREATE (:Year {year: $y})", {"y": int(y)})
            session.run(
                "MATCH (y:Year {year: $y}), (d:Decade {decade: $d}) CREATE (y)-[:IN_DECADE]->(d)",
                {"y": int(y), "d": decade},
            )

        # Quarters
        quarter_set = set()
        for y in years:
            for q in range(1, 5):
                quarter_set.add((int(y), q))
        for y, q in sorted(quarter_set):
            session.run("CREATE (:Quarter {year: $y, quarter: $q})", {"y": y, "q": q})
            session.run(
                "MATCH (q:Quarter {year: $y, quarter: $qq}), (yr:Year {year: $y}) CREATE (q)-[:IN_YEAR]->(yr)",
                {"y": y, "qq": q},
            )

        # Months
        month_set = set()
        for _, row in tycho[["year", "month"]].drop_duplicates().iterrows():
            month_set.add((int(row["year"]), int(row["month"])))
        for y, m in sorted(month_set):
            q = (m - 1) // 3 + 1
            session.run(
                "CREATE (:Month {year: $y, month: $m, month_name: $mn})",
                {"y": y, "m": m, "mn": MONTH_NAMES[m]},
            )
            session.run(
                "MATCH (m:Month {year: $y, month: $mm}), (q:Quarter {year: $y, quarter: $qq}) CREATE (m)-[:IN_QUARTER]->(q)",
                {"y": y, "mm": m, "qq": q},
            )

        # Weeks
        print("Creating Week nodes...")
        weeks = tycho[["epi_week", "week_number", "year", "month"]].drop_duplicates()
        week_batch = []
        for _, w in weeks.iterrows():
            week_batch.append({
                "ew": int(w["epi_week"]),
                "wn": int(w["week_number"]),
                "y": int(w["year"]),
                "m": int(w["month"]),
            })
        # Batch create weeks
        for i in range(0, len(week_batch), 500):
            batch = week_batch[i:i + 500]
            session.run(
                "UNWIND $batch AS w CREATE (:Week {epi_week: w.ew, week_number: w.wn, year: w.y, month: w.m})",
                {"batch": batch},
            )
        # Batch create IN_MONTH rels
        for i in range(0, len(week_batch), 500):
            batch = week_batch[i:i + 500]
            session.run(
                """UNWIND $batch AS w
                   MATCH (wk:Week {epi_week: w.ew}), (m:Month {year: w.y, month: w.m})
                   CREATE (wk)-[:IN_MONTH]->(m)""",
                {"batch": batch},
            )
        print(f"  {len(week_batch)} Week nodes created with hierarchy")

        # === Observation nodes (batch loading) ===
        print("Creating Observation nodes (this takes a while)...")
        obs_data = tycho[["epi_week", "state", "disease", "cases", "incidence_per_100000"]].values.tolist()
        batch_size = 5000
        total = len(obs_data)

        for i in range(0, total, batch_size):
            batch = [
                {
                    "ew": int(row[0]),
                    "state": str(row[1]),
                    "disease": str(row[2]),
                    "cases": int(row[3]),
                    "incidence": round(float(row[4]), 4),
                }
                for row in obs_data[i:i + batch_size]
            ]
            session.run(
                """UNWIND $batch AS row
                   MATCH (d:Disease {name: row.disease})
                   MATCH (s:State {code: row.state})
                   MATCH (w:Week {epi_week: row.ew})
                   CREATE (o:Observation {case_count: row.cases, incidence_rate: row.incidence})
                   CREATE (o)-[:AFFECTS]->(d)
                   CREATE (o)-[:OBSERVED_IN]->(s)
                   CREATE (o)-[:OBSERVED_AT]->(w)""",
                {"batch": batch},
            )
            loaded = min(i + batch_size, total)
            if loaded % 50000 == 0 or loaded == total:
                print(f"  {loaded:,} / {total:,} observations loaded")

        # === MonthlyAggregate nodes ===
        print("Creating MonthlyAggregate nodes...")
        session.run("""
            MATCH (o:Observation)-[:AFFECTS]->(d:Disease),
                  (o)-[:OBSERVED_IN]->(s:State),
                  (o)-[:OBSERVED_AT]->(w:Week)-[:IN_MONTH]->(m:Month)
            WITH d, s, m, SUM(o.case_count) AS total, AVG(o.incidence_rate) AS avg_rate, COUNT(o) AS cnt
            CREATE (a:MonthlyAggregate {
                total_cases: total,
                avg_incidence: avg_rate,
                observation_count: cnt
            })
            CREATE (a)-[:SUMMARIZES_DISEASE]->(d)
            CREATE (a)-[:SUMMARIZES_STATE]->(s)
            CREATE (a)-[:SUMMARIZES_MONTH]->(m)
        """)

    # Verify counts
    with driver.session() as session:
        counts = {}
        for label in ["Disease", "State", "Region", "Observation", "Week", "Month", "Year", "Decade", "MonthlyAggregate"]:
            result = session.run(f"MATCH (n:{label}) RETURN COUNT(n) AS cnt")
            counts[label] = result.single()["cnt"]

        border_cnt = session.run("MATCH ()-[r:BORDERS]->() RETURN COUNT(r) AS cnt").single()["cnt"]

    elapsed = time.time() - start
    print(f"\nNeo4j load complete in {elapsed:.1f}s")
    print("Node counts:")
    for label, cnt in counts.items():
        print(f"  {label}: {cnt:,}")
    print(f"  BORDERS relationships: {border_cnt}")

    driver.close()


if __name__ == "__main__":
    load_neo4j()
