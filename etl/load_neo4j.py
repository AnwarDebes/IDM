"""
Load data into the Neo4j graph data warehouse. Produces the time, place, and
disease hierarchies plus Observation nodes, and supports both STATE-grain and
CITY-grain rows so the real Project Tycho diphtheria series (city-level,
1916-1947) can coexist with the state-level rows used for every other disease.
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


def load_neo4j():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start = time.time()

    active_file = os.environ.get("TYCHO_DATA_FILE", "tycho_level1.csv")
    print("Reading source data (%s)..." % active_file)
    tycho = pd.read_csv(os.path.join(base, "data", "raw", active_file), low_memory=False)
    regions = pd.read_csv(os.path.join(base, "data", "reference", "us_regions.csv"))
    diseases = pd.read_csv(os.path.join(base, "data", "reference", "disease_metadata.csv"))
    borders = pd.read_csv(os.path.join(base, "data", "reference", "state_borders.csv"))
    print("  Raw rows %d" % len(tycho))

    tycho = tycho.dropna(subset=["cases", "disease", "state", "epi_week", "loc_type"])
    tycho["cases"] = pd.to_numeric(tycho["cases"], errors="coerce")
    tycho = tycho[tycho["cases"] > 0].copy()
    tycho["cases"] = tycho["cases"].astype(int)
    tycho["disease"] = tycho["disease"].str.strip().str.upper()
    tycho["state"] = tycho["state"].str.strip().str.upper()
    tycho["loc_type"] = tycho["loc_type"].str.strip().str.upper()
    tycho["loc"] = tycho["loc"].astype(str).str.strip().str.upper()
    tycho["epi_week"] = tycho["epi_week"].astype(int)
    tycho["incidence_per_100000"] = pd.to_numeric(tycho["incidence_per_100000"], errors="coerce").fillna(0.0)
    tycho["year"] = tycho["epi_week"] // 100
    tycho["week_number"] = tycho["epi_week"] % 100
    tycho["month"] = ((tycho["week_number"] - 1) * 12 // 52 + 1).clip(1, 12)

    state_rows = int((tycho["loc_type"] == "STATE").sum())
    city_rows = int((tycho["loc_type"] == "CITY").sum())
    print("  After cleaning %d rows (STATE=%d, CITY=%d)" % (len(tycho), state_rows, city_rows))

    driver = get_driver()

    with driver.session() as session:
        print("Clearing existing graph data...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Creating constraints and indexes...")
        for stmt in [
            "CREATE CONSTRAINT disease_name IF NOT EXISTS FOR (d:Disease) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT state_code IF NOT EXISTS FOR (s:State) REQUIRE s.code IS UNIQUE",
            "CREATE CONSTRAINT region_name IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE",
            "CREATE CONSTRAINT city_key IF NOT EXISTS FOR (c:City) REQUIRE (c.state_code, c.name) IS UNIQUE",
            "CREATE INDEX week_epi IF NOT EXISTS FOR (w:Week) ON (w.epi_week)",
            "CREATE INDEX month_year IF NOT EXISTS FOR (m:Month) ON (m.year, m.month)",
            "CREATE INDEX year_val IF NOT EXISTS FOR (y:Year) ON (y.year)",
        ]:
            session.run(stmt)

        # Disease nodes
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
        print("  %d Disease nodes created" % len(diseases))

        # Region and Country
        print("Creating Region and Country nodes...")
        region_names = sorted(regions["census_region"].unique())
        for r in region_names:
            session.run("CREATE (:Region {name: $name})", {"name": r})
        session.run("CREATE (:Country {name: 'USA'})")
        for r in region_names:
            session.run(
                "MATCH (r:Region {name: $r}), (c:Country {name: 'USA'}) CREATE (r)-[:IN_COUNTRY]->(c)",
                {"r": r},
            )
        print("  %d Region nodes, 1 Country node" % len(region_names))

        # State nodes
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
        print("  %d State nodes created" % len(regions))

        # City nodes (only for CITY-grain source rows, currently diphtheria only)
        city_keys = (
            tycho[tycho["loc_type"] == "CITY"][["state", "loc"]]
            .drop_duplicates()
            .values.tolist()
        )
        print("Creating City nodes...")
        if city_keys:
            city_batch = [{"state": str(s), "name": str(c)} for s, c in city_keys]
            session.run(
                """UNWIND $batch AS row
                   CREATE (c:City {state_code: row.state, name: row.name})""",
                {"batch": city_batch},
            )
            session.run(
                """UNWIND $batch AS row
                   MATCH (c:City {state_code: row.state, name: row.name}),
                         (s:State {code: row.state})
                   CREATE (c)-[:LOCATED_IN_STATE]->(s)""",
                {"batch": city_batch},
            )
        print("  %d City nodes created" % len(city_keys))

        # Border relationships
        print("Creating border relationships...")
        border_count = 0
        for _, b in borders.iterrows():
            for neighbor in str(b["borders"]).split():
                session.run(
                    "MATCH (s1:State {code: $s1}), (s2:State {code: $s2}) MERGE (s1)-[:BORDERS]->(s2)",
                    {"s1": b["state_code"], "s2": neighbor.strip()},
                )
                border_count += 1
        print("  %d BORDERS relationships created" % border_count)

        # Time hierarchy: Decade -> Year -> Quarter -> Month -> Week
        print("Creating time hierarchy...")

        decades = sorted(set(int((y // 10) * 10) for y in tycho["year"].unique()))
        for d in decades:
            session.run("CREATE (:Decade {decade: $d})", {"d": int(d)})

        years = sorted(int(y) for y in tycho["year"].unique())
        for y in years:
            decade = (int(y) // 10) * 10
            session.run("CREATE (:Year {year: $y})", {"y": int(y)})
            session.run(
                "MATCH (y:Year {year: $y}), (d:Decade {decade: $d}) CREATE (y)-[:IN_DECADE]->(d)",
                {"y": int(y), "d": decade},
            )

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

        month_rows = tycho[["year", "month"]].drop_duplicates()
        month_set = set((int(r["year"]), int(r["month"])) for _, r in month_rows.iterrows())
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
        week_batch = [
            {
                "ew": int(w["epi_week"]),
                "wn": int(w["week_number"]),
                "y": int(w["year"]),
                "m": int(w["month"]),
            }
            for _, w in weeks.iterrows()
        ]
        for i in range(0, len(week_batch), 500):
            batch = week_batch[i:i + 500]
            session.run(
                "UNWIND $batch AS w CREATE (:Week {epi_week: w.ew, week_number: w.wn, year: w.y, month: w.m})",
                {"batch": batch},
            )
        for i in range(0, len(week_batch), 500):
            batch = week_batch[i:i + 500]
            session.run(
                """UNWIND $batch AS w
                   MATCH (wk:Week {epi_week: w.ew}), (m:Month {year: w.y, month: w.m})
                   CREATE (wk)-[:IN_MONTH]->(m)""",
                {"batch": batch},
            )
        print("  %d Week nodes created with hierarchy" % len(week_batch))

        # Observation nodes. STATE-grain rows attach to a State; CITY-grain rows
        # attach to a City (which is already linked back to its State).
        print("Creating Observation nodes (this takes a while)...")
        obs_data = tycho[
            ["epi_week", "state", "loc", "loc_type", "disease", "cases", "incidence_per_100000"]
        ].values.tolist()
        batch_size = 5000
        total = len(obs_data)

        for i in range(0, total, batch_size):
            rows = obs_data[i:i + batch_size]
            state_batch = []
            city_batch = []
            for row in rows:
                payload = {
                    "ew": int(row[0]),
                    "state": str(row[1]),
                    "loc": str(row[2]),
                    "disease": str(row[4]),
                    "cases": int(row[5]),
                    "incidence": round(float(row[6]), 4),
                }
                if str(row[3]) == "STATE":
                    state_batch.append(payload)
                else:
                    city_batch.append(payload)

            if state_batch:
                session.run(
                    """UNWIND $batch AS row
                       MATCH (d:Disease {name: row.disease})
                       MATCH (s:State {code: row.state})
                       MATCH (w:Week {epi_week: row.ew})
                       CREATE (o:Observation {case_count: row.cases, incidence_rate: row.incidence, grain: 'STATE'})
                       CREATE (o)-[:AFFECTS]->(d)
                       CREATE (o)-[:OBSERVED_IN]->(s)
                       CREATE (o)-[:OBSERVED_AT]->(w)""",
                    {"batch": state_batch},
                )
            if city_batch:
                session.run(
                    """UNWIND $batch AS row
                       MATCH (d:Disease {name: row.disease})
                       MATCH (c:City {state_code: row.state, name: row.loc})
                       MATCH (w:Week {epi_week: row.ew})
                       CREATE (o:Observation {case_count: row.cases, incidence_rate: row.incidence, grain: 'CITY'})
                       CREATE (o)-[:AFFECTS]->(d)
                       CREATE (o)-[:OBSERVED_IN_CITY]->(c)
                       CREATE (o)-[:OBSERVED_AT]->(w)""",
                    {"batch": city_batch},
                )

            loaded = min(i + batch_size, total)
            if loaded % 50000 == 0 or loaded == total:
                print("  %d / %d observations loaded" % (loaded, total))

        # MonthlyAggregate nodes. Aggregated at STATE grain so the summaries are
        # comparable across diseases; CITY-grain observations are rolled up to
        # their parent state via LOCATED_IN_STATE.
        print("Creating MonthlyAggregate nodes...")
        session.run(
            """
            MATCH (o:Observation)-[:AFFECTS]->(d:Disease),
                  (o)-[:OBSERVED_AT]->(w:Week)-[:IN_MONTH]->(m:Month)
            OPTIONAL MATCH (o)-[:OBSERVED_IN]->(s1:State)
            OPTIONAL MATCH (o)-[:OBSERVED_IN_CITY]->(:City)-[:LOCATED_IN_STATE]->(s2:State)
            WITH d, m, coalesce(s1, s2) AS s, o
            WHERE s IS NOT NULL
            WITH d, s, m, SUM(o.case_count) AS total, AVG(o.incidence_rate) AS avg_rate, COUNT(o) AS cnt
            CREATE (a:MonthlyAggregate {
                total_cases: total,
                avg_incidence: avg_rate,
                observation_count: cnt
            })
            CREATE (a)-[:SUMMARIZES_DISEASE]->(d)
            CREATE (a)-[:SUMMARIZES_STATE]->(s)
            CREATE (a)-[:SUMMARIZES_MONTH]->(m)
            """
        )

    with driver.session() as session:
        counts = {}
        for label in ["Disease", "State", "City", "Region", "Observation",
                      "Week", "Month", "Year", "Decade", "MonthlyAggregate"]:
            result = session.run("MATCH (n:%s) RETURN COUNT(n) AS cnt" % label)
            counts[label] = result.single()["cnt"]
        border_cnt = session.run("MATCH ()-[r:BORDERS]->() RETURN COUNT(r) AS cnt").single()["cnt"]

    elapsed = time.time() - start
    print("\nNeo4j load complete in %.1fs" % elapsed)
    print("Node counts:")
    for label, cnt in counts.items():
        print("  %s: %d" % (label, cnt))
    print("  BORDERS relationships: %d" % border_cnt)

    driver.close()


if __name__ == "__main__":
    load_neo4j()
