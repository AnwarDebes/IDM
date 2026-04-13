"""
Transform module: cleans raw data and builds all structures needed for loading
into PostgreSQL (star schema), MongoDB (bucket documents), and Neo4j (graph CSVs).
"""

import os
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from etl.extract import RawData

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


@dataclass
class TransformResult:
    dim_time: pd.DataFrame
    dim_location: pd.DataFrame
    dim_disease: pd.DataFrame
    fact_table: pd.DataFrame
    mongo_documents: list[dict]
    neo4j_nodes: dict  # label -> list of dicts
    neo4j_rels: dict   # type -> list of dicts


def _clean_raw(tycho: pd.DataFrame) -> pd.DataFrame:
    """Clean raw data: drop bad rows, standardize fields."""
    df = tycho.copy()
    df = df.dropna(subset=["cases", "disease", "state", "epi_week"])
    df["cases"] = pd.to_numeric(df["cases"], errors="coerce")
    df = df[df["cases"] > 0].copy()
    df["cases"] = df["cases"].astype(int)
    df["disease"] = df["disease"].str.strip().str.upper()
    df["state"] = df["state"].str.strip().str.upper()
    df["epi_week"] = df["epi_week"].astype(int)
    df["incidence_per_100000"] = pd.to_numeric(df["incidence_per_100000"], errors="coerce").fillna(0.0)
    print(f"  After cleaning: {len(df):,} rows")
    return df


def _derive_time_fields(epi_week: int) -> dict:
    """Derive all time dimension fields from an epi_week integer (YYYYWW)."""
    year = epi_week // 100
    week = epi_week % 100
    # Approximate month from week number
    month = min(12, max(1, int((week - 1) * 12 / 52) + 1))
    quarter = (month - 1) // 3 + 1
    decade = (year // 10) * 10
    century = year // 100
    is_summer = month in (6, 7, 8)
    is_flu_season = month in (10, 11, 12, 1, 2, 3)
    return {
        "epi_week": epi_week,
        "week_number": week,
        "month": month,
        "month_name": MONTH_NAMES[month],
        "quarter": quarter,
        "year": year,
        "decade": decade,
        "century": century,
        "is_summer": is_summer,
        "is_flu_season": is_flu_season,
    }


def _build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """Build time dimension from unique epi_weeks."""
    unique_weeks = sorted(df["epi_week"].unique())
    records = [_derive_time_fields(ew) for ew in unique_weeks]
    dim = pd.DataFrame(records)
    dim["time_key"] = range(1, len(dim) + 1)
    print(f"  dim_time: {len(dim):,} rows")
    return dim


def _build_dim_location(df: pd.DataFrame, regions: pd.DataFrame) -> pd.DataFrame:
    """Build location dimension by joining with regions reference."""
    # Get unique state-level locations
    states = df[["state"]].drop_duplicates().copy()
    states.rename(columns={"state": "state_code"}, inplace=True)

    dim = states.merge(regions, on="state_code", how="left")
    dim["city_name"] = None
    dim["loc_type"] = "STATE"
    dim["location_key"] = range(1, len(dim) + 1)

    cols = ["location_key", "city_name", "state_code", "state_name",
            "census_region", "census_division", "loc_type", "latitude", "longitude"]
    dim = dim[cols].copy()
    print(f"  dim_location: {len(dim):,} rows")
    return dim


def _build_dim_disease(diseases: pd.DataFrame) -> pd.DataFrame:
    """Build disease dimension from reference data."""
    dim = diseases.copy()
    dim["disease_key"] = range(1, len(dim) + 1)
    dim.rename(columns={
        "disease_name": "disease_name",
        "disease_category": "disease_category",
        "transmission_type": "transmission_type",
        "is_vaccine_preventable": "is_vaccine_preventable",
        "vaccine_intro_year": "vaccine_intro_year",
    }, inplace=True)
    dim["is_vaccine_preventable"] = dim["is_vaccine_preventable"].map(
        lambda x: True if str(x).lower() == "true" else False
    )
    print(f"  dim_disease: {len(dim):,} rows")
    return dim


def _build_fact_table(
    df: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_disease: pd.DataFrame,
    populations: pd.DataFrame,
) -> pd.DataFrame:
    """Build fact table by mapping raw data to dimension keys."""
    # Create lookup maps
    time_map = dict(zip(dim_time["epi_week"], dim_time["time_key"]))
    loc_map = dict(zip(dim_location["state_code"], dim_location["location_key"]))
    disease_map = dict(zip(dim_disease["disease_name"], dim_disease["disease_key"]))

    # Build population lookup: (state, decade) -> population
    pop_map = {}
    for _, row in populations.iterrows():
        pop_map[(row["state_code"], int(row["decade"]))] = int(row["population"])

    fact_rows = []
    for _, row in df.iterrows():
        tk = time_map.get(row["epi_week"])
        lk = loc_map.get(row["state"])
        dk = disease_map.get(row["disease"])
        if tk is None or lk is None or dk is None:
            continue

        year = row["epi_week"] // 100
        decade = (year // 10) * 10
        pop = pop_map.get((row["state"], decade))

        fact_rows.append({
            "time_key": tk,
            "location_key": lk,
            "disease_key": dk,
            "case_count": int(row["cases"]),
            "incidence_rate": round(float(row["incidence_per_100000"]), 4),
            "population": pop,
        })

    fact = pd.DataFrame(fact_rows)
    fact["incidence_key"] = range(1, len(fact) + 1)
    print(f"  fact_table: {len(fact):,} rows")
    return fact


def _build_mongo_documents(
    df: pd.DataFrame,
    regions: pd.DataFrame,
    diseases: pd.DataFrame,
) -> list[dict]:
    """Build MongoDB bucket-pattern documents grouped by disease+state+year+month."""
    # Enrich df with time fields
    df = df.copy()
    df["year"] = df["epi_week"] // 100
    df["week_number"] = df["epi_week"] % 100
    df["month"] = df["week_number"].apply(lambda w: min(12, max(1, int((w - 1) * 12 / 52) + 1)))

    # Build lookup maps
    region_map = {}
    for _, r in regions.iterrows():
        region_map[r["state_code"]] = {
            "state_name": r["state_name"],
            "region": r["census_region"],
            "division": r["census_division"],
            "lat": r["latitude"],
            "lng": r["longitude"],
        }

    disease_map = {}
    for _, d in diseases.iterrows():
        disease_map[d["disease_name"]] = {
            "category": d["disease_category"],
            "transmission": d["transmission_type"],
            "vaccine_preventable": str(d["is_vaccine_preventable"]).lower() == "true",
            "vaccine_year": int(d["vaccine_intro_year"]),
        }

    # Group by disease + state + year + month
    grouped = df.groupby(["disease", "state", "year", "month"])
    docs = []
    for (disease, state, year, month), group in grouped:
        region_info = region_map.get(state, {})
        disease_info = disease_map.get(disease, {})

        weekly_obs = []
        for _, row in group.iterrows():
            weekly_obs.append({
                "epi_week": int(row["epi_week"]),
                "week": int(row["week_number"]),
                "cases": int(row["cases"]),
                "incidence_rate": round(float(row["incidence_per_100000"]), 4),
            })

        total_cases = sum(w["cases"] for w in weekly_obs)
        avg_rate = round(np.mean([w["incidence_rate"] for w in weekly_obs]), 4) if weekly_obs else 0
        peak = max(w["cases"] for w in weekly_obs) if weekly_obs else 0

        quarter = (month - 1) // 3 + 1
        decade = (year // 10) * 10

        doc = {
            "disease": {
                "name": disease,
                "category": disease_info.get("category", ""),
                "transmission": disease_info.get("transmission", ""),
                "vaccine_preventable": disease_info.get("vaccine_preventable", False),
                "vaccine_year": disease_info.get("vaccine_year"),
            },
            "location": {
                "city": None,
                "state_code": state,
                "state_name": region_info.get("state_name", state),
                "region": region_info.get("region", ""),
                "division": region_info.get("division", ""),
                "loc_type": "STATE",
                "coordinates": {
                    "lat": region_info.get("lat"),
                    "lng": region_info.get("lng"),
                },
            },
            "time_bucket": {
                "year": int(year),
                "month": int(month),
                "month_name": MONTH_NAMES[int(month)],
                "quarter": quarter,
                "decade": decade,
            },
            "weekly_observations": weekly_obs,
            "monthly_summary": {
                "total_cases": total_cases,
                "avg_incidence_rate": avg_rate,
                "peak_weekly_cases": peak,
                "observation_count": len(weekly_obs),
            },
        }
        docs.append(doc)

    print(f"  mongo_documents: {len(docs):,} bucket documents")
    return docs


def _build_neo4j_data(
    df: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_disease: pd.DataFrame,
    borders: pd.DataFrame,
) -> tuple[dict, dict]:
    """Build Neo4j node and relationship data for CSV import."""
    nodes = {}
    rels = {}

    # Disease nodes
    nodes["Disease"] = dim_disease[["disease_name", "disease_category", "transmission_type",
                                     "is_vaccine_preventable", "vaccine_intro_year"]].to_dict("records")

    # Region nodes
    regions = dim_location[["census_region"]].drop_duplicates()
    nodes["Region"] = [{"name": r} for r in sorted(regions["census_region"].unique())]

    # State nodes
    nodes["State"] = dim_location[["state_code", "state_name", "census_region",
                                    "census_division", "latitude", "longitude"]].to_dict("records")

    # Time hierarchy: Decade -> Year -> Quarter -> Month -> Week
    decades = sorted(dim_time["decade"].unique())
    nodes["Decade"] = [{"decade": int(d)} for d in decades]

    years = sorted(dim_time["year"].unique())
    nodes["Year"] = [{"year": int(y)} for y in years]

    # Build quarter data
    quarter_set = set()
    for _, row in dim_time.iterrows():
        quarter_set.add((int(row["year"]), int(row["quarter"])))
    nodes["Quarter"] = [{"year": y, "quarter": q} for y, q in sorted(quarter_set)]

    # Build month data
    month_set = set()
    for _, row in dim_time.iterrows():
        month_set.add((int(row["year"]), int(row["month"]), row["month_name"]))
    nodes["Month"] = [{"year": y, "month": m, "month_name": mn} for y, m, mn in sorted(month_set)]

    # Week nodes
    nodes["Week"] = dim_time[["epi_week", "week_number", "year", "month"]].to_dict("records")

    # Relationships
    # State -> Region (IN_REGION)
    rels["IN_REGION"] = dim_location[["state_code", "census_region"]].to_dict("records")

    # Week -> Month (IN_MONTH)
    rels["IN_MONTH"] = dim_time[["epi_week", "year", "month"]].to_dict("records")

    # Month -> Quarter (IN_QUARTER)
    rels["IN_QUARTER"] = [{"year": m["year"], "month": m["month"], "quarter": (m["month"] - 1) // 3 + 1}
                          for m in nodes["Month"]]

    # Quarter -> Year (IN_YEAR)
    rels["IN_YEAR_Q"] = [{"year": q["year"], "quarter": q["quarter"]} for q in nodes["Quarter"]]

    # Year -> Decade (IN_DECADE)
    rels["IN_DECADE"] = [{"year": y, "decade": (y // 10) * 10} for y in years]

    # Border relationships
    border_rels = []
    for _, row in borders.iterrows():
        state = row["state_code"]
        for neighbor in row["borders"].split():
            border_rels.append({"from": state, "to": neighbor.strip()})
    rels["BORDERS"] = border_rels

    # Observation data (for batch loading into Neo4j)
    # We store the raw fact data for creating Observation nodes
    enriched = df.copy()
    enriched["year"] = enriched["epi_week"] // 100
    rels["OBSERVATIONS"] = enriched[["epi_week", "state", "disease", "cases", "incidence_per_100000"]].to_dict("records")

    print(f"  neo4j nodes: {sum(len(v) for v in nodes.values()):,} total across {len(nodes)} labels")
    print(f"  neo4j rels: {sum(len(v) for v in rels.values()):,} total across {len(rels)} types")

    return nodes, rels


def transform_all(raw: RawData) -> TransformResult:
    """Run the full transformation pipeline."""
    print("\n--- Cleaning raw data ---")
    cleaned = _clean_raw(raw.tycho)

    print("\n--- Building dimensions ---")
    dim_time = _build_dim_time(cleaned)
    dim_location = _build_dim_location(cleaned, raw.regions)
    dim_disease = _build_dim_disease(raw.diseases)

    print("\n--- Building fact table ---")
    fact_table = _build_fact_table(cleaned, dim_time, dim_location, dim_disease, raw.populations)

    print("\n--- Building MongoDB documents ---")
    mongo_docs = _build_mongo_documents(cleaned, raw.regions, raw.diseases)

    print("\n--- Building Neo4j data ---")
    neo4j_nodes, neo4j_rels = _build_neo4j_data(
        cleaned, dim_time, dim_location, dim_disease, raw.borders
    )

    return TransformResult(
        dim_time=dim_time,
        dim_location=dim_location,
        dim_disease=dim_disease,
        fact_table=fact_table,
        mongo_documents=mongo_docs,
        neo4j_nodes=neo4j_nodes,
        neo4j_rels=neo4j_rels,
    )


if __name__ == "__main__":
    from etl.extract import extract_all

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw = extract_all(
        os.path.join(base, "data", "raw"),
        os.path.join(base, "data", "reference"),
    )
    result = transform_all(raw)
    print(f"\n=== Transform Summary ===")
    print(f"dim_time:    {len(result.dim_time):,}")
    print(f"dim_location: {len(result.dim_location):,}")
    print(f"dim_disease: {len(result.dim_disease):,}")
    print(f"fact_table:  {len(result.fact_table):,}")
    print(f"mongo_docs:  {len(result.mongo_documents):,}")
    print(f"neo4j_nodes: {len(result.neo4j_nodes)} labels")
    print(f"neo4j_rels:  {len(result.neo4j_rels)} types")
