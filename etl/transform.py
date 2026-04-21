"""
Transform module. Cleans the raw Tycho rows and builds every structure
needed by the three storage layers.

    PostgreSQL star schema, columnar fact plus three dimensions
    MongoDB bucket pattern, one document per disease, location, year, month
    Neo4j multi-hierarchy graph, time, place, and disease ontologies

The pipeline is dataset-agnostic. It accepts both the real Project Tycho
Level 1 export (which mixes STATE-grain and CITY-grain rows for
diphtheria) and the synthetic benchmark (which is pure STATE grain).
City rows propagate all the way through to PostgreSQL and MongoDB and
are surfaced via dim_location.loc_type plus the mv_yearly_disease_city
materialized view.
"""

import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

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
    mongo_documents: list
    neo4j_nodes: dict
    neo4j_rels: dict
    source_label: str


def _clean_raw(tycho):
    """Drop bad rows, normalize fields, retain loc_type variation."""
    df = tycho.copy()
    required = ["cases", "disease", "state", "epi_week", "loc_type"]
    df = df.dropna(subset=required)
    df["cases"] = pd.to_numeric(df["cases"], errors="coerce")
    df = df[df["cases"] > 0].copy()
    df["cases"] = df["cases"].astype(int)
    df["disease"] = df["disease"].str.strip().str.upper()
    df["state"] = df["state"].str.strip().str.upper()
    df["loc_type"] = df["loc_type"].str.strip().str.upper()
    df["loc"] = df["loc"].astype(str).str.strip().str.upper()
    df["epi_week"] = df["epi_week"].astype(int)
    df["incidence_per_100000"] = pd.to_numeric(df["incidence_per_100000"], errors="coerce").fillna(0.0)

    state_rows = (df["loc_type"] == "STATE").sum()
    city_rows = (df["loc_type"] == "CITY").sum()
    print("  After cleaning %d rows total, STATE=%d, CITY=%d" % (len(df), state_rows, city_rows))
    return df


def _derive_time_fields(epi_week):
    year = epi_week // 100
    week = epi_week % 100
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


def _build_dim_time(df):
    unique_weeks = sorted(df["epi_week"].unique())
    records = [_derive_time_fields(int(ew)) for ew in unique_weeks]
    dim = pd.DataFrame(records)
    dim["time_key"] = range(1, len(dim) + 1)
    print("  dim_time %d rows (years %d to %d)" % (len(dim), int(dim["year"].min()), int(dim["year"].max())))
    return dim


def _build_dim_location(df, regions):
    """Build location dimension covering both STATE and CITY grain."""
    state_rows = df[df["loc_type"] == "STATE"][["state"]].drop_duplicates().copy()
    state_rows.rename(columns={"state": "state_code"}, inplace=True)
    state_dim = state_rows.merge(regions, on="state_code", how="left")
    state_dim["city_name"] = None
    state_dim["loc_type"] = "STATE"

    city_rows = df[df["loc_type"] == "CITY"][["state", "loc"]].drop_duplicates().copy()
    city_rows.rename(columns={"state": "state_code", "loc": "city_name"}, inplace=True)
    city_dim = city_rows.merge(regions, on="state_code", how="left")
    city_dim["loc_type"] = "CITY"

    dim = pd.concat([state_dim, city_dim], ignore_index=True)
    dim["location_key"] = range(1, len(dim) + 1)

    cols = ["location_key", "city_name", "state_code", "state_name",
            "census_region", "census_division", "loc_type", "latitude", "longitude"]
    dim = dim[cols].copy()
    print("  dim_location %d rows (STATE=%d, CITY=%d)" % (
        len(dim),
        (dim["loc_type"] == "STATE").sum(),
        (dim["loc_type"] == "CITY").sum(),
    ))
    return dim


def _build_dim_disease(diseases):
    dim = diseases.copy()
    dim["disease_key"] = range(1, len(dim) + 1)
    dim["is_vaccine_preventable"] = dim["is_vaccine_preventable"].map(
        lambda x: True if str(x).lower() == "true" else False
    )
    print("  dim_disease %d rows" % len(dim))
    return dim


def _location_lookup(dim_location):
    """Build a (state_code, loc_type, city_name_or_None) -> location_key map."""
    lookup = {}
    for _, row in dim_location.iterrows():
        key = (row["state_code"], row["loc_type"], row["city_name"])
        lookup[key] = int(row["location_key"])
    return lookup


def _build_fact_table(df, dim_time, dim_location, dim_disease, populations):
    """Vectorized fact table build. Population is NULL for CITY-grain rows."""
    fact = df[["epi_week", "state", "loc", "loc_type", "disease",
               "cases", "incidence_per_100000"]].copy()

    # Time key
    time_map = dict(zip(dim_time["epi_week"], dim_time["time_key"]))
    fact["time_key"] = fact["epi_week"].map(time_map)

    # Disease key
    disease_map = dict(zip(dim_disease["disease_name"], dim_disease["disease_key"]))
    fact["disease_key"] = fact["disease"].map(disease_map)

    # Location key, joined separately for STATE and CITY then concatenated
    state_locs = dim_location[dim_location["loc_type"] == "STATE"][["state_code", "location_key"]]
    state_locs = state_locs.rename(columns={"state_code": "state", "location_key": "lk_state"})
    city_locs = dim_location[dim_location["loc_type"] == "CITY"][["state_code", "city_name", "location_key"]]
    city_locs = city_locs.rename(columns={"state_code": "state", "city_name": "loc", "location_key": "lk_city"})

    fact = fact.merge(state_locs, on="state", how="left")
    fact = fact.merge(city_locs, on=["state", "loc"], how="left")
    fact["location_key"] = np.where(
        fact["loc_type"] == "STATE",
        fact["lk_state"],
        fact["lk_city"],
    )

    # Population, joined on state and decade. NULL for CITY rows.
    fact["year"] = fact["epi_week"] // 100
    fact["decade"] = (fact["year"] // 10) * 10
    pop_df = populations.rename(columns={"state_code": "state"})[["state", "decade", "population"]]
    fact = fact.merge(pop_df, on=["state", "decade"], how="left")
    fact.loc[fact["loc_type"] == "CITY", "population"] = np.nan

    fact = fact.dropna(subset=["time_key", "location_key", "disease_key"])
    fact["case_count"] = fact["cases"].astype(int)
    fact["incidence_rate"] = fact["incidence_per_100000"].round(4)
    fact["population"] = fact["population"].astype("Int64")
    fact = fact[["time_key", "location_key", "disease_key", "case_count",
                 "incidence_rate", "population"]].reset_index(drop=True)
    fact["time_key"] = fact["time_key"].astype(int)
    fact["location_key"] = fact["location_key"].astype(int)
    fact["disease_key"] = fact["disease_key"].astype(int)
    fact["incidence_key"] = range(1, len(fact) + 1)
    print("  fact_table %d rows" % len(fact))
    return fact


def _build_mongo_documents(df, regions, diseases):
    """Vectorized mongo bucket build. One bucket per disease, location, year, month."""
    df = df.copy()
    df["year"] = df["epi_week"] // 100
    df["week_number"] = df["epi_week"] % 100
    df["month"] = np.minimum(12, np.maximum(1, ((df["week_number"] - 1) * 12 // 52) + 1))

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

    df = df.sort_values(["disease", "state", "loc", "loc_type", "year", "month", "epi_week"])

    docs = []
    group_cols = ["disease", "state", "loc", "loc_type", "year", "month"]
    for keys, group in df.groupby(group_cols, sort=False):
        disease, state, loc, loc_type, year, month = keys
        region_info = region_map.get(state, {})
        disease_info = disease_map.get(disease, {})

        epi_weeks = group["epi_week"].astype(int).tolist()
        weeks = group["week_number"].astype(int).tolist()
        cases_list = group["cases"].astype(int).tolist()
        rates = group["incidence_per_100000"].astype(float).round(4).tolist()
        weekly_obs = [
            {"epi_week": ew, "week": w, "cases": c, "incidence_rate": r}
            for ew, w, c, r in zip(epi_weeks, weeks, cases_list, rates)
        ]

        total_cases = int(sum(cases_list))
        avg_rate = float(round(sum(rates) / len(rates), 4)) if rates else 0.0
        peak = int(max(cases_list)) if cases_list else 0
        quarter = int((int(month) - 1) // 3 + 1)
        decade = int((int(year) // 10) * 10)

        doc = {
            "disease": {
                "name": disease,
                "category": disease_info.get("category", ""),
                "transmission": disease_info.get("transmission", ""),
                "vaccine_preventable": disease_info.get("vaccine_preventable", False),
                "vaccine_year": disease_info.get("vaccine_year"),
            },
            "location": {
                "city": loc if loc_type == "CITY" else None,
                "state_code": state,
                "state_name": region_info.get("state_name", state),
                "region": region_info.get("region", ""),
                "division": region_info.get("division", ""),
                "loc_type": loc_type,
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

    print("  mongo_documents %d bucket documents" % len(docs))
    return docs


def _build_neo4j_data(df, dim_time, dim_location, dim_disease, borders):
    """Construct Neo4j node and relationship payloads for batch loading."""
    nodes = {}
    rels = {}

    nodes["Disease"] = dim_disease[[
        "disease_name", "disease_category", "transmission_type",
        "is_vaccine_preventable", "vaccine_intro_year",
    ]].to_dict("records")

    state_only = dim_location[dim_location["loc_type"] == "STATE"]
    regions = state_only[["census_region"]].drop_duplicates()
    nodes["Region"] = [{"name": r} for r in sorted(regions["census_region"].dropna().unique())]

    nodes["State"] = state_only[[
        "state_code", "state_name", "census_region",
        "census_division", "latitude", "longitude",
    ]].to_dict("records")

    city_only = dim_location[dim_location["loc_type"] == "CITY"]
    nodes["City"] = city_only[["city_name", "state_code", "census_region"]].to_dict("records")

    decades = sorted(dim_time["decade"].unique())
    nodes["Decade"] = [{"decade": int(d)} for d in decades]

    years = sorted(dim_time["year"].unique())
    nodes["Year"] = [{"year": int(y)} for y in years]

    quarter_set = set()
    for _, row in dim_time.iterrows():
        quarter_set.add((int(row["year"]), int(row["quarter"])))
    nodes["Quarter"] = [{"year": y, "quarter": q} for y, q in sorted(quarter_set)]

    month_set = set()
    for _, row in dim_time.iterrows():
        month_set.add((int(row["year"]), int(row["month"]), row["month_name"]))
    nodes["Month"] = [{"year": y, "month": m, "month_name": mn} for y, m, mn in sorted(month_set)]

    nodes["Week"] = dim_time[["epi_week", "week_number", "year", "month"]].to_dict("records")

    rels["IN_REGION"] = state_only[["state_code", "census_region"]].to_dict("records")

    rels["LOCATED_IN_STATE"] = city_only[["city_name", "state_code"]].to_dict("records")

    rels["IN_MONTH"] = dim_time[["epi_week", "year", "month"]].to_dict("records")

    rels["IN_QUARTER"] = [
        {"year": m["year"], "month": m["month"], "quarter": (m["month"] - 1) // 3 + 1}
        for m in nodes["Month"]
    ]

    rels["IN_YEAR_Q"] = [{"year": q["year"], "quarter": q["quarter"]} for q in nodes["Quarter"]]

    rels["IN_DECADE"] = [{"year": y, "decade": (y // 10) * 10} for y in years]

    border_rels = []
    for _, row in borders.iterrows():
        state = row["state_code"]
        for neighbor in str(row["borders"]).split():
            border_rels.append({"from": state, "to": neighbor.strip()})
    rels["BORDERS"] = border_rels

    enriched = df.copy()
    enriched["year"] = enriched["epi_week"] // 100
    rels["OBSERVATIONS"] = enriched[[
        "epi_week", "state", "loc", "loc_type", "disease", "cases", "incidence_per_100000",
    ]].to_dict("records")

    print("  neo4j nodes %d total across %d labels" % (sum(len(v) for v in nodes.values()), len(nodes)))
    print("  neo4j rels  %d total across %d types" % (sum(len(v) for v in rels.values()), len(rels)))
    return nodes, rels


def transform_all(raw):
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
        source_label=raw.source_label,
    )


if __name__ == "__main__":
    from etl.extract import extract_all

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw = extract_all(
        os.path.join(base, "data", "raw"),
        os.path.join(base, "data", "reference"),
    )
    result = transform_all(raw)
    print("\n=== Transform Summary (source=%s) ===" % result.source_label)
    print("dim_time:    %d" % len(result.dim_time))
    print("dim_location:%d" % len(result.dim_location))
    print("dim_disease: %d" % len(result.dim_disease))
    print("fact_table:  %d" % len(result.fact_table))
    print("mongo_docs:  %d" % len(result.mongo_documents))
    print("neo4j_nodes: %d labels" % len(result.neo4j_nodes))
    print("neo4j_rels:  %d types" % len(result.neo4j_rels))
