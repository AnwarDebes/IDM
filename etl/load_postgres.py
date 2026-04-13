"""
Load data into the PostgreSQL star schema.
Uses vectorized pandas operations and COPY for fast bulk loading.
"""

import io
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 15432)),
        user=os.environ.get("POSTGRES_USER", "dw_admin"),
        password=os.environ.get("POSTGRES_PASSWORD", "dw_secure_2024"),
        dbname=os.environ.get("POSTGRES_DB", "epidemiological_dw"),
    )


def load_postgres_direct():
    """Build dimensions and facts directly with vectorized ops, then bulk load."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start = time.time()

    # Read source data
    print("Reading source data...")
    tycho = pd.read_csv(os.path.join(base, "data", "raw", "tycho_level1.csv"))
    regions = pd.read_csv(os.path.join(base, "data", "reference", "us_regions.csv"))
    diseases = pd.read_csv(os.path.join(base, "data", "reference", "disease_metadata.csv"))
    populations = pd.read_csv(os.path.join(base, "data", "reference", "state_populations.csv"))
    print(f"  Raw rows: {len(tycho):,}")

    # Clean
    tycho = tycho.dropna(subset=["cases", "disease", "state", "epi_week"])
    tycho["cases"] = pd.to_numeric(tycho["cases"], errors="coerce")
    tycho = tycho[tycho["cases"] > 0].copy()
    tycho["cases"] = tycho["cases"].astype(int)
    tycho["disease"] = tycho["disease"].str.strip().str.upper()
    tycho["state"] = tycho["state"].str.strip().str.upper()
    tycho["epi_week"] = tycho["epi_week"].astype(int)
    tycho["incidence_per_100000"] = pd.to_numeric(tycho["incidence_per_100000"], errors="coerce").fillna(0.0)
    print(f"  After cleaning: {len(tycho):,}")

    # === Build dim_time ===
    print("Building dim_time...")
    unique_weeks = pd.DataFrame({"epi_week": sorted(tycho["epi_week"].unique())})
    unique_weeks["week_number"] = unique_weeks["epi_week"] % 100
    unique_weeks["year"] = unique_weeks["epi_week"] // 100
    unique_weeks["month"] = ((unique_weeks["week_number"] - 1) * 12 // 52 + 1).clip(1, 12)
    unique_weeks["month_name"] = unique_weeks["month"].map(MONTH_NAMES)
    unique_weeks["quarter"] = (unique_weeks["month"] - 1) // 3 + 1
    unique_weeks["decade"] = (unique_weeks["year"] // 10) * 10
    unique_weeks["century"] = unique_weeks["year"] // 100
    unique_weeks["is_summer"] = unique_weeks["month"].isin([6, 7, 8])
    unique_weeks["is_flu_season"] = unique_weeks["month"].isin([10, 11, 12, 1, 2, 3])
    unique_weeks["time_key"] = range(1, len(unique_weeks) + 1)
    print(f"  dim_time: {len(unique_weeks):,} rows")

    # === Build dim_location ===
    print("Building dim_location...")
    states = tycho[["state"]].drop_duplicates().rename(columns={"state": "state_code"})
    dim_loc = states.merge(regions, on="state_code", how="left")
    dim_loc["city_name"] = None
    dim_loc["loc_type"] = "STATE"
    dim_loc["location_key"] = range(1, len(dim_loc) + 1)
    print(f"  dim_location: {len(dim_loc):,} rows")

    # === Build dim_disease ===
    print("Building dim_disease...")
    dim_disease = diseases.copy()
    dim_disease["disease_key"] = range(1, len(dim_disease) + 1)
    dim_disease["is_vaccine_preventable"] = dim_disease["is_vaccine_preventable"].map(
        lambda x: True if str(x).lower() == "true" else False
    )
    print(f"  dim_disease: {len(dim_disease):,} rows")

    # === Build fact table via merge (vectorized) ===
    print("Building fact table (vectorized merge)...")
    time_map = unique_weeks[["epi_week", "time_key"]]
    loc_map = dim_loc[["state_code", "location_key"]]
    disease_map = dim_disease[["disease_name", "disease_key"]]

    fact = tycho.merge(time_map, on="epi_week", how="inner")
    fact = fact.merge(loc_map, left_on="state", right_on="state_code", how="inner")
    fact = fact.merge(disease_map, left_on="disease", right_on="disease_name", how="inner")

    # Add population
    fact["year"] = fact["epi_week"] // 100
    fact["decade"] = (fact["year"] // 10) * 10
    pop_df = populations.rename(columns={"population": "pop"})
    pop_df["decade"] = pop_df["decade"].astype(int)
    fact = fact.merge(pop_df, left_on=["state", "decade"], right_on=["state_code", "decade"], how="left")

    fact["incidence_key"] = range(1, len(fact) + 1)
    fact = fact.rename(columns={"cases": "case_count", "incidence_per_100000": "incidence_rate", "pop": "population"})
    fact_cols_needed = ["incidence_key", "time_key", "location_key", "disease_key",
                        "case_count", "incidence_rate", "population"]
    fact = fact[fact_cols_needed].copy()
    print(f"  fact_table: {len(fact):,} rows")

    # === Load into PostgreSQL ===
    conn = get_connection()
    cur = conn.cursor()

    print("\nTruncating existing data...")
    cur.execute("TRUNCATE fact_disease_incidence, dim_time, dim_location, dim_disease CASCADE")
    conn.commit()

    # dim_time
    print(f"Loading dim_time...")
    time_cols = ["time_key", "epi_week", "week_number", "month", "month_name",
                 "quarter", "year", "decade", "century", "is_summer", "is_flu_season"]
    _copy_df(cur, conn, unique_weeks[time_cols], "dim_time", time_cols)

    # dim_location
    print(f"Loading dim_location...")
    loc_cols_db = ["location_key", "city_name", "state_code", "state_name",
                   "census_region", "census_division", "loc_type", "latitude", "longitude"]
    _copy_df(cur, conn, dim_loc[loc_cols_db], "dim_location", loc_cols_db)

    # dim_disease
    print(f"Loading dim_disease...")
    disease_cols_db = ["disease_key", "disease_name", "disease_category",
                       "transmission_type", "is_vaccine_preventable", "vaccine_intro_year"]
    _copy_df(cur, conn, dim_disease[disease_cols_db], "dim_disease", disease_cols_db)

    # fact table via COPY (fast!)
    print(f"Loading fact_disease_incidence ({len(fact):,} rows via COPY)...")
    _copy_df(cur, conn, fact, "fact_disease_incidence", fact_cols_needed)

    # Refresh materialized views
    print("Refreshing materialized views...")
    for mv in ["mv_monthly_disease_state", "mv_yearly_disease_region", "mv_decade_disease_national"]:
        cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
        conn.commit()
        print(f"  {mv} refreshed.")

    cur.execute("ANALYZE")
    conn.commit()

    elapsed = time.time() - start
    print(f"\nPostgreSQL load complete in {elapsed:.1f}s")
    cur.close()
    conn.close()


def _copy_df(cur, conn, df: pd.DataFrame, table: str, columns: list[str]):
    """Bulk load a DataFrame into a table using COPY protocol."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)
    cur.copy_from(buf, table, sep="\t", null="\\N", columns=columns)
    conn.commit()
    print(f"  {table}: {len(df):,} rows loaded.")


if __name__ == "__main__":
    load_postgres_direct()
