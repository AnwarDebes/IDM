"""
Load data into the PostgreSQL star schema. Uses vectorized pandas operations
and COPY for fast bulk loading. Handles both STATE-grain rows and the
CITY-grain rows that the real Project Tycho export uses for diphtheria.
"""

import io
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2

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
    """Build dimensions and facts with vectorized ops, then bulk load through COPY."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start = time.time()

    active_file = os.environ.get("TYCHO_DATA_FILE", "tycho_level1.csv")
    print("Reading source data (%s)..." % active_file)
    tycho = pd.read_csv(os.path.join(base, "data", "raw", active_file), low_memory=False)
    regions = pd.read_csv(os.path.join(base, "data", "reference", "us_regions.csv"))
    diseases = pd.read_csv(os.path.join(base, "data", "reference", "disease_metadata.csv"))
    populations = pd.read_csv(os.path.join(base, "data", "reference", "state_populations.csv"))
    print("  Raw rows %d" % len(tycho))

    # Clean
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
    state_rows = (tycho["loc_type"] == "STATE").sum()
    city_rows = (tycho["loc_type"] == "CITY").sum()
    print("  After cleaning %d rows (STATE=%d, CITY=%d)" % (len(tycho), state_rows, city_rows))

    # --- dim_time ---
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
    print("  dim_time %d rows" % len(unique_weeks))

    # --- dim_location, STATE plus CITY ---
    print("Building dim_location...")
    state_keys = tycho[tycho["loc_type"] == "STATE"][["state"]].drop_duplicates()
    state_keys = state_keys.rename(columns={"state": "state_code"})
    state_dim = state_keys.merge(regions, on="state_code", how="left")
    state_dim["city_name"] = None
    state_dim["loc_type"] = "STATE"

    city_keys = tycho[tycho["loc_type"] == "CITY"][["state", "loc"]].drop_duplicates()
    city_keys = city_keys.rename(columns={"state": "state_code", "loc": "city_name"})
    city_dim = city_keys.merge(regions, on="state_code", how="left")
    city_dim["loc_type"] = "CITY"

    dim_loc = pd.concat([state_dim, city_dim], ignore_index=True)
    dim_loc["location_key"] = range(1, len(dim_loc) + 1)
    print("  dim_location %d rows (STATE=%d, CITY=%d)" % (
        len(dim_loc),
        (dim_loc["loc_type"] == "STATE").sum(),
        (dim_loc["loc_type"] == "CITY").sum(),
    ))

    # --- dim_disease ---
    print("Building dim_disease...")
    dim_disease = diseases.copy()
    dim_disease["disease_key"] = range(1, len(dim_disease) + 1)
    dim_disease["is_vaccine_preventable"] = dim_disease["is_vaccine_preventable"].map(
        lambda x: True if str(x).lower() == "true" else False
    )
    print("  dim_disease %d rows" % len(dim_disease))

    # --- fact table, vectorized ---
    print("Building fact table (vectorized)...")
    time_map = unique_weeks[["epi_week", "time_key"]]
    disease_map = dim_disease[["disease_name", "disease_key"]]

    fact = tycho.merge(time_map, on="epi_week", how="inner")
    fact = fact.merge(disease_map, left_on="disease", right_on="disease_name", how="inner")

    # STATE-grain join
    state_loc = dim_loc[dim_loc["loc_type"] == "STATE"][["state_code", "location_key"]]
    state_loc = state_loc.rename(columns={"state_code": "state", "location_key": "lk_state"})
    fact = fact.merge(state_loc, on="state", how="left")

    # CITY-grain join
    city_loc = dim_loc[dim_loc["loc_type"] == "CITY"][["state_code", "city_name", "location_key"]]
    city_loc = city_loc.rename(columns={"state_code": "state", "city_name": "loc", "location_key": "lk_city"})
    fact = fact.merge(city_loc, on=["state", "loc"], how="left")

    fact["location_key"] = np.where(fact["loc_type"] == "STATE", fact["lk_state"], fact["lk_city"])
    fact = fact.dropna(subset=["location_key"])
    fact["location_key"] = fact["location_key"].astype(int)

    # Population (only meaningful for STATE grain)
    fact["year"] = fact["epi_week"] // 100
    fact["decade"] = (fact["year"] // 10) * 10
    pop_df = populations.rename(columns={"population": "pop"})[["state_code", "decade", "pop"]]
    pop_df["decade"] = pop_df["decade"].astype(int)
    fact = fact.merge(pop_df, left_on=["state", "decade"], right_on=["state_code", "decade"], how="left")
    fact.loc[fact["loc_type"] == "CITY", "pop"] = np.nan
    fact["pop"] = fact["pop"].astype("Int64")

    fact["incidence_key"] = range(1, len(fact) + 1)
    fact = fact.rename(columns={"cases": "case_count", "incidence_per_100000": "incidence_rate", "pop": "population"})
    fact_cols_needed = ["incidence_key", "time_key", "location_key", "disease_key",
                        "case_count", "incidence_rate", "population"]
    fact = fact[fact_cols_needed].copy()
    print("  fact_table %d rows" % len(fact))

    # --- Load ---
    conn = get_connection()
    cur = conn.cursor()

    print("\nTruncating existing data...")
    cur.execute("TRUNCATE fact_disease_incidence, dim_time, dim_location, dim_disease CASCADE")
    conn.commit()

    print("Loading dim_time...")
    time_cols = ["time_key", "epi_week", "week_number", "month", "month_name",
                 "quarter", "year", "decade", "century", "is_summer", "is_flu_season"]
    _copy_df(cur, conn, unique_weeks[time_cols], "dim_time", time_cols)

    print("Loading dim_location...")
    loc_cols_db = ["location_key", "city_name", "state_code", "state_name",
                   "census_region", "census_division", "loc_type", "latitude", "longitude"]
    _copy_df(cur, conn, dim_loc[loc_cols_db], "dim_location", loc_cols_db)

    print("Loading dim_disease...")
    disease_cols_db = ["disease_key", "disease_name", "disease_category",
                       "transmission_type", "is_vaccine_preventable", "vaccine_intro_year"]
    _copy_df(cur, conn, dim_disease[disease_cols_db], "dim_disease", disease_cols_db)

    print("Loading fact_disease_incidence (%d rows via COPY)..." % len(fact))
    _copy_df(cur, conn, fact, "fact_disease_incidence", fact_cols_needed)

    print("Refreshing materialized views...")
    for mv in (
        "mv_monthly_disease_state",
        "mv_yearly_disease_region",
        "mv_decade_disease_national",
        "mv_yearly_disease_city",
    ):
        cur.execute("REFRESH MATERIALIZED VIEW %s" % mv)
        conn.commit()
        print("  %s refreshed" % mv)

    cur.execute("ANALYZE")
    conn.commit()

    elapsed = time.time() - start
    print("\nPostgreSQL load complete in %.1fs" % elapsed)
    cur.close()
    conn.close()


def _copy_df(cur, conn, df, table, columns):
    """Bulk load a DataFrame into a table using the COPY protocol."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)
    cur.copy_from(buf, table, sep="\t", null="\\N", columns=columns)
    conn.commit()
    print("  %s %d rows loaded" % (table, len(df)))


if __name__ == "__main__":
    load_postgres_direct()
