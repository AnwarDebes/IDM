"""
Load data into MongoDB using the bucket pattern. Each bucket holds all weekly
observations for one (disease, location, year, month) cell, with a precomputed
monthly summary subdocument. Handles both STATE-grain and CITY-grain rows.
"""

import os
import sys
import time

import numpy as np
import pandas as pd
import pymongo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def get_client():
    return pymongo.MongoClient(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", 27017)),
        username=os.environ.get("MONGO_USER", "dw_admin"),
        password=os.environ.get("MONGO_PASSWORD", "dw_secure_2024"),
    )


def load_mongo():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start = time.time()

    active_file = os.environ.get("TYCHO_DATA_FILE", "tycho_level1.csv")
    print("Reading source data (%s)..." % active_file)
    tycho = pd.read_csv(os.path.join(base, "data", "raw", active_file), low_memory=False)
    regions = pd.read_csv(os.path.join(base, "data", "reference", "us_regions.csv"))
    diseases = pd.read_csv(os.path.join(base, "data", "reference", "disease_metadata.csv"))
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

    region_map = {}
    for _, r in regions.iterrows():
        region_map[r["state_code"]] = {
            "state_name": r["state_name"],
            "region": r["census_region"],
            "division": r["census_division"],
            "lat": float(r["latitude"]),
            "lng": float(r["longitude"]),
        }

    disease_map = {}
    for _, d in diseases.iterrows():
        disease_map[d["disease_name"]] = {
            "category": d["disease_category"],
            "transmission": d["transmission_type"],
            "vaccine_preventable": str(d["is_vaccine_preventable"]).lower() == "true",
            "vaccine_year": int(d["vaccine_intro_year"]),
        }

    print("Building bucket documents...")
    tycho = tycho.sort_values(["disease", "state", "loc", "loc_type", "year", "month", "epi_week"])
    docs = []
    group_cols = ["disease", "state", "loc", "loc_type", "year", "month"]
    for keys, group in tycho.groupby(group_cols, sort=False):
        disease, state, loc, loc_type, year, month = keys
        ri = region_map.get(state, {})
        di = disease_map.get(disease, {})

        epi_weeks = group["epi_week"].astype(int).tolist()
        weeks = group["week_number"].astype(int).tolist()
        cases_list = group["cases"].astype(int).tolist()
        rates = group["incidence_per_100000"].astype(float).round(4).tolist()
        weekly_obs = [
            {"epi_week": ew, "week": w, "cases": c, "incidence_rate": r}
            for ew, w, c, r in zip(epi_weeks, weeks, cases_list, rates)
        ]

        total_cases = int(sum(cases_list))
        avg_rate = round(sum(rates) / len(rates), 4) if rates else 0.0
        peak = int(max(cases_list)) if cases_list else 0
        quarter = (int(month) - 1) // 3 + 1
        decade = (int(year) // 10) * 10

        docs.append({
            "disease": {
                "name": disease,
                "category": di.get("category", ""),
                "transmission": di.get("transmission", ""),
                "vaccine_preventable": di.get("vaccine_preventable", False),
                "vaccine_year": di.get("vaccine_year"),
            },
            "location": {
                "city": loc if loc_type == "CITY" else None,
                "state_code": state,
                "state_name": ri.get("state_name", state),
                "region": ri.get("region", ""),
                "division": ri.get("division", ""),
                "loc_type": loc_type,
                "coordinates": {"lat": ri.get("lat"), "lng": ri.get("lng")},
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
        })

        if len(docs) % 50000 == 0:
            print("  Built %d documents..." % len(docs))

    print("  Total bucket documents %d" % len(docs))

    client = get_client()
    db = client["epidemiological_dw"]

    print("Dropping existing collections...")
    db.disease_observations.drop()
    db.summary_monthly_by_region.drop()
    db.summary_decade_national.drop()

    print("Inserting %d documents in batches..." % len(docs))
    batch_size = 10000
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        db.disease_observations.insert_many(batch)
        print("  %d / %d inserted" % (min(i + batch_size, len(docs)), len(docs)))

    print("Creating indexes...")
    db.disease_observations.create_index([
        ("disease.name", 1), ("time_bucket.year", 1), ("location.state_code", 1)
    ])
    db.disease_observations.create_index([
        ("location.region", 1), ("time_bucket.decade", 1)
    ])
    db.disease_observations.create_index([
        ("time_bucket.year", 1), ("time_bucket.month", 1)
    ])
    db.disease_observations.create_index([
        ("location.loc_type", 1), ("location.city", 1)
    ])

    print("Running pre-aggregation summary_monthly_by_region...")
    db.disease_observations.aggregate([
        {"$match": {"location.loc_type": "STATE"}},
        {"$group": {
            "_id": {
                "disease": "$disease.name",
                "region": "$location.region",
                "year": "$time_bucket.year",
                "month": "$time_bucket.month",
            },
            "total_cases": {"$sum": "$monthly_summary.total_cases"},
            "avg_incidence_rate": {"$avg": "$monthly_summary.avg_incidence_rate"},
            "states_reporting": {"$addToSet": "$location.state_code"},
        }},
        {"$addFields": {"states_reporting": {"$size": "$states_reporting"}}},
        {"$merge": {"into": "summary_monthly_by_region", "whenMatched": "replace"}},
    ])

    print("Running pre-aggregation summary_decade_national...")
    db.disease_observations.aggregate([
        {"$group": {
            "_id": {
                "disease": "$disease.name",
                "decade": "$time_bucket.decade",
            },
            "total_cases": {"$sum": "$monthly_summary.total_cases"},
            "avg_incidence_rate": {"$avg": "$monthly_summary.avg_incidence_rate"},
            "states_reporting": {"$addToSet": "$location.state_code"},
        }},
        {"$addFields": {"states_reporting": {"$size": "$states_reporting"}}},
        {"$merge": {"into": "summary_decade_national", "whenMatched": "replace"}},
    ])

    obs_count = db.disease_observations.count_documents({})
    monthly_count = db.summary_monthly_by_region.count_documents({})
    decade_count = db.summary_decade_national.count_documents({})

    elapsed = time.time() - start
    print("\nMongoDB load complete in %.1fs" % elapsed)
    print("  disease_observations %d" % obs_count)
    print("  summary_monthly_by_region %d" % monthly_count)
    print("  summary_decade_national %d" % decade_count)

    client.close()


if __name__ == "__main__":
    load_mongo()
