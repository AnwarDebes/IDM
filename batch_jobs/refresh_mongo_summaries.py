"""
Batch job: Refresh MongoDB pre-aggregated summary collections.
"""

import os
import sys
import time

import pymongo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_client():
    return pymongo.MongoClient(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", 27017)),
        username=os.environ.get("MONGO_USER", "dw_admin"),
        password=os.environ.get("MONGO_PASSWORD", "dw_secure_2024"),
    )


def refresh():
    client = get_client()
    db = client["epidemiological_dw"]
    start = time.time()

    print("Refreshing summary_monthly_by_region...")
    db.summary_monthly_by_region.drop()
    db.disease_observations.aggregate([
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
    print(f"  summary_monthly_by_region: {db.summary_monthly_by_region.count_documents({}):,} docs")

    print("Refreshing summary_decade_national...")
    db.summary_decade_national.drop()
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
    print(f"  summary_decade_national: {db.summary_decade_national.count_documents({}):,} docs")

    elapsed = time.time() - start
    print(f"\nMongo summaries refreshed in {elapsed:.1f}s")
    client.close()


if __name__ == "__main__":
    refresh()
