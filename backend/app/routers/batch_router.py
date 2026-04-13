"""
Batch operations router — refresh summaries and check status.
"""

import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from app.services.postgres_service import PostgresService
from app.services.mongo_service import MongoService

router = APIRouter(prefix="/api/v1/batch", tags=["batch"])

_pg = PostgresService()
_mongo = MongoService()

# In-memory tracking of last refresh times
_last_refresh: dict[str, str] = {}


@router.post("/refresh-summaries")
def refresh_summaries():
    """Trigger refresh of pre-aggregated summaries across backends."""
    results = {}

    # PostgreSQL: refresh materialized views
    try:
        start = time.time()
        conn = _pg._get_conn()
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW mv_disease_decade_summary")
        cur.execute("REFRESH MATERIALIZED VIEW mv_state_disease_annual")
        cur.execute("REFRESH MATERIALIZED VIEW mv_national_weekly_trend")
        conn.commit()
        cur.close()
        conn.close()
        elapsed = round((time.time() - start) * 1000, 2)
        results["postgres"] = {"status": "success", "execution_time_ms": elapsed}
    except Exception as e:
        results["postgres"] = {"status": "error", "error": str(e)}

    # MongoDB: re-run summary aggregation pipelines
    try:
        start = time.time()
        db = _mongo.db

        # summary_monthly_by_region
        db.disease_observations.aggregate([
            {"$unwind": "$weekly_observations"},
            {"$lookup": {
                "from": "region_lookup",
                "localField": "state",
                "foreignField": "state_code",
                "as": "region_info",
            }},
            {"$addFields": {"region": {"$arrayElemAt": ["$region_info.census_region", 0]}}},
            {"$group": {
                "_id": {"disease": "$disease", "year": "$year", "month": "$month", "region": "$region"},
                "total_cases": {"$sum": "$weekly_observations.cases"},
                "avg_incidence": {"$avg": "$weekly_observations.incidence_rate"},
                "weeks_reporting": {"$sum": 1},
            }},
            {"$merge": {"into": "summary_monthly_by_region", "whenMatched": "replace"}},
        ])

        # summary_decade_national
        db.disease_observations.aggregate([
            {"$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}}},
            {"$group": {
                "_id": {"disease": "$disease", "decade": "$decade"},
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
                "avg_incidence": {"$avg": "$monthly_summary.avg_incidence_rate"},
                "states_reporting": {"$addToSet": "$state"},
            }},
            {"$addFields": {"state_count": {"$size": "$states_reporting"}}},
            {"$project": {"states_reporting": 0}},
            {"$merge": {"into": "summary_decade_national", "whenMatched": "replace"}},
        ])

        elapsed = round((time.time() - start) * 1000, 2)
        results["mongodb"] = {"status": "success", "execution_time_ms": elapsed}
    except Exception as e:
        results["mongodb"] = {"status": "error", "error": str(e)}

    now = datetime.now(timezone.utc).isoformat()
    _last_refresh["last_refresh"] = now
    _last_refresh["details"] = results

    return {"message": "Refresh complete", "timestamp": now, "results": results}


@router.get("/status")
def batch_status():
    """Return last refresh timestamps."""
    if not _last_refresh:
        return {"message": "No refresh has been run yet", "last_refresh": None}
    return _last_refresh
