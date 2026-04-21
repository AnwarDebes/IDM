"""
MongoDB query service — implements all 10 decision support queries
using aggregation pipelines against the bucket-pattern document model.
"""

import json
import time
from typing import Any

import pymongo

from app.config import settings


class MongoService:
    def __init__(self):
        self.client = pymongo.MongoClient(
            host=settings.mongo_host,
            port=settings.mongo_port,
            username=settings.mongo_user,
            password=settings.mongo_password,
        )
        self.db = self.client[settings.mongo_db]

    def execute(self, collection: str, pipeline: list) -> tuple[list[dict], float, str]:
        """Execute an aggregation pipeline and return (results, time_ms, pipeline_text)."""
        start = time.time()
        results = list(self.db[collection].aggregate(pipeline))
        elapsed_ms = round((time.time() - start) * 1000, 2)
        # Clean ObjectId for JSON serialization
        for r in results:
            if "_id" in r and not isinstance(r["_id"], (str, int, float, dict)):
                r["_id"] = str(r["_id"])
        pipeline_text = json.dumps(pipeline, indent=2, default=str)
        return results, elapsed_ms, pipeline_text

    # ------------------------------------------------------------------
    # Q1: Total cases by disease, by decade
    # ------------------------------------------------------------------
    def q1(self, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$group": {
                "_id": {"disease": "$disease.name", "decade": "$time_bucket.decade"},
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
                "avg_incidence": {"$avg": "$monthly_summary.avg_incidence_rate"},
            }},
            {"$sort": {"_id.disease": 1, "_id.decade": 1}},
            {"$project": {
                "_id": 0,
                "disease_name": "$_id.disease",
                "decade": "$_id.decade",
                "total_cases": 1,
                "avg_incidence": {"$round": ["$avg_incidence", 2]},
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q2: Measles incidence by state for a specific year
    # ------------------------------------------------------------------
    def q2(self, year: int = 1960, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$match": {"disease.name": "MEASLES", "time_bucket.year": year}},
            {"$group": {
                "_id": "$location.state_name",
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
                "avg_incidence": {"$avg": "$monthly_summary.avg_incidence_rate"},
            }},
            {"$sort": {"total_cases": -1}},
            {"$project": {
                "_id": 0,
                "state_name": "$_id",
                "total_cases": 1,
                "avg_incidence": {"$round": ["$avg_incidence", 2]},
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q3: Top 10 states by total cases for disease + time range
    # ------------------------------------------------------------------
    def q3(self, disease: str = "MEASLES", start_year: int = 1950, end_year: int = 1970, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$match": {
                "disease.name": disease,
                "time_bucket.year": {"$gte": start_year, "$lte": end_year},
            }},
            {"$group": {
                "_id": "$location.state_name",
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$sort": {"total_cases": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "state_name": "$_id", "total_cases": 1}},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q4: Seasonal pattern — avg weekly cases per month
    # ------------------------------------------------------------------
    def q4(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$match": {"disease.name": disease}},
            {"$unwind": "$weekly_observations"},
            {"$group": {
                "_id": {"month": "$time_bucket.month", "month_name": "$time_bucket.month_name"},
                "avg_weekly_cases": {"$avg": "$weekly_observations.cases"},
            }},
            {"$sort": {"_id.month": 1}},
            {"$project": {
                "_id": 0,
                "month": "$_id.month",
                "month_name": "$_id.month_name",
                "avg_weekly_cases": {"$round": ["$avg_weekly_cases", 2]},
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q5: Year-over-year change using $setWindowFields
    # ------------------------------------------------------------------
    def q5(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$match": {"disease.name": disease}},
            {"$group": {
                "_id": {"state": "$location.state_name", "year": "$time_bucket.year"},
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$sort": {"_id.state": 1, "_id.year": 1}},
            {"$setWindowFields": {
                "partitionBy": "$_id.state",
                "sortBy": {"_id.year": 1},
                "output": {
                    "prev_year_cases": {
                        "$shift": {"output": "$total_cases", "by": -1}
                    }
                }
            }},
            {"$addFields": {
                "yoy_pct_change": {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": ["$prev_year_cases", None]},
                            {"$ne": ["$prev_year_cases", 0]},
                        ]},
                        "then": {"$round": [
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$total_cases", "$prev_year_cases"]},
                                    "$prev_year_cases"
                                ]},
                                100
                            ]},
                            2
                        ]},
                        "else": None
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "state_name": "$_id.state",
                "year": "$_id.year",
                "total_cases": 1,
                "prev_year_cases": 1,
                "yoy_pct_change": 1,
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q6: Disease co-occurrence by state and time
    # ------------------------------------------------------------------
    def q6(self, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$group": {
                "_id": {
                    "state": "$location.state_code",
                    "year": "$time_bucket.year",
                    "disease": "$disease.name",
                },
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$match": {"total_cases": {"$gt": 100}}},
            {"$group": {
                "_id": {"state": "$_id.state", "year": "$_id.year"},
                "diseases": {"$push": {"name": "$_id.disease", "cases": "$total_cases"}},
            }},
            {"$match": {"diseases.1": {"$exists": True}}},
            {"$unwind": {"path": "$diseases", "includeArrayIndex": "i"}},
            {"$lookup": {
                "from": "disease_observations",
                "pipeline": [{"$limit": 0}],
                "as": "_dummy",
            }},
            # Simplified: just count co-occurring disease pairs
            {"$group": {
                "_id": {"state": "$_id.state", "year": "$_id.year"},
                "disease_list": {"$push": "$diseases.name"},
            }},
            {"$unwind": {"path": "$disease_list", "includeArrayIndex": "idx"}},
            {"$group": {
                "_id": "$disease_list",
                "co_occurrence_count": {"$sum": 1},
            }},
            {"$sort": {"co_occurrence_count": -1}},
            {"$project": {
                "_id": 0,
                "disease_a": "$_id",
                "co_occurrence_count": 1,
            }},
        ]
        # Simplified co-occurrence: count how often each disease appears in state-years with multiple diseases
        pipeline2 = [
            {"$group": {
                "_id": {
                    "state": "$location.state_code",
                    "year": "$time_bucket.year",
                    "disease": "$disease.name",
                },
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$match": {"total_cases": {"$gt": 100}}},
            {"$group": {
                "_id": {"state": "$_id.state", "year": "$_id.year"},
                "diseases": {"$addToSet": "$_id.disease"},
                "count": {"$sum": 1},
            }},
            {"$match": {"count": {"$gte": 2}}},
            {"$unwind": "$diseases"},
            {"$group": {
                "_id": "$diseases",
                "co_occurrence_count": {"$sum": 1},
            }},
            {"$sort": {"co_occurrence_count": -1}},
            {"$project": {
                "_id": 0,
                "disease_a": "$_id",
                "co_occurrence_count": 1,
            }},
        ]
        return self.execute("disease_observations", pipeline2)

    # ------------------------------------------------------------------
    # Q7: Geographic spread — rank states by first report
    # ------------------------------------------------------------------
    def q7(self, disease: str = "MEASLES", **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$match": {"disease.name": disease, "monthly_summary.total_cases": {"$gt": 0}}},
            {"$group": {
                "_id": "$location.state_name",
                "first_reported_year": {"$min": "$time_bucket.year"},
            }},
            {"$sort": {"first_reported_year": 1}},
            {"$group": {
                "_id": None,
                "states": {"$push": {"state_name": "$_id", "first_reported_year": "$first_reported_year"}},
            }},
            {"$unwind": {"path": "$states", "includeArrayIndex": "rank"}},
            {"$project": {
                "_id": 0,
                "state_name": "$states.state_name",
                "first_reported_year": "$states.first_reported_year",
                "spread_rank": {"$add": ["$rank", 1]},
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q8: Vaccination impact — before/after comparison
    # ------------------------------------------------------------------
    def q8(self, **kwargs) -> tuple[list[dict], float, str]:
        start = time.time()
        diseases = list(self.db.disease_observations.aggregate([
            {"$group": {"_id": {
                "name": "$disease.name",
                "vaccine_year": "$disease.vaccine_year",
            }}},
        ]))

        results = []
        for d in diseases:
            name = d["_id"]["name"]
            vy = d["_id"]["vaccine_year"]
            if vy is None or vy < 1900:
                continue

            # Pre-vaccine avg annual cases
            pre = list(self.db.disease_observations.aggregate([
                {"$match": {
                    "disease.name": name,
                    "time_bucket.year": {"$gte": vy - 10, "$lt": vy},
                }},
                {"$group": {
                    "_id": "$time_bucket.year",
                    "yearly_total": {"$sum": "$monthly_summary.total_cases"},
                }},
                {"$group": {
                    "_id": None,
                    "avg_cases": {"$avg": "$yearly_total"},
                }},
            ]))

            post = list(self.db.disease_observations.aggregate([
                {"$match": {
                    "disease.name": name,
                    "time_bucket.year": {"$gt": vy, "$lte": vy + 10},
                }},
                {"$group": {
                    "_id": "$time_bucket.year",
                    "yearly_total": {"$sum": "$monthly_summary.total_cases"},
                }},
                {"$group": {
                    "_id": None,
                    "avg_cases": {"$avg": "$yearly_total"},
                }},
            ]))

            pre_avg = pre[0]["avg_cases"] if pre else 0
            post_avg = post[0]["avg_cases"] if post else 0
            pct = round((post_avg - pre_avg) / pre_avg * 100, 2) if pre_avg else 0

            results.append({
                "disease_name": name,
                "vaccine_intro_year": vy,
                "pre_vaccine_avg_annual_cases": round(pre_avg, 0),
                "post_vaccine_avg_annual_cases": round(post_avg, 0),
                "pct_change": pct,
            })

        results.sort(key=lambda x: x["pct_change"])
        pipeline_text = "Multiple aggregations per disease (pre/post vaccine comparison)"
        elapsed_ms = round((time.time() - start) * 1000, 2)
        return results, elapsed_ms, pipeline_text

    # ------------------------------------------------------------------
    # Q9: Anomaly detection — states > 2 std dev above national mean
    # ------------------------------------------------------------------
    def q9(self, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$group": {
                "_id": {
                    "disease": "$disease.name",
                    "state": "$location.state_name",
                    "year": "$time_bucket.year",
                },
                "state_total": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$group": {
                "_id": {"disease": "$_id.disease", "year": "$_id.year"},
                "states": {"$push": {"state": "$_id.state", "total": "$state_total"}},
                "national_mean": {"$avg": "$state_total"},
                "national_stddev": {"$stdDevPop": "$state_total"},
            }},
            {"$match": {"national_stddev": {"$gt": 0}}},
            {"$unwind": "$states"},
            {"$addFields": {
                "z_score": {
                    "$divide": [
                        {"$subtract": ["$states.total", "$national_mean"]},
                        "$national_stddev",
                    ]
                }
            }},
            {"$match": {"z_score": {"$gt": 2}}},
            {"$sort": {"z_score": -1}},
            {"$limit": 50},
            {"$project": {
                "_id": 0,
                "disease_name": "$_id.disease",
                "state_name": "$states.state",
                "year": "$_id.year",
                "state_total": "$states.total",
                "national_mean": {"$round": ["$national_mean", 0]},
                "z_score": {"$round": ["$z_score", 2]},
            }},
        ]
        return self.execute("disease_observations", pipeline)

    # ------------------------------------------------------------------
    # Q10: Cross-disease normalized trend comparison
    # ------------------------------------------------------------------
    def q10(self, **kwargs) -> tuple[list[dict], float, str]:
        pipeline = [
            {"$group": {
                "_id": {"disease": "$disease.name", "year": "$time_bucket.year"},
                "total_cases": {"$sum": "$monthly_summary.total_cases"},
            }},
            {"$sort": {"_id.disease": 1, "_id.year": 1}},
            {"$group": {
                "_id": "$_id.disease",
                "max_cases": {"$max": "$total_cases"},
                "years": {"$push": {"year": "$_id.year", "total_cases": "$total_cases"}},
            }},
            {"$unwind": "$years"},
            {"$project": {
                "_id": 0,
                "disease_name": "$_id",
                "year": "$years.year",
                "total_cases": "$years.total_cases",
                "normalized_index": {
                    "$round": [
                        {"$multiply": [
                            {"$divide": [
                                "$years.total_cases",
                                {"$max": [1, "$max_cases"]},
                            ]},
                            100,
                        ]},
                        2,
                    ]
                },
            }},
            {"$sort": {"disease_name": 1, "year": 1}},
        ]
        return self.execute("disease_observations", pipeline)
