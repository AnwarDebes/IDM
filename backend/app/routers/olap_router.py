"""
OLAP operations router — generic slice, dice, drilldown, rollup, pivot.
"""

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.postgres_service import PostgresService

router = APIRouter(prefix="/api/v1/olap", tags=["olap"])

_pg = PostgresService()


class OlapRequest(BaseModel):
    backend: str = "postgres"
    dimension: str
    value: Any = None
    values: dict[str, Any] = {}
    granularity: str = "year"


@router.post("/slice")
def olap_slice(req: OlapRequest):
    """Slice: fix one dimension, return all others."""
    dim_map = {
        "disease": ("d.disease_name", "dim_disease d", "f.disease_key = d.disease_key"),
        "state": ("l.state_name", "dim_location l", "f.location_key = l.location_key"),
        "year": ("t.year", "dim_time t", "f.time_key = t.time_key"),
        "decade": ("t.decade", "dim_time t", "f.time_key = t.time_key"),
        "region": ("l.census_region", "dim_location l", "f.location_key = l.location_key"),
    }
    if req.dimension not in dim_map:
        raise HTTPException(status_code=400, detail=f"Unknown dimension: {req.dimension}. Options: {list(dim_map.keys())}")

    col, table, join_cond = dim_map[req.dimension]
    query = f"""
        SELECT {col} AS slice_key, SUM(f.case_count) AS total_cases, COUNT(*) AS record_count
        FROM fact_disease_incidence f
        JOIN {table} ON {join_cond}
        WHERE {col} = %s
        GROUP BY {col}
    """
    # For broader context, also return breakdown by another dimension
    breakdown_query = f"""
        SELECT {col} AS slice_key, t2.year, SUM(f.case_count) AS total_cases
        FROM fact_disease_incidence f
        JOIN {table} ON {join_cond}
        JOIN dim_time t2 ON f.time_key = t2.time_key
        WHERE {col} = %s
        GROUP BY {col}, t2.year
        ORDER BY t2.year
    """
    try:
        summary, t1, q1 = _pg.execute(query, (req.value,))
        breakdown, t2, q2 = _pg.execute(breakdown_query, (req.value,))
        return {
            "operation": "slice",
            "dimension": req.dimension,
            "value": req.value,
            "summary": summary,
            "breakdown": breakdown,
            "execution_time_ms": round(t1 + t2, 2),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dice")
def olap_dice(req: OlapRequest):
    """Dice: filter on multiple dimension values."""
    conditions = []
    params = []
    joins = set()
    joins.add("JOIN dim_time t ON f.time_key = t.time_key")
    joins.add("JOIN dim_disease d ON f.disease_key = d.disease_key")
    joins.add("JOIN dim_location l ON f.location_key = l.location_key")

    filter_map = {
        "disease": "d.disease_name = %s",
        "state": "l.state_name = %s",
        "year": "t.year = %s",
        "decade": "t.decade = %s",
        "region": "l.census_region = %s",
    }

    for dim, val in req.values.items():
        if dim in filter_map:
            conditions.append(filter_map[dim])
            params.append(val)

    if not conditions:
        raise HTTPException(status_code=400, detail="Provide at least one dimension filter in 'values'")

    where_clause = " AND ".join(conditions)
    join_clause = " ".join(joins)

    query = f"""
        SELECT d.disease_name, l.state_name, t.year, SUM(f.case_count) AS total_cases
        FROM fact_disease_incidence f
        {join_clause}
        WHERE l.loc_type = 'STATE' AND {where_clause}
        GROUP BY d.disease_name, l.state_name, t.year
        ORDER BY t.year, d.disease_name
    """
    try:
        results, exec_time, query_text = _pg.execute(query, tuple(params))
        return {
            "operation": "dice",
            "filters": req.values,
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/drilldown")
def olap_drilldown(req: OlapRequest):
    """Drill-down: go from coarser to finer granularity."""
    gran_map = {
        "decade": ("t.decade", "t.year", "year"),
        "year": ("t.year", "t.quarter", "quarter"),
        "quarter": ("t.quarter", "t.month", "month"),
        "month": ("t.month", "t.week_number", "week"),
    }
    if req.granularity not in gran_map:
        raise HTTPException(status_code=400, detail=f"Cannot drill down from '{req.granularity}'. Options: {list(gran_map.keys())}")

    coarse_col, fine_col, fine_name = gran_map[req.granularity]
    query = f"""
        SELECT d.disease_name, {coarse_col} AS {req.granularity}, {fine_col} AS {fine_name},
               SUM(f.case_count) AS total_cases
        FROM fact_disease_incidence f
        JOIN dim_disease d ON f.disease_key = d.disease_key
        JOIN dim_time t ON f.time_key = t.time_key
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE l.loc_type = 'STATE'
    """
    params = []
    if req.value is not None:
        query += f" AND {coarse_col} = %s"
        params.append(req.value)
    query += f" GROUP BY d.disease_name, {coarse_col}, {fine_col} ORDER BY d.disease_name, {fine_col}"

    try:
        results, exec_time, query_text = _pg.execute(query, tuple(params))
        return {
            "operation": "drilldown",
            "from_granularity": req.granularity,
            "to_granularity": fine_name,
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rollup")
def olap_rollup(req: OlapRequest):
    """Roll-up: go from finer to coarser granularity."""
    gran_map = {
        "year": ("t.year", "t.decade", "decade"),
        "quarter": ("t.quarter", "t.year", "year"),
        "month": ("t.month", "t.quarter", "quarter"),
        "week": ("t.week_number", "t.month", "month"),
    }
    if req.granularity not in gran_map:
        raise HTTPException(status_code=400, detail=f"Cannot roll up from '{req.granularity}'. Options: {list(gran_map.keys())}")

    fine_col, coarse_col, coarse_name = gran_map[req.granularity]
    query = f"""
        SELECT d.disease_name, {coarse_col} AS {coarse_name},
               SUM(f.case_count) AS total_cases
        FROM fact_disease_incidence f
        JOIN dim_disease d ON f.disease_key = d.disease_key
        JOIN dim_time t ON f.time_key = t.time_key
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE l.loc_type = 'STATE'
        GROUP BY d.disease_name, {coarse_col}
        ORDER BY d.disease_name, {coarse_col}
    """
    try:
        results, exec_time, query_text = _pg.execute(query)
        return {
            "operation": "rollup",
            "from_granularity": req.granularity,
            "to_granularity": coarse_name,
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pivot")
def olap_pivot(req: OlapRequest):
    """Pivot: rotate dimensions — diseases as columns by time."""
    query = """
        SELECT t.decade,
               SUM(CASE WHEN d.disease_name = 'MEASLES' THEN f.case_count ELSE 0 END) AS measles,
               SUM(CASE WHEN d.disease_name = 'PERTUSSIS' THEN f.case_count ELSE 0 END) AS pertussis,
               SUM(CASE WHEN d.disease_name = 'POLIO' THEN f.case_count ELSE 0 END) AS polio,
               SUM(CASE WHEN d.disease_name = 'DIPHTHERIA' THEN f.case_count ELSE 0 END) AS diphtheria,
               SUM(CASE WHEN d.disease_name = 'HEPATITIS A' THEN f.case_count ELSE 0 END) AS hepatitis_a,
               SUM(CASE WHEN d.disease_name = 'MUMPS' THEN f.case_count ELSE 0 END) AS mumps,
               SUM(CASE WHEN d.disease_name = 'RUBELLA' THEN f.case_count ELSE 0 END) AS rubella,
               SUM(CASE WHEN d.disease_name = 'SMALLPOX' THEN f.case_count ELSE 0 END) AS smallpox
        FROM fact_disease_incidence f
        JOIN dim_disease d ON f.disease_key = d.disease_key
        JOIN dim_time t ON f.time_key = t.time_key
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE l.loc_type = 'STATE'
        GROUP BY t.decade
        ORDER BY t.decade
    """
    try:
        results, exec_time, query_text = _pg.execute(query)
        return {
            "operation": "pivot",
            "description": "Diseases as columns, decades as rows",
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
