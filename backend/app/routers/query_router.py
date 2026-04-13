"""
Query execution router — list queries and execute on a specific backend.
"""

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.query_registry import QUERY_REGISTRY, get_query_method
from app.services.postgres_service import PostgresService
from app.services.mongo_service import MongoService
from app.services.neo4j_service import Neo4jService

router = APIRouter(prefix="/api/v1", tags=["queries"])

# Singleton service instances
_services = {
    "postgres": PostgresService(),
    "mongodb": MongoService(),
    "neo4j": Neo4jService(),
}


class QueryRequest(BaseModel):
    query_id: str
    backend: str
    params: dict[str, Any] = {}


@router.get("/queries")
def list_queries():
    """List all available queries with metadata."""
    result = []
    for qid, meta in QUERY_REGISTRY.items():
        result.append({
            "query_id": qid,
            "name": meta["name"],
            "description": meta["description"],
            "olap_operation": meta["olap_operation"],
            "backends": meta["backends"],
            "params": meta["params"],
        })
    return {"queries": result, "total": len(result)}


@router.post("/query")
def execute_query(req: QueryRequest):
    """Execute a single query on a specific backend."""
    if req.query_id not in QUERY_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Query {req.query_id} not found")

    method_name = get_query_method(req.query_id, req.backend)
    if method_name is None:
        raise HTTPException(
            status_code=400,
            detail=f"Query {req.query_id} not available on {req.backend}",
        )

    service = _services.get(req.backend)
    if service is None:
        raise HTTPException(status_code=400, detail=f"Unknown backend: {req.backend}")

    method = getattr(service, method_name, None)
    if method is None:
        raise HTTPException(
            status_code=500,
            detail=f"Method {method_name} not found on {req.backend} service",
        )

    try:
        results, exec_time_ms, query_text = method(**req.params)
        return {
            "query_id": req.query_id,
            "backend": req.backend,
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time_ms,
            "query_text": query_text,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
