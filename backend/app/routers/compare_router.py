"""
Comparison router — execute the same query on all backends concurrently.
"""

import asyncio
import concurrent.futures
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.query_registry import QUERY_REGISTRY, get_query_method
from app.services.postgres_service import PostgresService
from app.services.mongo_service import MongoService
from app.services.neo4j_service import Neo4jService

router = APIRouter(prefix="/api/v1", tags=["comparison"])

_services = {
    "postgres": PostgresService(),
    "mongodb": MongoService(),
    "neo4j": Neo4jService(),
}

# Thread pool for running synchronous DB calls concurrently
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)


class CompareRequest(BaseModel):
    query_id: str
    params: dict[str, Any] = {}


def _run_query(backend: str, method_name: str, params: dict) -> dict:
    """Execute a query on one backend, returning a result dict."""
    service = _services[backend]
    method = getattr(service, method_name)
    try:
        results, exec_time_ms, query_text = method(**params)
        return {
            "backend": backend,
            "results": results,
            "row_count": len(results),
            "execution_time_ms": exec_time_ms,
            "query_text": query_text,
            "status": "success",
        }
    except Exception as e:
        return {
            "backend": backend,
            "results": [],
            "row_count": 0,
            "execution_time_ms": 0,
            "query_text": "",
            "status": "error",
            "error": str(e),
        }


@router.post("/compare")
async def compare_backends(req: CompareRequest):
    """Execute the same query on all available backends and compare results."""
    entry = QUERY_REGISTRY.get(req.query_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Query {req.query_id} not found")

    method_name = req.query_id.lower()
    loop = asyncio.get_event_loop()

    # Launch all backends concurrently in thread pool
    tasks = []
    for backend in entry["backends"]:
        if backend in _services:
            tasks.append(
                loop.run_in_executor(
                    _executor, _run_query, backend, method_name, req.params
                )
            )

    results = await asyncio.gather(*tasks)

    backend_results = {r["backend"]: r for r in results}

    return {
        "query_id": req.query_id,
        "query_name": entry["name"],
        "olap_operation": entry["olap_operation"],
        "backends": backend_results,
    }
