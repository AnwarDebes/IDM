"""
Metadata router — schema information for each backend.
"""

from fastapi import APIRouter, HTTPException

from app.services.postgres_service import PostgresService
from app.services.mongo_service import MongoService
from app.services.neo4j_service import Neo4jService

router = APIRouter(prefix="/api/v1/metadata", tags=["metadata"])

_pg = PostgresService()
_mongo = MongoService()
_neo4j = Neo4jService()


@router.get("/postgres")
def postgres_metadata():
    """Return PostgreSQL schema metadata: tables, row counts, indexes."""
    try:
        conn = _pg._get_conn()
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Table row counts
        cur.execute("""
            SELECT relname AS table_name,
                   reltuples::bigint AS estimated_row_count
            FROM pg_class
            WHERE relkind = 'r'
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ORDER BY reltuples DESC
        """)
        tables = [dict(r) for r in cur.fetchall()]

        # Materialized view counts
        cur.execute("""
            SELECT relname AS view_name,
                   reltuples::bigint AS estimated_row_count
            FROM pg_class
            WHERE relkind = 'm'
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ORDER BY relname
        """)
        views = [dict(r) for r in cur.fetchall()]

        # Index info
        cur.execute("""
            SELECT indexname, tablename, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        indexes = [dict(r) for r in cur.fetchall()]

        # Database size
        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size")
        db_size = cur.fetchone()["db_size"]

        cur.close()
        conn.close()

        return {
            "backend": "postgres",
            "database_size": db_size,
            "tables": tables,
            "materialized_views": views,
            "indexes": indexes,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mongodb")
def mongodb_metadata():
    """Return MongoDB schema metadata: collections, document counts, indexes."""
    try:
        db = _mongo.db
        collections = []
        for name in db.list_collection_names():
            stats = db.command("collStats", name)
            collections.append({
                "collection": name,
                "document_count": stats.get("count", 0),
                "size_bytes": stats.get("size", 0),
                "avg_document_size_bytes": stats.get("avgObjSize", 0),
                "indexes": [
                    {"name": idx_name, "keys": idx_info.get("key", {})}
                    for idx_name, idx_info in db[name].index_information().items()
                ],
            })

        db_stats = db.command("dbstats")
        return {
            "backend": "mongodb",
            "database_size_bytes": db_stats.get("dataSize", 0),
            "collections": sorted(collections, key=lambda c: c["document_count"], reverse=True),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/neo4j")
def neo4j_metadata():
    """Return Neo4j schema metadata: node labels, relationship types, counts."""
    try:
        with _neo4j.driver.session() as session:
            db_size_bytes = None
            db_size_source = None
            try:
                row = session.run(
                    "CALL apoc.monitor.store() YIELD logSize, stringStoreSize, arrayStoreSize, "
                    "relStoreSize, propStoreSize, totalStoreSize, nodeStoreSize "
                    "RETURN totalStoreSize AS total"
                ).single()
                if row and row["total"] is not None:
                    db_size_bytes = int(row["total"])
                    db_size_source = "apoc.monitor.store"
            except Exception:
                pass
            if db_size_bytes is None:
                try:
                    total = 0
                    for r in session.run("CALL dbms.queryJmx('org.neo4j:*')"):
                        attrs = r.get("attributes") or {}
                        for key in ("TotalStoreSize", "StoreSize"):
                            val = attrs.get(key, {}).get("value")
                            if isinstance(val, (int, float)) and val > total:
                                total = int(val)
                    if total > 0:
                        db_size_bytes = total
                        db_size_source = "dbms.queryJmx"
                except Exception:
                    pass

            # Node counts by label
            result = session.run("""
                CALL db.labels() YIELD label
                CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) AS cnt', {}) YIELD value
                RETURN label, value.cnt AS count
                ORDER BY value.cnt DESC
            """)
            nodes = [{"label": r["label"], "count": r["count"]} for r in result]

            # Relationship counts by type
            result = session.run("""
                CALL db.relationshipTypes() YIELD relationshipType AS type
                CALL apoc.cypher.run('MATCH ()-[r:`' + type + '`]->() RETURN count(r) AS cnt', {}) YIELD value
                RETURN type, value.cnt AS count
                ORDER BY value.cnt DESC
            """)
            rels = [{"type": r["type"], "count": r["count"]} for r in result]

            # Constraints
            result = session.run("SHOW CONSTRAINTS")
            constraints = [dict(r) for r in result]

            # Indexes
            result = session.run("SHOW INDEXES")
            indexes = [dict(r) for r in result]

        return {
            "backend": "neo4j",
            "database_size_bytes": db_size_bytes,
            "database_size_source": db_size_source,
            "node_labels": nodes,
            "relationship_types": rels,
            "constraints": constraints,
            "indexes": indexes,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{backend}")
def generic_metadata(backend: str):
    """Fallback for unknown backend."""
    raise HTTPException(status_code=404, detail=f"Unknown backend: {backend}. Use: postgres, mongodb, neo4j")
