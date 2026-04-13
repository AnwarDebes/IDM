import time

import psycopg2
import pymongo
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase

from app.config import settings
from app.routers.stream_router import router as stream_router
from app.routers.query_router import router as query_router
from app.routers.compare_router import router as compare_router
from app.routers.olap_router import router as olap_router
from app.routers.batch_router import router as batch_router
from app.routers.metadata_router import router as metadata_router

app = FastAPI(
    title="Epidemiological Data Warehouse API",
    description="Unified API for multi-backend epidemiological analytics",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def check_postgres() -> dict:
    try:
        conn = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            dbname=settings.postgres_db,
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_mongodb() -> dict:
    try:
        client = pymongo.MongoClient(
            host=settings.mongo_host,
            port=settings.mongo_port,
            username=settings.mongo_user,
            password=settings.mongo_password,
            serverSelectionTimeoutMS=5000,
        )
        client.admin.command("ping")
        client.close()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_neo4j() -> dict:
    try:
        driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        with driver.session() as session:
            session.run("RETURN 1")
        driver.close()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_kafka() -> dict:
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": settings.kafka_broker})
        metadata = admin.list_topics(timeout=5)
        return {"status": "healthy", "brokers": len(metadata.brokers)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


app.include_router(stream_router)
app.include_router(query_router)
app.include_router(compare_router)
app.include_router(olap_router)
app.include_router(batch_router)
app.include_router(metadata_router)


@app.get("/api/v1/health")
def health_check():
    start = time.time()
    services = {
        "postgres": check_postgres(),
        "mongodb": check_mongodb(),
        "neo4j": check_neo4j(),
        "kafka": check_kafka(),
    }
    all_healthy = all(s["status"] == "healthy" for s in services.values())
    return {
        "status": "ok" if all_healthy else "degraded",
        "services": services,
        "response_time_ms": round((time.time() - start) * 1000, 2),
    }


@app.get("/")
def root():
    return {
        "message": "Epidemiological Data Warehouse API",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
