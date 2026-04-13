"""
Startup script: waits for databases, checks if data is loaded,
and runs ETL loaders for any empty backends.
"""

import os
import subprocess
import sys
import time

import psycopg2
import pymongo
from neo4j import GraphDatabase


def get_env(key, default):
    return os.environ.get(key, default)


PG = {
    "host": get_env("POSTGRES_HOST", "postgres"),
    "port": int(get_env("POSTGRES_PORT", "5432")),
    "user": get_env("POSTGRES_USER", "dw_admin"),
    "password": get_env("POSTGRES_PASSWORD", "dw_secure_2024"),
    "dbname": get_env("POSTGRES_DB", "epidemiological_dw"),
}
MONGO = {
    "host": get_env("MONGO_HOST", "mongodb"),
    "port": int(get_env("MONGO_PORT", "27017")),
    "username": get_env("MONGO_USER", "dw_admin"),
    "password": get_env("MONGO_PASSWORD", "dw_secure_2024"),
}
MONGO_DB = get_env("MONGO_DB", "epidemiological_dw")
NEO4J_URI = get_env("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = get_env("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = get_env("NEO4J_PASSWORD", "dw_secure_2024")


def wait_for_postgres(max_retries=30):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**PG, connect_timeout=3)
            conn.close()
            print("  PostgreSQL: ready", flush=True)
            return True
        except Exception:
            time.sleep(2)
    print("  PostgreSQL: TIMEOUT after retries", flush=True)
    return False


def wait_for_mongo(max_retries=30):
    for i in range(max_retries):
        try:
            client = pymongo.MongoClient(**MONGO, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            client.close()
            print("  MongoDB: ready")
            return True
        except Exception:
            time.sleep(2)
    print("  MongoDB: TIMEOUT after retries")
    return False


def wait_for_neo4j(max_retries=30):
    for i in range(max_retries):
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            with driver.session() as s:
                s.run("RETURN 1")
            driver.close()
            print("  Neo4j: ready")
            return True
        except Exception:
            time.sleep(2)
    print("  Neo4j: TIMEOUT after retries")
    return False


def check_postgres():
    """Returns row count in fact table, or 0 if empty/error."""
    try:
        conn = psycopg2.connect(**PG)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_disease_incidence")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception:
        return 0


def check_mongo():
    """Returns document count in disease_observations, or 0 if empty/error."""
    try:
        client = pymongo.MongoClient(**MONGO)
        db = client[MONGO_DB]
        count = db.disease_observations.count_documents({})
        client.close()
        return count
    except Exception:
        return 0


def check_neo4j():
    """Returns Observation node count, or 0 if empty/error."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            result = session.run("MATCH (o:Observation) RETURN count(o) AS cnt")
            count = result.single()["cnt"]
        driver.close()
        return count
    except Exception:
        return 0


def run_loader(script_name):
    """Run an ETL loader script as a subprocess."""
    script_path = f"/app/etl/{script_name}"
    print(f"  Running {script_name}...", flush=True)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    result = subprocess.run(
        [sys.executable, "-u", script_path],
        cwd="/app",
        env=env,
    )
    if result.returncode != 0:
        print(f"  WARNING: {script_name} exited with code {result.returncode}", flush=True)
    else:
        print(f"  {script_name} completed successfully.", flush=True)


def main():
    # Step 1: Wait for all databases
    print("Waiting for databases...")
    pg_ready = wait_for_postgres()
    mongo_ready = wait_for_mongo()
    neo4j_ready = wait_for_neo4j()

    if not (pg_ready and mongo_ready and neo4j_ready):
        print("WARNING: Not all databases ready. Proceeding anyway...")

    # Step 2: Check data file exists
    data_file = "/app/data/raw/tycho_level1.csv"
    if not os.path.exists(data_file):
        print(f"\nData file not found: {data_file}")
        print("Skipping ETL — generate data first with: python etl/generate_synthetic_data.py")
        return

    # Step 3: Check each backend and load if empty
    print("\nChecking data status...")

    pg_count = check_postgres() if pg_ready else 0
    mongo_count = check_mongo() if mongo_ready else 0
    neo4j_count = check_neo4j() if neo4j_ready else 0

    print(f"  PostgreSQL: {pg_count:,} rows")
    print(f"  MongoDB:    {mongo_count:,} documents")
    print(f"  Neo4j:      {neo4j_count:,} observation nodes")

    if pg_count > 0 and mongo_count > 0 and neo4j_count > 0:
        print("\nAll backends already loaded. Skipping ETL.")
        return

    print("\nEmpty backends detected — running ETL loaders...")

    if pg_count == 0 and pg_ready:
        print("\n=== Loading PostgreSQL ===")
        run_loader("load_postgres.py")

    if mongo_count == 0 and mongo_ready:
        print("\n=== Loading MongoDB ===")
        run_loader("load_mongo.py")

    if neo4j_count == 0 and neo4j_ready:
        print("\n=== Loading Neo4j ===")
        run_loader("load_neo4j.py")

    print("\nETL loading complete.")


if __name__ == "__main__":
    main()
