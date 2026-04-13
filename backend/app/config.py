from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "dw_admin"
    postgres_password: str = "dw_secure_2024"
    postgres_db: str = "epidemiological_dw"

    # MongoDB
    mongo_host: str = "mongodb"
    mongo_port: int = 27017
    mongo_user: str = "dw_admin"
    mongo_password: str = "dw_secure_2024"
    mongo_db: str = "epidemiological_dw"

    # Neo4j
    neo4j_uri: str = "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "dw_secure_2024"

    # Kafka
    kafka_broker: str = "kafka:29092"

    class Config:
        env_file = ".env"


settings = Settings()
