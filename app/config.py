from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_events_raw: str = "events.raw"
    kafka_topic_events_dlq: str = "events.dlq"
    kafka_consumer_group: str = "event-processors"
    kafka_producer_acks: str = "1"

    postgres_user: str = "events_user"
    postgres_password: str = "events_password"
    postgres_db: str = "events_db"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    database_url: str = "postgresql+asyncpg://events_user:events_password@localhost:5432/events_db"

    consumer_max_retries: int = 3
    consumer_retry_backoff_base_seconds: float = 1.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
