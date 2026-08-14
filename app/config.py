from functools import lru_cache

from pydantic import model_validator
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

    # POST /events reads the events table before publishing so an obvious
    # duplicate can be answered 409 immediately. That read puts the database
    # back on the request path, which is what Decision 1 set out to avoid, and
    # load testing showed API latency tracking database load because of it.
    # Disabling it is safe: the check was never the correctness mechanism, only
    # a fast-path courtesy. See Decision 4.
    enable_duplicate_precheck: bool = True

    postgres_user: str = "events_user"
    postgres_password: str = "events_password"
    postgres_db: str = "events_db"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    # Left unset by default and derived from the postgres_* fields above, so there is a
    # single source of truth for the connection string. Set DATABASE_URL explicitly to
    # override (e.g. for a managed DB whose URL doesn't decompose into the fields above).
    database_url: str | None = None

    consumer_max_retries: int = 3
    consumer_retry_backoff_base_seconds: float = 1.0
    # The consumer serves its own Prometheus exposition, since it is a separate
    # process from the API and its counters live in a separate registry.
    consumer_metrics_port: int = 9100

    # Ingest poller: pulls public APIs and posts them to the API over HTTP.
    ingest_api_url: str = "http://localhost:8000"
    ingest_sources: str = "usgs,weather"
    ingest_interval_seconds: float = 30.0
    ingest_metrics_port: int = 9200

    @model_validator(mode="after")
    def _derive_database_url(self) -> "Settings":
        if self.database_url is None:
            self.database_url = (
                f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
