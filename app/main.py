import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import dlq, events, health, metrics
from app.config import get_settings
from app.db.connection import dispose_engine
from app.kafka.producer import producer as event_producer

settings = get_settings()
logging.basicConfig(level=settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await event_producer.start()
    yield
    await event_producer.stop()
    await dispose_engine()


app = FastAPI(title="Event Ingestion API", lifespan=lifespan)

app.include_router(health.router)
app.include_router(events.router)
app.include_router(dlq.router)
app.include_router(metrics.router)
