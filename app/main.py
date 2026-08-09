import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import events, health
from app.config import get_settings
from app.kafka.producer import producer as event_producer

settings = get_settings()
logging.basicConfig(level=settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await event_producer.start()
    yield
    await event_producer.stop()


app = FastAPI(title="Event Ingestion API", lifespan=lifespan)

app.include_router(health.router)
app.include_router(events.router)
