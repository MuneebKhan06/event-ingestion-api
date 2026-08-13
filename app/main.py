import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.middleware import RequestIDFilter, RequestIDMiddleware
from app.api.routes import dlq, events, health, metrics
from app.config import get_settings
from app.db.connection import dispose_engine
from app.kafka.producer import producer as event_producer

settings = get_settings()
logging.basicConfig(
    level=settings.log_level,
    format="%(levelname)s:%(name)s:[%(request_id)s] %(message)s",
)

# Attached to handlers rather than to a single logger: filters on a logger don't
# apply to records propagated up from its children, so library log records would
# arrive without request_id and the format string would raise on them.
for _handler in logging.getLogger().handlers:
    _handler.addFilter(RequestIDFilter())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await event_producer.start()
    yield
    await event_producer.stop()
    await dispose_engine()


app = FastAPI(title="Event Ingestion API", lifespan=lifespan)

app.add_middleware(RequestIDMiddleware)

app.include_router(health.router)
app.include_router(events.router)
app.include_router(dlq.router)
app.include_router(metrics.router)
