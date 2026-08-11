import logging

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.db.connection import get_engine
from app.kafka.producer import producer as event_producer

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health_check(response: Response) -> dict:
    kafka_status = "connected" if event_producer.is_started else "disconnected"

    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        logger.exception("Health check DB connectivity probe failed")
        db_status = "disconnected"

    healthy = kafka_status == "connected" and db_status == "connected"

    # A degraded service has to answer with a non-2xx status, not just say
    # "degraded" in a 200 body. Everything that consumes this endpoint —
    # the image's HEALTHCHECK, load balancers, k8s probes — decides on the
    # status code alone and would keep routing traffic to a broken instance.
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "healthy" if healthy else "degraded",
        "kafka": kafka_status,
        "database": db_status,
    }
