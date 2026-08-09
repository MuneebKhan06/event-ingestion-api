import logging

from fastapi import APIRouter
from sqlalchemy import text

from app.db.connection import engine
from app.kafka.producer import producer as event_producer

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health_check() -> dict:
    kafka_status = "connected" if event_producer.is_started else "disconnected"

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        logger.exception("Health check DB connectivity probe failed")
        db_status = "disconnected"

    overall = "healthy" if kafka_status == "connected" and db_status == "connected" else "degraded"
    return {"status": overall, "kafka": kafka_status, "database": db_status}
