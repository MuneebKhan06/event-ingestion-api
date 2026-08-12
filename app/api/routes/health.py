import logging

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.db.connection import get_engine
from app.kafka.producer import producer as event_producer

logger = logging.getLogger(__name__)
router = APIRouter()


async def _probe_database() -> str:
    """Report *how* the database is failing, not just that it is.

    "Cannot reach it at all" (wrong host, refused connection, bad
    credentials) and "reached it but the query failed" (missing schema,
    revoked permissions) have different causes and different fixes. Collapsing
    both into one status makes the endpoint an operator checks first during an
    incident say the least useful thing it could.

    Connecting and querying are therefore separate steps, using start() rather
    than a `with` block so a failure can be attributed to the right one.
    """
    connection = get_engine().connect()
    try:
        await connection.start()
    except Exception:
        logger.exception("Health probe: database unreachable")
        return "unreachable"

    try:
        await connection.execute(text("SELECT 1"))
        return "connected"
    except Exception:
        logger.exception("Health probe: connected to the database but the query failed")
        return "query_failed"
    finally:
        await connection.close()


@router.get("/health")
async def health_check(response: Response) -> dict:
    # Note this only reports whether the producer was started, not whether the
    # broker is currently reachable — a genuinely weaker signal than the
    # database probe below, and deliberately so: a real broker round trip on
    # every health check would make the probe expensive and flappy.
    kafka_status = "connected" if event_producer.is_started else "disconnected"

    db_status = await _probe_database()

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
