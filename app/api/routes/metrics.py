from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter()


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus exposition for this process.

    Reads nothing but in-memory counters, so a scrape can't fail or hang
    because Kafka or the database is unhealthy — which is exactly when the
    metrics are most worth having. Liveness of those dependencies is what
    /health is for.
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
