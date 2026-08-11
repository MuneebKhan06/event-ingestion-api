import json
import logging
from uuid import UUID

from aiokafka import AIOKafkaProducer

from app.config import get_settings

logger = logging.getLogger(__name__)


def _resolve_acks(value: str) -> int | str:
    return value if value == "all" else int(value)


class EventProducer:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        # Read at connect time, not import time, so the broker address reflects
        # the configuration in force when the producer actually starts.
        settings = get_settings()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks=_resolve_acks(settings.kafka_producer_acks),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info("Kafka producer started (bootstrap=%s)", settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")

    @property
    def is_started(self) -> bool:
        return self._producer is not None

    async def send_event(self, topic: str, event_id: UUID, payload: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer has not been started")
        await self._producer.send_and_wait(topic, value=payload, key=str(event_id))


producer = EventProducer()
