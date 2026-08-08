import json
import logging
from uuid import UUID

from aiokafka import AIOKafkaProducer

from app.config import get_settings

logger = logging.getLogger(__name__)

_settings = get_settings()


def _resolve_acks(value: str) -> int | str:
    return value if value == "all" else int(value)


class EventProducer:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=_settings.kafka_bootstrap_servers,
            acks=_resolve_acks(_settings.kafka_producer_acks),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info("Kafka producer started (bootstrap=%s)", _settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")

    async def send_event(self, topic: str, event_id: UUID, payload: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer has not been started")
        await self._producer.send_and_wait(topic, value=payload, key=str(event_id))


producer = EventProducer()
