import json
import logging
from collections.abc import AsyncIterator

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from app.config import get_settings

logger = logging.getLogger(__name__)


class EventConsumer:
    """Thin async wrapper around AIOKafkaConsumer with manual offset commit.

    Manual commits (enable_auto_commit=False) ensure an offset only advances
    after the message has actually been persisted downstream, not merely read.
    """

    def __init__(self, *topics: str, group_id: str | None = None) -> None:
        self._topics = topics
        self._group_id = group_id or get_settings().kafka_consumer_group
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        # As in the producer: resolved when connecting, not at import.
        settings = get_settings()
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        logger.info(
            "Kafka consumer started (topics=%s, group=%s)", self._topics, self._group_id
        )

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
            logger.info("Kafka consumer stopped")

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        if self._consumer is None:
            raise RuntimeError("Consumer has not been started")
        return self._consumer.__aiter__()

    async def get_one(self) -> ConsumerRecord:
        """Fetch a single message, waiting if none is available yet.

        Unlike iterating directly, this can be raced against another
        awaitable (e.g. a shutdown signal) via asyncio.wait, so a caller
        idling on an empty topic can still react immediately.
        """
        if self._consumer is None:
            raise RuntimeError("Consumer has not been started")
        return await self._consumer.getone()

    async def commit(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer has not been started")
        await self._consumer.commit()
