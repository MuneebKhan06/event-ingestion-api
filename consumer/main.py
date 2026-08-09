import asyncio
import logging

from app.config import get_settings
from app.core.dlq import DLQHandler
from app.db.connection import async_session_factory
from app.db.repository import EventRepository
from app.kafka.consumer import EventConsumer
from app.kafka.producer import EventProducer
from app.kafka.topics import CONSUMER_GROUP, EVENTS_RAW
from consumer.processor import process_message

settings = get_settings()
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


async def run() -> None:
    consumer = EventConsumer(EVENTS_RAW, group_id=CONSUMER_GROUP)
    dlq_producer = EventProducer()

    await consumer.start()
    await dlq_producer.start()
    logger.info("Consumer service started (topic=%s, group=%s)", EVENTS_RAW, CONSUMER_GROUP)

    try:
        async for message in consumer:
            async with async_session_factory() as session:
                repository = EventRepository(session)
                dlq_handler = DLQHandler(dlq_producer, repository)
                await process_message(message, repository, dlq_handler)
            # Offset is only committed after the message has been durably
            # handled (inserted or routed to DLQ), never before.
            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        logger.info("Consumer service stopped")


if __name__ == "__main__":
    asyncio.run(run())
