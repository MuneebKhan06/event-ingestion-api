import asyncio
import logging
import signal

from app.config import get_settings
from app.core.dlq import DLQHandler
from app.db.connection import async_session_factory, dispose_engine
from app.db.repository import EventRepository
from app.kafka.consumer import EventConsumer
from app.kafka.producer import EventProducer
from app.kafka.topics import CONSUMER_GROUP, EVENTS_RAW
from consumer.processor import process_message

settings = get_settings()
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


async def _wait_for_message_or_shutdown(consumer: EventConsumer, shutdown: asyncio.Event):
    """Race fetching the next message against a shutdown signal, so a
    consumer idling on an empty topic still reacts to SIGTERM/SIGINT
    immediately instead of blocking until the next message arrives."""
    get_task = asyncio.ensure_future(consumer.get_one())
    shutdown_task = asyncio.ensure_future(shutdown.wait())
    try:
        done, _ = await asyncio.wait(
            {get_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if shutdown_task in done:
            return None
        return get_task.result()
    finally:
        for task in (get_task, shutdown_task):
            if not task.done():
                task.cancel()


async def run() -> None:
    consumer = EventConsumer(EVENTS_RAW, group_id=CONSUMER_GROUP)
    dlq_producer = EventProducer()
    shutdown_requested = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_requested.set)

    await consumer.start()
    await dlq_producer.start()
    logger.info("Consumer service started (topic=%s, group=%s)", EVENTS_RAW, CONSUMER_GROUP)

    try:
        while not shutdown_requested.is_set():
            message = await _wait_for_message_or_shutdown(consumer, shutdown_requested)
            if message is None:
                logger.info("Shutdown signal received, exiting")
                break

            async with async_session_factory() as session:
                repository = EventRepository(session)
                dlq_handler = DLQHandler(dlq_producer, repository)
                await process_message(message, repository, dlq_handler)
            # Offset is only committed after the message has been durably
            # handled (inserted or routed to DLQ), never before — including
            # on shutdown: a message already fetched here always finishes
            # processing and commits before the loop checks for shutdown again.
            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await dispose_engine()
        logger.info("Consumer service stopped")


if __name__ == "__main__":
    asyncio.run(run())
