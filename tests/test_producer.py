from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from app.kafka.producer import EventProducer


async def test_send_event_calls_send_and_wait():
    producer = EventProducer()
    mock_kafka_producer = AsyncMock()

    with patch("app.kafka.producer.AIOKafkaProducer", return_value=mock_kafka_producer):
        await producer.start()
        assert producer.is_started

        event_id = uuid4()
        await producer.send_event("events.raw", event_id, {"foo": "bar"})

        mock_kafka_producer.send_and_wait.assert_awaited_once()
        _, kwargs = mock_kafka_producer.send_and_wait.call_args
        assert mock_kafka_producer.send_and_wait.call_args.args[0] == "events.raw"
        assert kwargs["value"] == {"foo": "bar"}
        assert kwargs["key"] == str(event_id)

        await producer.stop()
        assert not producer.is_started


async def test_send_event_before_start_raises():
    producer = EventProducer()
    with pytest.raises(RuntimeError):
        await producer.send_event("events.raw", uuid4(), {})
