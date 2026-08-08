from app.config import get_settings

_settings = get_settings()

EVENTS_RAW = _settings.kafka_topic_events_raw
EVENTS_DLQ = _settings.kafka_topic_events_dlq
CONSUMER_GROUP = _settings.kafka_consumer_group
