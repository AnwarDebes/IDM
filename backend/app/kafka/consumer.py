"""
Kafka consumer for SSE streaming to the frontend.
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import settings

_executor = ThreadPoolExecutor(max_workers=1)


def _ensure_topic(topic: str):
    """Create the topic if it doesn't exist."""
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_broker})
        metadata = admin.list_topics(timeout=5)
        if topic not in metadata.topics:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    except Exception:
        pass


def _poll_sync(consumer, timeout=1.0):
    """Synchronous poll wrapper for use in executor."""
    return consumer.poll(timeout)


async def consume_events(topics: list[str] = None) -> AsyncGenerator[dict | None, None]:
    """Async generator that yields Kafka messages as dicts.
    Yields None for heartbeats when no messages are available.
    """
    if topics is None:
        topics = ["raw-disease-events"]

    # Ensure topic exists
    for t in topics:
        _ensure_topic(t)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_broker,
        "group.id": "sse-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(topics)

    loop = asyncio.get_running_loop()

    try:
        while True:
            msg = await loop.run_in_executor(_executor, _poll_sync, consumer, 1.0)
            if msg is None:
                yield None  # heartbeat
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    yield None  # treat as heartbeat
                    continue
                break
            try:
                value = json.loads(msg.value().decode("utf-8"))
                yield value
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()
