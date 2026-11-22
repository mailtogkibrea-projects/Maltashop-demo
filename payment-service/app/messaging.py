"""Thin RabbitMQ helper shared (copied) by every service.

- One durable topic exchange carries all domain events.
- Each service owns one durable queue and binds only the routing keys it needs.
- Consumers use manual acknowledgements; a message whose handler raises is
  rejected without requeue and lands in the dead-letter queue.
- The correlation ID travels in the AMQP `correlation_id` property.
"""
import json
import logging
import threading
import time
import uuid

import pika

from .logging_setup import correlation_id

EXCHANGE = "maltashop.events"
DEAD_LETTER_EXCHANGE = "maltashop.dlx"
DEAD_LETTER_QUEUE = "maltashop.dead"

log = logging.getLogger("messaging")

# Set once the queue is declared and bound; used by the /ready probe.
ready = threading.Event()


def _connect(url):
    return pika.BlockingConnection(pika.URLParameters(url))


def publish(url, routing_key, payload):
    """Publish one persistent event with publisher confirms enabled."""
    message_id = str(uuid.uuid4())
    props = pika.BasicProperties(
        delivery_mode=2,  # persistent
        content_type="application/json",
        message_id=message_id,
        correlation_id=correlation_id.get(),
    )
    conn = _connect(url)
    try:
        channel = conn.channel()
        channel.exchange_declare(EXCHANGE, "topic", durable=True)
        channel.confirm_delivery()
        channel.basic_publish(EXCHANGE, routing_key, json.dumps(payload).encode(), props)
    finally:
        conn.close()
    log.info("event published", extra={"ctx": {"event": routing_key, "message_id": message_id}})


def start_consumer(url, service, bindings, handler):
    """Run a reconnecting consumer in a daemon thread."""
    queue = f"{service}.events"

    def on_message(channel, method, props, body):
        token = correlation_id.set(props.correlation_id or "-")
        try:
            log.info("event received", extra={"ctx": {"event": method.routing_key}})
            handler(method.routing_key, json.loads(body))
            channel.basic_ack(method.delivery_tag)
        except Exception:
            log.exception("handler failed; message sent to dead-letter queue")
            channel.basic_nack(method.delivery_tag, requeue=False)
        finally:
            correlation_id.reset(token)

    def run():
        while True:
            try:
                conn = _connect(url)
                channel = conn.channel()
                channel.exchange_declare(EXCHANGE, "topic", durable=True)
                channel.exchange_declare(DEAD_LETTER_EXCHANGE, "fanout", durable=True)
                channel.queue_declare(DEAD_LETTER_QUEUE, durable=True)
                channel.queue_bind(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE)
                channel.queue_declare(
                    queue, durable=True, arguments={"x-dead-letter-exchange": DEAD_LETTER_EXCHANGE}
                )
                for key in bindings:
                    channel.queue_bind(queue, EXCHANGE, key)
                channel.basic_qos(prefetch_count=10)
                channel.basic_consume(queue, on_message)
                ready.set()
                log.info("consumer ready", extra={"ctx": {"queue": queue, "bindings": bindings}})
                channel.start_consuming()
            except Exception:
                ready.clear()
                log.exception("broker connection lost; retrying in 3s")
                time.sleep(3)

    thread = threading.Thread(target=run, name=f"{service}-consumer", daemon=True)
    thread.start()
    return thread
