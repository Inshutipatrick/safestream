"""MQTT subscriber -> validation -> Redis Stream. No alerting in Phase 1."""
import logging
import sys
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import redis
from pydantic import ValidationError

from .config import MQTT_TOPIC_FILTER, Settings, site_topic
from .models import ProximityEvent

log = logging.getLogger("safestream.ingestion")


class InvalidEvent(ValueError):
    """Payload does not conform to the contract or MQTT topic."""


def parse_event(topic: str, payload: bytes) -> ProximityEvent:
    try:
        event = ProximityEvent.model_validate_json(payload)
    except ValidationError as exc:
        raise InvalidEvent(str(exc)) from exc
    if topic != site_topic(event.site_id):
        raise InvalidEvent(f"topic {topic!r} does not match site {event.site_id!r}")
    return event


def store_message(store: redis.Redis, stream: str, topic: str, payload: bytes) -> tuple[ProximityEvent, str]:
    event = parse_event(topic, payload)
    entry_id = store.xadd(stream, {
        "event_id": str(event.event_id),
        "site_id": event.site_id,
        "topic": topic,
        "payload": event.model_dump_json(),
        "received_at": datetime.now(timezone.utc).isoformat(),
    })
    if isinstance(entry_id, bytes):
        entry_id = entry_id.decode("ascii")
    return event, entry_id


def handle_message(client, store: redis.Redis, stream: str, message) -> bool:
    """Process one MQTT delivery. Acknowledge only after a successful XADD.

    Returns True for a stored event, False for an intentionally rejected event.
    Redis errors propagate without ACK so the caller can end the connection.
    """
    try:
        event, entry_id = store_message(store, stream, message.topic, message.payload)
    except InvalidEvent as exc:
        log.warning("INVALID topic=%s error=%s", message.topic, exc)
        client.ack(message.mid, message.qos)  # Discard malformed messages deliberately.
        return False
    log.info("INGESTED event_id=%s site=%s distance_m=%.2f redis_id=%s",
             event.event_id, event.site_id, event.distance_m, entry_id)
    client.ack(message.mid, message.qos)
    return True


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Settings()
    store = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, socket_connect_timeout=3, socket_timeout=3)
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=cfg.ingestion_client_id,
        protocol=mqtt.MQTTv311,
        clean_session=False,
        manual_ack=True,
    )
    # Terminate the process on loss of the Redis sink. Compose restarts it; MQTT
    # broker holds the QoS 1 in-flight message for the persistent subscriber.
    failed = False

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT connection rejected: %s", reason_code)
            return
        client.subscribe(MQTT_TOPIC_FILTER, qos=1)
        log.info("SUBSCRIBED filter=%s qos=1", MQTT_TOPIC_FILTER)

    def on_message(client, userdata, message):
        nonlocal failed
        try:
            handle_message(client, store, cfg.redis_stream, message)
        except redis.RedisError:
            log.exception("REDIS_WRITE_FAILED: stopping ingestion without MQTT ack")
            failed = True
            client.disconnect()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    try:
        log.info("Starting ingestion mqtt=%s:%s redis=%s:%s stream=%s",
                 cfg.mqtt_host, cfg.mqtt_port, cfg.redis_host, cfg.redis_port, cfg.redis_stream)
        # Reconnect after startup failures; Docker readiness is not sufficient.
        mqtt_client.connect_async(cfg.mqtt_host, cfg.mqtt_port, keepalive=60)
        mqtt_client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        log.info("Ingestion stopped")
    except Exception:
        log.exception("Ingestion crashed")
        return 1
    finally:
        mqtt_client.disconnect()
        store.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
