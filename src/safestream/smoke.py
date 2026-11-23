"""End-to-end smoke check: send a unique MQTT message and find it in Redis."""
import argparse
import logging
import sys
import time
from threading import Event

import paho.mqtt.client as mqtt
import redis

from .config import Settings, site_topic
from .models import new_event

log = logging.getLogger("safestream.smoke")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = Settings()
    store = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True)
    event = new_event(sequence=99999, distance_m=2.4)
    connected = Event()
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"safestream-smoke-{event.event_id}")

    def on_connect(client, userdata, flags, reason_code, properties):
        if not reason_code.is_failure:
            connected.set()

    client.on_connect = on_connect
    deadline = time.monotonic() + args.timeout
    try:
        previous = store.xrevrange(cfg.redis_stream, count=1)
        cursor = previous[0][0] if previous else "0-0"
        client.connect(cfg.mqtt_host, cfg.mqtt_port)
        client.loop_start()
        if not connected.wait(timeout=min(10, args.timeout)):
            log.error("FAIL broker connection not established")
            return 1
        last_publish = 0.0
        while time.monotonic() < deadline:
            now = time.monotonic()
            if now - last_publish >= 3:
                info = client.publish(site_topic(event.site_id), event.model_dump_json(), qos=1)
                info.wait_for_publish(timeout=5)
                last_publish = now
            records = store.xread({cfg.redis_stream: cursor}, count=100, block=1000)
            for _, entries in records:
                for redis_id, fields in entries:
                    cursor = redis_id
                    if fields.get("event_id") == str(event.event_id):
                        log.info("PASS MQTT -> ingestion -> Redis Streams")
                        log.info("event_id=%s redis_id=%s", event.event_id, redis_id)
                        return 0
        log.error("FAIL event_id=%s not found in %s in %ds", event.event_id,
                  cfg.redis_stream, args.timeout)
        return 1
    except (redis.RedisError, OSError, ValueError, RuntimeError):
        log.exception("FAIL smoke test failed")
        return 1
    finally:
        client.disconnect()
        client.loop_stop()
        store.close()


if __name__ == "__main__":
    sys.exit(main())
