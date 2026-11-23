"""Repeatable synthetic approach-and-retreat scenario over MQTT QoS 1."""
import argparse
import logging
import sys
import time
from threading import Event

import paho.mqtt.client as mqtt

from .config import Settings, site_topic
from .models import new_event

log = logging.getLogger("safestream.simulator")
DISTANCE_CYCLE_M = (8.0, 6.0, 4.0, 2.4, 1.2, 2.8, 5.5, 8.0)


def distance_for_sequence(sequence: int) -> float:
    return DISTANCE_CYCLE_M[sequence % len(DISTANCE_CYCLE_M)]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Publish simulated proximity telemetry")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between publications")
    parser.add_argument("--count", type=int, default=0, help="0 means publish indefinitely")
    parser.add_argument("--site-id", default="site-01")
    parser.add_argument("--worker-id", default="worker-07")
    parser.add_argument("--equipment-id", default="excavator-02")
    args = parser.parse_args(argv)
    if args.interval <= 0 or args.count < 0:
        parser.error("--interval must be > 0 and --count must be >= 0")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Settings()
    connected = Event()
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="safestream-simulator", protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT rejected connection: %s", reason_code)
        else:
            connected.set()
            log.info("CONNECTED MQTT broker=%s:%s", cfg.mqtt_host, cfg.mqtt_port)

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        connected.clear()
        log.warning("DISCONNECTED MQTT reason=%s", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect_async(cfg.mqtt_host, cfg.mqtt_port, keepalive=60)
    client.loop_start()
    topic = site_topic(args.site_id)
    seq = 0
    try:
        while args.count == 0 or seq < args.count:
            if not connected.wait(timeout=10):
                log.warning("Waiting for MQTT broker")
                continue
            event = new_event(sequence=seq, distance_m=distance_for_sequence(seq),
                              site_id=args.site_id, worker_id=args.worker_id,
                              equipment_id=args.equipment_id)
            info = client.publish(topic, event.model_dump_json(), qos=1, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log.warning("PUBLISH_QUEUE_FAILED rc=%s", info.rc)
                continue
            try:
                info.wait_for_publish(timeout=10)
            except (RuntimeError, ValueError) as exc:
                log.warning("Publish confirmation failed: %s", exc)
                continue
            if not info.is_published():
                log.warning("Publish not confirmed in 10s")
                continue
            log.info("PUBLISHED event_id=%s seq=%s distance_m=%.2f topic=%s",
                     event.event_id, seq, event.distance_m, topic)
            seq += 1
            if args.count == 0 or seq < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Simulator stopped")
    finally:
        client.disconnect()
        client.loop_stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
