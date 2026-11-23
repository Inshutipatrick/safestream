"""Integration gate: new MQTT CRITICAL event -> one Redis alert after replay."""
import argparse
import logging
import sys
import time
from threading import Event

import paho.mqtt.client as mqtt
import redis

from .config import Settings, site_topic
from .models import new_event
from .processor import ProcessorConfig
from .rules import SafetyAlert, Severity

log = logging.getLogger("safestream.phase2_smoke")


def most_recent_id(store, stream: str) -> str:
    items = store.xrevrange(stream, count=1)
    return items[0][0] if items else "0-0"


def id_as_tuple(stream_id: str) -> tuple[int, int]:
    a, b = stream_id.split("-", 1)
    return int(a), int(b)


def publish(client, event) -> None:
    info = client.publish(site_topic(event.site_id), event.model_dump_json(), qos=1, retain=False)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"MQTT publish returned code {info.rc}")
    info.wait_for_publish(timeout=5)
    if not info.is_published():
        raise TimeoutError("MQTT broker did not confirm publish")


def find_alert(store, stream: str, start_id: str, event_id: str, deadline: float):
    cursor = start_id
    while time.monotonic() < deadline:
        messages = store.xread({stream: cursor}, count=100, block=1000)
        for _, entries in messages:
            for message_id, fields in entries:
                cursor = message_id
                if fields.get("event_id") == event_id:
                    return message_id, fields
    raise TimeoutError(f"alert not found for event {event_id}")


def find_telemetry_ids(store, stream: str, start_id: str, event_id: str,
                       count: int, deadline: float) -> list[str]:
    cursor = start_id
    found = []
    while time.monotonic() < deadline and len(found) < count:
        messages = store.xread({stream: cursor}, count=100, block=1000)
        for _, entries in messages:
            for message_id, fields in entries:
                cursor = message_id
                if fields.get("event_id") == event_id:
                    found.append(message_id)
                    if len(found) >= count:
                        break
    if len(found) < count:
        raise TimeoutError(f"only {len(found)} of {count} MQTT deliveries reached Redis")
    return found


def wait_until_acknowledged(store, cfg: ProcessorConfig, message_id: str,
                            deadline: float) -> None:
    while time.monotonic() < deadline:
        groups = store.xinfo_groups(cfg.telemetry_stream)
        our_group = next((g for g in groups if g["name"] == cfg.group), None)
        if our_group is None:
            raise RuntimeError(f"consumer group {cfg.group} absent")
        delivered_id = our_group.get("last-delivered-id") or "0-0"
        if id_as_tuple(delivered_id) >= id_as_tuple(message_id):
            pending = store.xpending_range(cfg.telemetry_stream, cfg.group,
                                           min=message_id, max=message_id, count=1)
            if not pending:
                return
        time.sleep(0.25)
    raise TimeoutError(f"processor did not ACK replayed entry {message_id}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=45)
    args = parser.parse_args(argv)
    if args.timeout < 10:
        parser.error("timeout must be at least 10 seconds")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = Settings()
    pcfg = ProcessorConfig.from_env()
    store = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True,
                        socket_connect_timeout=3, socket_timeout=10)
    event = new_event(sequence=888888, distance_m=1.2)
    connected = Event()
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"safestream-phase2-smoke-{event.event_id}")

    def on_connect(client, userdata, flags, reason_code, properties):
        if not reason_code.is_failure:
            connected.set()

    client.on_connect = on_connect
    deadline = time.monotonic() + args.timeout
    try:
        baseline_alert_id = most_recent_id(store, pcfg.alerts_stream)
        baseline_telemetry_id = most_recent_id(store, pcfg.telemetry_stream)
        client.connect(cfg.mqtt_host, cfg.mqtt_port)
        client.loop_start()
        if not connected.wait(timeout=5):
            raise TimeoutError("MQTT broker connection failed")
        publish(client, event)
        alert_redis_id, alert_fields = find_alert(store, pcfg.alerts_stream, baseline_alert_id,
                                                  str(event.event_id), deadline)
        alert = SafetyAlert.model_validate_json(alert_fields["payload"])
        if alert.severity != Severity.CRITICAL or alert.distance_m != 1.2:
            raise AssertionError(f"unexpected alert: {alert.model_dump_json()}")
        # Replaying an identical payload tests logical deduplication at the worker.
        publish(client, event)
        telemetry_ids = find_telemetry_ids(store, pcfg.telemetry_stream,
                                           baseline_telemetry_id, str(event.event_id), 2, deadline)
        wait_until_acknowledged(store, pcfg, telemetry_ids[1], deadline)
        all_alerts = store.xrange(pcfg.alerts_stream, min=baseline_alert_id, max="+")
        matching = [item for item in all_alerts if item[1].get("event_id") == str(event.event_id)]
        if len(matching) != 1:
            raise AssertionError(f"expected 1 alert for replayed event, found {len(matching)}")
        log.info("PASS Phase 2: MQTT -> Redis consumer -> CRITICAL alert + deduplication")
        log.info("event_id=%s alert_id=%s stream_id=%s duplicate_telemetry_entries=%s",
                 event.event_id, alert.alert_id, alert_redis_id, len(telemetry_ids))
        return 0
    except Exception:
        log.exception("FAIL Phase 2 integration gate")
        return 1
    finally:
        client.disconnect()
        client.loop_stop()
        store.close()


if __name__ == "__main__":
    sys.exit(main())
