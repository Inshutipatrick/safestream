"""Phase 2.2 live failure-injection probe for Redis Streams consumer recovery.

Use only in a local development instance. `inject` intentionally exits with code
42 after XREADGROUP, without acknowledging the message. It requires the normal
processor and simulator to be stopped, with an empty consumer-group backlog.
`verify` confirms that the restarted processor reclaimed and processed the
entry, then checks duplicate suppression by replaying the same payload.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

import redis

from .config import Settings, site_topic
from .models import new_event
from .processor import ProcessorConfig
from .rules import SafetyAlert, Severity

STATE_KEY = "safestream:test:recovery:v1:active"
STATE_EXPIRY_SECONDS = 7200
INJECTED_EXIT_CODE = 42


def stream_id_tuple(stream_id: str) -> tuple[int, int]:
    parts = stream_id.split("-", 1)
    return int(parts[0]), int(parts[1])


def group_info(store: redis.Redis, cfg: ProcessorConfig) -> dict:
    groups = store.xinfo_groups(cfg.telemetry_stream)
    match = next((g for g in groups if g["name"] == cfg.group), None)
    if match is None:
        raise RuntimeError(f"Consumer group {cfg.group!r} does not exist. Start the processor first.")
    return match


def preflight(store: redis.Redis, cfg: ProcessorConfig) -> None:
    """Refuse to steal normal production data or collide with another probe."""
    if store.exists(STATE_KEY):
        raise RuntimeError(
            f"A recovery probe is already active ({STATE_KEY}). "
            "Finish verification or wait for the state-key expiry."
        )
    info = group_info(store, cfg)
    if info.get("pending") != 0 or info.get("lag") != 0:
        raise RuntimeError(
            "Consumer group is not drained: "
            f"pending={info.get('pending')} lag={info.get('lag')}. "
            "Stop simulator, wait for processing to drain, then stop processor."
        )


def pending_for(store: redis.Redis, cfg: ProcessorConfig, message_id: str) -> list[dict]:
    return store.xpending_range(
        cfg.telemetry_stream, cfg.group, min=message_id, max=message_id, count=1
    )


def matching_alerts(store: redis.Redis, cfg: ProcessorConfig,
                    baseline_alert_id: str, event_id: str) -> list[tuple]:
    # Exclusive lower bound: do not count alerts from before this test.
    entries = store.xrange(cfg.alerts_stream, min=f"({baseline_alert_id}", max="+")
    return [(entry_id, fields) for entry_id, fields in entries
            if fields.get("event_id") == event_id]


def event_fields(event) -> dict[str, str]:
    """Match the shape of records produced by the Phase 1 ingestion bridge."""
    return {
        "event_id": str(event.event_id),
        "site_id": event.site_id,
        "topic": site_topic(event.site_id),
        "payload": event.model_dump_json(),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def inject(store: redis.Redis, cfg: ProcessorConfig) -> None:
    preflight(store, cfg)
    last_alert = store.xrevrange(cfg.alerts_stream, count=1)
    baseline_alert_id = last_alert[0][0] if last_alert else "0-0"
    event = new_event(sequence=2200001, distance_m=1.2)
    fields = event_fields(event)
    source_id = store.xadd(cfg.telemetry_stream, fields)
    abandoned_consumer = f"fault-injector-{uuid4().hex[:12]}"
    batches = store.xreadgroup(
        cfg.group, abandoned_consumer, {cfg.telemetry_stream: ">"}, count=1, block=2000
    )
    delivered = [entry for _stream, messages in batches for entry in messages]
    if len(delivered) != 1 or delivered[0][0] != source_id:
        raise RuntimeError(
            f"Probe did not exclusively claim its event {source_id}; got {delivered!r}. "
            "The processor may still be running. Stop it and inspect the pending list."
        )
    pending = pending_for(store, cfg, source_id)
    if len(pending) != 1 or pending[0].get("consumer") != abandoned_consumer:
        raise AssertionError(f"Expected a pending event owned by {abandoned_consumer}: {pending}")
    state = {
        "event_id": str(event.event_id),
        "source_id": source_id,
        "abandoned_consumer": abandoned_consumer,
        "baseline_alert_id": baseline_alert_id,
        "fields": fields,
    }
    if not store.set(STATE_KEY, json.dumps(state), ex=STATE_EXPIRY_SECONDS, nx=True):
        raise RuntimeError(f"Another recovery probe is active: {STATE_KEY}")
    print("INJECTED: CRITICAL event is pending but NOT acknowledged", flush=True)
    print(f"event_id={event.event_id} source_id={source_id} consumer={abandoned_consumer}", flush=True)
    print(f"INTENTIONAL CRASH: exiting with code {INJECTED_EXIT_CODE} before processing/XACK", flush=True)
    # Bypass Python cleanup to emulate a failed consumer process. This is not
    # the normal processor; it is a one-off probe in its own container.
    os._exit(INJECTED_EXIT_CODE)


def verify(store: redis.Redis, cfg: ProcessorConfig, timeout: int) -> None:
    raw_state = store.get(STATE_KEY)
    if raw_state is None:
        raise RuntimeError("No active injected failure state. Run 'inject' first.")
    state = json.loads(raw_state)
    event_id = state["event_id"]
    source_id = state["source_id"]
    baseline = state["baseline_alert_id"]
    deadline = time.monotonic() + timeout

    # A pending entry must disappear only after a worker acknowledges it.
    while time.monotonic() < deadline:
        pending = pending_for(store, cfg, source_id)
        processed = bool(store.sismember(cfg.processed_set, event_id))
        alerts = matching_alerts(store, cfg, baseline, event_id)
        if not pending and processed and alerts:
            break
        time.sleep(0.35)
    else:
        raise TimeoutError(
            f"Timed out waiting for recovery: source_id={source_id}, "
            f"pending={pending_for(store, cfg, source_id)}; "
            "check the processor logs for XAUTOCLAIM failures."
        )

    if len(alerts) != 1:
        raise AssertionError(f"Expected one recovered alert, found {len(alerts)}")
    alert = SafetyAlert.model_validate_json(alerts[0][1]["payload"])
    if alert.severity is not Severity.CRITICAL or alert.distance_m != 1.2:
        raise AssertionError(f"Wrong recovered alert: {alert.model_dump_json()}")

    # Re-insert the EXACT original event_id, analogous to MQTT redelivery.
    replay_id = store.xadd(cfg.telemetry_stream, state["fields"])
    while time.monotonic() < deadline:
        info = group_info(store, cfg)
        read_past = stream_id_tuple(info["last-delivered-id"]) >= stream_id_tuple(replay_id)
        if read_past and not pending_for(store, cfg, replay_id):
            break
        time.sleep(0.35)
    else:
        raise TimeoutError(f"Duplicate event {replay_id} was not consumed and ACKed")
    alerts_after_replay = matching_alerts(store, cfg, baseline, event_id)
    if len(alerts_after_replay) != 1:
        raise AssertionError(f"Expected one logical alert after replay, got {len(alerts_after_replay)}")
    store.delete(STATE_KEY)
    print("PASS Phase 2.2: abandoned pending event recovered + replay deduplicated")
    print(f"event_id={event_id} recovered_source_id={source_id} replay_id={replay_id}")
    print(f"alert_id={alert.alert_id} matching_alerts={len(alerts_after_replay)} pending=0")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inject", "verify"])
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)
    if args.timeout < 15:
        parser.error("timeout must be >= 15 seconds")
    env = Settings()
    cfg = ProcessorConfig.from_env()
    store = redis.Redis(
        host=env.redis_host, port=env.redis_port, decode_responses=True,
        socket_connect_timeout=3, socket_timeout=10,
    )
    try:
        if args.action == "inject":
            inject(store, cfg)
        else:
            verify(store, cfg, args.timeout)
        return 0
    except Exception as exc:
        print(f"FAIL Phase 2.2 {args.action}: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
