"""Redis Streams -> proximity rules -> idempotent alerts / dead letters.

The Lua script atomically records an outcome, marks the event processed, and
acknowledges the input stream message. This provides logical deduplication in
one Redis instance, not end-to-end exactly-once delivery or power-loss safety.
"""
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass

import redis
from pydantic import ValidationError

from .config import Settings
from .models import ProximityEvent
from .rules import make_alert

log = logging.getLogger("safestream.processor")

# Pre-check key types before any write to avoid partial effects from a wrong-type
# error. Redis Lua is atomic for concurrent clients, but DOES NOT rollback errors.
ATOMIC_OUTCOME_LUA = """
local expected = {'set', 'stream', 'stream', 'stream'}
for i = 1, 4 do
  local t = redis.call('TYPE', KEYS[i]).ok
  if t ~= 'none' and t ~= expected[i] then
    return redis.error_reply('wrong Redis key type for KEYS[' .. i .. ']')
  end
end
local created = 0
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  if ARGV[4] == 'alert' then
    redis.call('XADD', KEYS[2], '*', 'event_id', ARGV[5],
               'severity', ARGV[6], 'alert_id', ARGV[7],
               'payload', ARGV[8])
  elseif ARGV[4] == 'dead_letter' then
    redis.call('XADD', KEYS[4], '*', 'source_id', ARGV[3],
               'reason', ARGV[8])
  elseif ARGV[4] ~= 'safe' then
    return redis.error_reply('unknown outcome')
  end
  redis.call('SADD', KEYS[1], ARGV[1])
  created = 1
end
local acked = redis.call('XACK', KEYS[3], ARGV[2], ARGV[3])
return {created, acked}
"""


@dataclass(frozen=True)
class ProcessorConfig:
    telemetry_stream: str = "safestream:telemetry"
    alerts_stream: str = "safestream:alerts"
    dead_letter_stream: str = "safestream:dead-letter"
    processed_set: str = "safestream:processed-event-ids"
    group: str = "safestream-safety-v1"
    min_idle_ms: int = 5000

    @classmethod
    def from_env(cls) -> "ProcessorConfig":
        return cls(
            telemetry_stream=os.getenv("REDIS_STREAM", "safestream:telemetry"),
            alerts_stream=os.getenv("ALERTS_STREAM", "safestream:alerts"),
            dead_letter_stream=os.getenv("DEAD_LETTER_STREAM", "safestream:dead-letter"),
            processed_set=os.getenv("PROCESSED_SET", "safestream:processed-event-ids"),
            group=os.getenv("REDIS_GROUP", "safestream-safety-v1"),
            min_idle_ms=int(os.getenv("MIN_IDLE_MS", "5000")),
        )


def ensure_group(store: redis.Redis, cfg: ProcessorConfig) -> None:
    """Start from new messages; existing Phase 1 history is intentionally skipped."""
    try:
        store.xgroup_create(cfg.telemetry_stream, cfg.group, id="$", mkstream=True)
        log.info("GROUP_CREATED stream=%s group=%s initial_id=$", cfg.telemetry_stream, cfg.group)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        log.info("GROUP_REUSED stream=%s group=%s", cfg.telemetry_stream, cfg.group)


def parse_stream_event(fields: dict[str, str]) -> ProximityEvent:
    if "payload" not in fields:
        raise ValueError("missing payload")
    try:
        event = ProximityEvent.model_validate_json(fields["payload"])
    except ValidationError as exc:
        raise ValueError(f"bad telemetry schema: {exc}") from exc
    if fields.get("event_id") != str(event.event_id):
        raise ValueError("stream event_id does not match payload")
    if fields.get("site_id") != event.site_id:
        raise ValueError("stream site_id does not match payload")
    return event


def process_entry(store: redis.Redis, script, cfg: ProcessorConfig,
                  redis_id: str, fields: dict[str, str]) -> tuple[str, bool]:
    """Process one stream entry. The script must atomically XACK the source.

    Returns (outcome, whether this is the first processed occurrence of event_id).
    Any Redis failure raises; the unacknowledged input can be reclaimed later.
    """
    event_id = ""
    alert_id = ""
    severity = ""
    try:
        event = parse_stream_event(fields)
    except (ValueError, TypeError) as exc:
        outcome = "dead_letter"
        dedupe_key = f"invalid:{redis_id}"
        event_id = fields.get("event_id", "")
        payload = str(exc)[:1000]
    else:
        event_id = str(event.event_id)
        dedupe_key = event_id
        alert = make_alert(event)
        if alert is None:
            outcome = "safe"
            payload = ""
        else:
            outcome = "alert"
            alert_id = str(alert.alert_id)
            severity = alert.severity.value
            payload = alert.model_dump_json()

    result = script(
        keys=[cfg.processed_set, cfg.alerts_stream, cfg.telemetry_stream, cfg.dead_letter_stream],
        args=[dedupe_key, cfg.group, redis_id, outcome, event_id, severity, alert_id, payload],
        client=store,
    )
    created, acked = [int(value) for value in result]
    if acked != 1:
        # An already-acknowledged entry may have raced with a reclaim. No output
        # should be repeated, because the processed-event set suppresses it.
        log.warning("ACK_MISSING stream_id=%s group=%s", redis_id, cfg.group)
    if outcome == "alert":
        log.info("%s event_id=%s severity=%s alert_id=%s source_id=%s",
                 "ALERT" if created else "DUPLICATE", event_id, severity, alert_id, redis_id)
    elif outcome == "dead_letter":
        log.warning("%s source_id=%s reason=%s", "DEAD_LETTER" if created else "DUPLICATE_INVALID",
                    redis_id, payload)
    else:
        log.info("%s event_id=%s source_id=%s", "SAFE" if created else "DUPLICATE",
                 event_id, redis_id)
    return outcome, bool(created)


def run_forever(store: redis.Redis, cfg: ProcessorConfig) -> None:
    ensure_group(store, cfg)
    script = store.register_script(ATOMIC_OUTCOME_LUA)
    consumer = f"{socket.gethostname()}-{os.getpid()}"
    log.info("PROCESSOR_READY stream=%s group=%s consumer=%s", cfg.telemetry_stream,
             cfg.group, consumer)
    last_claim = 0.0
    while True:
        if time.monotonic() - last_claim >= 5.0:
            # Recover unacknowledged entries abandoned by workers that crashed.
            # Redis 7 returns cursor, claimed entries, deleted IDs.
            _, reclaimed, _ = store.xautoclaim(
                cfg.telemetry_stream, cfg.group, consumer,
                min_idle_time=cfg.min_idle_ms, start_id="0-0", count=20,
            )
            for entry_id, fields in reclaimed:
                process_entry(store, script, cfg, entry_id, fields)
            last_claim = time.monotonic()
        batches = store.xreadgroup(cfg.group, consumer, {cfg.telemetry_stream: ">"},
                                   count=20, block=1000)
        for _stream, messages in batches:
            for entry_id, fields in messages:
                process_entry(store, script, cfg, entry_id, fields)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = ProcessorConfig.from_env()
    host_cfg = Settings()
    store = redis.Redis(host=host_cfg.redis_host, port=host_cfg.redis_port,
                        decode_responses=True, socket_connect_timeout=3, socket_timeout=10)
    try:
        run_forever(store, cfg)
    except KeyboardInterrupt:
        log.info("Processor stopped")
        return 0
    except Exception:
        log.exception("PROCESSOR_FAILED; Compose will restart the consumer")
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
