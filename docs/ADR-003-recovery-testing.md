# ADR-003 — Deterministic consumer-failure experiment (Phase 2.2)

**Status:** Accepted as a local controlled experiment (2026-11-23).

## Context

The processor currently uses a Redis Streams consumer group to read from
`safestream:telemetry`, periodically reclaims abandoned entries using
`XAUTOCLAIM` (`MIN_IDLE_MS=5000`), and atomically combines the processed-ID
set update, alert output, and input `XACK` in a Lua script.

A passing duplicate-message smoke test does not demonstrate recovery of an
entry left in a Redis pending entries list (PEL) when a consumer dies after
`XREADGROUP` but before the acknowledgment.

## Decision

Introduce a **development-only, one-off failure probe** that:

1. Requires the simulator/processor to be stopped and the group drained.
2. Inserts one uniquely identified 1.2-meter telemetry event into the
   existing `safestream:telemetry` stream (bypassing MQTT to isolate recovery).
3. Uses the **real consumer group** to deliver the event to a distinct
   `fault-injector` consumer.
4. Verifies the event is pending, then terminates abnormally without XACK.
5. Restarts the original processor, verifies recovery and correct CRITICAL
   output, then replays the original event ID to check deduplication.

Do not add fault-trigger environment variables or branches into the real
processor at this milestone. Keep normal ingestion and processing code intact.

## Evidence and limitations

This tests the same Redis PEL/XAUTOCLAIM path used after an abandoned delivery;
it is an **injected consumer process failure**, not a timed `SIGKILL` of an
actively executing normal processor. It does not evaluate Redis node failure,
AOF loss windows, performance, or multi-node failover. All telemetry and
thresholds are synthetic and must not be interpreted as validated safety
engineering parameters.

If this test passes, subsequent work can test abrupt termination of the
normal processor under concurrency, Redis availability disruptions, and
bounded deduplication storage as separate experiments.

## Recorded verification (2026-11-23)

The developer reported `38 passed` from pytest and a live probe result of `PASS Phase 2.2: abandoned pending event recovered + replay deduplicated`, with `matching_alerts=1` and `pending=0`. The normal processor and simulator were restarted. This evidence applies to the one-off fault-injector experiment described above; it does not imply that Redis outages or concurrent processor failures were tested.
