# ADR-002 — Event processing, deduplication, and recovery

**Status:** Proposed for local acceptance (Phase 2 initial implementation).

## Decision

- Preserve MQTT -> ingestion -> `safestream:telemetry` from Phase 1.
- Add a separate `processor` service with Redis Streams consumer group `safestream-safety-v1`.
- Create the group at `$` so already-ingested Phase 1 telemetry is not backprocessed; **new arrivals** are processed. Subsequent worker restarts reuse the existing group.
- Classify validated telemetry: `distance <= 1.5` = CRITICAL, `1.5 < distance <= 3` = WARNING, `distance > 3` = SAFE. These are **synthetic test parameters**, not real safety thresholds.
- Emit structured WARNING / CRITICAL alerts to `safestream:alerts`. SAFE is processed and acknowledged without an alert.
- Generate a stable UUIDv5 alert ID from the telemetry `event_id`; one logical alert per input event ID.
- Use a Redis Lua script to atomically write an outcome, record a processed event ID, and `XACK` the telemetry entry. Duplicate telemetry entries remain in `safestream:telemetry`, but create only one *logical alert* while the processed set is preserved.
- Route malformed stream records to `safestream:dead-letter` and ACK them; do not quietly lose validation errors.
- Use `XAUTOCLAIM` to reclaim pending events older than five seconds after a worker crashes. Redis 7+ required.

## Failure and correctness boundaries

- Redis Lua execution is serial/atomic across concurrent clients, but **does not roll back writes on an unexpected runtime error**. The script performs key-type pre-checks to reduce this risk; operational key schema is a necessary precondition.
- Redis AOF with `appendfsync everysec` is **not power-loss durable**. Data can be lost after a host/power failure. No exactly-once or safety-critical guarantees.
- A crash before the Lua commit leaves the stream entry pending; XAUTOCLAIM can recover it. A crash after the commit does not create another alert for the same event ID.
- The processed-ID set grows without bound; it is suitable only for this low-throughput demo. Design retention and/or a durable SQL idempotency ledger before production-scale throughput.
- Alerts represent *individual events*, not correlated incidents. Continuous proximity therefore generates repeated alerts with distinct event IDs. Incident grouping, hysteresis, acknowledgment lifecycle, TimescaleDB, dashboards, real-sensor accuracy, security hardening, and chaos/load testing remain future work.
- The group starts at `$` to avoid retrospectively alerting on Phase 1 data. This means pre-existing telemetry is excluded on the first run, by design.
- SafeStream is a **research prototype**, not a real-world life-safety system.

## Acceptance

1. All Phase 1 tests still pass.
2. New boundary/processor unit tests pass.
3. A new 1.2 m MQTT event yields a `CRITICAL` alert in Redis.
4. Replaying the same event ID does not add a second logical alert.
5. A worker restart after a pending message can reclaim it (to be verified in an explicit failure-injection gate).
