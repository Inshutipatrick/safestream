# ADR-001 — Phase 1 message boundary

**Status:** Accepted for prototype.

**Decision:** Four Docker Compose services: Mosquitto, simulator, ingestion bridge, Redis. Simulator publishes versioned proximity events over MQTT QoS 1; ingestion subscribes with a stable client ID and durable MQTT 3.1.1 session (`clean_session=False`); once validated, ingestion writes `XADD` to `safestream:telemetry` and then acknowledges MQTT.

**Why Redis Streams now?** The goal is a working vertical slice of broker -> application -> event queue without implementing rules or analytics. MQTT is the device transport; Redis Streams is the internal event backbone. They play different roles.

**Known limitations:**
- QoS 1 is at-least-once, **not exactly-once**. Crash after `XADD` and before MQTT ACK can produce duplicate stream entries. Deduplication by `event_id` is planned for Phase 2.
- MQTT session is persistent on broker, but the Python Paho client has limits around client-side session state across process restarts. We do not claim mathematically lossless delivery.
- Redis AOF with `appendfsync everysec` can lose the most recent data after power loss; this is a learning environment, not a safety-certified system.
- Simulator generates synthetic proximity *measurements*. It does not simulate true sensor physics, coordinate uncertainty, or real worker safety.
- `allow_anonymous true` and no TLS are **development-only**. Container services share a private Docker network and host-published ports bind to 127.0.0.1.
- Redis Stream entries are append-only without retention or deduplication in Phase 1. The simulator's default 1 event/second is deliberately modest.

**Explicitly out of scope in Phase 1:** risk thresholds, alert state machines, Redis consumer groups, TimescaleDB, Grafana, Kafka, live safety deployment.

**Phase 1 acceptance:** A unique event published by the smoke test appears in Redis Stream after passing through a running ingestion MQTT subscriber. `python -m safestream.smoke` exits 0 and prints PASS.
