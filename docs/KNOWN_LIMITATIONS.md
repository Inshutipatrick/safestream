# Known limitations and prioritized engineering backlog

> SafeStream is a learning prototype; the items below are **not** validated production safety controls.

## Must address before any external deployment

1. **Authentication, authorization, and transport security.** Mosquitto currently allows anonymous, plaintext connections. Redis is reachable within the Compose network without application credentials. No external deployment until credentials, ACLs, TLS, secret management, and network isolation are designed and tested.
2. **Durable event ledger.** Redis is a single-instance store. AOF `appendfsync everysec` can lose recent writes after power failure; the Redis Set of processed event IDs is not a permanent SQL audit trail. Decide and test recovery-point/recovery-time objectives, backups, and event reprocessing guarantees.
3. **Alert interpretation.** Current rule thresholds (3.0 m warning, 1.5 m critical) are illustrative only. Sensor uncertainty, velocity, site hazards, worker-specific context, false positive/negative rates, and regulatory requirements are not modeled.
4. **Incident lifecycle and alert fatigue.** One hazardous reading with a fresh event ID creates one alert. Correlation windows, hysteresis, notification throttling, escalation, acknowledgment, and resolution have not been implemented.

## Next experiments / features

| Priority | Work item | Acceptance evidence |
| --- | --- | --- |
| P1 | Abruptly terminate the **actual** processor during concurrent event traffic | No lost acknowledged events within documented failure model; one logical alert per ID |
| P1 | Redis downtime and restart, AOF persistence behavior | Measured loss window and recovery behavior under explicit conditions |
| P1 | Stream and deduplication retention policy | Bounded memory with documented deduplication horizon |
| P1 | Conflict detection for duplicated `event_id` with differing payloads | Reject, quarantine, or audit conflicting replays |
| P2 | Latency, throughput, and queue-lag metrics | Reproducible p50/p95/p99 and backlog benchmarks |
| P2 | TimescaleDB persistence / immutable event audit | Verified replay, timestamp handling, and auditability |
| P2 | FastAPI / authenticated client API and Grafana dashboards | Integration tests, access controls, meaningful SLO dashboards |
| P2 | Structured logs, metrics, and distributed traces | Cross-service correlation by event ID; failure investigation |

## Proven versus unproven

**Proven by local experiments:** MQTT-to-Redis ingestion, proximity classification and logical duplicate suppression, one deterministic abandoned-message recovery using the real Redis consumer group.

**Not proven:** exactly-once end-to-end delivery, real-time deadlines, fault tolerance across Redis failures, database durability, concurrent failure recovery, physically valid safety thresholds, production security, and regulatory compliance.
