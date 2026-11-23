# SafeStream

**Event-driven safety alerts — a distributed-systems learning prototype**

SafeStream ingests simulated worker–equipment proximity measurements over MQTT, persists them in Redis Streams, processes events with a Redis consumer group, and emits WARNING or CRITICAL alerts. It is designed to demonstrate asynchronous delivery, event validation, idempotent processing, and recovery of pending messages.

> **Research/development prototype only.** Synthetic measurements and thresholds are not validated for construction-site operations. Do not use this software as a personnel-protection or life-safety system.

## Current status

| Milestone | What was verified | Status |
| --- | --- | --- |
| Phase 1 — telemetry and MQTT | MQTT → ingestion → Redis smoke test; 13 unit tests | Passed locally |
| Phase 2.1 — alert processing | CRITICAL event and duplicate replay; 29 unit tests | Passed locally |
| Phase 2.2 — recovery experiment | Pending message reclaimed and acknowledged; exactly one alert after replay; 38 unit tests | Passed locally |
| Further reliability work | Real processor termination, Redis outage, load/latency, retention limits | Not yet verified |
| Phase 3+ | TimescaleDB, FastAPI, Grafana, production-style observability | Not implemented |

The results above are from the project developer's Windows/Docker environment on **2026-11-23**; they are not claims of formal certification or an independent production audit.

## Architecture

```text
simulator (Python)
    │ proximity events, MQTT QoS 1
    ▼
Mosquitto MQTT broker
    │ topic: safestream/v1/sites/+/proximity
    ▼
ingestion (Python) ─── validates payload, XADD before MQTT ACK
    │
    ▼
Redis Stream: safestream:telemetry
    │ consumer group: safestream-safety-v1
    ▼
processor (Python) ─── rules: SAFE / WARNING / CRITICAL
    ├──► Redis Stream: safestream:alerts (WARNING, CRITICAL)
    ├──► Redis Stream: safestream:dead-letter (invalid records)
    └──► Redis Set: safestream:processed-event-ids (deduplication)
         (Lua outcome + dedup record + XACK on one Redis instance)
```

The stack has **five services**: `mosquitto`, `redis`, `ingestion`, `simulator`, and `processor`. `compose.phase2.yaml` adds the fifth service to the existing Phase 1 Compose file. Keep both files in Compose commands until a separately tested consolidation is approved.

## Prerequisites

- Docker Desktop with Linux containers and Docker Compose on Windows, or Docker Engine plus Compose on Linux.
- Python 3.12 for local tests. Containerized operation does not require a host Python installation.
- **No Windows host-port bindings are needed.** Docker services communicate internally as `mosquitto:1883` and `redis:6379`. The locally corrected `compose.yaml` has had both `ports:` sections removed to avoid conflicts with other projects. Check your own `compose.yaml` before starting; do not reintroduce these bindings.

## Quick start — Windows PowerShell

From the repository root:

```powershell
cd D:\Dev\safestream
docker compose -f compose.yaml -f compose.phase2.yaml config --quiet
docker compose -f compose.yaml -f compose.phase2.yaml up --build -d
docker compose -f compose.yaml -f compose.phase2.yaml ps
```

Inspect logs:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml logs --tail=40 ingestion simulator processor
```

Do not expose the unauthenticated MQTT broker or Redis to public networks.

## Required verification

### Unit tests

Create the local environment only if `.venv` does not already exist:

```powershell
py -3.12 -m venv .venv
```

**PowerShell activation is not required** (useful when execution policy blocks `.ps1` scripts):

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
```

At the Phase 2.2 checkpoint, the expected unit-test baseline is **38 passed**. The GitHub Actions workflow runs these tests on pushes and pull requests; it does not execute Docker integration tests.

### Phase 1 smoke test

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec ingestion python -m safestream.smoke --timeout 30
```

Expected: `PASS MQTT -> ingestion -> Redis Streams`.

### Phase 2.1 smoke test

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec processor python -m safestream.phase2_smoke --timeout 45
```

Expected: `PASS Phase 2: MQTT -> Redis consumer -> CRITICAL alert + deduplication`.

### Phase 2.2 controlled recovery test

**Do not run this blindly on a live/shared environment.** It intentionally stops the simulator and processor and injects an abandoned pending message. Follow the preflight, injection, verification, and cleanup sequence in [PHASE2-2.md](PHASE2-2.md).

Expected: `PASS Phase 2.2: abandoned pending event recovered + replay deduplicated`.

## Example event

MQTT topic: `safestream/v1/sites/site-01/proximity`

```json
{
  "schema_version": "1.0",
  "event_id": "0a0587ea-bbe7-49af-ac36-84fdfd9fed20",
  "site_id": "site-01",
  "worker_id": "worker-07",
  "equipment_id": "excavator-02",
  "distance_m": 2.4,
  "observed_at": "2026-11-22T10:30:00Z",
  "source": "simulator",
  "sequence": 3
}
```

Classification (simulation only): **SAFE** when distance > 3.0 m, **WARNING** when 1.5 m < distance <= 3.0 m, and **CRITICAL** when distance <= 1.5 m. The processor emits a new alert for *each distinct hazardous event ID*; it does not yet aggregate consecutive readings into incidents.

## Data and operations

- Telemetry: `safestream:telemetry`
- Alerts: `safestream:alerts`
- Malformed events: `safestream:dead-letter`
- Processed IDs: `safestream:processed-event-ids`
- Consumer group: `safestream-safety-v1`

Inspect stream length and group status:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XLEN safestream:telemetry
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XINFO GROUPS safestream:telemetry
```

Stop the stack **without deleting volumes**:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml down
```

**Do not run `down -v` unless you intentionally want to destroy stored telemetry, alerts, and deduplication state.**

## Project map

```text
compose.yaml                 Four core services (keep your local no-host-port fix)
compose.phase2.yaml          Adds the processor service
src/safestream/              Runtime services, event models, rules, and diagnostic probes
tests/                       Unit tests (38 at Phase 2.2)
mosquitto/config/            Development broker configuration
PHASE2.md                   Historical Phase 2.1 execution guide
PHASE2-2.md                 Controlled recovery experiment guide
docs/ADR-*.md                Architectural decisions and limitations
docs/KNOWN_LIMITATIONS.md    Prioritized technical debt
.github/workflows/tests.yml  Unit-test CI (no Docker)
```

The two Phase 2 guides are **milestone runbooks**, not duplicate service implementations. Keep them for reproducibility, but treat this README as the up-to-date onboarding document.

## Engineering limitations and security

This project currently uses an anonymous, unencrypted development MQTT listener; Redis has no application-level authentication; default Redis AOF persistence can lose recently acknowledged writes after a power loss; and deduplication state grows unbounded. No hard real-time guarantees, field-sensor calibration, access controls, or incident-level alert suppression are implemented. See [Known limitations](docs/KNOWN_LIMITATIONS.md) and [architecture decision records](docs/ADR-001-architecture.md).

**Never commit `.env`, `.venv`, credentials, broker/Redis data volumes, or generated test outputs.** The system must remain isolated to a local development environment.
