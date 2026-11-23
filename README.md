# SafeStream — Phase 1: Telemetry and MQTT

A minimal, reproducible event-driven safety telemetry pipeline for learning distributed systems.

```text
simulator (Python) --MQTT QoS 1--> Mosquitto
                                      |
                                      v
                             ingestion (Python)
                              validate schema
                                      |
                                Redis XADD
                                      v
                            safestream:telemetry
```

## Requirements

- Docker Desktop with Linux containers and Docker Compose v2 (Windows 10/11), or Docker Engine + Compose on Linux.
- An available local port **1883** (MQTT) and **6379** (Redis).
- No host Python installation is needed for the four-service demo.

## Start (Windows PowerShell)

Open PowerShell in this folder. Ensure Docker Desktop is running.

```powershell
docker compose version
docker compose config
docker compose up --build -d
docker compose ps
docker compose logs -f --tail=30 ingestion simulator
```

`Ctrl+C` stops watching logs; it does **not** stop the Compose services.

## Phase 1 acceptance test

Run in another PowerShell terminal in the same folder:

```powershell
docker compose exec ingestion python -m safestream.smoke --timeout 30
```

Expected output (event ID and Redis ID will differ):

```text
PASS MQTT -> ingestion -> Redis Streams
event_id=<generated-uuid> redis_id=<generated-id>
```

The smoke test publishes a **new unique MQTT event** and verifies the matching event ID appears in the Redis Stream; it does not fake a Redis insertion. The already-running `ingestion` service must process it.

View stream length and recent entries:

```powershell
docker compose exec redis redis-cli XLEN safestream:telemetry
docker compose exec redis redis-cli XREVRANGE safestream:telemetry + - COUNT 2
```

Observe ingress logs only:

```powershell
docker compose logs --tail=20 ingestion
```

Stop services (keeps named volumes):

```powershell
docker compose down
```

**Warning:** `docker compose down -v` deletes recorded MQTT and Redis data.

## Event contract

- MQTT topic: `safestream/v1/sites/site-01/proximity` (ingestion subscribes to `safestream/v1/sites/+/proximity`).
- QoS: 1, retain: false.
- Version: `schema_version=1.0`.
- Fields: `event_id` (UUID), `site_id`, `worker_id`, `equipment_id`, `distance_m` (nonnegative finite float), `observed_at` (timezone-aware UTC), `sequence` (nonnegative integer), `source=simulator`.
- Stream: `safestream:telemetry`; stored fields are `event_id`, `site_id`, `topic`, `payload`, and `received_at`.

Sample message:

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

The simulator repeats this distance sequence in meters: `8.0, 6.0, 4.0, 2.4, 1.2, 2.8, 5.5, 8.0` (approach / retreat), one unique event ID each time.

## Run tests (optional)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

You can alternatively run tests in the image by adding pytest to a development Docker image; the production image intentionally installs only `requirements.txt`.

## Troubleshooting

- **`docker` not recognized:** install and start Docker Desktop. Docker Compose requires a running Docker engine; Python alone is not sufficient.
- **`docker compose ps` shows exited services:** inspect `docker compose logs --tail=100 ingestion mosquitto redis simulator`.
- **Port already in use:** find the other MQTT/Redis process, or change only the host-side port (`127.0.0.1:1883:1883`). Container-to-container ports remain 1883/6379.
- **Smoke times out:** check ingestion logs for `SUBSCRIBED` and `INGESTED`; check Redis `XLEN` and restart with `docker compose restart ingestion` if necessary.
- **PowerShell blocks script activation:** use `.venv\Scripts\python.exe -m pytest -q` without activation.

## Scope and engineering notes

This is **Phase 1 only**: telemetry -> MQTT -> verified ingestion -> Redis Stream. It intentionally has **no alert rule engine** yet. Redis Streams is provisioned now to build a complete transport vertical slice, not to add Phase 2 consumer groups prematurely.

Ingestion uses manual MQTT acknowledgments *after* the Redis write. Messages can still be duplicated if ingestion crashes between Redis XADD and the MQTT ACK; Phase 2 adds a deduplication policy and idempotent processing.

The broker uses anonymous access and no TLS; host ports bind to loopback. Do **not** expose it to a real network or treat the demo as an operational life-safety service.

See `docs/ADR-001-architecture.md` for decisions and limits.

## Next acceptance gate (Phase 2)

Consume `safestream:telemetry` with a Redis consumer group; evaluate proximity rules; create stable alert IDs; acknowledge only after a durable processing result; demonstrate idempotency and restart recovery.
