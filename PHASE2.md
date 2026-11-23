# SafeStream — Phase 2.1: Event consumer and proximity rules

This is an **additive upgrade** to your already functioning Phase 1 Git repository.
It deliberately **does not replace `compose.yaml`**, since you removed host port mappings to avoid conflicts with other local projects.

## What it adds

```text
simulator -> MQTT -> ingestion -> safestream:telemetry
                                      |
                              Redis consumer group
                                      |
                                 processor
                             /          |          \
                      CRITICAL /      SAFE        Invalid
                       WARNING        XACK          |
                         |              |           v
                  safestream:alerts  processed   dead-letter
```

The application does **not** contact Redis via Windows host ports; all services use Docker networking.

## Apply the upgrade in PowerShell

1. Open a **new** PowerShell and make sure Docker Desktop is running.
2. From `D:\Dev\safestream`, check you are on the Phase 1 repository:

```powershell
cd D:\Dev\safestream
git status
git tag --list
```

3. Make a development branch. If `feature/phase2-event-processor` already exists, switch to it instead.

```powershell
git switch -c feature/phase2-event-processor
```

4. Extract the overlay archive directly **into the existing** `D:\Dev\safestream` folder. For example, if the ZIP is in Downloads:

```powershell
Expand-Archive -LiteralPath "$HOME\Downloads\safestream-phase2-m1-overlay.zip" -DestinationPath "D:\Dev\safestream" -Force
```

Only *new* paths are contained in the ZIP: `compose.phase2.yaml`, `PHASE2.md`, `docs/ADR-002-processing.md`, `src/safestream/rules.py`, `src/safestream/processor.py`, `src/safestream/phase2_smoke.py`, and two tests. Your local `compose.yaml`, Phase 1 sources, `.venv`, Redis volume and Git history are preserved.

5. Run unit tests without activating the venv:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

6. Validate the combined Compose configuration and launch the additional worker:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml config --quiet
docker compose -f compose.yaml -f compose.phase2.yaml up --build -d
docker compose -f compose.yaml -f compose.phase2.yaml ps
```

7. Inspect rules worker log:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml logs --tail=30 processor
```

8. Run the Phase 2.1 integration gate; this publishes a new **1.2 m** event twice, waits for the worker to acknowledge the replay, and checks that exactly one CRITICAL alert was written for that event ID:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec processor python -m safestream.phase2_smoke --timeout 45
```

9. Inspect alert stream and group status:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XREVRANGE safestream:alerts + - COUNT 2
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XINFO GROUPS safestream:telemetry
```

If successful, commit **only after reviewing changes**:

```powershell
git status
git add PHASE2.md compose.phase2.yaml docs/ADR-002-processing.md src/safestream/rules.py src/safestream/processor.py src/safestream/phase2_smoke.py tests/test_rules.py tests/test_processor.py
git commit -m "feat: add phase 2.1 Redis consumer and proximity alerts"
```

Do **not** tag the entire Phase 2 as completed yet: failure-injection and recovery verification remain outstanding.

## Design decisions

- **New messages only:** On first startup the processor creates a group at `$`; it intentionally does not replay hours of Phase 1 history. Restarting the processor reuses the group.
- **Threshold boundary:** 1.5 m = CRITICAL; 3.0 m = WARNING; >3.0 m = SAFE. Test values only.
- **Deduplication:** processed IDs are stored in Redis Set `safestream:processed-event-ids`. WARNING/CRITICAL alerts are published to `safestream:alerts` with stable UUIDv5 alert IDs.
- **Atomic behavior:** Lua combines alert output + processed record + XACK to prevent duplicate logical output on normal retries. It is not a guarantee of permanent exactly-once delivery.
- **Crash recovery:** Uses XAUTOCLAIM for pending unacknowledged events. An actual injected-crash test is the next acceptance milestone.
- **Malformed events:** preserved in `safestream:dead-letter` (they are not treated as proximity alerts).
- **Open limitations:** unbounded processed-ID set and streams; no persistent SQL storage, incident-level aggregation, external notifications, identity/access controls, or real-world safety certification.

## How to shut down or restart

Use **the same two `-f` flags** in later Compose commands until we consolidate Phase 2 into the base Compose file. For example:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml down
```

This retains the named volumes. Do not use `-v` unless you intend to delete persisted telemetry and alert history.
