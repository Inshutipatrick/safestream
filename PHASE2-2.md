# SafeStream — Phase 2.2: Controlled consumer failure and Redis recovery

## Goal and scope

Prove that a **real Redis pending entry** left behind by a crashed one-off consumer
is recovered by SafeStream's existing processor via `XAUTOCLAIM`. Then redeliver
that same event and prove it generates **only one logical CRITICAL alert**.

This probes Redis consumer-group recovery and logical deduplication; it does **not**
yet prove power-loss durability, Redis failover, high-load reliability, or a real
worker crash in the middle of a production callback.

**Important:** This is a development-only experiment. It intentionally stops
`simulator` and `processor`, but leaves Mosquitto, Redis, and ingestion running.
Do not use it on a shared/production deployment.

## Step 1: Work on the existing Phase 2 branch

In a **new Windows PowerShell terminal**, from the same repository:

```powershell
cd D:\Dev\safestream
git status
git branch --show-current
```

Expected branch: `feature/phase2-event-processor` with a clean working tree
before applying this overlay.

Unzip this overlay **into `D:\Dev\safestream`**. It contains only new files:

- `PHASE2-2.md`
- `src/safestream/recovery_probe.py`
- `tests/test_recovery_probe.py`
- `docs/ADR-003-recovery-testing.md`

It does **not** overwrite `compose.yaml`, your host-port fix, or Phase 2.1 files.

## Step 2: Run all unit tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Expected if the repository matches the Phase 2.1 release: **38 passed**
(29 existing tests + 9 new probe tests). Unit tests use fake Redis objects for
probe checks; these tests do not simulate a real crashed container.

## Step 3: Rebuild the processor with the new probe module

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml config --quiet
docker compose -f compose.yaml -f compose.phase2.yaml up --build -d
docker compose -f compose.yaml -f compose.phase2.yaml ps
```

All five services should be running.

## Step 4: Quiesce the pipeline before injecting a failure

Stop the traffic generator, then give ingestion/processing time to drain:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml stop simulator
Start-Sleep -Seconds 4
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XINFO GROUPS safestream:telemetry
```

The `safestream-safety-v1` group should report `pending` **0** and `lag` **0**.
If either value is nonzero, do **not** continue until the backlog drains and
any errors are investigated. Then stop only the normal processor:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml stop processor
```

Both services must remain stopped while `inject` runs.

## Step 5: Inject an intentionally abandoned pending message

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml run --rm --no-deps processor python -m safestream.recovery_probe inject
```

Expected output resembles:

```text
INJECTED: CRITICAL event is pending but NOT acknowledged
event_id=<uuid> source_id=<stream-id> consumer=fault-injector-<id>
INTENTIONAL CRASH: exiting with code 42 before processing/XACK
```

**Exit code 42 is intentional**, not a failed test. This one-off process reads
one newly written 1.2 m telemetry record from the real group and terminates
abruptly without `XACK`. It stores a small, temporary test manifest in Redis
(`safestream:test:recovery:v1:active`, with two-hour expiry). The normal
processor has not yet been restarted.

To independently inspect pending status:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec redis redis-cli XPENDING safestream:telemetry safestream-safety-v1
```

Look for at least one pending message (the injected entry). If `inject`
reports any error **other than the intentional exit 42**, stop and inspect
`processor` and Redis logs rather than manually deleting any queue entries.

## Step 6: Restart the normal processor and verify recovery

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml start processor
docker compose -f compose.yaml -f compose.phase2.yaml exec processor python -m safestream.recovery_probe verify --timeout 60
```

The processor's configured `MIN_IDLE_MS=5000` means abandoned entries must be
idle for at least five seconds before `XAUTOCLAIM` can take them. The polling
loop may take several seconds to observe the result.

Expected output:

```text
PASS Phase 2.2: abandoned pending event recovered + replay deduplicated
event_id=<uuid> recovered_source_id=<stream-id> replay_id=<stream-id>
alert_id=<uuid> matching_alerts=1 pending=0
```

`verify` waits for the original entry's pending record to disappear, checks
that its event ID is marked processed, validates one CRITICAL alert, then
replays the **same exact payload and event ID** in Redis. It waits for the
replay to be consumed/acknowledged and re-checks that only one logical alert
exists. The temporary manifest is removed on success.

Check the source ID in the processor logs:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml logs --tail=100 processor
```

## Step 7: Resume simulator and preserve evidence

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml start simulator
docker compose -f compose.yaml -f compose.phase2.yaml ps
.\.venv\Scripts\python.exe -m pytest -q
git status
```

Only after the live recovery probe and tests pass, commit the new files:

```powershell
git add PHASE2-2.md docs/ADR-003-recovery-testing.md src/safestream/recovery_probe.py tests/test_recovery_probe.py
git commit -m "test: verify redis consumer recovery after crash"
```

No new tag is required yet. The broader Phase 2 milestone is still open.

## Troubleshooting and safeguards

- **`FAIL ... group is not drained`:** check `XINFO GROUPS`; if the processor
  was stopped too early, start it again temporarily, let it drain, then stop.
  Keep the simulator stopped while doing so.
- **`FAIL ... probe is already active`:** a previous injection left temporary
  state. Resume with `verify` before trying another injection. The state expires
  automatically after two hours; do not clear arbitrary Redis keys.
- **`FAIL ... did not exclusively claim its event`:** do not assume success.
  Another writer or processor might still be running; inspect Compose status
  and group pending entries.
- **`verify` times out:** inspect processor logs, `XPENDING`, and the group
  name. Do **not** delete Redis volumes or run `docker compose down -v`.
- **Port collisions:** this overlay does not expose any Windows host ports.
  Keep your previously corrected base Compose configuration unchanged.
- **Other SafeStream commands:** always include **both** `-f` flags until
  we formally consolidate the Compose files.

## Acceptance criteria

- The injected message is recorded in the Redis group's pending list.
- The one-off consumer demonstrably exits without acknowledging the message.
- The standard processor reclaims and acknowledges the abandoned entry.
- Exactly one CRITICAL alert is present for the injected event ID.
- A duplicate replay is acknowledged without producing another logical alert.
- Existing automated tests continue to pass; all five services run afterward.
