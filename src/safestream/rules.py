"""Pure, deterministic Phase 2 proximity rules (prototype only)."""
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict

from .models import ProximityEvent


class Severity(str, Enum):
    SAFE = "SAFE"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


# Simulation parameters, NOT engineered or certified safety distances.
WARNING_M = 3.0
CRITICAL_M = 1.5
RULE_VERSION = "proximity-v1"
# Stable namespace ensures the same telemetry event gets the same logical alert ID.
ALERT_NAMESPACE = UUID("1eaa3691-1bc2-4f2e-aa91-6f1b8aa9987d")


def classify(distance_m: float) -> Severity:
    """Input has been validated by ProximityEvent; do not use for raw sensor data."""
    if distance_m <= CRITICAL_M:
        return Severity.CRITICAL
    if distance_m <= WARNING_M:
        return Severity.WARNING
    return Severity.SAFE


class SafetyAlert(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str = "1.0"
    alert_id: UUID
    event_id: UUID
    site_id: str
    worker_id: str
    equipment_id: str
    distance_m: float
    severity: Severity
    rule_version: str = RULE_VERSION
    observed_at: datetime
    triggered_at: datetime
    status: str = "OPEN"


def make_alert(event: ProximityEvent, *, now: datetime | None = None) -> SafetyAlert | None:
    severity = classify(event.distance_m)
    if severity == Severity.SAFE:
        return None
    return SafetyAlert(
        alert_id=uuid5(ALERT_NAMESPACE, str(event.event_id)),
        event_id=event.event_id,
        site_id=event.site_id,
        worker_id=event.worker_id,
        equipment_id=event.equipment_id,
        distance_m=event.distance_m,
        severity=severity,
        observed_at=event.observed_at,
        triggered_at=now or datetime.now(timezone.utc),
    )
