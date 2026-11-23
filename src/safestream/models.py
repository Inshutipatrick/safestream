"""Versioned event contract shared by the simulator, ingestion and smoke test."""
from datetime import datetime, timezone
from uuid import UUID, uuid4
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProximityEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    event_id: UUID
    site_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    worker_id: str = Field(min_length=1, max_length=64)
    equipment_id: str = Field(min_length=1, max_length=64)
    distance_m: float = Field(ge=0, allow_inf_nan=False)
    observed_at: datetime
    source: Literal["simulator"] = "simulator"
    sequence: int = Field(ge=0)

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        return value.astimezone(timezone.utc)


def new_event(*, sequence: int, distance_m: float, site_id: str = "site-01",
              worker_id: str = "worker-07", equipment_id: str = "excavator-02") -> ProximityEvent:
    return ProximityEvent(
        event_id=uuid4(), site_id=site_id, worker_id=worker_id,
        equipment_id=equipment_id, distance_m=distance_m,
        observed_at=datetime.now(timezone.utc), sequence=sequence,
    )
