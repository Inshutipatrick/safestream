from datetime import datetime, timezone
import pytest
from pydantic import ValidationError
from safestream.models import ProximityEvent, new_event
from safestream.simulator import distance_for_sequence


def test_valid_event_roundtrip():
    event = new_event(sequence=2, distance_m=4.0)
    parsed = ProximityEvent.model_validate_json(event.model_dump_json())
    assert parsed.event_id == event.event_id
    assert parsed.observed_at.tzinfo is not None
    assert parsed.distance_m == 4.0


@pytest.mark.parametrize("distance", [-1, float("inf"), float("nan")])
def test_reject_bad_distance(distance):
    with pytest.raises(ValidationError):
        new_event(sequence=0, distance_m=distance)


def test_reject_naive_timestamp():
    event = new_event(sequence=0, distance_m=3.0)
    data = event.model_dump()
    data["observed_at"] = datetime(2026, 1, 1)
    with pytest.raises(ValidationError):
        ProximityEvent.model_validate(data)


def test_distance_cycle_is_repeatable():
    assert distance_for_sequence(0) == 8.0
    assert distance_for_sequence(4) == 1.2
    assert distance_for_sequence(8) == 8.0
