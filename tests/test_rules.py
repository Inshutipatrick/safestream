"""Pure safety rule boundaries and idempotent alert IDs."""
import pytest
from safestream.models import new_event
from safestream.rules import Severity, classify, make_alert


@pytest.mark.parametrize("distance,expected", [
    (0.0, Severity.CRITICAL), (1.2, Severity.CRITICAL),
    (1.5, Severity.CRITICAL), (1.500001, Severity.WARNING),
    (2.4, Severity.WARNING), (3.0, Severity.WARNING),
    (3.000001, Severity.SAFE), (8.0, Severity.SAFE),
])
def test_classification_edges(distance, expected):
    assert classify(distance) is expected


def test_critical_alert_contains_original_event():
    event = new_event(sequence=1, distance_m=1.2)
    alert = make_alert(event)
    assert alert is not None
    assert alert.severity == Severity.CRITICAL
    assert alert.event_id == event.event_id
    assert alert.distance_m == 1.2
    assert alert.status == "OPEN"


def test_safe_event_emits_no_alert():
    assert make_alert(new_event(sequence=1, distance_m=3.000001)) is None


def test_replayed_event_has_stable_alert_id():
    event = new_event(sequence=1, distance_m=2.4)
    assert make_alert(event).alert_id == make_alert(event).alert_id
