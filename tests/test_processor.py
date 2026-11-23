"""Processor behavior with a fake atomic script; live Redis is tested by smoke."""
import json
from safestream.models import new_event
from safestream.processor import ProcessorConfig, process_entry, parse_stream_event

CFG = ProcessorConfig()


class FakeScript:
    def __init__(self):
        self.seen = set()
        self.calls = []

    def __call__(self, *, keys, args, client):
        self.calls.append((keys, args))
        first = args[0] not in self.seen
        self.seen.add(args[0])
        return [int(first), 1]


def fields_for(event):
    return {"event_id": str(event.event_id), "site_id": event.site_id,
            "payload": event.model_dump_json()}


def test_critical_event_produces_alert():
    script = FakeScript()
    event = new_event(sequence=1, distance_m=1.2)
    result = process_entry(object(), script, CFG, "1000-0", fields_for(event))
    assert result == ("alert", True)
    keys, args = script.calls[0]
    assert keys[1] == "safestream:alerts"
    assert args[3] == "alert"
    assert args[5] == "CRITICAL"
    assert json.loads(args[7])["distance_m"] == 1.2


def test_duplicate_event_does_not_create_another_logical_alert():
    script = FakeScript()
    event = new_event(sequence=1, distance_m=1.2)
    assert process_entry(object(), script, CFG, "1000-0", fields_for(event)) == ("alert", True)
    assert process_entry(object(), script, CFG, "1001-0", fields_for(event)) == ("alert", False)


def test_safe_event_is_recorded_without_alert():
    script = FakeScript()
    event = new_event(sequence=1, distance_m=8)
    assert process_entry(object(), script, CFG, "1002-0", fields_for(event)) == ("safe", True)
    assert script.calls[0][1][3] == "safe"


def test_malformed_entry_routes_to_dead_letter():
    script = FakeScript()
    fields = {"event_id": "bad", "site_id": "site-01", "payload": "not json"}
    assert process_entry(object(), script, CFG, "1003-0", fields) == ("dead_letter", True)
    assert script.calls[0][1][0] == "invalid:1003-0"
    assert script.calls[0][1][3] == "dead_letter"


def test_event_id_mismatch_is_rejected():
    event = new_event(sequence=1, distance_m=1.2)
    fields = fields_for(event)
    fields["event_id"] = "different-event-id"
    import pytest
    with pytest.raises(ValueError, match="event_id does not match"):
        parse_stream_event(fields)
