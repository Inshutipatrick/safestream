"""Pure validation of the Phase 2.2 probe's safety checks and helper logic."""
import pytest
from safestream.models import new_event
from safestream.processor import ProcessorConfig
from safestream.recovery_probe import (
    STATE_KEY, event_fields, group_info, matching_alerts, preflight, stream_id_tuple,
)

CFG = ProcessorConfig()


class FakeRedis:
    def __init__(self, *, pending=0, lag=0, group_exists=True, active=False):
        self.pending = pending
        self.lag = lag
        self.group_exists = group_exists
        self.active = active

    def exists(self, key):
        assert key == STATE_KEY
        return int(self.active)

    def xinfo_groups(self, stream):
        assert stream == CFG.telemetry_stream
        return ([{"name": CFG.group, "pending": self.pending, "lag": self.lag}]
                if self.group_exists else [])

    def xrange(self, name, min, max):
        assert name == CFG.alerts_stream
        assert min == "(123-0"
        assert max == "+"
        return [
            ("124-0", {"event_id": "target", "payload": "{}"}),
            ("125-0", {"event_id": "other", "payload": "{}"}),
        ]


def test_event_fields_preserve_id_and_payload():
    event = new_event(sequence=4, distance_m=1.2)
    fields = event_fields(event)
    assert fields["event_id"] == str(event.event_id)
    assert fields["site_id"] == event.site_id
    assert fields["topic"] == "safestream/v1/sites/site-01/proximity"
    assert fields["payload"] == event.model_dump_json()


def test_stream_id_comparison_is_numeric_not_lexical():
    assert stream_id_tuple("100-3") > stream_id_tuple("99-10")


def test_preflight_accepts_drained_group():
    preflight(FakeRedis(), CFG)


@pytest.mark.parametrize("pending,lag", [(1, 0), (0, 3), (1, 3)])
def test_preflight_refuses_unprocessed_work(pending, lag):
    with pytest.raises(RuntimeError, match="not drained"):
        preflight(FakeRedis(pending=pending, lag=lag), CFG)


def test_preflight_refuses_parallel_probe():
    with pytest.raises(RuntimeError, match="already active"):
        preflight(FakeRedis(active=True), CFG)


def test_preflight_requires_existing_consumer_group():
    with pytest.raises(RuntimeError, match="does not exist"):
        preflight(FakeRedis(group_exists=False), CFG)


def test_matching_alerts_ignores_other_ids():
    matches = matching_alerts(FakeRedis(), CFG, "123-0", "target")
    assert [item[0] for item in matches] == ["124-0"]
