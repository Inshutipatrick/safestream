import json
import pytest
from safestream.models import new_event
from safestream.config import site_topic
from safestream.ingestion import InvalidEvent, parse_event, store_message, handle_message


class FakeRedis:
    def __init__(self):
        self.items = []

    def xadd(self, stream, fields):
        self.items.append((stream, fields))
        return b"1234-0"


def test_store_event_without_redis_server():
    event = new_event(sequence=7, distance_m=2.4)
    fake = FakeRedis()
    saved, redis_id = store_message(fake, "safestream:telemetry", site_topic(event.site_id),
                                    event.model_dump_json().encode())
    assert saved.event_id == event.event_id
    assert redis_id == "1234-0"
    assert fake.items[0][1]["event_id"] == str(event.event_id)


def test_reject_topic_mismatch():
    event = new_event(sequence=0, distance_m=1.0)
    with pytest.raises(InvalidEvent, match="does not match site"):
        parse_event(site_topic("site-02"), event.model_dump_json().encode())


def test_reject_malformed_json():
    with pytest.raises(InvalidEvent):
        parse_event(site_topic("site-01"), b"not json")


def test_reject_unknown_fields():
    event = json.loads(new_event(sequence=0, distance_m=2.0).model_dump_json())
    event["untrusted_field"] = 1
    with pytest.raises(InvalidEvent):
        parse_event(site_topic("site-01"), json.dumps(event).encode())


class FakeMessage:
    def __init__(self, event):
        self.topic = site_topic(event.site_id)
        self.payload = event.model_dump_json().encode()
        self.mid = 42
        self.qos = 1


class FakeMqtt:
    def __init__(self, calls):
        self.calls = calls

    def ack(self, mid, qos):
        self.calls.append("ack")


def test_ack_happens_after_stream_write():
    calls = []
    event = new_event(sequence=1, distance_m=3.0)

    class OrderedFakeRedis:
        def xadd(self, stream, fields):
            calls.append("xadd")
            return "1234-0"

    assert handle_message(FakeMqtt(calls), OrderedFakeRedis(),
                          "safestream:telemetry", FakeMessage(event)) is True
    assert calls == ["xadd", "ack"]


def test_invalid_message_is_acknowledged_without_xadd():
    calls = []
    event = new_event(sequence=1, distance_m=3.0)
    message = FakeMessage(event)
    message.payload = b"invalid json"

    class NoWriteRedis:
        def xadd(self, stream, fields):
            raise AssertionError("should not write malformed events")

    assert handle_message(FakeMqtt(calls), NoWriteRedis(),
                          "safestream:telemetry", message) is False
    assert calls == ["ack"]


def test_failed_stream_write_is_not_acknowledged():
    calls = []
    event = new_event(sequence=1, distance_m=3.0)

    class FailingRedis:
        def xadd(self, stream, fields):
            raise RuntimeError("simulated Redis failure")

    with pytest.raises(RuntimeError):
        handle_message(FakeMqtt(calls), FailingRedis(),
                       "safestream:telemetry", FakeMessage(event))
    assert calls == []
