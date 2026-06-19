import pytest
import uuid
import time

class TestPersistence:
    def test_published_event_retrievable(self, make_event, publish_event, get_events):
        topic = f"persist-retrieve-{uuid.uuid4().hex[:8]}"
        ev = make_event(topic=topic)
        publish_event(ev)
        time.sleep(2) # Tunggu consumer process

        result = get_events(topic=topic)
        assert result["count"] >= 1
        
        stored_ids = [e["event_id"] for e in result["events"]]
        assert ev["event_id"] in stored_ids

    def test_payload_consistency_after_roundtrip(self, publish_event, get_events, make_event):
        topic = f"persist-payload-{uuid.uuid4().hex[:8]}"
        ev = make_event(topic=topic)
        ev["payload"] = {
            "key1": "value1",
            "key2": 123,
            "nested": {"a": True, "b": [1, 2, 3]}
        }

        publish_event(ev)
        time.sleep(2)

        result = get_events(topic=topic)
        assert result["count"] >= 1
        
        saved_ev = next(e for e in result["events"] if e["event_id"] == ev["event_id"])
        assert saved_ev["payload"] == ev["payload"]
        assert saved_ev["topic"] == ev["topic"]
        assert saved_ev["source"] == ev["source"]

    def test_topic_isolation(self, publish_event, get_events, make_event):
        topic1 = f"iso-1-{uuid.uuid4().hex[:8]}"
        topic2 = f"iso-2-{uuid.uuid4().hex[:8]}"
        
        ev1 = make_event(topic=topic1)
        ev2 = make_event(topic=topic2)
        
        publish_event(ev1)
        publish_event(ev2)
        time.sleep(2)
        
        r1 = get_events(topic=topic1)
        r2 = get_events(topic=topic2)
        
        assert all(e["topic"] == topic1 for e in r1["events"])
        assert all(e["topic"] == topic2 for e in r2["events"])