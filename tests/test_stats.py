import pytest
import time
import uuid

class TestStats:
    def test_stats_response_format(self, get_stats_api):
        stats = get_stats_api()
        
        assert "received" in stats
        assert "unique_processed" in stats
        assert "duplicate_dropped" in stats
        assert "topics" in stats
        assert "uptime_seconds" in stats

        assert isinstance(stats["received"], int)
        assert isinstance(stats["unique_processed"], int)
        assert isinstance(stats["duplicate_dropped"], int)
        assert isinstance(stats["topics"], list)
        assert isinstance(stats["uptime_seconds"], (int, float))

    def test_stats_counters_non_negative(self, get_stats_api):
        stats = get_stats_api()
        
        assert stats["received"] >= 0
        assert stats["unique_processed"] >= 0
        assert stats["duplicate_dropped"] >= 0
        assert stats["uptime_seconds"] >= 0

    def test_unique_increases_after_new_event(self, make_event, publish_event, get_stats_api):
        stats_before = get_stats_api()
        
        topic = f"stats-unique-{uuid.uuid4().hex[:8]}"
        ev = make_event(topic=topic)
        publish_event(ev)
        time.sleep(2)
        
        stats_after = get_stats_api()
        assert stats_after["unique_processed"] > stats_before["unique_processed"]
        assert stats_after["received"] > stats_before["received"]

    def test_duplicate_counter_increases(self, make_event, publish_event, get_stats_api):
        topic = f"stats-dup-{uuid.uuid4().hex[:8]}"
        ev = make_event(topic=topic)
        
        # Kirim 1x
        publish_event(ev)
        time.sleep(2)
        stats_before = get_stats_api()
        
        # Kirim duplikat
        publish_event(ev)
        time.sleep(2)
        stats_after = get_stats_api()
        
        assert stats_after["duplicate_dropped"] > stats_before["duplicate_dropped"]
        assert stats_after["received"] > stats_before["received"]
        # unique_processed tetap sama
        assert stats_after["unique_processed"] == stats_before["unique_processed"]

    def test_new_topic_appears(self, make_event, publish_event, get_stats_api):
        topic = f"stats-new-topic-{uuid.uuid4().hex[:8]}"
        ev = make_event(topic=topic)
        
        publish_event(ev)
        time.sleep(2)
        
        stats = get_stats_api()
        assert topic in stats["topics"]