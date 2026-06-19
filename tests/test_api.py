import pytest
import requests

class TestAPI:
    def test_publish_single_event_success(self, base_url, sample_event):
        resp = requests.post(f"{base_url}/publish", json=sample_event, timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["queued"] == 1

    def test_publish_batch_event_success(self, base_url, make_event):
        events = [make_event(topic="api-batch-test") for _ in range(3)]
        resp = requests.post(
            f"{base_url}/publish",
            json=events,
            timeout=10,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["queued"] == 3

    def test_publish_invalid_body_returns_422(self, base_url):
        invalid_body = {"invalid": "data"}
        resp = requests.post(f"{base_url}/publish", json=invalid_body, timeout=10)
        assert resp.status_code == 422

    def test_publish_invalid_uuid_returns_422(self, base_url, sample_event):
        sample_event["event_id"] = "not-a-uuid"
        resp = requests.post(f"{base_url}/publish", json=sample_event, timeout=10)
        assert resp.status_code == 422

    def test_publish_invalid_timestamp_returns_422(self, base_url, sample_event):
        sample_event["timestamp"] = "not-a-date"
        resp = requests.post(f"{base_url}/publish", json=sample_event, timeout=10)
        assert resp.status_code == 422

    def test_get_events_without_filter_success(self, base_url):
        resp = requests.get(f"{base_url}/events", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert "events" in data

    def test_get_events_with_filter_success(self, base_url, make_event, publish_event):
        topic = "api-filter-test"
        ev = make_event(topic=topic)
        publish_event(ev)
        
        # We might need to wait for consumer to process, but this is just API format test
        resp = requests.get(f"{base_url}/events?topic={topic}", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["events"], list)

    def test_health_check(self, base_url):
        resp = requests.get(f"{base_url}/health", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"