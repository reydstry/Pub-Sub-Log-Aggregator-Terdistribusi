import asyncio
import pytest
import os
import uuid
import datetime
import requests

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

BASE_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")

@pytest.fixture(scope="session")
def base_url():
    return BASE_URL

@pytest.fixture
def publish_event(base_url):
    def _publish(event_data):
        resp = requests.post(f"{base_url}/publish", json=event_data, timeout=10)
        return resp.json()
    return _publish

@pytest.fixture
def get_events(base_url):
    def _get(topic=None):
        url = f"{base_url}/events"
        if topic: url += f"?topic={topic}"
        resp = requests.get(url, timeout=10)
        return resp.json()
    return _get

@pytest.fixture
def get_stats_api(base_url):
    def _get():
        resp = requests.get(f"{base_url}/stats", timeout=10)
        return resp.json()
    return _get

class MockConnCtx:
    def __init__(self, pool):
        self.pool = pool
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass
    async def execute(self, query, *args):
        if "INSERT INTO events" in query:
            topic, event_id = args[0], args[1]
            key = (topic, event_id)
            if key in self.pool._events: return "INSERT 0 0"
            else:
                self.pool._events.add(key)
                return "INSERT 0 1"
        elif "UPDATE stats" in query:
            if "unique_processed + 1" in query:
                self.pool._stats["unique_processed"] += 1
                self.pool._stats["received"] += 1
            elif "duplicate_dropped + 1" in query:
                self.pool._stats["duplicate_dropped"] += 1
                self.pool._stats["received"] += 1
        return "UPDATE 1"
    async def fetch(self, query, *args):
        if "SELECT" in query and "events" in query:
            return [{"topic": topic, "event_id": eid} for (topic, eid) in self.pool._events]
        return []
    async def fetchrow(self, query, *args):
        if "SELECT" in query and "stats" in query: return self.pool._stats
        return None

class MockPool:
    def __init__(self):
        self._events = set()
        self._stats = {"received": 0, "unique_processed": 0, "duplicate_dropped": 0, "topics": [], "started_at": datetime.datetime.now(datetime.timezone.utc)}
    def acquire(self): return MockConnCtx(self)
    async def close(self): pass

@pytest.fixture
def mock_pool():
    return MockPool()

@pytest.fixture
def make_event():
    def _make(topic="test-topic"):
        return {
            "topic": topic,
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {"test": True}
        }
    return _make

@pytest.fixture
def sample_event(make_event):
    return make_event()
