import asyncio
import uuid
import pytest
import random
from datetime import datetime, timezone
from database import insert_event, get_stats

@pytest.mark.asyncio
async def test_concurrent_insert_same_event(mock_pool):
    event_id = str(uuid.uuid4())
    event = {
        "topic": "concurrency.test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "parallel",
        "payload": {}
    }
    
    tasks = [insert_event(mock_pool, event) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    
    assert sum(results) == 1
    
    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 9

@pytest.mark.asyncio
async def test_concurrent_insert_different_events(mock_pool):
    events = []
    for i in range(10):
        events.append({
            "topic": "concurrency.diff",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": f"worker-{i}",
            "payload": {}
        })
        
    tasks = [insert_event(mock_pool, ev) for ev in events]
    results = await asyncio.gather(*tasks)
    
    assert sum(results) == 10
    
    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 10
    assert stats["duplicate_dropped"] == 0

@pytest.mark.asyncio
async def test_stats_consistency_under_load(mock_pool):
    unique_events = []
    for i in range(70):
        unique_events.append({
            "topic": "concurrency.mixed",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": f"worker-{i}",
            "payload": {}
        })
    duplicates = random.sample(unique_events, 30)
    all_events = unique_events + duplicates
    random.shuffle(all_events)
    
    tasks = [insert_event(mock_pool, ev) for ev in all_events]
    results = await asyncio.gather(*tasks)
    
    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 70
    assert stats["duplicate_dropped"] == 30
    assert stats["received"] == 100