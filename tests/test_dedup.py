import pytest
import uuid
from database import insert_event, get_stats

@pytest.mark.asyncio
async def test_duplicate_not_reprocessed(mock_pool, sample_event):
    result1 = await insert_event(mock_pool, sample_event)
    assert result1 is True

    result2 = await insert_event(mock_pool, sample_event)
    assert result2 is False

    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 1
    assert stats["received"] == 2

@pytest.mark.asyncio
async def test_different_event_ids_both_processed(mock_pool, make_event):
    ev1 = make_event(topic="dedup-same-topic")
    ev2 = make_event(topic="dedup-same-topic")
    
    r1 = await insert_event(mock_pool, ev1)
    r2 = await insert_event(mock_pool, ev2)

    assert r1 is True
    assert r2 is True

    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 2
    assert stats["duplicate_dropped"] == 0

@pytest.mark.asyncio
async def test_same_event_id_different_topics_both_processed(mock_pool, make_event):
    shared_id = str(uuid.uuid4())
    ev1 = make_event(topic="topic-alpha")
    ev1["event_id"] = shared_id
    ev2 = make_event(topic="topic-beta")
    ev2["event_id"] = shared_id

    r1 = await insert_event(mock_pool, ev1)
    r2 = await insert_event(mock_pool, ev2)

    assert r1 is True
    assert r2 is True

    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 2

@pytest.mark.asyncio
async def test_batch_with_duplicates(mock_pool, make_event):
    unique_events = [make_event(topic="batch-dedup") for _ in range(5)]
    all_events = unique_events + unique_events

    results = []
    for ev in all_events:
        r = await insert_event(mock_pool, ev)
        results.append(r)

    assert results[:5] == [True] * 5
    assert results[5:] == [False] * 5

    stats = await get_stats(mock_pool)
    assert stats["unique_processed"] == 5
    assert stats["duplicate_dropped"] == 5
    assert stats["received"] == 10