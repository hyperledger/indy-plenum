import asyncio
from random import gauss

import time

import pytest

from plenum.common.metrics_collector import MetricsName, KvStoreMetricsCollector, KvStoreMetricsFormat, MetricsEvent, \
    measure_time, async_measure_time
from plenum.common.value_accumulator import ValueAccumulator
from plenum.test.metrics.helper import gen_next_timestamp, gen_metrics_name, generate_events, MockTimestamp, \
    MockMetricsCollector
from storage.kv_store import KeyValueStorage


def test_metrics_collector_dont_add_events_when_accumulating():
    mc = MockMetricsCollector()

    mc.acc_event(gen_metrics_name(), 3.0)

    assert mc.events == []


def test_metrics_collector_dont_add_events_when_flushing_empty():
    mc = MockMetricsCollector()

    mc.flush_accumulated()

    assert mc.events == []


def test_metrics_collector_adds_events_when_flushing_accumulated():
    mc = MockMetricsCollector()
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.flush_accumulated()

    assert len(mc.events) == 1
    assert mc.events[0] == (MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)


def test_metrics_collector_accumulate_same_events_into_one():
    mc = MockMetricsCollector()
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 1
    assert mc.events[0] == (MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 5.0)


def test_metrics_collector_separates_different_events():
    mc = MockMetricsCollector()
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.acc_event(MetricsName.BACKUP_ORDERED_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 2
    assert (MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0) in mc.events
    assert (MetricsName.BACKUP_ORDERED_BATCH_SIZE, 2.0) in mc.events


def test_metrics_collector_resets_accumulated_after_flush():
    mc = MockMetricsCollector()
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.flush_accumulated()
    mc.acc_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 2
    assert mc.events[0] == (MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    assert mc.events[1] == (MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2.0)


def test_metrics_collector_measures_time():
    mc = MockMetricsCollector()
    with mc.measure_time(MetricsName.LOOPER_RUN_TIME_SPENT):
        time.sleep(0.1)
    assert len(mc.events) == 0

    mc.flush_accumulated()

    assert len(mc.events) == 1
    assert mc.events[0][0] == MetricsName.LOOPER_RUN_TIME_SPENT
    assert abs(mc.events[0][1] - 0.1) < 0.001  # we want at least 1 ms precision


def test_measure_time_decorator():
    class Example:
        def __init__(self, metrics):
            self.metrics = metrics
            self.data = 2

        @measure_time(MetricsName.LOOPER_RUN_TIME_SPENT)
        def slow_add(self, a, b):
            time.sleep(0.1)
            return self.data + a + b

    mc = MockMetricsCollector()
    e = Example(mc)
    r = e.slow_add(1, 3)
    assert len(mc.events) == 0
    assert r == 6

    mc.flush_accumulated()
    assert len(mc.events) == 1
    assert mc.events[0][0] == MetricsName.LOOPER_RUN_TIME_SPENT
    assert abs(mc.events[0][1] - 0.1) < 0.001  # we want at least 1 ms precision


def test_async_measure_time_decorator(looper):
    class Example:
        def __init__(self, metrics):
            self.metrics = metrics
            self.data = 2

        @async_measure_time(MetricsName.LOOPER_RUN_TIME_SPENT)
        async def slow_add(self, a, b):
            await asyncio.sleep(0.1)
            return self.data + a + b

    mc = MockMetricsCollector()
    e = Example(mc)
    r = looper.loop.run_until_complete(e.slow_add(1, 3))
    assert len(mc.events) == 0
    assert r == 6

    mc.flush_accumulated()
    assert len(mc.events) == 1
    assert mc.events[0][0] == MetricsName.LOOPER_RUN_TIME_SPENT
    assert abs(mc.events[0][1] - 0.1) < 0.005  # we want at least 5 ms precision


def test_kv_store_decode_restores_encoded_event():
    event = MetricsEvent(gen_next_timestamp(), gen_metrics_name(), 4.2)

    k, v = KvStoreMetricsFormat.encode(event)
    decoded_event = KvStoreMetricsFormat.decode(k, v)

    assert event == decoded_event


def test_kv_store_encode_generate_different_keys_for_different_seq_no():
    event = MetricsEvent(gen_next_timestamp(), gen_metrics_name(), 4.2)

    k1, v1 = KvStoreMetricsFormat.encode(event, 1)
    k2, v2 = KvStoreMetricsFormat.encode(event, 2)

    assert k1 != k2
    assert v1 == v2


@pytest.mark.parametrize("value", [4.2, ValueAccumulator([42, -7, 0])])
def test_kv_store_metrics_collector_stores_properly_encoded_data(storage: KeyValueStorage, value):
    ts = MockTimestamp(gen_next_timestamp())
    metrics = KvStoreMetricsCollector(storage, ts)
    assert len([(k, v) for k, v in storage.iterator()]) == 0

    id = gen_metrics_name()
    event = MetricsEvent(ts.value, id, value)
    encoded_key, encoded_value = KvStoreMetricsFormat.encode(event)

    metrics.add_event(id, value)
    stored_events = [(k, v) for k, v in storage.iterator()]

    assert len(stored_events) == 1
    assert stored_events[0][0] == encoded_key
    assert stored_events[0][1] == encoded_value


def test_kv_store_metrics_collector_store_all_data_in_order(storage: KeyValueStorage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    events = generate_events(10)

    for e in events:
        ts.value = e.timestamp
        metrics.add_event(e.name, e.value)
    stored_events = [KvStoreMetricsFormat.decode(k, v) for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(stored_events) == len(events)
    # Check that all events are stored in correct order
    assert sorted(stored_events, key=lambda v: v.timestamp) == stored_events
    # Check that all events stored were in source events
    for ev in stored_events:
        assert ev in events
    # Check that all source events are in stored events
    for ev in events:
        assert ev in stored_events


def test_kv_store_metrics_collector_store_all_events_with_same_timestamp(storage: KeyValueStorage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    values = [10, 2, 54, 2]

    for v in values:
        metrics.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, v)
    events = [KvStoreMetricsFormat.decode(k, v) for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(events) == len(values)
    # Check that all events are stored in correct order
    assert sorted(events, key=lambda ev: ev.timestamp) == events
    # Check that all events stored were in source events
    for ev in events:
        assert ev.value in values
