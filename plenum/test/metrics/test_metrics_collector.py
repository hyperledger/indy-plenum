import asyncio

import time
from typing import Callable

import pytest

from plenum.common.metrics_collector import MetricsName, KvStoreMetricsCollector, KvStoreMetricsFormat, MetricsEvent, \
    measure_time, async_measure_time
from plenum.common.value_accumulator import ValueAccumulator
from plenum.test.metrics.helper import gen_next_timestamp, gen_metrics_name, generate_events, MockTimestamp, \
    MockMetricsCollector, MockEvent
from storage.kv_store import KeyValueStorage


def test_metrics_collector_dont_add_events_when_accumulating():
    mc = MockMetricsCollector()

    mc.add_event(gen_metrics_name(), 3.0)

    assert mc.events == []


def test_metrics_collector_dont_add_events_when_flushing_empty():
    mc = MockMetricsCollector()

    mc.flush_accumulated()

    assert mc.events == []


def test_metrics_collector_adds_events_when_flushing_accumulated():
    mc = MockMetricsCollector()
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.flush_accumulated()

    assert len(mc.events) == 1
    assert mc.events[0] == MockEvent(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1, 3.0)


def test_metrics_collector_accumulate_same_events_into_one():
    mc = MockMetricsCollector()
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 1
    assert mc.events[0] == MockEvent(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2, 5.0)


def test_metrics_collector_separates_different_events():
    mc = MockMetricsCollector()
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.add_event(MetricsName.BACKUP_ORDERED_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 2
    assert MockEvent(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1, 3.0) in mc.events
    assert MockEvent(MetricsName.BACKUP_ORDERED_BATCH_SIZE, 1, 2.0) in mc.events


def test_metrics_collector_resets_accumulated_after_flush():
    mc = MockMetricsCollector()
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 3.0)
    mc.flush_accumulated()
    mc.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 2.0)
    mc.flush_accumulated()

    assert len(mc.events) == 2
    assert mc.events[0] == MockEvent(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1, 3.0) in mc.events
    assert mc.events[1] == MockEvent(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1, 2.0) in mc.events


TIMING_ITER_COUNT = 30
TIMING_METRIC_NAME = MetricsName.LOOPER_RUN_TIME_SPENT
TIMING_FUNC_DURATION = 0.1


def check_precision(mc: MockMetricsCollector,
                    func: Callable,
                    minimum_precision: float,
                    maximum_overhead: float,
                    looper = None):
    if asyncio.iscoroutinefunction(func):
        async def bench():
            for _ in range(TIMING_ITER_COUNT):
                await func()
        start = time.perf_counter()
        looper.loop.run_until_complete(bench())
    else:
        start = time.perf_counter()
        for _ in range(TIMING_ITER_COUNT):
            func()
    overhead = (time.perf_counter() - start) / TIMING_ITER_COUNT - TIMING_FUNC_DURATION
    mc.flush_accumulated()
    precision = abs(mc.events[0].avg - TIMING_FUNC_DURATION)

    assert len(mc.events) == 1
    assert mc.events[0].name == TIMING_METRIC_NAME
    assert mc.events[0].count == TIMING_ITER_COUNT
    assert precision < minimum_precision, \
        "Expected precision {}, actual {} ms".format(1000 * minimum_precision, 1000 * precision)
    assert 0 < overhead < maximum_overhead, \
        "Expected overhead {}, actual {} ms".format(1000 * maximum_overhead, 1000 * overhead)


def test_metrics_collector_measures_time():
    mc = MockMetricsCollector()
    def f():
        with mc.measure_time(TIMING_METRIC_NAME):
            time.sleep(TIMING_FUNC_DURATION)

    # We want at least 0.5 ms precision and no more than 1 ms overhead
    check_precision(mc, f, minimum_precision=0.0005, maximum_overhead=0.001)


def test_measure_time_decorator():
    mc = MockMetricsCollector()

    class Example:
        def __init__(self, metrics):
            self.metrics = metrics
            self.data = 2

        @measure_time(TIMING_METRIC_NAME)
        def slow_add(self, a, b):
            time.sleep(TIMING_FUNC_DURATION)
            return self.data + a + b

    # We want at least 0.5 ms precision and no more than 1 ms overhead
    e = Example(mc)
    check_precision(mc, lambda: e.slow_add(1, 3),
                    minimum_precision=0.0005, maximum_overhead=0.001)

    # Check that decorated function works correctly
    e = Example(mc)
    r = e.slow_add(1, 3)
    assert r == 6


def test_async_measure_time_decorator(looper):
    mc = MockMetricsCollector()

    class Example:
        def __init__(self, metrics):
            self.metrics = metrics
            self.data = 2

        @async_measure_time(TIMING_METRIC_NAME)
        async def slow_add(self, a, b):
            await asyncio.sleep(TIMING_FUNC_DURATION)
            return self.data + a + b

    e = Example(mc)
    async def f():
        await e.slow_add(1, 3)

    # We want at least 5 ms precision and no more than 5 ms overhead
    check_precision(mc, f,
                    minimum_precision=0.005, maximum_overhead=0.005,
                    looper=looper)

    # Check that decorated async function works correctly
    e = Example(mc)
    r = looper.loop.run_until_complete(e.slow_add(1, 3))
    assert r == 6


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

    metrics.store_event(id, value)
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
        metrics.store_event(e.name, e.value)
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
        metrics.store_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, v)
    events = [KvStoreMetricsFormat.decode(k, v) for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(events) == len(values)
    # Check that all events are stored in correct order
    assert sorted(events, key=lambda ev: ev.timestamp) == events
    # Check that all events stored were in source events
    for ev in events:
        assert ev.value in values
