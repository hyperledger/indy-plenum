from datetime import datetime, timedelta
from random import shuffle

import pytest
import statistics

from plenum.common.metrics_collector import KvStoreMetricsCollector, MetricsType
from plenum.common.metrics_stats import trunc_ts, ValueAccumulator, MetricsStatsFrame, \
    MetricsStats, load_metrics_from_kv_store
from plenum.test.test_metrics_collector import generate_events, MockTimestamp
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_rocksdb import KeyValueStorageRocksdb

db_no = 0


@pytest.yield_fixture(params=['rocksdb', 'leveldb'])
def storage(request, tdir) -> KeyValueStorage:
    global db_no
    if request.param == 'leveldb':
        db = KeyValueStorageLeveldb(tdir, 'metrics_ldb_{}'.format(db_no))
    else:
        db = KeyValueStorageRocksdb(tdir, 'metrics_rdb_{}'.format(db_no))
    db_no += 1
    yield db
    db.close()


def _value_accumulator(values):
    acc = ValueAccumulator()
    for v in values:
        acc.add(v)
    return acc


def _metrics_stats_frame(events):
    frame = MetricsStatsFrame()
    for id, _, value in events:
        frame.add(id, value)
    return frame


def test_trunc_ts():
    base = datetime(year=2018, month=7, day=30, hour=13)

    assert trunc_ts(base.replace(minute=36, second=13, microsecond=300000), timedelta(seconds=1)) == \
           base.replace(minute=36, second=13)
    assert trunc_ts(base.replace(minute=36, second=13, microsecond=800000), timedelta(seconds=1)) == \
           base.replace(minute=36, second=13)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(seconds=10)) == base.replace(minute=36, second=10)
    assert trunc_ts(base.replace(minute=36, second=17), timedelta(seconds=10)) == base.replace(minute=36, second=10)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(seconds=20)) == base.replace(minute=36, second=0)
    assert trunc_ts(base.replace(minute=36, second=23), timedelta(seconds=20)) == base.replace(minute=36, second=20)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(minutes=5)) == base.replace(minute=35)
    assert trunc_ts(base.replace(minute=39, second=59), timedelta(minutes=5)) == base.replace(minute=35)
    assert trunc_ts(base.replace(minute=40, second=0), timedelta(minutes=5)) == base.replace(minute=40)


def test_value_accumulator_dont_return_anything_when_created():
    acc = ValueAccumulator()
    assert acc.count == 0
    assert acc.sum == 0
    assert acc.avg is None
    assert acc.stddev is None
    assert acc.min is None
    assert acc.max is None


def test_value_accumulator_can_add_value():
    value = 4.2
    acc = ValueAccumulator()
    acc.add(value)
    assert acc.count == 1
    assert acc.sum == value
    assert acc.avg == value
    assert acc.stddev is None
    assert acc.min == value
    assert acc.max == value


def test_value_accumulator_handles_same_values():
    value = 4.2
    count = 5
    acc = ValueAccumulator()
    for _ in range(count):
        acc.add(value)

    assert acc.count == count
    assert acc.sum == value * count
    assert acc.avg == value
    assert acc.stddev == 0
    assert acc.min == value
    assert acc.max == value


def test_value_accumulator_can_add_several_values():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator()
    for value in values:
        acc.add(value)

    assert acc.count == len(values)
    assert acc.sum == sum(values)
    assert acc.avg == statistics.mean(values)
    assert acc.stddev == statistics.stdev(values)
    assert acc.min == min(values)
    assert acc.max == max(values)


def test_value_accumulator_eq_has_value_semantics():
    a = ValueAccumulator()
    b = ValueAccumulator()
    assert a == b

    a.add(1.0)
    assert a != b

    b.add(1.0)
    assert a == b

    a.add(2.0)
    b.add(3.0)
    assert a != b


def test_value_accumulator_can_merge():
    values = [4.2, -1.3, 10.8]
    acc = _value_accumulator(values)

    other_values = [3.7, 7.6, -8.5]
    other_acc = _value_accumulator(other_values)

    all_values = values + other_values
    all_acc = _value_accumulator(all_values)

    acc.merge(other_acc)
    assert acc == all_acc


def test_metrics_stats_frame_can_add_values():
    events_transport_batch_size = [10, 2, 54]
    events_looper_run_time_spent = [0.1, 3.4, 0.01, 0.5]

    events = []
    for v in events_transport_batch_size:
        events.append((MetricsType.TRANSPORT_BATCH_SIZE, v))
    for v in events_looper_run_time_spent:
        events.append((MetricsType.LOOPER_RUN_TIME_SPENT, v))
    shuffle(events)

    frame = MetricsStatsFrame()
    for id, value in events:
        frame.add(id, value)

    assert frame.get(MetricsType.TRANSPORT_BATCH_SIZE) == _value_accumulator(events_transport_batch_size)
    assert frame.get(MetricsType.LOOPER_RUN_TIME_SPENT) == _value_accumulator(events_looper_run_time_spent)
    assert frame.get(MetricsType.THREE_PC_BATCH_SIZE) == ValueAccumulator()


def test_metrics_stats_frame_eq_has_value_semantics():
    a = MetricsStatsFrame()
    b = MetricsStatsFrame()
    assert a == b

    a.add(MetricsType.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a != b

    b.add(MetricsType.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a == b

    a.add(MetricsType.THREE_PC_BATCH_SIZE, 1)
    b.add(MetricsType.TRANSPORT_BATCH_SIZE, 2)
    assert a != b


# TODO: This test should be split into several ones
def test_metrics_stats_can_add_values():
    stats = MetricsStats()

    min_ts = trunc_ts(datetime.utcnow(), stats.timestep)
    next_ts = min_ts + stats.timestep
    gap_ts = next_ts + 3 * stats.timestep
    max_ts = gap_ts + stats.timestep

    first_events = [v for v in generate_events(10, min_ts) if v[1] < min_ts + stats.timestep]
    next_events = [v for v in generate_events(10, next_ts) if v[1] < next_ts + stats.timestep]
    after_gap_events = [v for v in generate_events(10, gap_ts) if v[1] < gap_ts + stats.timestep]

    all_events = first_events + next_events + after_gap_events
    for id, ts, value in all_events:
        stats.add(id, ts, value)

    assert stats.min_ts == min_ts
    assert stats.max_ts == max_ts
    assert sorted(ts for ts, _ in stats.frames()) == [min_ts, next_ts, gap_ts]

    assert stats.frame(min_ts) == _metrics_stats_frame(first_events)
    assert stats.frame(next_ts) == _metrics_stats_frame(next_events)
    assert stats.frame(gap_ts) == _metrics_stats_frame(after_gap_events)
    assert stats.frame(max_ts) == MetricsStatsFrame()

    assert stats.frame(min_ts - 0.2 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(min_ts + 0.8 * stats.timestep) == _metrics_stats_frame(first_events)
    assert stats.frame(next_ts + 1.1 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(gap_ts - 0.1 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(max_ts + 4.2 * stats.timestep) == MetricsStatsFrame()


def test_metrics_stats_total_is_merge_of_all_frames():
    events = generate_events(50)

    stats = MetricsStats()
    for id, ts, value in events:
        stats.add(id, ts, value)

    expected_total = MetricsStatsFrame()
    for _, frame in stats.frames():
        expected_total.merge(frame)

    assert stats.total == expected_total


def test_metrics_stats_eq_has_value_semantics():
    ts = datetime.utcnow()

    a = MetricsStats()
    b = MetricsStats()
    assert a == b

    a.add(MetricsType.LOOPER_RUN_TIME_SPENT, ts, 2.0)
    assert a != b

    b.add(MetricsType.LOOPER_RUN_TIME_SPENT, ts, 2.0)
    assert a == b

    a.add(MetricsType.THREE_PC_BATCH_SIZE, datetime.utcnow(), 1)
    b.add(MetricsType.TRANSPORT_BATCH_SIZE, datetime.utcnow(), 2)
    assert a != b


def test_load_metrics_from_kv_store_can_load_all_values(storage):
    events = generate_events(10)
    step = timedelta(seconds=5)
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    expected_stats = MetricsStats(step)

    for id, time, value in events:
        ts.value = time
        metrics.add_event(id, value)
        expected_stats.add(id, time, value)

    stats = load_metrics_from_kv_store(storage, step=step)
    assert stats == expected_stats


def test_load_metrics_from_kv_store_can_filter_values(storage):
    events = generate_events(10)
    step = timedelta(seconds=3)
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    expected_stats = MetricsStats(step)

    timestamps = sorted(v[1] for v in events)
    min_ts = timestamps[len(events) // 3]
    max_ts = timestamps[2 * len(events) // 3]

    for id, time, value in events:
        ts.value = time
        metrics.add_event(id, value)
        if min_ts <= time <= max_ts:
            expected_stats.add(id, time, value)

    stats = load_metrics_from_kv_store(storage, min_ts, max_ts, step)
    assert stats == expected_stats
