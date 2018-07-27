from datetime import datetime

import pytest
import statistics

from plenum.common.metrics_collector import KvStoreMetricsCollector, MetricsType
from plenum.common.metrics_stats import ValueAccumulator, load_metrics_from_kv_store, MetricsStats
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


def test_value_accumulator_can_merge():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator()
    for value in values:
        acc.add(value)

    other_values = [3.7, 7.6, -8.5]
    other_acc = ValueAccumulator()
    for value in other_values:
        other_acc.add(value)

    acc.merge(other_acc)
    all_values = values + other_values
    assert acc.count == len(all_values)
    assert acc.sum == sum(all_values)
    assert acc.avg == statistics.mean(all_values)
    assert acc.stddev == statistics.stdev(all_values)
    assert acc.min == min(all_values)
    assert acc.max == max(all_values)


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


def test_metrics_stats_remember_min_max_timestamps():
    stats = MetricsStats()

    events = generate_events(10)
    for id, time, value in events:
        stats.add(id, time, value)

    assert stats.min_ts == min(v[1] for v in events)
    assert stats.max_ts == max(v[1] for v in events)


def test_load_metrics_from_kv_store_can_load_all_values(storage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    events = generate_events(10)
    expected_stats = MetricsStats()

    for id, time, value in events:
        ts.value = time
        metrics.add_event(id, value)
        expected_stats.add(id, time, value)

    stats = load_metrics_from_kv_store(storage)
    assert stats == expected_stats


def test_load_metrics_from_kv_store_can_filter_values(storage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    events = generate_events(10)
    expected_stats = MetricsStats()

    timestamps = sorted(v[1] for v in events)
    min_ts = timestamps[len(events) // 3]
    max_ts = timestamps[2 * len(events) // 3]

    for id, time, value in events:
        ts.value = time
        metrics.add_event(id, value)
        if min_ts <= time <= max_ts:
            expected_stats.add(id, time, value)

    stats = load_metrics_from_kv_store(storage, min_ts, max_ts)
    assert stats == expected_stats
