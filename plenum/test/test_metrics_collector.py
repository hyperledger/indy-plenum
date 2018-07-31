from random import uniform, gauss, choice
from typing import Tuple, List

import pytest
from datetime import datetime, timedelta

from plenum.common.metrics_collector import MetricsName, KvStoreMetricsCollector, KvStoreMetricsFormat, MetricsEvent
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


def gen_metrics_name() -> MetricsName:
    return choice(list(MetricsName))


def gen_next_timestamp(prev=None) -> datetime:
    def round_ts(ts: datetime) -> datetime:
        us = round(ts.microsecond - 500, -3)
        return ts.replace(microsecond=us)

    if prev is None:
        return round_ts(datetime.utcnow())

    return round_ts(prev + timedelta(seconds=uniform(0.001, 10.0)))


def generate_events(num: int, min_ts=None) -> List[MetricsEvent]:
    ts = gen_next_timestamp(min_ts)
    result = []
    for _ in range(num):
        ts = gen_next_timestamp(ts)
        name = gen_metrics_name()
        value = gauss(0.0, 100.0)
        result += [MetricsEvent(ts, name, value)]
    return result


class MockTimestamp:
    def __init__(self, value=datetime.utcnow()):
        self.value = value

    def __call__(self):
        return self.value


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


def test_kv_store_metrics_collector_stores_properly_encoded_data(storage: KeyValueStorage):
    ts = MockTimestamp(gen_next_timestamp())
    metrics = KvStoreMetricsCollector(storage, ts)
    assert len([(k, v) for k, v in storage.iterator()]) == 0

    id, value = gen_metrics_name(), gauss(0.0, 100.0)
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


def test_kv_store_metrics_collector_store_all_events_with_same_timestamp(storage: KeyValueStorage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    values = [10, 2, 54, 2]

    for v in values:
        metrics.add_event(MetricsName.THREE_PC_BATCH_SIZE, v)
    events = [KvStoreMetricsFormat.decode(k, v) for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(events) == len(values)
    # Check that all events are stored in correct order
    assert sorted(events, key=lambda ev: ev.timestamp) == events
    # Check that all events stored were in source events
    for ev in events:
        assert ev.value in values

