import struct
import pytest
from datetime import datetime, timedelta

from plenum.common.metrics_collector import MetricType, KvStoreMetricsCollector
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


class MockTimestamp:
    def __init__(self, value=datetime.utcnow()):
        self.value = value

    def __call__(self):
        return self.value


def decode_key(key: bytes) -> (MetricType, datetime):
    key = int.from_bytes(key, byteorder='big', signed=False)
    id = key >> 48
    ts = key & ((1 << 48) - 1)
    return MetricType(id), datetime.utcfromtimestamp(ts / 1000)


def decode_value(value: bytes) -> float:
    return struct.unpack('d', value)[0]


def test_kv_store_metrics_collector_can_store_data(storage: KeyValueStorage):
    ts = MockTimestamp(datetime(2018, month=7, day=24, hour=18, minute=12, second=35, microsecond=456000))
    metrics = KvStoreMetricsCollector(storage, ts)
    assert len([(k, v) for k, v in storage.iterator()]) == 0

    id = MetricType.LOOPER_RUN_TIME_SPENT
    value = 6.2
    metrics.add_event(id, value)
    result = [(k, v) for k, v in storage.iterator()]
    assert len(result) == 1

    k, v = result[0]
    decoded_id, decoded_ts = decode_key(k)
    assert decoded_id == id
    assert decoded_ts == ts.value
    assert decode_value(v) == value


def test_kv_store_metrics_collector_store_all_data_in_order(storage: KeyValueStorage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)

    events = [
        (MetricType.CLIENT_STACK_MESSAGES_PROCESSED, 30),
        (MetricType.LOOPER_RUN_TIME_SPENT, 1.2),
        (MetricType.CLIENT_STACK_MESSAGES_PROCESSED, 163),
        (MetricType.LOOPER_RUN_TIME_SPENT, 5.2),
        (MetricType.CLIENT_STACK_MESSAGES_PROCESSED, 6),
        (MetricType.LOOPER_RUN_TIME_SPENT, 0.3)
    ]
    for id, value in events:
        metrics.add_event(id, value)
        ts.value += timedelta(seconds=0.1)
    result = [(*decode_key(k), decode_value(v)) for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(result) == 6
    # Check that all events are stored in correct order
    assert sorted(result, key=lambda v: (v[0], v[1])) == result
    # Check that all events stored were in source events
    for v in result:
        assert (v[0], v[2]) in events
