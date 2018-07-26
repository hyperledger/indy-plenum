import struct
import pytest
from datetime import datetime, timedelta

from plenum.common.metrics_collector import MetricsType, KvStoreMetricsCollector, KvStoreMetricsFormat
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


def test_kv_store_codec_for_key_is_correct():
    id = MetricsType.LOOPER_RUN_TIME_SPENT
    ts = datetime(2018, month=7, day=24, hour=18, minute=12, second=35, microsecond=456000)

    data = KvStoreMetricsFormat.encode_key(id, ts)
    decoded_id, decoded_ts = KvStoreMetricsFormat.decode_key(data)

    assert id == decoded_id
    assert ts == decoded_ts


def test_kv_store_codec_for_value_is_correct():
    value = 4.2

    data = KvStoreMetricsFormat.encode_value(value)
    decoded_value = KvStoreMetricsFormat.decode_value(data)

    assert value == decoded_value


def test_kv_store_metrics_collector_stores_properly_encoded_data(storage: KeyValueStorage):
    ts = MockTimestamp(datetime(2018, month=7, day=24, hour=18, minute=12, second=35, microsecond=456000))
    metrics = KvStoreMetricsCollector(storage, ts)
    assert len([(k, v) for k, v in storage.iterator()]) == 0

    id, value = MetricsType.LOOPER_RUN_TIME_SPENT, 6.2
    encoded_key = KvStoreMetricsFormat.encode_key(id, ts.value)
    encoded_value = KvStoreMetricsFormat.encode_value(value)
    metrics.add_event(id, value)

    result = [(k, v) for k, v in storage.iterator()]
    assert len(result) == 1
    assert result[0][0] == encoded_key
    assert result[0][1] == encoded_value


def test_kv_store_metrics_collector_store_all_data_in_order(storage: KeyValueStorage):
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)

    events = [
        (MetricsType.CLIENT_STACK_MESSAGES_PROCESSED, 30),
        (MetricsType.LOOPER_RUN_TIME_SPENT, 1.2),
        (MetricsType.CLIENT_STACK_MESSAGES_PROCESSED, 163),
        (MetricsType.LOOPER_RUN_TIME_SPENT, 5.2),
        (MetricsType.CLIENT_STACK_MESSAGES_PROCESSED, 6),
        (MetricsType.LOOPER_RUN_TIME_SPENT, 0.3)
    ]
    for id, value in events:
        metrics.add_event(id, value)
        ts.value += timedelta(seconds=0.1)
    result = [(*KvStoreMetricsFormat.decode_key(k), KvStoreMetricsFormat.decode_value(v))
              for k, v in storage.iterator()]

    # Check that all events are stored
    assert len(result) == 6
    # Check that all events are stored in correct order
    assert sorted(result, key=lambda v: (v[0], v[1])) == result
    # Check that all events stored were in source events
    for v in result:
        assert (v[0], v[2]) in events
