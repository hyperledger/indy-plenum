import pytest

from plenum.common.metrics_collector import KvStoreMetricsFormat, MetricsType
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from storage.helper import initKeyValueStorage


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=3) as tconf:
        old_type = tconf.METRICS_COLLECTOR_TYPE
        tconf.METRICS_COLLECTOR_TYPE = 'kv'
        yield tconf
        tconf.METRICS_COLLECTOR_TYPE = old_type


def check_metrics_data(storage):
    data = [(*KvStoreMetricsFormat.decode_key(k), KvStoreMetricsFormat.decode_value(v))
              for k, v in storage.iterator()]

    # Check that metrics are actually written
    assert len(data) > 0

    # Check that all events are stored in correct order
    assert sorted(data, key=lambda v: (v[0], v[1])) == data

    # Check that all event types happened during test
    metric_types = {v[0] for v in data}
    for t in MetricsType:
        assert t in metric_types


def test_kv_store_metrics_config(looper, txnPoolNodeSet, tdir, tconf, sdk_pool_handle, sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 15)

    for node in txnPoolNodeSet:
        storage = initKeyValueStorage(tconf.METRICS_KV_STORAGE,
                                      node.dataLocation,
                                      tconf.METRICS_KV_DB_NAME,
                                      read_only=True)

        check_metrics_data(storage)
