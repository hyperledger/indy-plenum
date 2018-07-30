import pytest

from plenum.common.metrics_collector import KvStoreMetricsCollector, MetricType
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from plenum.test.test_metrics_collector import decode_key, decode_value


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=3) as tconf:
        old_type = tconf.METRICS_COLLECTOR_TYPE
        tconf.METRICS_COLLECTOR_TYPE = 'kv'
        yield tconf
        tconf.METRICS_COLLECTOR_TYPE = old_type


def test_kv_store_metrics_config(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    node = txnPoolNodeSet[0]
    metrics = node.metrics
    assert isinstance(metrics, KvStoreMetricsCollector)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 15)

    storage = metrics._storage
    result = [(*decode_key(k), decode_value(v)) for k, v in storage.iterator()]

    # Check that metrics are actually written
    assert len(result) > 0

    # Check that all events are stored in correct order
    assert sorted(result, key=lambda v: (v[0], v[1])) == result

    # Check that all event types happened during test
    metric_types = {v[0] for v in result}
    for t in MetricType:
        assert t in metric_types
