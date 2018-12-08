import pytest

from plenum.common import metrics_names
from plenum.common.metrics_collector import KvStoreMetricsFormat
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from plenum.test.metrics.helper import plenum_metrics
from storage.helper import initKeyValueStorage


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=3) as tconf:
        old_type = tconf.METRICS_COLLECTOR_TYPE
        tconf.METRICS_COLLECTOR_TYPE = 'kv'
        yield tconf
        tconf.METRICS_COLLECTOR_TYPE = old_type


def test_kv_store_metrics_config(looper, txnPoolNodeSet, tdir, tconf, sdk_pool_handle, sdk_wallet_client):
    total_time = 1.5 * tconf.PerfCheckFreq
    total_iters = 5
    iter_time = total_time / total_iters

    for _ in range(total_iters):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 15)
        looper.runFor(iter_time)

    for node in txnPoolNodeSet:
        storage = initKeyValueStorage(tconf.METRICS_KV_STORAGE,
                                      node.dataLocation,
                                      tconf.METRICS_KV_DB_NAME,
                                      read_only=True)

        events = [KvStoreMetricsFormat.decode(k, v) for k, v in storage.iterator()]

        # Check that metrics are actually written
        assert len(events) > 0

        # Check that all events are stored in correct order
        assert sorted(events, key=lambda v: v.timestamp) == events

        # Prepare list of all expected events
        expected_events = {name for name in plenum_metrics if not name.startswith(b'_')}

        # We don't expect some events in this test
        expected_events -= {
            metrics_names.CATCHUP_TXNS_SENT,
            metrics_names.CATCHUP_TXNS_RECEIVED,

            metrics_names.GC_UNCOLLECTABLE_OBJECTS,

            metrics_names.PROCESS_CHECKPOINT_TIME,
            metrics_names.SEND_CHECKPOINT_TIME,
            metrics_names.BACKUP_PROCESS_CHECKPOINT_TIME,
            metrics_names.BACKUP_SEND_CHECKPOINT_TIME,
            metrics_names.PROCESS_CONSISTENCY_PROOF_TIME,
            metrics_names.PROCESS_CATCHUP_REQ_TIME,
            metrics_names.PROCESS_CATCHUP_REP_TIME,
            metrics_names.NODE_CHECK_NODE_REQUEST_SPIKE,
            metrics_names.NODE_SEND_REJECT_TIME,

            # TODO: reduce monitor window so these events are also captured
            metrics_names.MONITOR_AVG_THROUGHPUT,
            metrics_names.BACKUP_MONITOR_AVG_THROUGHPUT,

            # Temporary metrics
            metrics_names.STORAGE_IDR_CACHE,
            metrics_names.STORAGE_ATTRIBUTE_STORE,
        }

        # Don't expect some metrics from master primary
        if node.master_replica.isPrimary:
            expected_events -= {metrics_names.PROCESS_PREPREPARE_TIME,
                                metrics_names.SEND_PREPARE_TIME}
        else:
            expected_events -= {metrics_names.SEND_PREPREPARE_TIME,
                                metrics_names.CREATE_3PC_BATCH_TIME,
                                metrics_names.BLS_UPDATE_PREPREPARE_TIME}

        # Don't expect some metrics from backup primary
        assert node.replicas.num_replicas == 2
        if node.replicas[1].isPrimary:
            expected_events -= {metrics_names.BACKUP_PROCESS_PREPREPARE_TIME,
                                metrics_names.BACKUP_SEND_PREPARE_TIME}
        else:
            expected_events -= {metrics_names.BACKUP_SEND_PREPREPARE_TIME,
                                metrics_names.BACKUP_CREATE_3PC_BATCH_TIME,
                                metrics_names.BLS_UPDATE_PREPREPARE_TIME}

        # Check that all expected event types happened during test
        metric_names = {ev.name for ev in events}
        for expected in expected_events:
            assert any(name.startswith(expected) for name in metric_names), \
                "{} not found in stored metrics".format(expected)
