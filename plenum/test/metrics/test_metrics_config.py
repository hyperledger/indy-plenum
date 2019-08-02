import pytest

from plenum.common.metrics_collector import KvStoreMetricsFormat, MetricsName, TMP_METRIC
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
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

        # We don't expect some events in this test
        unexpected_events = {
            MetricsName.CATCHUP_TXNS_SENT,
            MetricsName.CATCHUP_TXNS_RECEIVED,

            MetricsName.GC_GEN2_TIME,
            MetricsName.GC_UNCOLLECTABLE_OBJECTS,
            MetricsName.GC_GEN2_COLLECTED_OBJECTS,

            MetricsName.PROCESS_CHECKPOINT_TIME,
            MetricsName.SEND_CHECKPOINT_TIME,
            MetricsName.BACKUP_PROCESS_CHECKPOINT_TIME,
            MetricsName.BACKUP_SEND_CHECKPOINT_TIME,
            MetricsName.PROCESS_CONSISTENCY_PROOF_TIME,
            MetricsName.PROCESS_CATCHUP_REQ_TIME,
            MetricsName.PROCESS_CATCHUP_REP_TIME,
            MetricsName.NODE_CHECK_NODE_REQUEST_SPIKE,
            MetricsName.NODE_SEND_REJECT_TIME,
            MetricsName.AUTH_RULES_FROM_STATE_COUNT,

            # Obsolete metrics
            MetricsName.DESERIALIZE_DURING_UNPACK_TIME,

            # TODO: reduce monitor window so these events are also captured
            MetricsName.MONITOR_AVG_THROUGHPUT,
            MetricsName.BACKUP_MONITOR_AVG_THROUGHPUT,
            MetricsName.MONITOR_AVG_LATENCY,
            MetricsName.BACKUP_MONITOR_AVG_LATENCY,

            # Temporary metrics
            MetricsName.STORAGE_IDR_CACHE_READERS,
            MetricsName.STORAGE_IDR_CACHE_TABLES_NUM,
            MetricsName.STORAGE_IDR_CACHE_TABLES_SIZE,
            MetricsName.STORAGE_ATTRIBUTE_STORE_READERS,
            MetricsName.STORAGE_ATTRIBUTE_STORE_TABLES_NUM,
            MetricsName.STORAGE_ATTRIBUTE_STORE_TABLES_SIZE
        }

        # Don't expect some metrics from master primary
        if node.master_replica.isPrimary:
            unexpected_events.add(MetricsName.PROCESS_PREPREPARE_TIME)
            unexpected_events.add(MetricsName.SEND_PREPARE_TIME)
        else:
            unexpected_events.add(MetricsName.SEND_PREPREPARE_TIME)
            unexpected_events.add(MetricsName.CREATE_3PC_BATCH_TIME)
            unexpected_events.add(MetricsName.BLS_UPDATE_PREPREPARE_TIME)

        # Don't expect some metrics from backup primary
        assert node.replicas.num_replicas == 2
        if node.replicas[1].isPrimary:
            unexpected_events.add(MetricsName.BACKUP_PROCESS_PREPREPARE_TIME)
            unexpected_events.add(MetricsName.BACKUP_SEND_PREPARE_TIME)
        else:
            unexpected_events.add(MetricsName.BACKUP_SEND_PREPREPARE_TIME)
            unexpected_events.add(MetricsName.BACKUP_CREATE_3PC_BATCH_TIME)
            unexpected_events.add(MetricsName.BLS_UPDATE_PREPREPARE_TIME)

        if not node.primaryDecider:
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_ACTION_QUEUE)
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_AQ_STASH)
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_REPEATING_ACTIONS)
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_SCHEDULED)
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_INBOX)
            unexpected_events.add(MetricsName.PRIMARY_DECIDER_OUTBOX)

        # Check that all event types happened during test
        metric_names = {ev.name for ev in events}
        for t in MetricsName:
            if t in unexpected_events or t > TMP_METRIC:
                continue
            assert t in metric_names
