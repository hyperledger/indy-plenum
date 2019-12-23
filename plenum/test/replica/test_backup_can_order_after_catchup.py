import pytest

from plenum.server.replicas import MASTER_REPLICA_INDEX
from plenum.test.delayers import cDelay, pDelay, ppDelay, old_view_pp_request_delay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change


REQUEST_COUNT = 3
BACKUP_INST_ID = 1


@pytest.fixture(scope="module")
def tconf(tconf):
    old_b_size = tconf.Max3PCBatchSize

    tconf.Max3PCBatchSize = 1
    yield tconf

    tconf.Max3PCBatchSize = old_b_size


def test_backup_can_order_after_catchup(txnPoolNodeSet,
                                        looper,
                                        sdk_pool_handle,
                                        sdk_wallet_client):
    # We expect that after VC Gamma will be primary on backup
    delayed_node = txnPoolNodeSet[-2]
    with delay_rules_without_processing(delayed_node.nodeIbStasher,
                                        pDelay(instId=MASTER_REPLICA_INDEX),
                                        cDelay(instId=MASTER_REPLICA_INDEX),
                                        ppDelay(instId=MASTER_REPLICA_INDEX)):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQUEST_COUNT)
        with delay_rules_without_processing([n.nodeIbStasher for n in txnPoolNodeSet],
                                            old_view_pp_request_delay()):
            ensure_view_change(looper, txnPoolNodeSet)
            ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
            assert delayed_node.replicas._replicas[BACKUP_INST_ID].isPrimary
            # Check, that backup cannot order
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQUEST_COUNT)
            for n in txnPoolNodeSet:
                assert n.replicas._replicas[BACKUP_INST_ID].last_ordered_3pc[1] == 0
            # Forcing catchup
            delayed_node.start_catchup()
            ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

            # Check, that backup can order after catchup
            b_pp_seq_no_before = delayed_node.replicas._replicas[BACKUP_INST_ID].last_ordered_3pc[1]
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQUEST_COUNT)
            assert delayed_node.replicas._replicas[BACKUP_INST_ID].last_ordered_3pc[1] == \
                   b_pp_seq_no_before + REQUEST_COUNT
