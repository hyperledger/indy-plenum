import pytest

from plenum.common.messages.node_messages import CatchupReq
from plenum.test.delayers import cDelay, pDelay, cqDelay, cr_delay, msg_req_delay, msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules_without_processing
from stp_core.loop.eventually import eventually

nodeCount = 7

from stp_core.common.log import Logger
Logger().enableStdLogging()

@pytest.fixture(scope="module")
def tconf(tconf):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1

    yield tconf
    tconf.Max3PCBatchSize = oldSize


def test_no_catchup_req_with_absent_req(looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client):
    lagged_node_1 = txnPoolNodeSet[3]
    lagged_node_2 = txnPoolNodeSet[-1]
    with delay_rules_without_processing(lagged_node_1.nodeIbStasher, cDelay(), pDelay(), msg_rep_delay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)

        with delay_rules_without_processing(lagged_node_2.nodeIbStasher, cDelay(), pDelay(), msg_rep_delay()):
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 7)
            ensure_all_nodes_have_same_data(looper, set(txnPoolNodeSet) - {lagged_node_2, lagged_node_1})
            lagged_node_2.start_catchup()
            # Ugly waiting, just for debugging
            looper.runFor(20)
            assert lagged_node_1.spylog.count(lagged_node_1.ledgerManager.processCatchupReq) == 0
