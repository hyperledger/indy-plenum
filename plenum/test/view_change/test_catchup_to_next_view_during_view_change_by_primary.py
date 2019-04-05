import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import icDelay, vcd_delay, delay_for_view
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, waitForViewChange, view_change_timeout
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkProtocolInstanceSetup, ensureElectionsDone

nodeCount = 7
VIEW_CHANGE_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, VIEW_CHANGE_TIMEOUT):
        yield tconf


def test_catchup_to_next_view_during_view_change_by_primary(txnPoolNodeSet, looper,
                                                               sdk_pool_handle, sdk_wallet_steward):
    lagging_node = txnPoolNodeSet[1]
    other_nodes = list(set(txnPoolNodeSet) - {lagging_node})
    initial_view_no = checkViewNoForNodes(txnPoolNodeSet)
    initial_last_ordered = lagging_node.master_last_ordered_3PC

    with delay_rules(lagging_node.nodeIbStasher, icDelay(viewNo=2), vcd_delay(viewNo=2)):
        with delay_rules(lagging_node.nodeIbStasher, delay_for_view(viewNo=0), delay_for_view(viewNo=1)):
            # view change to viewNo=2
            for n in txnPoolNodeSet:
                n.view_changer.on_master_degradation()
            waitForViewChange(looper,
                              other_nodes,
                              expectedViewNo=initial_view_no + 2,
                              customTimeout=30)
            checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(3))
            ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

            # order some txns
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_steward, 5)

            assert initial_view_no == lagging_node.viewNo
            assert initial_last_ordered == lagging_node.master_last_ordered_3PC

            # order some txns
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_steward, 5)

            assert len(lagging_node.master_replica.requestQueues[DOMAIN_LEDGER_ID]) > 0

        # make sure that the first View Change happened on lagging node
        waitForViewChange(looper, [lagging_node], expectedViewNo=initial_view_no + 1,
                          customTimeout=20)
        assert initial_view_no + 1 == lagging_node.viewNo

    # make sure that the second View Change happened on lagging node
    waitForViewChange(looper, [lagging_node], expectedViewNo=initial_view_no + 2,
                      customTimeout=20)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
