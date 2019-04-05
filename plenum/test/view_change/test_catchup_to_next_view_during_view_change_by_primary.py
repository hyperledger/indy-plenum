import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delay_for_view
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
    '''
    1) Lagging node is a primary for view=1
    2) All nodes except the lagging one start a view change (to view=1)
    3) The nodes can not finish it on time since the Primary for view=1 is lagging
    4) All nodes except the lagging one go to view=2 then
    5) All nodes except the lagging one order txns on view=2
    6) Lagging node gets InstanceChanges for view=1 => it changes to view=2, and catches up till txns from view=2
    7) Lagging node gets InstanceChanges for view=2 => it changes to view=2
    8) Make sure that the lagging node is up to date, and can participate in consensus
    '''
    lagging_node = txnPoolNodeSet[1]
    other_nodes = list(set(txnPoolNodeSet) - {lagging_node})
    initial_view_no = checkViewNoForNodes(txnPoolNodeSet)
    initial_last_ordered = lagging_node.master_last_ordered_3PC

    with delay_rules(lagging_node.nodeIbStasher, delay_for_view(viewNo=2)):
        with delay_rules(lagging_node.nodeIbStasher, delay_for_view(viewNo=0), delay_for_view(viewNo=1)):
            # view change to viewNo=2 since a primary for viewNo=1 is a lagging node
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
