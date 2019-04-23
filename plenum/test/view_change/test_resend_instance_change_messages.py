import pytest
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from stp_core.loop.eventually import eventually
from functools import partial
from plenum.test.delayers import icDelay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

nodeCount = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout = tconf.INSTANCE_CHANGE_TIMEOUT
    tconf.INSTANCE_CHANGE_TIMEOUT = 5
    yield tconf
    tconf.INSTANCE_CHANGE_TIMEOUT = old_timeout


def check_count_connected_node(nodes, expected_count):
    assert set([n.connectedNodeCount for n in nodes]) == {expected_count}


def test_resend_instance_change_messages(looper,
                                         txnPoolNodeSet,
                                         tconf,
                                         sdk_wallet_steward,
                                         sdk_pool_handle):
    primary_node = txnPoolNodeSet[0]
    old_view_no = checkViewNoForNodes(txnPoolNodeSet, 0)
    assert primary_node.master_replica.isPrimary
    for n in txnPoolNodeSet:
        n.nodeIbStasher.delay(icDelay(3 * tconf.INSTANCE_CHANGE_TIMEOUT))
    assert set([n.view_changer.instance_change_rounds for n in txnPoolNodeSet]) == {0}
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            primary_node,
                                            stopNode=False)
    txnPoolNodeSet.remove(primary_node)
    looper.run(eventually(partial(check_count_connected_node, txnPoolNodeSet, 4),
                          timeout=5,
                          acceptableExceptions=[AssertionError]))
    looper.runFor(2 * tconf.INSTANCE_CHANGE_TIMEOUT)
    assert set([n.view_changer.instance_change_rounds for n in txnPoolNodeSet]) == {1}

    looper.runFor(tconf.INSTANCE_CHANGE_TIMEOUT)
    looper.run(eventually(partial(checkViewNoForNodes, txnPoolNodeSet, expectedViewNo=old_view_no + 1),
                          timeout=tconf.VIEW_CHANGE_TIMEOUT))
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
