import pytest

from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import node_sent_instance_changes_count
from stp_core.loop.eventually import eventually
from plenum.test.delayers import icDelay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

nodeCount = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout = tconf.NEW_VIEW_TIMEOUT
    tconf.NEW_VIEW_TIMEOUT = 5
    yield tconf
    tconf.NEW_VIEW_TIMEOUT = old_timeout


def check_count_connected_node(nodes, expected_count):
    assert {n.connectedNodeCount for n in nodes} == {expected_count}


def check_sent_instance_changes_count(nodes, expected_count):
    assert {node_sent_instance_changes_count(n) for n in nodes} == {expected_count}


def test_resend_instance_change_messages(looper,
                                         txnPoolNodeSet,
                                         tconf,
                                         sdk_wallet_steward,
                                         sdk_pool_handle):
    primary_node = txnPoolNodeSet[0]
    old_view_no = checkViewNoForNodes(txnPoolNodeSet, 0)
    assert primary_node.master_replica.isPrimary
    for n in txnPoolNodeSet:
        n.nodeIbStasher.delay(icDelay(3 * tconf.NEW_VIEW_TIMEOUT))
    check_sent_instance_changes_count(txnPoolNodeSet, 0)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            primary_node,
                                            stopNode=False)
    txnPoolNodeSet.remove(primary_node)
    looper.run(eventually(check_count_connected_node, txnPoolNodeSet, 4,
                          timeout=5,
                          acceptableExceptions=[AssertionError]))
    looper.run(eventually(check_sent_instance_changes_count, txnPoolNodeSet, 1,
                          timeout=2*tconf.NEW_VIEW_TIMEOUT))

    looper.run(eventually(checkViewNoForNodes, txnPoolNodeSet, old_view_no + 1,
                          timeout=3*tconf.NEW_VIEW_TIMEOUT))
    ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
