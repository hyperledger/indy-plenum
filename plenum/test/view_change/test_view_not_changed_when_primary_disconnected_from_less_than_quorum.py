import types

import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas, get_master_primary_node
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import looper
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check


def test_view_not_changed_when_primary_disconnected_from_less_than_quorum(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Less than quorum nodes lose connection with primary, this should not
    trigger view change as the protocol can move ahead
    """
    pr_node = get_master_primary_node(txnPoolNodeSet)
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    partitioned_rep = npr[0]
    partitioned_node = partitioned_rep.node

    lost_pr_calls = partitioned_node.spylog.count(
        partitioned_node.lost_master_primary.__name__)

    recv_inst_chg_calls = {node.name: node.spylog.count(
        node.view_changer.process_instance_change_msg.__name__) for node in txnPoolNodeSet
        if node != partitioned_node and node != pr_node}

    view_no = checkViewNoForNodes(txnPoolNodeSet)
    orig_retry_meth = partitioned_node.nodestack.retryDisconnected

    def wont_retry(self, exclude=None):
        # Do not attempt to retry connection
        pass

    # simulating a partition here
    # Disconnect a node from only the primary of the master and dont retry to
    # connect to it
    partitioned_node.nodestack.retryDisconnected = types.MethodType(
        wont_retry, partitioned_node.nodestack)
    r = partitioned_node.nodestack.getRemote(pr_node.nodestack.name)
    r.disconnect()

    def chk1():
        # Check that the partitioned node detects losing connection with
        # primary and sends an instance change which is received by other
        # nodes except the primary (since its disconnected from primary)
        assert partitioned_node.spylog.count(
            partitioned_node.lost_master_primary.__name__) > lost_pr_calls
        for node in txnPoolNodeSet:
            if node != partitioned_node and node != pr_node:
                assert node.view_changer.spylog.count(
                    node.view_changer.process_instance_change_msg.__name__) > recv_inst_chg_calls[node.name]

    looper.run(eventually(chk1, retryWait=1, timeout=10))

    def chk2():
        # Check the view does not change
        with pytest.raises(AssertionError):
            assert checkViewNoForNodes(txnPoolNodeSet) == view_no + 1

    looper.run(eventually(chk2, retryWait=1, timeout=10))
    # Send some requests and make sure the request execute
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    # Repair the connection so the node is no longer partitioned
    partitioned_node.nodestack.retryDisconnected = types.MethodType(
        orig_retry_meth, partitioned_node.nodestack)

    # Send some requests and make sure the request execute
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    # Partitioned node should have the same ledger and state as others
    # eventually
    waitNodeDataEquality(looper, partitioned_node,
                         *[n for n in txnPoolNodeSet if n != partitioned_node])
