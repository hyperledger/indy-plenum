import types

import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas, get_master_primary_node
from plenum.test.view_change.helper import node_received_instance_changes_count
from stp_core.loop.eventually import eventually
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check


def node_primary_disconnected_calls(node):
    pcm_service = node.master_replica._primary_connection_monitor_service
    return pcm_service.spylog.count(pcm_service._primary_disconnected)


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

    primary_disconnected_calls = node_primary_disconnected_calls(partitioned_node)

    recv_inst_chg_calls = {node.name: node_received_instance_changes_count(node) for node in txnPoolNodeSet
                           if node != partitioned_node and node != pr_node}

    view_no = checkViewNoForNodes(txnPoolNodeSet)

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
        assert node_primary_disconnected_calls(partitioned_node) > primary_disconnected_calls
        for node in txnPoolNodeSet:
            if node != partitioned_node and node != pr_node:
                assert node_received_instance_changes_count(node) > recv_inst_chg_calls[node.name]

    looper.run(eventually(chk1, retryWait=1, timeout=10))

    def chk2():
        # Check the view does not change
        with pytest.raises(AssertionError):
            assert checkViewNoForNodes(txnPoolNodeSet) == view_no + 1

    looper.run(eventually(chk2, retryWait=1, timeout=10))
    # Send some requests and make sure the request execute
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    # Partitioned node should have the same ledger and state as others as it gets reqs from all nodes
    waitNodeDataEquality(looper, partitioned_node,
                         *[n for n in txnPoolNodeSet if n != partitioned_node],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
