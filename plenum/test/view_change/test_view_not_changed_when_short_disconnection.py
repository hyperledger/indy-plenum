from plenum.test.view_change.helper import node_received_instance_changes_count
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from stp_core.loop.eventually import eventually
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.test_node import get_master_primary_node
from plenum.test.view_change.helper import start_stopped_node


def node_primary_disconnect_count(node):
    pcm_service = node.master_replica._primary_connection_monitor_service
    return pcm_service.spylog.count(pcm_service._primary_disconnected)


def test_view_not_changed_when_short_disconnection(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client,
                                                   tdir, tconf, allPluginsPath):
    """
    When primary is disconnected but not long enough to trigger the timeout,
    view change should not happen
    """

    pr_node = get_master_primary_node(txnPoolNodeSet)
    view_no = checkViewNoForNodes(txnPoolNodeSet)

    # TODO: Do we really need go so deep in integration tests?
    primary_disconnect_calls = {node.name: node_primary_disconnect_count(node)
                                for node in txnPoolNodeSet if node != pr_node}

    recv_inst_chg_calls = {node.name: node_received_instance_changes_count(node) for node in txnPoolNodeSet
                           if node != pr_node}

    # Disconnect master's primary
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, pr_node, timeout=2)
    txnPoolNodeSet.remove(pr_node)
    looper.removeProdable(name=pr_node.name)

    timeout = min(tconf.ToleratePrimaryDisconnection - 1, 1)

    # Reconnect master's primary
    pr_node = start_stopped_node(pr_node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(pr_node)

    def chk2():
        # Schedule an instance change but do not send it
        # since primary joins again
        for node in txnPoolNodeSet:
            if node != pr_node:
                assert node_primary_disconnect_count(node) > primary_disconnect_calls[node.name]
                assert node_received_instance_changes_count(node) == recv_inst_chg_calls[node.name]

    looper.run(eventually(chk2, retryWait=.2, timeout=timeout + 1))

    assert checkViewNoForNodes(txnPoolNodeSet) == view_no

    # Send some requests and make sure the request execute
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
