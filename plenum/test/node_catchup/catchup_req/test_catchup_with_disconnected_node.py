from logging import getLogger

from plenum.test.logging.conftest import logsearch
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node

logger = getLogger()


def test_catchup_with_disconnected_node(tdir, tconf,
                                        looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client,
                                        allPluginsPath,
                                        logsearch):
    '''
    1. Stop the node Delta
    2. Order 9 txns. And check that txns were ordered on nodes [Alpha, Beta, Gamma].
    3. Stop Gamma
    4. Start Delta
    5. Check that all enabled nodes have equality data.
    6. Check that Delta didn't re-ask CatchupRep or did it only once.
    7. Check that Delta asked Gamma only in first round or didn't ask
    if Delta already know that Gamma was disconnected.
    '''
    # Prepare nodes
    restarted_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]
    lagging_node = rest_nodes[-1]

    # Stop Delta
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            restarted_node,
                                            stopNode=True)
    looper.removeProdable(restarted_node)

    # Send more requests to active nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, len(rest_nodes) * 3)
    waitNodeDataEquality(looper, *rest_nodes)

    # Stop Gamma
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Restart Delta and wait for successful catch up
    restarted_node = start_stopped_node(restarted_node,
                                        looper,
                                        tconf,
                                        tdir,
                                        allPluginsPath,
                                        start=False,
                                        )

    logs_request_txns, _ = logsearch(msgs=['requesting .* missing transactions after timeout'])
    logs_catchup_req, _ = logsearch(
        msgs=['{} sending message CATCHUP_REQ.*{}'.format(restarted_node.name, lagging_node.name)])
    old_request_txns = len(logs_request_txns)
    old_catchup_req = len(logs_catchup_req)

    looper.add(restarted_node)
    txnPoolNodeSet[-1] = restarted_node
    looper.run(checkNodesConnected([*rest_nodes[:-1], restarted_node]))

    waitNodeDataEquality(looper, *[*rest_nodes[:-1], restarted_node], customTimeout=120)
    assert len(logs_catchup_req) - old_catchup_req <= 1
    assert len(logs_request_txns) - old_request_txns <= 1