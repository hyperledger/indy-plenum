from plenum.common.messages.node_messages import CatchupRep
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.pool_transactions.conftest import \
    client1, wallet1, client1Connected
from plenum.test.test_node import checkNodesConnected
from stp_core.validators.message_length_validator import MessageLenValidator


def decrease_max_request_size(node):
    old = node.nodestack.prepare_for_sending

    def prepare_for_sending(msg, signer, message_splitter=lambda x: None):
        if isinstance(msg, CatchupRep):
            node.nodestack.prepare_for_sending = old
            part_bytes = node.nodestack.sign_and_serialize(msg, signer)
            # Decrease at least 6 times to increase probability of
            # unintentional shuffle
            new_limit = len(part_bytes) / 6
            node.nodestack.msg_len_val = MessageLenValidator(new_limit)
        return old(msg, signer, message_splitter)

    node.nodestack.prepare_for_sending = prepare_for_sending


def test_large_catchup(looper,
                       txnPoolNodeSet,
                       wallet1,
                       client1,
                       client1Connected,
                       tconf,
                       allPluginsPath,
                       tdirWithPoolTxns):
    """
    Checks that node can catchup large ledgers
    """
    # Prepare nodes
    lagging_node = txnPoolNodeSet[0]
    rest_nodes = txnPoolNodeSet[1:]
    all_nodes = txnPoolNodeSet
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Prepare client
    client, wallet = client1, wallet1
    looper.run(client.ensureConnectedToNodes())

    # Check that requests executed well
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=10)

    # Stop one node
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)
    disconnect_node_and_ensure_disconnected(looper,
                                            rest_nodes,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Send more requests to active nodes
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=100)
    waitNodeDataEquality(looper, *rest_nodes)

    # Make message size limit smaller to ensure that catchup response is
    # larger exceeds the limit
    for node in rest_nodes:
        decrease_max_request_size(node)

    # Restart stopped node and wait for successful catch up
    looper.add(lagging_node)
    reconnect_node_and_ensure_connected(looper, all_nodes, lagging_node)
    waitNodeDataEquality(looper, *all_nodes)
