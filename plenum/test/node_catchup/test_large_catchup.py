import pytest

from plenum.common.messages.node_messages import CatchupRep
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality

from stp_core.validators.message_length_validator import MessageLenValidator


TestRunningTimeLimitSec = 125


def decrease_max_request_size(node):
    old = node.nodestack.prepare_for_sending

    def prepare_for_sending(msg, signer, message_splitter=lambda x: None):
        if isinstance(msg, CatchupRep):
            node.nodestack.prepare_for_sending = old
            part_bytes = node.nodestack.sign_and_serialize(msg, signer)
            # Decrease at least 6 times to increase probability of
            # unintentional shuffle
            new_limit = len(part_bytes) // 6
            node.nodestack.msg_len_val = MessageLenValidator(new_limit)
        return old(msg, signer, message_splitter)

    node.nodestack.prepare_for_sending = prepare_for_sending


@pytest.fixture(scope="module")
def tconf(request, tconf):
    old_size = tconf.CLIENT_REPLY_TIMEOUT
    tconf.CLIENT_REPLY_TIMEOUT = 60

    def reset():
        tconf.CLIENT_REPLY_TIMEOUT = old_size

    request.addfinalizer(reset)
    return tconf


def test_large_catchup(tdir, tconf,
                       looper,
                       testNodeClass,
                       txnPoolNodeSet,
                       wallet1,
                       client1,
                       client1Connected,
                       allPluginsPath):
    """
    Checks that node can catchup large ledgers
    """
    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]
    all_nodes = txnPoolNodeSet

    # Prepare client
    client, wallet = client1, wallet1

    # Check that requests executed well
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=10)

    # Stop one node
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)
    disconnect_node_and_ensure_disconnected(looper,
                                            all_nodes,
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
    # Not calling start since it does not start states
    config_helper = PNodeConfigHelper(lagging_node.name, tconf, chroot=tdir)
    lagging_node = testNodeClass(lagging_node.name,
                                 config_helper=config_helper,
                                 config=tconf, pluginPaths=allPluginsPath)
    looper.add(lagging_node)
    txnPoolNodeSet[-1] = lagging_node
    waitNodeDataEquality(looper, *all_nodes)
