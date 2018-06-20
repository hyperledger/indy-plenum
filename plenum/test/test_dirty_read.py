from plenum.common.txn_util import get_seq_no, get_payload_data
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_get_and_check_replies
from plenum.common.constants import DATA
from plenum.common.messages.node_messages import Ordered
from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, \
    sdk_sign_and_send_prepared_request
from stp_core.common.log import getlogger

logger = getlogger()


def make_node_slow(node):
    old = node.serviceReplicas

    async def serviceReplicas(limit):
        for replica in node.replicas:
            for index, message in enumerate(list(replica.outBox)):
                if isinstance(message, Ordered):
                    del replica.outBox[index]
        return await old(limit)

    node.serviceReplicas = serviceReplicas


def test_dirty_read(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    Tests the case when read request comes before write request is
    not executed on some nodes
    """

    slow_nodes = list(txnPoolNodeSet)[2:4]
    for node in slow_nodes:
        logger.debug("Making node {} slow".format(node))
        make_node_slow(node)

    received_replies = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                                 sdk_pool_handle,
                                                 sdk_wallet_client,
                                                 1)
    result = received_replies[0][1]["result"]
    seq_no = get_seq_no(result)
    _, did = sdk_wallet_client
    req = sdk_build_get_txn_request(looper, did, seq_no)
    request = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                 sdk_pool_handle, req)
    received_replies = sdk_get_and_check_replies(looper, [request])
    results = [str(get_payload_data(reply['result'][DATA])) for _, reply in received_replies]

    assert len(set(results)) == 1
