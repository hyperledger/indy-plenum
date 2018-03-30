import random

import pytest

from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, sdk_sign_and_send_prepared_request
from stp_core.common.log import getlogger

from plenum.test.helper import stopNodes, sdk_send_random_and_check, \
    sdk_get_and_check_replies

logger = getlogger()

nodeCount = 4


@pytest.mark.skip(reason='sdk client cannot do that yet')
def test_client_can_send_get_request_to_one_node(looper,
                                                 sdk_pool_handle,
                                                 sdk_wallet_client,
                                                 txnPoolNodeSet):
    """
    Check that read only request can be sent
    without having connection to all nodes
    """
    logger.info("Send set request")
    req = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client, 1)[0]

    logger.info("Stopping nodes")
    nodes_to_stop = list(txnPoolNodeSet)[1:]
    stopNodes(nodes_to_stop, looper)

    logger.info("Send get request")
    _, did = sdk_wallet_client
    req = sdk_build_get_txn_request(looper, did, req[0]['reqId'])
    request = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                 sdk_pool_handle, req)
    replies = sdk_get_and_check_replies(looper, [request])
    pass
