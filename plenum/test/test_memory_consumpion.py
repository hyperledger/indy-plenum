import pytest

from stp_core.common.log import getlogger
from plenum.common.perf_util import get_size
from plenum.test.helper import sdk_send_random_requests
from plenum.test.pool_transactions.helper import sdk_add_new_nym

logger = getlogger()


@pytest.mark.skip('Unskip if needed')
def testRequestsSize(looper, txnPoolNodeSet, sdk_pool_handle,
                     sdk_wallet_steward, noRetryReq):
    clients = []
    for i in range(4):
        clients.append(sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward))
    numRequests = 250

    for (_, nym) in clients:
        logger.debug("{} sending {} requests".format(nym, numRequests))
        sdk_send_random_requests(looper, sdk_pool_handle,
                                 sdk_wallet_steward, numRequests)
        logger.debug("{} sent {} requests".format(nym, numRequests))

    for node in txnPoolNodeSet:
        logger.debug("{} has requests {} with size {}".
                     format(node, len(node.requests), get_size(node.requests)))
        for replica in node.replicas.values():
            logger.debug("{} has prepares {} with size {}".
                         format(replica, len(replica.prepares),
                                get_size(replica.prepares)))
            logger.debug("{} has commits {} with size {}".
                         format(replica, len(replica.commits),
                                get_size(replica.commits)))
