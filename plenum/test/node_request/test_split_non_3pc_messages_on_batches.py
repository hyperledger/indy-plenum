from functools import partial

import pytest

from plenum.test import waits

from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests, checkReqAck
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from stp_core.loop.eventually import eventuallyAll
from stp_core.validators.message_length_validator import MessageLenValidator

from plenum.test.pool_transactions.conftest import looper, client1Connected  # noqa
from plenum.test.pool_transactions.conftest import clientAndWallet1, client1, wallet1  # noqa


def test_msg_max_length_check_node_to_node(tconf,
                                           tdir,
                                           looper,
                                           txnPoolNodeSet,
                                           client1,
                                           wallet1,
                                           client1Connected,
                                           clientAndWallet2):
    """
    Two clients send 2*N requests each at the same time.
    N < MSG_LEN_LIMIT but 2*N > MSG_LEN_LIMIT so the requests pass the max
    length check for client-node requests but do not pass the check
    for node-node requests.
    """
    N = 10
    # it is an empirical value for N random requests
    # it has to be adjusted if the world changed (see pydoc)
    max_len_limit = 3000

    patch_msg_len_validators(max_len_limit, txnPoolNodeSet)

    client2, wallet2 = clientAndWallet2

    reqs1 = sendRandomRequests(wallet1, client1, N)
    reqs2 = sendRandomRequests(wallet2, client2, N)

    check_reqacks(client1, looper, reqs1, txnPoolNodeSet)
    check_reqacks(client2, looper, reqs2, txnPoolNodeSet)

    waitForSufficientRepliesForRequests(looper, client1, requests=reqs1)
    waitForSufficientRepliesForRequests(looper, client2, requests=reqs2)


def patch_msg_len_validators(max_len_limit, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert hasattr(node.nodestack, 'msgLenVal')
        assert hasattr(node.nodestack, 'msg_len_val')
        node.nodestack.msgLenVal = MessageLenValidator(max_len_limit)
        node.nodestack.msg_len_val = MessageLenValidator(max_len_limit)


def check_reqacks(client, looper, reqs, txnPoolNodeSet):
    reqack_coros = []
    for req in reqs:
        reqack_coros.extend([partial(checkReqAck, client, node, req.identifier,
                                     req.reqId, None) for node in txnPoolNodeSet])
    timeout = waits.expectedReqAckQuorumTime()
    looper.run(eventuallyAll(*reqack_coros, totalTimeout=timeout))


@pytest.fixture(scope="module")
def clientAndWallet2(looper, poolTxnClientData, tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)

    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    yield client, wallet
    client.stop()
