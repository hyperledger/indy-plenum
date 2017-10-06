from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests, signed_random_requests, \
    send_signed_requests, checkReqNackWithReason
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from stp_core.loop.eventually import eventually


def test_request_no_protocol_version(tconf, looper, txnPoolNodeSet,
                                     client1, client1Connected,
                                     wallet1):
    reqs = signed_random_requests(wallet1, 2)
    for req in reqs:
        req.protocolVersion = None
    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_version_set_by_default(tconf, looper, txnPoolNodeSet,
                                client1, client1Connected,
                                wallet1):
    reqs = signed_random_requests(wallet1, 1)
    assert reqs[0].protocolVersion
    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_request_with_correct_version(tconf, looper,
                                      txnPoolNodeSet, client1, client1Connected,
                                      wallet1):
    reqs = signed_random_requests(wallet1, 2)
    for req in reqs:
        req.protocolVersion = CURRENT_PROTOCOL_VERSION
    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_request_with_invalid_version(tconf, looper, txnPoolNodeSet,
                                      client1, client1Connected,
                                      wallet1):
    reqs = signed_random_requests(wallet1, 2)
    for req in reqs:
        req.protocolVersion = -1
    send_signed_requests(client1, reqs)
    for node in txnPoolNodeSet:
        looper.run(eventually(checkReqNackWithReason, client1,
                              'Unknown protocol version value -1',
                              node.clientstack.name, retryWait=1))
