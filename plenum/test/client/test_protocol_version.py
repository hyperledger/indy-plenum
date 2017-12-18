import pytest
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import Request
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    send_signed_requests, checkReqNackWithReason, random_request_objects, \
    sign_request_objects, signed_random_requests, random_requests
# noinspection PyUnresolvedReferences
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from stp_core.loop.eventually import eventually


@pytest.yield_fixture(scope="function", params=['1', '2'])
def request_num(request):
    return int(request.param)

def test_request_no_protocol_version(looper, txnPoolNodeSet,
                                     client1, client1Connected,
                                     wallet1,
                                     request_num):
    reqs = random_request_objects(request_num, protocol_version=None)
    reqs = sign_request_objects(wallet1, reqs)
    for req in reqs:
        assert req.protocolVersion is None

    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_version_not_set_by_default(looper, txnPoolNodeSet,
                                client1, client1Connected,
                                wallet1,
                                request_num):
    req_dicts = random_requests(request_num)
    reqs = [Request(operation=op) for op in req_dicts]
    for req in reqs:
        assert req.protocolVersion is None
    reqs = sign_request_objects(wallet1, reqs)
    for req in reqs:
        assert req.protocolVersion is None

    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_request_with_correct_version(looper,
                                      txnPoolNodeSet, client1, client1Connected,
                                      wallet1,
                                      request_num):
    reqs = random_request_objects(request_num, protocol_version=CURRENT_PROTOCOL_VERSION)
    reqs = sign_request_objects(wallet1, reqs)
    for req in reqs:
        assert req.protocolVersion == CURRENT_PROTOCOL_VERSION

    send_signed_requests(client1, reqs)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)


def test_request_with_invalid_version(looper, txnPoolNodeSet,
                                      client1, client1Connected,
                                      wallet1,
                                      request_num):
    reqs = random_request_objects(request_num, protocol_version=-1)
    reqs = sign_request_objects(wallet1, reqs)
    for req in reqs:
        assert req.protocolVersion == -1

    send_signed_requests(client1, reqs)
    for node in txnPoolNodeSet:
        looper.run(eventually(checkReqNackWithReason, client1,
                              'Unknown protocol version value -1',
                              node.clientstack.name, retryWait=1))
