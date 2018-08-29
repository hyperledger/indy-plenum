import functools

from plenum.common.exceptions import InvalidClientMessageException, RequestRejectedException
from plenum.test.helper import sdk_send_random_and_check


REQ_COUNT = 10


def randomDynamicValidation(self, req):
    if list(self.requests.keys()).index(req.key) % 2:
        raise InvalidClientMessageException('aaaaaaaa',
                                             req.reqId,
                                             "not valid req")

def check_count_reqs(nodes):
    for node in nodes:
        reqs_count = set([c for c, _ in node.monitor.numOrderedRequests])
        assert len(reqs_count) == 1


def test_invalid_reqs(looper,
                      txnPoolNodeSet,
                      sdk_wallet_steward,
                      sdk_pool_handle):
    for node in txnPoolNodeSet:
        node.doDynamicValidation = functools.partial(randomDynamicValidation, node)
    has_invalid_reqs = False
    try:
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_steward,
                                  REQ_COUNT)
    except RequestRejectedException:
        has_invalid_reqs = True
    assert has_invalid_reqs
    check_count_reqs(txnPoolNodeSet)