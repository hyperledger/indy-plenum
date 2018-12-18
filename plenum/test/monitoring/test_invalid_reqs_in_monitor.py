import functools

import pytest

from plenum.common.exceptions import InvalidClientMessageException, RequestRejectedException
from plenum.test.helper import sdk_send_random_and_check

COUNT_VALID_REQS = 1
COUNT_INVALID_REQS = 2



def randomDynamicValidation(self, req):
    raise InvalidClientMessageException('aaaaaaaa',
                                        req.reqId,
                                        "not valid req")

def check_count_reqs(nodes):
    for node in nodes:
        reqs_count = set([c for c, _ in node.monitor.numOrderedRequests.values() if c == COUNT_VALID_REQS + COUNT_INVALID_REQS])
        assert len(reqs_count) == 1


def test_invalid_reqs(looper,
                      txnPoolNodeSet,
                      sdk_wallet_steward,
                      sdk_pool_handle):
    """Send 1 valid request and 2 invalid. Then checked, that all 3 requests are stored into monitor."""
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              COUNT_VALID_REQS)
    for node in txnPoolNodeSet:
        node.doDynamicValidation = functools.partial(randomDynamicValidation, node)
    with pytest.raises(RequestRejectedException, match='not valid req'):
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_steward,
                                  COUNT_INVALID_REQS)
    check_count_reqs(txnPoolNodeSet)