import pytest

from functools import partial

from plenum.test.conftest import getValueFromModule

from plenum.test import waits
from plenum.test.helper import checkLastClientReqForNode, \
    chk_all_funcs, sdk_json_to_request_object, sdk_send_random_requests
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
    checkPropagated, checkPrepared
from plenum.test.node_request.node_request_helper import checkCommitted
from plenum.test.pool_transactions.conftest import looper

from plenum.common.util import getNoInstances


@pytest.fixture(scope="module")
def committed1(looper, txnPoolNodeSet, prepared1, faultyNodes):
    checkCommitted(looper,
                   txnPoolNodeSet,
                   prepared1,
                   range(getNoInstances(len(txnPoolNodeSet))),
                   faultyNodes)
    return prepared1


@pytest.fixture(scope="module")
def prepared1(looper, txnPoolNodeSet, preprepared1, faultyNodes):
    checkPrepared(looper,
                  txnPoolNodeSet,
                  preprepared1,
                  range(getNoInstances(len(txnPoolNodeSet))),
                  faultyNodes)
    return preprepared1


@pytest.fixture(scope="module")
def preprepared1(looper, txnPoolNodeSet, propagated1, faultyNodes):
    checkPrePrepared(looper,
                     txnPoolNodeSet,
                     propagated1,
                     range(getNoInstances(len(txnPoolNodeSet))),
                     faultyNodes)
    return propagated1


@pytest.fixture(scope="module")
def propagated1(looper,
                txnPoolNodeSet,
                reqAcked1,
                faultyNodes):
    checkPropagated(looper, txnPoolNodeSet, reqAcked1, faultyNodes)
    return reqAcked1


@pytest.fixture(scope="module")
def reqAcked1(looper, txnPoolNodeSet, sent1, faultyNodes):
    numerOfNodes = len(txnPoolNodeSet)

    request = sdk_json_to_request_object(sent1[0][0])

    # Wait until request received by all nodes
    propTimeout = waits.expectedClientToPoolRequestDeliveryTime(numerOfNodes)
    coros = [partial(checkLastClientReqForNode, node, request)
             for node in txnPoolNodeSet]
    chk_all_funcs(looper, coros, acceptable_fails=faultyNodes,
                  timeout=propTimeout)
    return request


@pytest.fixture(scope="module")
def sent1(looper, sdk_pool_handle,
          sdk_wallet_client):
    request_couple_json = sdk_send_random_requests(
        looper, sdk_pool_handle, sdk_wallet_client, 1)
    return request_couple_json


@pytest.fixture(scope="module")
def noRetryReq(tconf, request):
    oldRetryAck = tconf.CLIENT_MAX_RETRY_ACK
    oldRetryReply = tconf.CLIENT_MAX_RETRY_REPLY
    tconf.CLIENT_MAX_RETRY_ACK = 0
    tconf.CLIENT_MAX_RETRY_REPLY = 0

    def reset():
        tconf.CLIENT_MAX_RETRY_ACK = oldRetryAck
        tconf.CLIENT_MAX_RETRY_REPLY = oldRetryReply

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope="module")
def faultyNodes(request):
    return getValueFromModule(request, "faultyNodes", 0)
