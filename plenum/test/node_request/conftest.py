import operator
import importlib
import inspect
import itertools
import logging
import os
import shutil
import re
import warnings
import json
from functools import partial
from plenum.test import waits
from plenum.common.util import getNoInstances

import pytest
from stp_core.common.log import getlogger, Logger
from plenum.test.helper import randomOperation, \
    checkReqAck, checkLastClientReqForNode, waitForSufficientRepliesForRequests, \
    waitForViewChange, requestReturnedToNode, randomText, \
    mockGetInstalledDistributions, mockImportModule, chk_all_funcs, \
    create_new_test_node, sdk_send_random_request, sdk_json_to_request_object
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
     checkPropagated, checkPrepared
from plenum.test.node_request.node_request_helper import checkCommitted

from plenum.test.pool_transactions.conftest import looper

Logger.setLogLevel(logging.NOTSET)
logger = getlogger()


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

    # Wait until request received by all nodes
    propTimeout = waits.expectedClientToPoolRequestDeliveryTime(numerOfNodes)
    coros = [partial(checkLastClientReqForNode, node, sent1)
             for node in txnPoolNodeSet]
    # looper.run(eventuallyAll(*coros,
    #                          totalTimeout=propTimeout,
    #                          acceptableFails=faultyNodes))
    chk_all_funcs(looper, coros, acceptable_fails=faultyNodes,
                  timeout=propTimeout)

    #TODO: check that sdk response fail when we don't have quorum for answer
    #TODO: check sdk timeout for failed submit (if it more than 90 seconds in evilNodes fixture)

    # # Wait until sufficient number of acks received
    # coros2 = [
    #     partial(
    #         checkReqAck,
    #         client1,
    #         node,
    #         sent1.identifier,
    #         sent1.reqId) for node in txnPoolNodeSet]
    # ackTimeout = waits.expectedReqAckQuorumTime()
    # # looper.run(eventuallyAll(*coros2,
    # #                          totalTimeout=ackTimeout,
    # #                          acceptableFails=faultyNodes))
    # chk_all_funcs(looper, coros2, acceptable_fails=faultyNodes,
    #               timeout=ackTimeout)
    return sent1

@pytest.fixture(scope="module")
def sent1(looper, sdk_pool_handle,
          sdk_wallet_client):
    json_request = sdk_send_random_request(
        looper, sdk_pool_handle, sdk_wallet_client)[0]
    request = sdk_json_to_request_object(json_request)
    return request


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