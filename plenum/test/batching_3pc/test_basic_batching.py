import json
import types
from functools import partial

import pytest
from plenum.test import waits
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from plenum.test.test_client import genTestClient

from stp_core.loop.eventually import eventually, eventuallyAll
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test.helper import checkReqNackWithReason, sendRandomRequests, \
    checkRejectWithReason, waitForSufficientRepliesForRequests, signed_random_requests, send_signed_requests, \
    checkReqAck
from stp_core.validators.message_length_validator import MessageLenValidator


def testRequestStaticValidation(tconf, looper, txnPoolNodeSet, client,
                                wallet1):
    """
    Check that for requests which fail static validation, REQNACK is sent
    :return:
    """
    reqs = [wallet1.signOp((lambda: {'something': 'nothing'})()) for _ in
            range(tconf.Max3PCBatchSize)]
    client.submitReqs(*reqs)
    for node in txnPoolNodeSet:
        looper.run(eventually(checkReqNackWithReason, client, '',
                              node.clientstack.name, retryWait=1))


def test3PCOverBatchWithThresholdReqs(tconf, looper, txnPoolNodeSet, client,
                                      wallet1):
    """
    Check that 3 phase commit happens when threshold number of requests are
    received and propagated.
    :return:
    """
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs)


def test3PCOverBatchWithLessThanThresholdReqs(tconf, looper, txnPoolNodeSet,
                                              client, wallet1):
    """
    Check that 3 phase commit happens when threshold number of requests are
    not received but threshold time has passed
    :return:
    """
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize - 1)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs)


def testTreeRootsCorrectAfterEachBatch(tconf, looper, txnPoolNodeSet,
                                       client, wallet1):
    """
    Check if both state root and txn tree root are correct and same on each
    node after each batch
    :return:
    """
    # Send 1 batch
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs)
    checkNodesHaveSameRoots(txnPoolNodeSet)

    # Send 2 batches
    reqs = sendRandomRequests(wallet1, client, 2 * tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs)
    checkNodesHaveSameRoots(txnPoolNodeSet)


def testRequestDynamicValidation(tconf, looper, txnPoolNodeSet,
                                 client, wallet1):
    """
    Check that for requests which fail dynamic (state based) validation,
    REJECT is sent to the client
    :return:
    """
    origMethods = []
    names = {node.name: 0 for node in txnPoolNodeSet}

    def rejectingMethod(self, req):
        names[self.name] += 1
        # Raise rejection for last request of batch
        if tconf.Max3PCBatchSize - names[self.name] == 0:
            raise UnauthorizedClientRequest(req.identifier,
                                            req.reqId,
                                            'Simulated rejection')

    for node in txnPoolNodeSet:
        origMethods.append(node.doDynamicValidation)
        node.doDynamicValidation = types.MethodType(rejectingMethod, node)

    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client, requests=reqs[:-1])

    with pytest.raises(AssertionError):
        waitForSufficientRepliesForRequests(looper, client, requests=reqs[-1:])

    for node in txnPoolNodeSet:
        looper.run(eventually(checkRejectWithReason, client,
                              'Simulated rejection', node.clientstack.name,
                              retryWait=1))

    for i, node in enumerate(txnPoolNodeSet):
        node.doDynamicValidation = origMethods[i]


def test_msg_max_length_check_node_to_node(tconf,
                                           tdir,
                                           looper,
                                           txnPoolNodeSet,
                                           client,
                                           wallet1,
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

    reqs1 = sendRandomRequests(wallet1, client, N)
    reqs2 = sendRandomRequests(wallet2, client2, N)

    check_reqacks(client, looper, reqs1, txnPoolNodeSet)
    check_reqacks(client2, looper, reqs2, txnPoolNodeSet)

    waitForSufficientRepliesForRequests(looper, client, requests=reqs1)
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
