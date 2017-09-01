import types

import pytest

from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test.helper import checkReqNackWithReason, sendRandomRequests, \
    checkRejectWithReason, waitForSufficientRepliesForRequests
from stp_core.loop.eventually import eventually


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
