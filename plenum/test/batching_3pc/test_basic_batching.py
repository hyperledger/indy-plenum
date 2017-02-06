from plenum.common.eventually import eventually
from plenum.common.types import DOMAIN_LEDGER_ID
from plenum.test.batching_3pc.helper import checkSufficientRepliesRecvdForReqs, \
    checkNodesHaveSameRoots
from plenum.test.helper import checkReqNackWithReason, sendRandomRequests, \
    checkSufficientRepliesRecvd
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


def testRequestStaticValidation(tconf, looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected):
    """
    Check that for requests which fail static validation, REQNACK is sent
    :return:
    """
    reqs = [wallet1.signOp((lambda : {'something': 'nothing'})()) for _ in
            range(tconf.Max3PCBatchSize)]
    client1.submitReqs(*reqs)
    for node in txnPoolNodeSet:
        looper.run(eventually(checkReqNackWithReason, client1, '',
                              node.clientstack.name, retryWait=1))


def test3PCOverBatchWithThresholdReqs(tconf, looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected):
    """
    Check that 3 phase commit happens when threshold number of requests are
    received and propagated.
    :return:
    """
    reqs = sendRandomRequests(wallet1, client1, tconf.Max3PCBatchSize)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client1,
                                       tconf.Max3PCBatchWait-1)


def test3PCOverBatchWithLessThanThresholdReqs(tconf, looper, txnPoolNodeSet,
                                              client1, wallet1,
                                              client1Connected):
    """
    Check that 3 phase commit happens when threshold number of requests are
    not received but threshold time has passed
    :return:
    """
    reqs = sendRandomRequests(wallet1, client1, tconf.Max3PCBatchSize-1)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client1,
                                       tconf.Max3PCBatchWait+1)


def testTreeRootsCorrectAfterEachBatch(tconf, looper, txnPoolNodeSet,
                                              client1, wallet1,
                                              client1Connected):
    """
    Check if both state root and txn tree root are correct and same on each
    node after each batch
    :return:
    """
    # Send 1 batch
    reqs = sendRandomRequests(wallet1, client1, tconf.Max3PCBatchSize)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client1,
                                       tconf.Max3PCBatchWait)
    checkNodesHaveSameRoots(txnPoolNodeSet)

    # Send 2 batches
    reqs = sendRandomRequests(wallet1, client1, 2*tconf.Max3PCBatchSize)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client1,
                                       2*tconf.Max3PCBatchWait)
    checkNodesHaveSameRoots(txnPoolNodeSet)


def testRequestDynamicValidation():
    """
    Check that for requests which fail dynamic (state based) validation,
    REJECT is sent to the client
    :return:
    """
    pass
