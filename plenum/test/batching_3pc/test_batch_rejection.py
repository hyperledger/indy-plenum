import types

import pytest

from plenum.common.eventually import eventually
from plenum.common.types import DOMAIN_LEDGER_ID
from plenum.common.util import updateNamedTuple
from plenum.test.batching_3pc.helper import checkSufficientRepliesRecvdForReqs
from plenum.test.helper import sendRandomRequests, getPrimaryReplica
from plenum.test.test_node import getNonPrimaryReplicas


@pytest.fixture(scope="module")
def setup(tconf, looper, txnPoolNodeSet, client, wallet1):
    pr, otherR = getPrimaryReplica(txnPoolNodeSet, instId=0), \
                 getNonPrimaryReplicas(txnPoolNodeSet, instId=0)

    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client,
                                       tconf.Max3PCBatchWait)
    stateRoot = pr.stateRootHash(DOMAIN_LEDGER_ID, toHex=False)

    origMethod = pr.create3PCBatch
    malignedOnce = None

    def badMethod(self, ledgerId):
        nonlocal malignedOnce
        pp = origMethod(ledgerId)
        if not malignedOnce:
            pp = updateNamedTuple(pp, digest=pp.digest + '123')
            malignedOnce = True
        return pp

    pr.create3PCBatch = types.MethodType(badMethod, pr)
    sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    return pr, otherR, stateRoot


# def testAllDependentBatchesDiscardedAfterBatchReject(setup):
#     """
#     Check if a batch if rejected, each batch that was created based on the
#     rejected batch is discarded.
#     :return:
#     """


@pytest.fixture(scope="module")
def reverted(setup, looper):
    pr, otherR, oldStateRoot = setup

    def chkPps(n):
        assert len(pr.batches) == n

    def chkStateRoot(root):
        for r in [pr]+otherR:
            r.stateRootHash(DOMAIN_LEDGER_ID, toHex=False) == root

    looper.run(eventually(chkPps, 1, retryWait=1, timeout=5))
    looper.run(eventually(chkStateRoot, oldStateRoot))


@pytest.fixture(scope="module")
def viewChanged(reverted, looper, txnPoolNodeSet):
    def chk():
        for n in txnPoolNodeSet:
            assert n.viewNo == 1
            assert all([r.primaryName for r in n.replicas])

    looper.run(eventually(chk, retryWait=1, timeout=15))


def testTreeStateRevertedAfterBatchRejection(reverted):
    """"
    After a batch is rejected, all nodes revert their trees to last known
    correct state
    """
    pass


def testViewChangeAfterBatchRejected(viewChanged):
    """"
    After a batch is rejected and each batch that was created based on the
    rejected batch is discarded, the discarded batches are tried again
    """
    pass


def testMoreBatchesWillBeSentAfterViewChange(reverted, viewChanged, wallet1,
                                             client, tconf, looper):
    """
    After retrying discarded batches, new batches are sent
    :return:
    """
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    checkSufficientRepliesRecvdForReqs(looper, reqs, client,
                                       tconf.Max3PCBatchWait)
