import types

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import updateNamedTuple
from plenum.test.helper import sendRandomRequests, \
    waitForSufficientRepliesForRequests
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica


@pytest.fixture(scope="module")
def setup(tconf, looper, txnPoolNodeSet, client, wallet1):
    # Patch the 3phase request sending method to send incorrect digest and
    pr, otherR = getPrimaryReplica(txnPoolNodeSet, instId=0), \
        getNonPrimaryReplicas(txnPoolNodeSet, instId=0)

    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(
        looper,
        client,
        requests=reqs,
        customTimeoutPerReq=tconf.Max3PCBatchWait)
    stateRoot = pr.stateRootHash(DOMAIN_LEDGER_ID, to_str=False)

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


@pytest.fixture(scope="module")
def reverted(setup, looper):
    pr, otherR, oldStateRoot = setup

    def chkStateRoot(root):
        for r in [pr] + otherR:
            r.stateRootHash(DOMAIN_LEDGER_ID, to_str=False) == root

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


def testViewChangeAfterBatchRejected(viewChanged):
    """"
    After a batch is rejected and each batch that was created based on the
    rejected batch is discarded, the discarded batches are tried again
    """


def testMoreBatchesWillBeSentAfterViewChange(reverted, viewChanged, wallet1,
                                             client, tconf, looper):
    """
    After retrying discarded batches, new batches are sent
    :return:
    """
    reqs = sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(
        looper,
        client,
        requests=reqs,
        customTimeoutPerReq=tconf.Max3PCBatchWait)
