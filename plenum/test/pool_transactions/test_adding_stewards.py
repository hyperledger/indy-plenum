import pytest

from plenum.common.constants import STEWARD
from plenum.test.helper import waitRejectWithReason
from plenum.test.pool_transactions.helper import addNewClient, sendAddNewClient


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldThreshold = tconf.stewardThreshold
    tconf.stewardThreshold = 5

    def reset():
        tconf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return tconf


def testOnlyAStewardCanAddAnotherSteward(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, poolTxnClientData,
                                         steward1, stewardWallet,
                                         client1, wallet1, client1Connected):
    addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward1")

    sendAddNewClient(STEWARD, "testSteward2", client1, wallet1)
    for node in txnPoolNodeSet:
        waitRejectWithReason(
            looper,
            client1,
            'Only Steward is allowed to do these transactions',
            node.clientstack.name)


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper, tconf,
                                                      txnPoolNodeSet,
                                                      tdirWithPoolTxns,
                                                      poolTxnStewardData,
                                                      steward1, stewardWallet):
    addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward3")

    sendAddNewClient(STEWARD, "testSteward4", steward1, stewardWallet)
    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             'New stewards cannot be added by other '
                             'stewards as there are already {} '
                             'stewards in the system'.format(
                                 tconf.stewardThreshold),
                             node.clientstack.name)
