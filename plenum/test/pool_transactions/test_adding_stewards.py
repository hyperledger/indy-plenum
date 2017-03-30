import pytest

from plenum.common.eventually import eventually
from plenum.common.signer_simple import SimpleSigner
from plenum.common.constants import TXN_TYPE, TARGET_NYM, ROLE, STEWARD, NYM, \
    ALIAS
from plenum.common.util import randomSeed
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet, \
    addNewClient


@pytest.fixture(scope="module")
def tconf(conf, tdir, request):
    oldThreshold = conf.stewardThreshold
    conf.baseDir = tdir
    conf.stewardThreshold = 5

    def reset():
        conf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return conf


def testOnlyAStewardCanAddAnotherSteward(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, poolTxnClientData,
                                         steward1, stewardWallet,
                                         client1, wallet1):
    addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward1")
    with pytest.raises(AssertionError):
        addNewClient(STEWARD, looper, client1, wallet1, "testSteward2")


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper, tconf,
                                                      txnPoolNodeSet,
                                                      tdirWithPoolTxns,
                                                      poolTxnStewardData,
                                                      steward1, stewardWallet):
    addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward3")
    with pytest.raises(AssertionError):
        addNewClient(STEWARD, looper, steward1, stewardWallet, "testSteward4")

