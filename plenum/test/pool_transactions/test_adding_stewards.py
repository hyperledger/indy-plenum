import pytest

from plenum.common.signer_simple import SimpleSigner
from plenum.common.txn import TXN_TYPE, TARGET_NYM, ROLE, STEWARD, NYM, \
    ALIAS
from plenum.test.eventually import eventually
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet


@pytest.fixture(scope="module")
def tconf(conf, tdir, request):
    oldThreshold = conf.stewardThreshold
    conf.baseDir = tdir
    conf.stewardThreshold = 1

    def reset():
        conf.stewardThreshold = oldThreshold

    request.addfinalizer(reset)
    return conf


def testOnlyAStewardCanAddAnotherSteward(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, poolTxnClientData):
    return checkStewardAdded(looper, poolTxnClientData, tdirWithPoolTxns)


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(looper, tconf,
                                                      txnPoolNodeSet,
                                                      tdirWithPoolTxns,
                                                      poolTxnStewardData):
    return checkStewardAdded(looper, poolTxnStewardData, tdirWithPoolTxns)


def checkStewardAdded(looper, poolTxnStewardData, tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    sigseed = b'55555555555555555555555555555555'
    newSigner = SimpleSigner(sigseed)
    op = {
        TXN_TYPE: NYM,
        ROLE: STEWARD,
        TARGET_NYM: newSigner.verkey,
        ALIAS: "Robert",
    }
    req = wallet.signOp(op)
    client.submitReqs(req)

    def chk():
        assert client.getReply(*req.key) == (None, "NOT_FOUND")

    looper.run(eventually(chk, retryWait=1, timeout=5))
