import pytest

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.txn import TXN_TYPE, TARGET_NYM, ROLE, STEWARD, NYM, \
    ALIAS
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient
from plenum.common.port_dispenser import genHa
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


def testOnlyAStewardCanAddAnotherSteward(txnPoolNodeSet,
                                         tdirWithPoolTxns, poolTxnClientData):
    return checkStewardAdded(poolTxnClientData, tdirWithPoolTxns)


def testStewardsCanBeAddedOnlyTillAThresholdIsReached(
        tconf, txnPoolNodeSet, tdirWithPoolTxns, poolTxnStewardData):
    return checkStewardAdded(poolTxnStewardData, tdirWithPoolTxns)


def checkStewardAdded(poolTxnStewardData, tdirWithPoolTxns):
    with Looper(debug=True) as looper:
        client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                                  tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        sigseed = b'55555555555555555555555555555555'
        newSigner = SimpleSigner(sigseed)
        op = {
            TXN_TYPE: NYM,
            ROLE: STEWARD,
            TARGET_NYM: newSigner.verstr,
            ALIAS: "Robert",
        }
        req = wallet.signOp(op)
        client.submitReqs(req)

        def chk():
            assert client.getReply(req.reqId) == (None, "NOT_FOUND")

        looper.run(eventually(chk, retryWait=1, timeout=5))
