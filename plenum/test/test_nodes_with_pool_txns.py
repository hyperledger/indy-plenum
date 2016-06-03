from raet.nacling import Privateer

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.txn import TXN_TYPE, NEW_CLIENT, TARGET_NYM, ORIGIN, DATA
from plenum.common.util import getConfig, getlogger
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, \
    sendReqsToNodesAndVerifySuffReplies, checkSufficientRepliesRecvd


logger = getlogger()


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(txnPoolNodeSet, tdirWithPoolTxns,
                               poolTxnClientData, txnPoolCliNodeReg):

    with Looper(debug=True) as looper:
        name, pkseed, sigseed = poolTxnClientData
        signer = SimpleSigner(seed=sigseed)
        client = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                            signer=signer, basedirpath=tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        sendReqsToNodesAndVerifySuffReplies(looper, client, 1)


def testAddNewClient(txnPoolNodeSet, tdirWithPoolTxns,
                     poolTxnStewardData, txnPoolCliNodeReg):
    with Looper(debug=True) as looper:
        name, pkseed, sigseed = poolTxnStewardData
        stewardSigner = SimpleSigner(seed=sigseed)
        client = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                            signer=stewardSigner, basedirpath=tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        sigseed = b'55555555555555555555555555555555'
        pkseed = b'66666666666666666666666666666666'
        newSigner = SimpleSigner(sigseed)
        priver = Privateer(pkseed)
        req, = client.submit({
            TXN_TYPE: NEW_CLIENT,
            ORIGIN: client.defaultIdentifier,
            TARGET_NYM: newSigner.verstr,
            DATA: {
                "pubkey": priver.pubhex.decode(),
                "alias": "Robert"
            }
        })

        looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                              req.reqId, 1,
                              retryWait=1, timeout=5))

        def chk():
            for node in txnPoolNodeSet:
                assert newSigner.verstr in node.clientAuthNr.clients

        looper.run(eventually(chk, retryWait=1, timeout=5))
