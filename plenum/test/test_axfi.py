import pytest

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.txn import AMOUNT, TARGET_NYM, TXN_TYPE, CREDIT, DATA, \
    GET_BAL, GET_ALL_TXNS, BALANCE, ALL_TXNS
from plenum.common.util import getlogger
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, checkSufficientRepliesRecvd, \
    checkReqNack


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def sendMoney(looper, frm: TestClient, to: TestClient, amount: int, nodes,
              expected: bool=True):
    req, = frm.submit({
            TXN_TYPE: CREDIT,
            TARGET_NYM: to.defaultIdentifier,
            DATA: {
                AMOUNT: amount
            }})
    if expected:
        looper.run(eventually(checkSufficientRepliesRecvd, frm.inBox,
                              req.reqId, 1,
                              retryWait=1, timeout=5))
    else:
        for node in nodes:
            looper.run(eventually(checkReqNack, frm, node,
                                  req.reqId, None,
                                  retryWait=1, timeout=5))
    return req


def checkBalance(looper, client: TestClient):
    req, = client.submit({
        TXN_TYPE: GET_BAL,
        TARGET_NYM: client.defaultIdentifier
    })

    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=5))

    return req


def checkTxns(looper, client: TestClient):
    req, = client.submit({
        TXN_TYPE: GET_ALL_TXNS,
        TARGET_NYM: client.defaultIdentifier
    })

    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=5))

    return req


@pytest.fixture(scope="module")
def connectedClients(txnPoolNodeSet, looper, txnPoolCliNodeReg, tdirWithPoolTxns,
                  poolTxnClientNames, poolTxnData):
    clients = {}
    for name in poolTxnClientNames:
        seeds = poolTxnData["seeds"][name]
        pkseed, sigseed = tuple(s.encode() for s in seeds)
        signer = SimpleSigner(seed=sigseed)
        clients[name] = TestClient(name=name, nodeReg=txnPoolCliNodeReg,
                                   ha=genHa(),
                                   signer=signer, basedirpath=tdirWithPoolTxns)
        looper.add(clients[name])
        looper.run(clients[name].ensureConnectedToNodes())
    return clients


def testSendMoney(txnPoolNodeSet, looper, txnPoolCliNodeReg, tdirWithPoolTxns,
                  poolTxnClientNames, poolTxnData, connectedClients):
    clients = connectedClients
    jason, john, les = poolTxnClientNames[1:]
    balReqJl = checkBalance(looper, clients[jason])
    balReqJb = checkBalance(looper, clients[john])
    bal1Jason = clients[jason].hasConsensus(balReqJl.reqId)[BALANCE]
    bal1John = clients[john].hasConsensus(balReqJb.reqId)[BALANCE]
    logger.display("Balance for Jason Law is {}".format(bal1Jason))
    logger.display("Balance for John Best is {}".format(bal1John))
    logger.display("Jason Law transfers 100USD to John Best")
    req = sendMoney(looper, clients[jason], clients[john], 100, txnPoolNodeSet)
    assert clients[jason].hasConsensus(req.reqId)
    balReqJl = checkBalance(looper, clients[jason])
    balReqJb = checkBalance(looper, clients[john])
    bal2Jason = clients[jason].hasConsensus(balReqJl.reqId)[BALANCE]
    bal2John = clients[john].hasConsensus(balReqJb.reqId)[BALANCE]
    assert bal1Jason - bal2Jason == 100
    assert bal2John - bal1John == 100
    logger.display("Balance for Jason Law is {}".format(bal2Jason))
    logger.display("Balance for John Best is {}".format(bal2John))
    req = sendMoney(looper, clients[jason], clients[john], 100, txnPoolNodeSet)
    txnReqJl = checkTxns(looper, clients[jason])
    txnReqJb = checkTxns(looper, clients[john])
    txnJason = clients[jason].hasConsensus(txnReqJl.reqId)[ALL_TXNS]
    txnJohn = clients[john].hasConsensus(txnReqJb.reqId)[ALL_TXNS]
    assert txnJason == txnJohn
