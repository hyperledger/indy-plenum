import pytest

from plenum.common.txn import TARGET_NYM, TXN_TYPE, DATA
from plenum.common.util import getlogger
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, checkSufficientRepliesRecvd, \
    checkReqNack, TestNodeSet, setupClients
from plenum.test.plugin.conftest import BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE, \
    BANK_REQ_PROCESSOR_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath
from plenum.test.plugin.bank_req_validation.plugin_bank_req_validation import \
    CREDIT, AMOUNT, GET_BAL, GET_ALL_TXNS
from plenum.test.plugin.bank_req_processor.plugin_bank_req_processor import \
    BALANCE, ALL_TXNS

logger = getlogger()


@pytest.fixture(scope="module")
def pluginVerPath():
    return getPluginPath(BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def pluginPrcPath():
    return getPluginPath(BANK_REQ_PROCESSOR_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def allPluginPaths(pluginVerPath, pluginPrcPath):
    return [pluginVerPath, pluginPrcPath]


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg, allPluginPaths):
    """
    Overrides the fixture from conftest.py
    """
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     pluginPaths=allPluginPaths) as ns:

        for n in ns:  # type: Node
            assert n.reqProcessors is not None
            assert len(n.reqProcessors) == 1
            reqProcessor, = n.reqProcessors
            assert reqProcessor.count == 0
        yield ns


@pytest.fixture(scope="module")
def clients(looper, nodeSet, tdir):
    return setupClients(3, looper, nodeSet, tmpdir=tdir)


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
                          1, retryWait=1, timeout=10))

    return req


def checkTxns(looper, client: TestClient):
    req, = client.submit({
        TXN_TYPE: GET_ALL_TXNS,
        TARGET_NYM: client.defaultIdentifier
    })

    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=5))

    return req


def testBankTransactions(nodeSet, up, looper, clients):
    jason, john, les = tuple(clients.keys())
    balReqJl = checkBalance(looper, clients[jason])
    balReqJb = checkBalance(looper, clients[john])
    bal1Jason = clients[jason].hasConsensus(balReqJl.reqId)[BALANCE]
    bal1John = clients[john].hasConsensus(balReqJb.reqId)[BALANCE]
    logger.display("Balance for Jason Law is {}".format(bal1Jason))
    logger.display("Balance for John Best is {}".format(bal1John))
    logger.display("Jason Law transfers 100USD to John Best")
    req = sendMoney(looper, clients[jason], clients[john], 100, nodeSet)
    assert clients[jason].hasConsensus(req.reqId)
    balReqJl = checkBalance(looper, clients[jason])
    balReqJb = checkBalance(looper, clients[john])
    bal2Jason = clients[jason].hasConsensus(balReqJl.reqId)[BALANCE]
    bal2John = clients[john].hasConsensus(balReqJb.reqId)[BALANCE]
    assert bal1Jason - bal2Jason == 100
    assert bal2John - bal1John == 100
    logger.display("Balance for Jason Law is {}".format(bal2Jason))
    logger.display("Balance for John Best is {}".format(bal2John))
    req = sendMoney(looper, clients[jason], clients[john], 100, nodeSet)
    txnReqJl = checkTxns(looper, clients[jason])
    txnReqJb = checkTxns(looper, clients[john])
    txnJason = clients[jason].hasConsensus(txnReqJl.reqId)[ALL_TXNS]
    txnJohn = clients[john].hasConsensus(txnReqJb.reqId)[ALL_TXNS]
    assert txnJason == txnJohn
