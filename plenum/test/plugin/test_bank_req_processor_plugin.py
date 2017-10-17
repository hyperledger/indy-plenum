import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.constants import TARGET_NYM, TXN_TYPE, DATA
from plenum.test import waits
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    setupClients, checkReqNack
from plenum.test.plugin.bank_req_processor.plugin_bank_req_processor import \
    BALANCE, ALL_TXNS
from plenum.test.plugin.bank_req_validation.plugin_bank_req_validation import \
    CREDIT, AMOUNT, GET_BAL, GET_ALL_TXNS
from plenum.test.plugin.conftest import BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE, \
    BANK_REQ_PROCESSOR_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath, App
from plenum.test.test_node import TestNodeSet

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


@pytest.fixture(scope="module")
def apps(looper, nodeSet, tdir):
    cs, ws = setupClients(3, looper, nodeSet, tmpdir=tdir)
    return [AccountApp(ws[k], cs[k], looper) for k in cs.keys()]


class AccountApp(App):
    def sendMoney(self, to: str, amount: int, nodes, expected: bool = True):
        req = self.submit({
            TXN_TYPE: CREDIT,
            TARGET_NYM: to,
            DATA: {
                AMOUNT: amount
            }})
        if expected:
            waitForSufficientRepliesForRequests(self.looper, self.client,
                                                requests=[req])
        else:
            timeout = waits.expectedReqNAckQuorumTime()
            for node in nodes:
                self.looper.run(eventually(checkReqNack, self.client, node,
                                           req.identifier, req.reqId, None,
                                           retryWait=1, timeout=timeout))
        return req

    def getBalance(self) -> int:
        req = self.submit({
            TXN_TYPE: GET_BAL,
            TARGET_NYM: self.wallet.defaultId
        })
        waitForSufficientRepliesForRequests(self.looper, self.client,
                                            requests=[req])

        return self.client.hasConsensus(*req.key)[BALANCE]

    def checkTxns(self):
        req = self.submit({
            TXN_TYPE: GET_ALL_TXNS,
            TARGET_NYM: self.wallet.defaultId
        })
        waitForSufficientRepliesForRequests(self.looper, self.client,
                                            requests=[req])

        return req


def testBankTransactions(nodeSet, up, looper, apps):
    jason, john, les = apps

    bal1Jason = jason.getBalance()
    bal1John = john.getBalance()

    logger.display("Balance for Jason Law is {}".format(bal1Jason))
    logger.display("Balance for John Best is {}".format(bal1John))
    logger.display("Jason Law transfers 100USD to John Best")
    req = jason.sendMoney(john.wallet.defaultId, 100, nodeSet)
    assert jason.client.hasConsensus(*req.key)
    bal2Jason = jason.getBalance()
    bal2John = john.getBalance()
    assert bal1Jason - bal2Jason == 100
    assert bal2John - bal1John == 100
    logger.display("Balance for Jason Law is {}".format(bal2Jason))
    logger.display("Balance for John Best is {}".format(bal2John))
    jason.sendMoney(john.wallet.defaultId, 100, nodeSet)
    txnReqJl = jason.checkTxns()
    txnReqJb = john.checkTxns()
    txnJason = jason.client.hasConsensus(*txnReqJl.key)[ALL_TXNS]
    txnJohn = john.client.hasConsensus(*txnReqJb.key)[ALL_TXNS]
    assert txnJason == txnJohn
