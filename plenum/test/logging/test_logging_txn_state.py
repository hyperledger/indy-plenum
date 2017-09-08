import functools

from stp_core.loop.eventually import eventually

from plenum.common.constants import STEWARD
from plenum.common.util import getMaxFailures

from plenum.test.pool_transactions.conftest import looper, stewardAndWallet1, \
    steward1, stewardWallet, client1, clientAndWallet1, client1Connected
from plenum.test.pool_transactions.helper import sendAddNewClient
from plenum.test import waits
from plenum.test.helper import checkSufficientRepliesReceived, \
    ensureRejectsRecvd

ERORR_MSG = "something went wrong"
whitelist = [ERORR_MSG]


def testLoggingTxnStateForValidRequest(
        looper, steward1, stewardWallet, logsearch):
    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsOrdered, _ = logsearch(
        levels=['INFO'], files=['replica.py'],
        funcs=['order_3pc_key'], msgs=['ordered batch request']
    )

    logsCommited, _ = logsearch(
        levels=['INFO'], files=['node.py'],
        funcs=['executeBatch'], msgs=['committed batch request']
    )

    req, wallet = sendAddNewClient(None, "name", steward1, stewardWallet)
    nNodes = len(steward1.inBox)

    timeout = waits.expectedTransactionExecutionTime(nNodes)
    looper.run(
        eventually(checkSufficientRepliesReceived, steward1.inBox,
                   req.reqId, getMaxFailures(nNodes),
                   retryWait=1, timeout=timeout))

    reqId = str(req.reqId)
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsOrdered)
    assert any(reqId in record.getMessage() for record in logsCommited)


def testLoggingTxnStateForInvalidRequest(
        looper, txnPoolNodeSet, clientAndWallet1, client1Connected, logsearch):

    client, clientWallet = clientAndWallet1

    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsReject, _ = logsearch(
        levels=['WARNING'], files=['replica.py'],
        funcs=['processReqDuringBatch'],
        msgs=['encountered exception.*while processing.*will reject']
    )

    req, wallet = sendAddNewClient(STEWARD, "name", client, clientWallet)

    ensureRejectsRecvd(
        looper, txnPoolNodeSet, client,
        reason="Only Steward is allowed to do these transactions",
        timeout=waits.expectedReqRejectQuorumTime()
    )

    reqId = str(req.reqId)
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsReject)


def testLoggingTxnStateWhenCommitFails(
        looper, txnPoolNodeSet, steward1, stewardWallet, logsearch):
    logsPropagate, _ = logsearch(
        levels=['INFO'], files=['propagator.py'],
        funcs=['propagate'], msgs=['propagating.*request.*from client']
    )

    logsOrdered, _ = logsearch(
        levels=['INFO'], files=['replica.py'],
        funcs=['order_3pc_key'], msgs=['ordered batch request']
    )

    logsCommitFail, _ = logsearch(
        levels=['WARNING'], files=['node.py'],
        funcs=['executeBatch'], msgs=['commit failed for batch request']
    )

    req, wallet = sendAddNewClient(None, "name", steward1, stewardWallet)

    class SomeError(Exception):
        pass

    def commitPatched(*args, **kwargs):
        raise SomeError(ERORR_MSG)

    def executeBatchPatched(executeBatchOrig, *args, **kwargs):
        try:
            executeBatchOrig(*args, **kwargs)
        except SomeError:
            pass

    for node in txnPoolNodeSet:
        node.reqHandler.commit = commitPatched
        node.executeBatch = functools.partial(
            executeBatchPatched, node.executeBatch)

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    try:
        looper.runFor(timeout)
    except SomeError:
        pass

    reqId = str(req.reqId)
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsOrdered)
    assert any(reqId in record.getMessage() for record in logsCommitFail)
    assert any(ERORR_MSG in record.getMessage() for record in logsCommitFail)
