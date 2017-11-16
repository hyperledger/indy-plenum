import functools

from stp_core.loop.eventually import eventually

from plenum.common.constants import STEWARD

from plenum.test.pool_transactions.conftest import looper, stewardAndWallet1, \
    steward1, stewardWallet, client1, clientAndWallet1, client1Connected
from plenum.test.pool_transactions.helper import sendAddNewClient
from plenum.test import waits
from plenum.test.helper import ensureRejectsRecvd, \
    check_sufficient_replies_received

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

    looper.run(eventually(check_sufficient_replies_received,
                          steward1, req.identifier, req.reqId,
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

    def commitPatched(node, commitOrig, *args, **kwargs):
        node.reqHandler.commit = commitOrig
        raise SomeError(ERORR_MSG)

    excCounter = 0

    def executeBatchPatched(node, executeBatchOrig, *args, **kwargs):
        nonlocal excCounter
        try:
            executeBatchOrig(*args, **kwargs)
        except SomeError:
            excCounter += 1
            node.executeBatch = executeBatchOrig
            pass

    def checkSufficientExceptionsHappend():
        assert excCounter == len(txnPoolNodeSet)
        return

    for node in txnPoolNodeSet:
        node.reqHandler.commit = functools.partial(
            commitPatched, node, node.reqHandler.commit
        )
        node.executeBatch = functools.partial(
            executeBatchPatched, node, node.executeBatch
        )

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(
        eventually(checkSufficientExceptionsHappend,
                   retryWait=1, timeout=timeout))

    reqId = str(req.reqId)
    assert any(reqId in record.getMessage() for record in logsPropagate)
    assert any(reqId in record.getMessage() for record in logsOrdered)
    assert any(reqId in record.getMessage() for record in logsCommitFail)
    assert any(ERORR_MSG in record.getMessage() for record in logsCommitFail)
