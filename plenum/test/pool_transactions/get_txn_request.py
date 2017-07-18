from plenum.common.messages.node_messages import *
from random import randint
from plenum.test.pool_transactions.helper import sendAddNewClient
from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.helper import checkSufficientRepliesReceived
from plenum.common.util import getMaxFailures
import json
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId

c_delay = 10
fValue = getMaxFailures(4)


def testSendGetTxnReqForExistsSeqNo(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        DATA: 1
    }
    req = Request(identifier=stewardWallet.defaultId, operation=op, reqId=getTimeBasedId())
    steward1.submitReqs(req)

    timeout = waits.expectedTransactionExecutionTime(len(steward1.inBox)) + c_delay
    get_txn_response = looper.run(
        eventually(checkSufficientRepliesReceived, steward1.inBox,
                   req.reqId, fValue,
                   retryWait=1, timeout=timeout))

    assert get_txn_response[DATA]


def testSendGetTxnReqForNotExistsSeqNo(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        DATA: randint(100, 1000)
    }
    req = Request(identifier=stewardWallet.defaultId, operation=op, reqId=getTimeBasedId())
    steward1.submitReqs(req)

    timeout = waits.expectedTransactionExecutionTime(len(steward1.inBox)) + c_delay
    get_txn_response = looper.run(
        eventually(checkSufficientRepliesReceived, steward1.inBox,
                   req.reqId, fValue,
                   retryWait=1, timeout=timeout))

    assert not get_txn_response[DATA]


def testSendGetTxnReqSameAsExpected(looper, steward1, stewardWallet):
    req, wallet = sendAddNewClient(STEWARD, "name", steward1, stewardWallet)

    timeout = waits.expectedTransactionExecutionTime(len(steward1.inBox)) + c_delay
    nym_response = looper.run(
        eventually(checkSufficientRepliesReceived, steward1.inBox,
                   req.reqId, fValue,
                   retryWait=1, timeout=timeout))

    op = {
        TXN_TYPE: GET_TXN,
        DATA: nym_response['seqNo']
    }
    req = Request(identifier=stewardWallet.defaultId, operation=op, reqId=getTimeBasedId())
    steward1.submitReqs(req)

    get_txn_response = looper.run(
        eventually(checkSufficientRepliesReceived, steward1.inBox,
                   req.reqId, fValue,
                   retryWait=1, timeout=timeout))
    get_txn_response = json.loads(get_txn_response[DATA])

    del nym_response['txnTime']
    del get_txn_response['txnTime']

    assert nym_response == get_txn_response
