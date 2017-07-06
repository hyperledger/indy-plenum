from plenum.common.messages.node_messages import *
from plenum.test.helper import send_signed_requests, getRepliesFromClientInbox
from stp_core.loop.eventually import eventually
from random import randint


def testSendGetTxnReqForExistsSeqNo(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        DATA: 1
    }
    req = stewardWallet.signOp(op)
    send_signed_requests(steward1, [req])

    def chk():
        receivedReplies = getRepliesFromClientInbox(steward1.inBox,
                                                    req.reqId)
        assert len(receivedReplies) == len(steward1.nodeReg)
        assert receivedReplies[0][f.RESULT.nm][DATA]

    looper.run(eventually(chk))


def testSendGetTxnReqForNotExistsSeqNo(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        DATA: randint(100, 1000)
    }
    req = stewardWallet.signOp(op)
    send_signed_requests(steward1, [req])

    def chk():
        receivedReplies = getRepliesFromClientInbox(steward1.inBox,
                                                    req.reqId)
        assert len(receivedReplies) == 4
        assert not receivedReplies[0][f.RESULT.nm][DATA]

    looper.run(eventually(chk))
