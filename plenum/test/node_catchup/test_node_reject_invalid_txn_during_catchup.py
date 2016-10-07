import types
from base64 import b64encode

from plenum.common.txn import TXN_TYPE
from plenum.common.types import CatchupReq, f, CatchupRep
from plenum.common.log import getlogger
from plenum.test.eventually import eventually
from plenum.test.helper import sendRandomRequests, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality

logger = getlogger()


def testNodeRejectingInvalidTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes replies with incorrect transactions. The newly
    joined node detects that and rejects the transactions and thus blacklists
    the node. Ii thus cannot complete the process till the timeout and then
    requests the missing transactions.
    """
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns

    # So nodes wont tell the clients about the newly joined node so they
    # dont send any request to the newly joined node
    for node in txnPoolNodeSet:
        node.sendPoolInfoToClients = types.MethodType(lambda x, y: None, node)

    def sendIncorrectTxns(self, req, frm):
        ledgerType = getattr(req, f.LEDGER_TYPE.nm)
        if ledgerType == 1:
            logger.info("{} being malicious and sending incorrect transactions"
                        " for catchup request {} from {}".
                        format(self, req, frm))
            start, end = getattr(req, f.SEQ_NO_START.nm), \
                         getattr(req, f.SEQ_NO_END.nm)
            ledger = self.getLedgerForMsg(req)
            txns = ledger.getAllTxn(start, end)
            for seqNo in txns.keys():
                # Since the type of random request is `buy`
                if txns[seqNo].get(TXN_TYPE) == "buy":
                    txns[seqNo][TXN_TYPE] = "randomtype"
            consProof = [b64encode(p).decode() for p in
                     ledger.tree.consistency_proof(end, ledger.size)]
            self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_TYPE.nm), txns,
                                       consProof), to=frm)
        else:
            self.processCatchupReq(req, frm)

    # One of the node does not process catchup request.
    txnPoolNodeSet[0].nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        sendIncorrectTxns, txnPoolNodeSet[0].ledgerManager)

    sendRandomRequests(wallet, client, 10)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=60))
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=45))

    assert newNode.isNodeBlacklisted(txnPoolNodeSet[0].name)
