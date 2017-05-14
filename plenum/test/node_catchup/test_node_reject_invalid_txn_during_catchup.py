import types

from plenum.common.ledger import Ledger
from stp_core.common.log import getlogger
from plenum.common.constants import TXN_TYPE, DOMAIN_LEDGER_ID
from plenum.common.types import CatchupReq, f, CatchupRep
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()


txnCount = 10


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
        ledgerId = getattr(req, f.LEDGER_ID.nm)
        if ledgerId == DOMAIN_LEDGER_ID:
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
            consProof = [Ledger.hashToStr(p) for p in
                         ledger.tree.consistency_proof(end, ledger.size)]
            self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_ID.nm), txns,
                                       consProof), to=frm)
        else:
            self.processCatchupReq(req, frm)

    # One of the node sends incorrect txns in catchup reply.
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    badReplica = npr[0]
    badNode = badReplica.node
    badNode.nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        sendIncorrectTxns, badNode.ledgerManager)
    logger.debug(
        'Catchup request processor of {} patched'.format(badNode))

    sendRandomRequests(wallet, client, 10)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Since one of the nodes will send a bad catchup reply, this node will
    # request transactions from another node, hence large timeout.
    # Dont reduce it.
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=45)

    assert newNode.isNodeBlacklisted(badNode.name)
