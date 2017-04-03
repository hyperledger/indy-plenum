import types

from plenum.common.types import CatchupReq, f
from plenum.common.util import randomString
from plenum.test.delayers import crDelay
from plenum.test.helper import sendRandomRequests, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected, TestNode
from stp_core.loop.eventually import eventually


def testNewNodeCatchupWhileIncomingRequests(looper, txnPoolNodeSet,
                                            tdirWithPoolTxns, tconf,
                                            steward1, stewardWallet,
                                            allPluginsPath):
    """
    A new node joins while transactions are happening, its catchup requests
    include till where it has to catchup, which would be less than the other
    node's ledger size. In the meantime, the new node will stash all requests
    """

    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5, 1)

    def chkAfterCall(self, req, frm):
        r = self.processCatchupReq(req, frm)
        typ = getattr(req, f.LEDGER_ID.nm)
        if typ == 1:
            ledger = self.getLedgerForMsg(req)
            assert req.catchupTill < ledger.size
        return r

    for node in txnPoolNodeSet:
        node.nodeMsgRouter.routes[CatchupReq] = types.MethodType(
            chkAfterCall, node.ledgerManager)
        node.nodeIbStasher.delay(crDelay(3))

    print('Sending 10 requests')
    sendRandomRequests(stewardWallet, steward1, 5)
    looper.runFor(1)
    newStewardName = randomString()
    newNodeName = "Epsilon"
    newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
        looper, steward1, stewardWallet, newStewardName, newNodeName,
        tdirWithPoolTxns, tconf, allPluginsPath=allPluginsPath, autoStart=True)
    txnPoolNodeSet.append(newNode)
    looper.runFor(2)
    sendRandomRequests(stewardWallet, steward1, 5)
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=80))
    assert newNode.spylog.count(TestNode.processStashedOrderedReqs) > 0
