import types

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.types import f
from plenum.common.messages.node_messages import CatchupReq
from plenum.common.util import randomString
from plenum.test.delayers import cqDelay
from plenum.test.helper import sendRandomRequests, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import checkNodeDataForEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import TestNode
from stp_core.loop.eventually import eventually


def testNewNodeCatchupWhileIncomingRequests(looper, txnPoolNodeSet,
                                            testNodeClass, tdir,
                                            tdirWithClientPoolTxns, tconf,
                                            steward1, stewardWallet,
                                            allPluginsPath):
    """
    A new node joins while transactions are happening, its catchup requests
    include till where it has to catchup, which would be less than the other
    node's ledger size. In the meantime, the new node will stash all requests
    """

    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)

    def chkAfterCall(self, req, frm):
        r = self.processCatchupReq(req, frm)
        typ = getattr(req, f.LEDGER_ID.nm)
        if typ == DOMAIN_LEDGER_ID:
            ledger = self.getLedgerForMsg(req)
            assert req.catchupTill <= ledger.size
        return r

    for node in txnPoolNodeSet:
        node.nodeMsgRouter.routes[CatchupReq] = \
            types.MethodType(chkAfterCall, node.ledgerManager)
        node.nodeIbStasher.delay(cqDelay(3))

    print('Sending 5 requests')
    sendRandomRequests(stewardWallet, steward1, 5)
    looper.runFor(1)
    newStewardName = randomString()
    newNodeName = "Epsilon"
    newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
        looper, steward1, stewardWallet, newStewardName, newNodeName,
        tdir, tdirWithClientPoolTxns, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True)
    txnPoolNodeSet.append(newNode)
    looper.runFor(2)
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)
    # TODO select or create a timeout for this case in 'waits'
    looper.run(eventually(checkNodeDataForEquality, newNode,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=80))
    assert newNode.spylog.count(TestNode.processStashedOrderedReqs) > 0
