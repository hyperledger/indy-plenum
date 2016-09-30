import asyncio
import base64
import types
from random import randint

from plenum.common.types import LedgerStatus
from plenum.common.util import runWithLoop
from plenum.test.eventually import eventually
from plenum.test.helper import lsDelay, ppgDelay, sendRandomRequests, \
    checkNodesConnected, cDelay, pDelay, ppDelay, TestLedgerManager
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality


def testNodeRequestingConsProof(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    """
    All of the 4 old nodes delay the processing of LEDGER_STATUS from the newly
    joined node while they are processing requests which results in them sending
    consistency proofs which are not same so that the newly joined node cannot
    conclude about the state of transactions in the system. So the new node
    requests consistency proof for a particular range from all nodes.
    """
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns

    # So nodes wont tell the clients about the newly joined node so they
    # dont send any request to the newly joined node
    for node in txnPoolNodeSet:
        node.sendPoolInfoToClients = types.MethodType(lambda x, y: None, node)

    txnPoolNodeSet.append(newNode)
    # The new node does not sends different ledger statuses to every node so it
    # does not get enough similar consistency proofs
    sentSizes = set()

    def sendDLStatus(self, name):
        size = self.primaryStorage.size
        newSize = randint(1, size)
        while newSize in sentSizes:
            newSize = randint(1, size)
        print("new size {}".format(newSize))
        newRootHash = base64.b64encode(
            self.domainLedger.tree.merkle_tree_hash(0, newSize)).decode()
        ledgerStatus = LedgerStatus(1, newSize,
                                    newRootHash)

        print("dl status {}".format(ledgerStatus))
        rid = self.nodestack.getRemote(name).uid
        self.send(ledgerStatus, rid)
        sentSizes.add(newSize)

    newNode.sendDomainLedgerStatus = types.MethodType(sendDLStatus, newNode)

    print("sending 10 requests")
    sendRandomRequests(wallet, client, 10)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=60))

    # `ConsistencyProofsTimeout` is set to 60 sec, so need to wait more than
    # 60 sec.
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=75))
    for node in txnPoolNodeSet[:-1]:
        assert node.ledgerManager.spylog.count(
            TestLedgerManager.processConsistencyProofReq.__name__) > 0
