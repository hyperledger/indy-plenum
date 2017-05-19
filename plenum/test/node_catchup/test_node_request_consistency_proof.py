import types
from random import randint

import pytest

from plenum.common.ledger import Ledger
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.types import LedgerStatus
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_node import checkNodesConnected
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist


logger = getlogger()


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
    # The new node sends different ledger statuses to every node so it
    # does not get enough similar consistency proofs
    sentSizes = set()

    def sendDLStatus(self, name):
        size = self.primaryStorage.size
        newSize = randint(1, size)
        while newSize in sentSizes:
            newSize = randint(1, size)
        print("new size {}".format(newSize))
        newRootHash = Ledger.hashToStr(
            self.domainLedger.tree.merkle_tree_hash(0, newSize))
        ledgerStatus = LedgerStatus(1, newSize,
                                    newRootHash)

        print("dl status {}".format(ledgerStatus))
        rid = self.nodestack.getRemote(name).uid
        self.send(ledgerStatus, rid)
        sentSizes.add(newSize)

    newNode.sendDomainLedgerStatus = types.MethodType(sendDLStatus, newNode)
    logger.debug(
        'Domain Ledger status sender of {} patched'.format(newNode))

    sendRandomRequests(wallet, client, 10)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    #  wait more than `ConsistencyProofsTimeout`
    # TODO: apply configurable timeout here
    # `ConsistencyProofsTimeout` is set to 60 sec, so need to wait more than
    # 60 sec, hence large timeout. Dont reduce it.
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=75)

    # Other nodes should have received a `ConsProofRequest` and processed it.
    for node in txnPoolNodeSet[:-1]:
        assert node.ledgerManager.spylog.count(
            TestLedgerManager.processConsistencyProofReq.__name__) > 0
