from typing import Iterable

from plenum.common.types import HA
from plenum.test.eventually import eventually
from plenum.test.helper import TestNode, TestClient, assertLength, \
    assertEquality


# TODO: This should just take an arbitrary number of nodes and check for their
#  ledgers to be equal
def checkNodeLedgersForEquality(node: TestNode,
                                *otherNodes: Iterable[TestNode]):
    for n in otherNodes:
        assertLength(node.domainLedger, n.domainLedger.size)
        assertLength(node.poolLedger, n.poolLedger.size)
        assertEquality(node.domainLedger.root_hash, n.domainLedger.root_hash)
        assertEquality(node.poolLedger.root_hash, n.poolLedger.root_hash)


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())


def checkClientPoolLedgerSameAsNodes(client: TestClient,
                                     *nodes: Iterable[TestNode]):
    for n in nodes:
        assertLength(client.ledger, n.poolLedger.size)
        assertEquality(client.ledger.root_hash, n.poolLedger.root_hash)
        # assert client.ledger.size == n.poolLedger.size
        # assert client.ledger.root_hash == n.poolLedger.root_hash


def ensureClientConnectedToNodesAndPoolLedgerSame(looper, client: TestClient,
                                                  *nodes:Iterable[TestNode]):
    looper.run(eventually(checkClientPoolLedgerSameAsNodes, client,
                          *nodes, retryWait=1,
                          timeout=3*len(nodes)))
    looper.run(client.ensureConnectedToNodes())
