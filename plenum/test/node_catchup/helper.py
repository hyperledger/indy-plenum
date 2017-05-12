from stp_zmq.zstack import KITZStack
from typing import Iterable

from stp_core.loop.eventually import eventually
from stp_core.types import HA
from plenum.test.helper import checkLedgerEquality
from plenum.test.test_client import TestClient
from plenum.test.test_node import TestNode
from plenum.test import waits
from plenum.common import util


# TODO: This should just take an arbitrary number of nodes and check for their
#  ledgers to be equal
def checkNodeLedgersForEquality(node: TestNode,
                                *otherNodes: Iterable[TestNode]):
    for n in otherNodes:
        checkLedgerEquality(node.domainLedger, n.domainLedger)
        if n.poolLedger:
            checkLedgerEquality(node.poolLedger, n.poolLedger)


def waitNodeLedgersEquality(looper,
                            referenceNode: TestNode,
                            *otherNodes: Iterable[TestNode],
                            customTimeout = None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    looper.run(eventually(checkNodeLedgersForEquality,
                          referenceNode,
                          *otherNodes,
                          retryWait=1, timeout=timeout))


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())


def checkClientPoolLedgerSameAsNodes(client: TestClient,
                                     *nodes: Iterable[TestNode]):
    for n in nodes:
        checkLedgerEquality(client.ledger, n.poolLedger)


def ensureClientConnectedToNodesAndPoolLedgerSame(looper,
                                                  client: TestClient,
                                                  *nodes:Iterable[TestNode]):
    looper.run(client.ensureConnectedToNodes())
    timeout = waits.expectedPoolGetReadyTimeout(len(nodes))
    looper.run(eventually(checkClientPoolLedgerSameAsNodes,
                          client,
                          *nodes,
                          timeout=timeout))
