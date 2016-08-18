from typing import Iterable

from plenum.common.types import HA
from plenum.test.helper import TestNode, TestClient


def checkNodeLedgersForEquality(node: TestNode, *otherNodes: Iterable[TestNode]):
    for n in otherNodes:
        assert node.primaryStorage.size == n.primaryStorage.size
        assert node.poolLedger.size == n.poolLedger.size
        assert node.domainLedger.root_hash == n.domainLedger.root_hash
        assert node.poolLedger.root_hash == n.poolLedger.root_hash


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())
