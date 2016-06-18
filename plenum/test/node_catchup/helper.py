from typing import Iterable

from plenum.common.types import HA
from plenum.test.helper import TestNode, \
    TestClient


def checkNodeLedgersForEqualSize(node: TestNode, *otherNodes: Iterable[TestNode]):
    for n in otherNodes:
        assert node.primaryStorage.size == n.primaryStorage.size
        assert node.poolManager.poolTxnStore.size == \
               n.poolManager.poolTxnStore.size


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())
