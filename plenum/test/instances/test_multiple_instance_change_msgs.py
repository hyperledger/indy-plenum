import pytest

from plenum.common.eventually import eventually
from plenum.common.exceptions import SuspiciousNode
from plenum.common.types import InstanceChange
from plenum.server.node import Node
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.spy_helpers import getAllArgs

nodeCount = 7


@pytest.mark.xfail(reason="Not yet implemented")
def testMultipleInstanceChangeMsgsMarkNodeAsSuspicious(looper, nodeSet, up):
    maliciousNode = nodeSet.Alpha
    for i in range(0, 5):
        maliciousNode.send(InstanceChange(i))

    def chk(instId):
        for node in nodeSet:
            if node.name != maliciousNode.name:
                args = getAllArgs(node, Node.processInstanceChange)
                assert len(args) == 5
                for arg in args:
                    assert arg['frm'] == maliciousNode.name

    for i in range(0, 5):
        looper.run(eventually(chk, i, retryWait=1, timeout=20))

    def g():
        for node in nodeSet:
            if node.name != maliciousNode.name:
                frm, reason, code = getAllArgs(node, Node.reportSuspiciousNode)
                assert frm == maliciousNode.name
                assert isinstance(reason, SuspiciousNode)
                assert len(getNodeSuspicions(node,
                                             Suspicions.FREQUENT_INST_CHNG.code)) == 13

    looper.run(eventually(g, retryWait=1, timeout=20))
