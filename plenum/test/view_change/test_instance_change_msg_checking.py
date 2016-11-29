import pytest
import types
from plenum.common.types import InstanceChange
from plenum.test.eventually import eventually
from plenum.test.test_node import TestNode

DISCARD_REASON = 'viewNo has incorrect type'

whitelist = [DISCARD_REASON,]


def testInstanceChangeMsgTypeChecking(nodeSet, looper, up):
    nodeA = nodeSet.Alpha
    nodeB = nodeSet.Beta
    
    ridBeta = nodeA.nodestack.getRemote(nodeB.name).uid
    badViewNo = "BAD"
    nodeA.send(InstanceChange(badViewNo), ridBeta)
    looper.runFor(0.2)
    params = nodeB.spylog.getLastParams(TestNode.discard)

    def chk():
        assert isinstance(params['msg'], InstanceChange)
        assert DISCARD_REASON in params['reason']

    looper.run(eventually(chk, timeout=5))
