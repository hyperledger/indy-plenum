from stp_core.loop.eventually import eventually
from plenum.common.types import InstanceChange
from plenum.test import waits
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

    timeout = waits.expectedNodeToNodeMessageDeliveryTime()
    looper.run(eventually(chk, timeout=timeout))
