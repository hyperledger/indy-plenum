import re

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import InstanceChange
from plenum.test import waits
from plenum.test.test_node import TestNode

DISCARD_REASON = "validation error \[InstanceChange\]: expected types"

whitelist = [DISCARD_REASON, ]


def testInstanceChangeMsgTypeChecking(txnPoolNodeSet, looper):
    nodeA = txnPoolNodeSet[0]
    nodeB = txnPoolNodeSet[1]

    ridBeta = nodeA.nodestack.getRemote(nodeB.name).uid

    def createInstanceChangeMessage():
        # Creating a message this way to exclude
        # client-side validation of viewNo

        goodViewNo = 1
        badViewNo = "BAD"
        icMsg = txnPoolNodeSet[0].view_changer._create_instance_change_msg(goodViewNo, 0)
        icMsg._fields["viewNo"] = badViewNo
        return icMsg

    icMsg = createInstanceChangeMessage()
    nodeA.send(icMsg, ridBeta)
    looper.runFor(0.2)

    params = nodeB.spylog.getLastParams(TestNode.discard)

    def chk():
        assert re.search(DISCARD_REASON, str(params['reason']))

    timeout = waits.expectedNodeToNodeMessageDeliveryTime()
    looper.run(eventually(chk, timeout=timeout))
