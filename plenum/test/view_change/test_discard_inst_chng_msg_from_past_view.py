from stp_core.loop.eventually import eventually
from plenum.common.types import InstanceChange
from plenum.server.node import Node
from plenum.test.helper import checkDiscardMsg, checkViewNoForNodes


# noinspection PyIncorrectDocstring
def testDiscardInstChngMsgFrmPastView(nodeSet, looper, ensureView):
    """
    Once a view change is done, any further INSTANCE_CHANGE messages for that
    view must be discarded by the node.
    """

    curViewNo = ensureView

    # Send an instance change for an old instance message to all nodes
    icMsg = InstanceChange(curViewNo - 1)
    nodeSet.Alpha.send(icMsg)

    # ensure every node but Alpha discards the invalid instance change request
    looper.run(eventually(checkDiscardMsg, nodeSet, icMsg,
                          'less than its view no', nodeSet.Alpha, timeout=5))

    # Check that that message is discarded.
    looper.run(eventually(checkViewNoForNodes, nodeSet, timeout=3))


# noinspection PyIncorrectDocstring
def testDoNotSendInstChngMsgIfMasterDoesntSeePerformanceProblem(
        nodeSet, looper, ensureView):
    """
    A node that received an INSTANCE_CHANGE message must not send an
    INSTANCE_CHANGE message if it doesn't observe too much difference in
    performance between its replicas.
    """

    curViewNo = ensureView

    # Count sent instance changes of all nodes
    sentInstChanges = {}
    instChngMethodName = Node.sendInstanceChange.__name__
    for n in nodeSet:
        sentInstChanges[n.name] = n.spylog.count(instChngMethodName)

    # Send an instance change message to all nodes
    icMsg = InstanceChange(curViewNo)
    nodeSet.Alpha.send(icMsg)

    # Check that that message is discarded.
    looper.run(eventually(checkViewNoForNodes, nodeSet, timeout=3))
    # No node should have sent a view change and thus must not have called
    # `sendInstanceChange`
    for n in nodeSet:
        assert n.spylog.count(instChngMethodName) == \
                   sentInstChanges.get(n.name, 0)
