from plenum.test.helper import stopNodes
from plenum.test.test_node import checkProtocolInstanceSetup, getRequiredInstances
from plenum.test import waits


def test_view_change_wo_pri(nodeSet, looper):
    stopNodes(nodeSet, looper)
    looper.removeProdable(nodeSet["Alpha"])

    #run all but first
    nn = []
    for n in nodeSet:
        if n.name != "Alpha":
            n.start(looper.loop)
            nn.append(n)
            looper.runFor(5)

    looper.runFor(10)

    checkProtocolInstanceSetup(looper=looper, nodes=nn, retryWait=1,
                               customTimeout=waits.expectedPoolElectionTimeout(len(nodeSet)),
                               numInstances=getRequiredInstances(len(nodeSet)))
