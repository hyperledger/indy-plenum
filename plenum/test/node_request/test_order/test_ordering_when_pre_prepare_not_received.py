import types

from stp_core.loop.eventually import eventually
from plenum.test.delayers import ppDelay
from plenum.test.helper import sendRandomRequest
from plenum.test.test_node import getNonPrimaryReplicas


def testOrderingWhenPrePrepareNotReceived(looper, nodeSet, up, client1,
                                          wallet1):
    """
    Send commits and prepares but delay pre-prepare such that enough prepares
    and commits are received, now the request should not be ordered until
    pre-prepare is received and ordering should just happen once,
    """
    nonPrimReps = getNonPrimaryReplicas(nodeSet, 0)
    slowRep = nonPrimReps[0]
    slowNode = slowRep.node
    slowNode.nodeIbStasher.delay(ppDelay(10, 0))

    stash = []
    origMethod = slowRep.processReqDigest

    def patched(self, msg):
        stash.append(msg)

    patchedMethod = types.MethodType(patched, slowRep)
    slowRep.processReqDigest = patchedMethod

    def chk1():
        assert len(slowRep.commitsWaitingForPrepare) > 0

    sendRandomRequest(wallet1, client1)
    looper.run(eventually(chk1, timeout=4))

    for item in stash:
        origMethod(item)

    def chk2():
        assert len(slowRep.commitsWaitingForPrepare) == 0
        assert slowRep.spylog.count(slowRep.doOrder.__name__) == 1

    looper.run(eventually(chk2, timeout=12))

