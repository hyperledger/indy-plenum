import types

from plenum.test.eventually import eventually
from plenum.test.helper import getNonPrimaryReplicas, ppDelay, \
    sendRandomRequest


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
    req = sendRandomRequest(wallet1, client1)

    stash = []
    origMethod = slowRep.processReqDigest

    def patched(self, msg):
        stash.append(msg)

    patchedMethod = types.MethodType(patched, slowRep)
    slowRep.processReqDigest = patchedMethod

    def chk1():
        assert len(slowRep.commitsPendedForOrdering) > 0

    looper.run(eventually(chk1, timeout=4))

    for item in stash:
        origMethod(item)

    def chk2():
        assert len(slowRep.commitsPendedForOrdering) == 0
        assert slowRep.spylog.count(slowRep.orderPendingCommit.__name__) > 0
        assert slowRep.spylog.count(slowRep.doOrder.__name__) == 1

    looper.run(eventually(chk2, timeout=12))

