from zeno.common.util import getNoInstances
from zeno.test.helper import getPrimaryReplica


def testPrePrepareWithHighSeqNo(looper, nodeSet, propagated1):
    for instId in range(getNoInstances(len(nodeSet))):
        primary = getPrimaryReplica(nodeSet, instId)
        primary.sendPrePrepare(propagated1.reqDigest)
    pass