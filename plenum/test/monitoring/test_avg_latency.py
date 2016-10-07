import logging

from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.server.node import Node
from plenum.test.eventually import eventually
from plenum.test.helper import TestNodeSet, sendRandomRequest, \
    checkSufficientRepliesRecvd

nodeCount = 4


logger = getlogger()


# noinspection PyIncorrectDocstring
def testAvgReqLatency(looper: Looper, nodeSet: TestNodeSet, wallet1, client1):
    """
    Checking if average latency is being set
    """

    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        looper.run(eventually(checkSufficientRepliesRecvd,
                              client1.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))

    for node in nodeSet:  # type: Node
        mLat = node.monitor.getAvgLatencyForClient(wallet1.defaultId,
                                                   node.instances.masterId)
        bLat = node.monitor.getAvgLatencyForClient(wallet1.defaultId,
                                                   *node.instances.backupIds)
        logger.debug("Avg. master latency : {}. Avg. backup latency: {}".
                      format(mLat, bLat))
        assert mLat > 0
        assert bLat > 0
