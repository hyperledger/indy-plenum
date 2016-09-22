import logging
from typing import Iterable

from plenum.server.node import Node
from plenum.test.eventually import eventually
from plenum.test.helper import sendRandomRequest, \
    checkSufficientRepliesRecvd


nodeCount = 4


# noinspection PyIncorrectDocstring
def testThroughput(looper, nodeSet: Iterable[Node], wallet1, client1):
    """
    Checking if the throughput is being set
    """
    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        looper.run(eventually(checkSufficientRepliesRecvd,
                              client1.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))

    for node in nodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(node.instances.masterId)
        logger.debug("Master throughput: {}. Avg. backup throughput: {}".
                      format(masterThroughput, avgBackupThroughput))
        assert masterThroughput > 0
        assert avgBackupThroughput > 0