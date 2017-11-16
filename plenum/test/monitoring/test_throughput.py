from typing import Iterable

from stp_core.common.log import getlogger
from plenum.server.node import Node
from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests


nodeCount = 4
logger = getlogger()


# noinspection PyIncorrectDocstring
def testThroughput(looper, nodeSet: Iterable[Node], wallet1, client1):
    """
    Checking if the throughput is being set
    """
    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1, requests=[req])

    for node in nodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(
            node.instances.masterId)
        logger.debug("Master throughput: {}. Avg. backup throughput: {}".
                     format(masterThroughput, avgBackupThroughput))
        assert masterThroughput > 0
        assert avgBackupThroughput > 0
