import logging
from typing import Iterable

from zeno.server.node import Node
from zeno.test.eventually import eventually
from zeno.test.helper import sendRandomRequest, \
    checkSufficientRepliesRecvd


nodeCount = 4


# noinspection PyIncorrectDocstring
def testThroughput(looper, nodeSet: Iterable[Node], client1):
    """
    Checking if the throughput is being set
    """
    client = client1
    for i in range(5):
        req = sendRandomRequest(client)
        looper.run(eventually(checkSufficientRepliesRecvd,
                              client.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))

    for node in nodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(node.instances.masterId)
        logging.debug("Master throughput: {}. Avg. backup throughput: {}".
                      format(masterThroughput, avgBackupThroughput))
        assert masterThroughput > 0
        assert avgBackupThroughput > 0