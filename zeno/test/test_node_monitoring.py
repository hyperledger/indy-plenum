import logging

from zeno.test.eventually import eventually

from zeno.common.looper import Looper
from zeno.test.helper import TestNodeSet, sendRandomRequest, \
    checkSufficientRepliesRecvd


nodeCount = 4


# noinspection PyIncorrectDocstring
def testThroughput(looper, nodeSet, client1):
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
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(node.masterInst)
        logging.debug("Master throughput: {}. Avg. backup throughput: {}".
                      format(masterThroughput, avgBackupThroughput))
        assert masterThroughput > 0
        assert avgBackupThroughput > 0


# noinspection PyIncorrectDocstring
def testAvgReqLatency(looper: Looper, nodeSet: TestNodeSet, client1):
    """
    Checking if average latency is being set
    """

    for i in range(5):
        req = sendRandomRequest(client1)
        looper.run(eventually(checkSufficientRepliesRecvd,
                              client1.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))

    for node in nodeSet:
        mLat = node.monitor.getAvgLatencyForClient(client1.clientId,
                                                   node.masterInst)
        bLat = node.monitor.getAvgLatencyForClient(client1.clientId,
                                                   *node.nonMasterInsts)
        logging.debug("Avg. master latency : {}. Avg. backup latency: {}".
                      format(mLat, bLat))
        assert mLat > 0
        assert bLat > 0
