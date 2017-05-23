import time
import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.server.node import Node
from plenum.common.request import Request
from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests
from plenum.test.test_node import TestReplica, TestNode, TestNodeSet
from plenum.test.conftest import getValueFromModule


nodeCount = 4


logger = getlogger()
class TestNodeLocal(TestNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adjust_times_test = {}

    def doDynamicValidation(self, *args, **kwargs):
        return super().doDynamicValidation(*args, **kwargs)

    def applyReq(self, *args, **kwargs):
        return super().applyReq(*args, **kwargs)


@pytest.yield_fixture(scope="module")
def nodeSet(request, tdir, nodeReg, allPluginsPath, patchPluginManager):
    primaryDecider = getValueFromModule(request, "PrimaryDecider", None)
    with TestNodeSet(nodeReg=nodeReg, tmpdir=tdir,
                     primaryDecider=primaryDecider,
                     pluginPaths=allPluginsPath,
                     testNodeClass=TestNodeLocal) as ns:
        yield ns

# noinspection PyIncorrectDocstring
def testAdjustMonitor(looper: Looper, nodeSet: TestNodeSet, wallet1, client1):
    """
    Checking if average latency is being set
    """

    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1,
                                            requests=[req], fVal=1)
    for node in nodeSet:  # type: Node
        logger.debug("adjust_times {}: isMasterDegraded {} "
                     "masterReqExtraLatencies {} totalMasterReqExtraLatency {}".format(
                        node,
                        node.monitor.isMasterDegraded(),
                        node.monitor.masterReqExtraLatencies,
                        node.monitor.totalMasterReqExtraLatency))
        assert node.viewNo == 0
        assert not node.monitor.isMasterDegraded()
