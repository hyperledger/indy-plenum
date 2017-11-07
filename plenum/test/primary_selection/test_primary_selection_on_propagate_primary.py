import pytest

from stp_core.common.log import getlogger

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies

from plenum.test.test_node import TestNode
from plenum.test.view_change.helper import ensure_view_change_complete

from plenum.test.node_catchup.conftest import nodeCreatedAfterSomeTxns, \
    nodeSetWithNodeAddedAfterSomeTxns

logger = getlogger()

nodes_wth_bls = 0


class TestNodeWithAdjustedViewNo(TestNode):
    def __init__(self, *args, **kwargs):
        self.__viewNo = 3
        super().__init__(*args, **kwargs)

    @property
    def viewNo(self):
        return self.__viewNo

    @viewNo.setter
    def viewNo(self, value):
        if value != 0:
            self.__viewNo = value


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return TestNodeWithAdjustedViewNo


@pytest.fixture("module")
def txnPoolNodeSet(txnPoolNodeSet, looper, client1, wallet1, client1Connected,
        tconf, tdirWithPoolTxns, allPluginsPath):
    logger.debug("Do several view changes to round the list of primaries")

    assert txnPoolNodeSet[0].viewNo == len(txnPoolNodeSet) - 1

    logger.debug("Do view change to reach viewNo {}".format(txnPoolNodeSet[0].viewNo + 1))
    ensure_view_change_complete(looper, txnPoolNodeSet)
    logger.debug("Send requests to ensure that pool is working properly, "
                 "viewNo: {}".format(txnPoolNodeSet[0].viewNo))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, numReqs=3)

    return txnPoolNodeSet


def test_primary_selection_on_propogate_primary(txnPoolNodeSet,
        nodeSetWithNodeAddedAfterSomeTxns):
    looper, _, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns

    logger.debug("Send requests to ensure that pool is working properly, "
                 "viewNo: {}".format(txnPoolNodeSet[0].viewNo))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=3)
