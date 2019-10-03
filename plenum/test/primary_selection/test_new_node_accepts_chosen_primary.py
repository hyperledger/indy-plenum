import pytest

from plenum.server.view_change.node_view_changer import create_view_changer
from stp_core.common.log import getlogger

from plenum.test.helper import sdk_send_random_and_check

from plenum.test.test_node import TestNode, TestViewChanger
from plenum.test.view_change.helper import ensure_view_change_complete

from plenum.test.node_catchup.helper import waitNodeDataEquality

logger = getlogger()


class TestViewChangerWithAdjustedViewNo(TestViewChanger):
    def __init__(self, *args, **kwargs):
        self.__view_no = 3
        super().__init__(*args, **kwargs)

    @property
    def view_no(self):
        return self.__view_no

    @view_no.setter
    def view_no(self, value):
        if value != 0:
            self.__view_no = value
            self.provider.view_setting_handler(value)


class TestNodeWithAdjustedViewNo(TestNode):
    def newViewChanger(self):
        return create_view_changer(self, TestViewChangerWithAdjustedViewNo)


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return TestNodeWithAdjustedViewNo


@pytest.fixture("module")
def txnPoolNodeSet(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_steward,
                   tconf, tdirWithPoolTxns, allPluginsPath):
    logger.debug("Do several view changes to round the list of primaries")

    assert txnPoolNodeSet[0].viewNo == len(txnPoolNodeSet) - 1

    logger.debug("Do view change to reach viewNo {}".format(txnPoolNodeSet[0].viewNo + 1))
    ensure_view_change_complete(looper, txnPoolNodeSet)
    logger.debug("Send requests to ensure that pool is working properly, "
                 "viewNo: {}".format(txnPoolNodeSet[0].viewNo))
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    logger.debug("Pool is ready, current viewNo: {}".format(txnPoolNodeSet[0].viewNo))

    # TODO find out and fix why additional view change could happen
    # because of degarded master. It's critical for current test to have
    # view change completed for the time when new node is joining.
    # Thus, disable master degradation check as it won't impact the case
    # and guarantees necessary state.
    for node in txnPoolNodeSet:
        node.monitor.isMasterDegraded = lambda: False

    return txnPoolNodeSet
