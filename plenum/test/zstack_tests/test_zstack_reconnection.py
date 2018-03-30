import pytest
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.helper import stopNodes, sdk_send_random_and_check
from plenum.test.test_node import TestNode, ensureElectionsDone
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()

TestRunningTimeLimitSec = 300


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.UseZStack = True
    return tconf


def testZStackNodeReconnection(tconf, looper, txnPoolNodeSet,
                               sdk_pool_handle,
                               sdk_wallet_client,
                               tdir):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)

    npr = [n for n in txnPoolNodeSet if not n.hasPrimary]
    nodeToCrash = npr[0]
    idxToCrash = txnPoolNodeSet.index(nodeToCrash)
    otherNodes = [_ for _ in txnPoolNodeSet if _ != nodeToCrash]

    def checkFlakyConnected(conn=True):
        for node in otherNodes:
            if conn:
                assert nodeToCrash.nodestack.name in node.nodestack.connecteds
            else:
                assert nodeToCrash.nodestack.name not in node.nodestack.connecteds

    checkFlakyConnected(True)
    nodeToCrash.stop()
    logger.debug('Stopped node {}'.format(nodeToCrash))
    looper.removeProdable(nodeToCrash)
    looper.runFor(1)
    stopNodes([nodeToCrash], looper)
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    looper.run(eventually(checkFlakyConnected, False, retryWait=1, timeout=60))

    looper.runFor(1)
    config_helper = PNodeConfigHelper(nodeToCrash.name, tconf, chroot=tdir)
    node = TestNode(
        nodeToCrash.name,
        config_helper=config_helper,
        config=tconf,
        ha=nodeToCrash.nodestack.ha,
        cliha=nodeToCrash.clientstack.ha)
    looper.add(node)
    txnPoolNodeSet[idxToCrash] = node

    # TODO Select or create the timeout from 'waits'. Don't use constant.
    looper.run(eventually(checkFlakyConnected, True, retryWait=2, timeout=50))
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=2)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 10)
